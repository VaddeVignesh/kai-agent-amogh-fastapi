"""
One-off / repeatable audit: MongoDB (kai_agent) + Postgres (public).

Loads .env from repo root when run as: python scripts/data_store_audit.py
"""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

try:
    from dotenv import load_dotenv

    load_dotenv(REPO_ROOT / ".env")
except Exception:
    pass

REF_NAMES = ["Stena Imperial", "Stena Primorsk", "Stenaweco Energy"]
REF_IMOS = ["9667485", "9299147"]


def main() -> int:
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
    mongo_db = os.getenv("MONGO_DB_NAME", "kai_agent")
    dsn = os.getenv("POSTGRES_DSN", "")

    sep = "=" * 72
    print(sep)
    print("DATA STORE AUDIT (Mongo + Postgres)")
    print(sep)

    # ---------- Mongo ----------
    print("\n## MONGODB")
    print(f"URI: {mongo_uri!r}")
    print(f"Database: {mongo_db!r}")
    try:
        from pymongo import MongoClient

        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=8000)
        client.admin.command("ping")
        db = client[mongo_db]
        cols = sorted(db.list_collection_names())
        print("Connected: OK")
        print(f"Collections ({len(cols)}): {cols}")

        for cname in ("vessels", "voyages"):
            if cname not in cols:
                print(f"  [{cname}] MISSING")
                continue
            n = db[cname].estimated_document_count()
            print(f"  [{cname}] estimated_document_count: {n}")

        if "vessels" in cols:
            vessels = db["vessels"]
            print("\n### Mongo vessels — doc reference IMOs / names")
            for imo in REF_IMOS:
                d = vessels.find_one({"imo": imo}, {"_id": 0})
                status = "FOUND" if d else "NOT FOUND"
                print(f"  IMO {imo}: {status}")
                if d:
                    print(f"    name: {d.get('name')!r}")
                    for k in (
                        "scrubber",
                        "isVesselOperating",
                        "hireRate",
                        "marketType",
                        "accountCode",
                        "vesselId",
                    ):
                        if k in d:
                            print(f"    {k}: {d.get(k)!r}")
                    tags = d.get("tags")
                    if isinstance(tags, list) and tags:
                        print(f"    tags (first 3): {tags[:3]!r}")
                    ch = d.get("contract_history")
                    if isinstance(ch, dict):
                        lst = ch.get("list")
                        ln = len(lst) if isinstance(lst, list) else "n/a"
                        print(f"    contract_history.list count: {ln}")
                    cps = d.get("consumption_profiles")
                    if isinstance(cps, list):
                        print(f"    consumption_profiles: list[{len(cps)}]")
                    else:
                        print(f"    consumption_profiles: {type(cps).__name__}")

            for name in REF_NAMES:
                rx = re.escape(name)
                cur = list(
                    vessels.find(
                        {"name": {"$regex": rx, "$options": "i"}},
                        {"_id": 0, "name": 1, "imo": 1},
                    ).limit(5)
                )
                print(f"  name ~ {name!r}: {len(cur)} match(es)")
                for d in cur[:3]:
                    print(f"    -> {d.get('name')!r} IMO {d.get('imo')!r}")

            sample = vessels.find_one({}, {"_id": 0})
            if sample:
                print("\n### Mongo vessels — top-level keys (one sample doc)")
                for k in sorted(sample.keys()):
                    v = sample[k]
                    if isinstance(v, list):
                        print(f"  {k}: list len={len(v)}")
                    elif isinstance(v, dict):
                        print(f"  {k}: dict keys={list(v.keys())[:24]}")
                    else:
                        print(f"  {k}: {type(v).__name__}")

            stena = vessels.count_documents({"name": {"$regex": "^Stena", "$options": "i"}})
            print(f"\n  Vessels with name starting 'Stena': {stena}")

        if "voyages" in cols:
            voyages = db["voyages"]
            print("\n### Mongo voyages — reference vesselName")
            for name in REF_NAMES:
                rx = re.escape(name)
                n = voyages.count_documents({"vesselName": {"$regex": rx, "$options": "i"}})
                print(f"  vesselName ~ {name!r}: {n} docs")

        client.close()
    except Exception as e:
        print(f"Mongo ERROR: {type(e).__name__}: {e}")

    # ---------- Postgres ----------
    print("\n## POSTGRES")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    pdb = os.getenv("POSTGRES_DB", "postgres")
    print(f"Target: {host}:{port}/{pdb} (password not shown)")
    if not dsn:
        print("POSTGRES_DSN empty — skip Postgres")
        print(sep)
        return 0

    try:
        import psycopg2

        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='public' AND table_type='BASE TABLE'
            ORDER BY table_name
            """
        )
        tables = [r[0] for r in cur.fetchall()]
        print("Connected: OK")
        print(f"Public tables ({len(tables)}): {tables}")

        for t in ("finance_voyage_kpi", "ops_voyage_summary"):
            if t not in tables:
                print(f"  [{t}] MISSING")
                continue
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            print(f"  [{t}] row count: {cur.fetchone()[0]}")

        if "ops_voyage_summary" in tables:
            print("\n### Postgres ops_voyage_summary — reference vessels")
            for name in REF_NAMES:
                cur.execute(
                    """
                    SELECT COUNT(DISTINCT vessel_imo), COUNT(*)
                    FROM ops_voyage_summary WHERE vessel_name ILIKE %s
                    """,
                    (f"%{name}%",),
                )
                d_imo, d_rows = cur.fetchone()
                print(f"  vessel_name ILIKE %...{name}...% : {d_rows} rows, {d_imo} distinct vessel_imo")

            for imo in REF_IMOS:
                cur.execute(
                    """
                    SELECT vessel_name, COUNT(*) FROM ops_voyage_summary
                    WHERE TRIM(TRAILING '.0' FROM vessel_imo::text) = %s
                       OR vessel_imo::text IN (%s, %s)
                    GROUP BY vessel_name
                    """,
                    (imo, imo, imo + ".0"),
                )
                rows = cur.fetchall()
                print(f"  vessel_imo ~ {imo}: {rows if rows else 'no rows'}")

            cur.execute(
                """
                SELECT DISTINCT vessel_name, vessel_imo FROM ops_voyage_summary
                WHERE vessel_name ILIKE %s OR vessel_name ILIKE %s OR vessel_name ILIKE %s
                LIMIT 25
                """,
                ("%Imperial%", "%Primorsk%", "%Stenaweco%"),
            )
            print("  distinct (name, imo) matching Imperial / Primorsk / Stenaweco:")
            for r in cur.fetchall():
                print(f"    {r[0]!r} imo={r[1]!r}")

        if "finance_voyage_kpi" in tables and "ops_voyage_summary" in tables:
            cur.execute(
                """
                SELECT COUNT(*) FROM finance_voyage_kpi fv
                INNER JOIN ops_voyage_summary o ON o.voyage_id = fv.voyage_id
                WHERE o.vessel_name ILIKE %s
                """,
                ("%Stena Imperial%",),
            )
            print(f"\n  finance rows (join ops) for Stena Imperial: {cur.fetchone()[0]}")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Postgres ERROR: {type(e).__name__}: {e}")

    print("\n### Slot-validation proxy (fetch_vessel_by_name, graph_router-style)")
    try:
        from pymongo import MongoClient

        from app.adapters.mongo_adapter import MongoAdapter

        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=8000)
        adapter = MongoAdapter(client, db_name=mongo_db)
        for name in REF_NAMES:
            imo = adapter.get_vessel_imo_by_name(name)
            doc = adapter.fetch_vessel_by_name(name, projection={"_id": 1})
            ok = bool(doc)
            print(f"  {name!r} -> imo={imo!r} -> record by name: {ok}")
        client.close()
    except Exception as e:
        print(f"  (skip) {type(e).__name__}: {e}")

    print("\n## CONCLUSION (automated hints)")
    print(
        "- **Mongo** (`vessels`, `voyages`): used for vessel.metadata-style answers (tags, scrubber, consumption_profiles, contracts).\n"
        "- **Postgres** (`ops_voyage_summary`, `finance_voyage_kpi`): used for voyage/finance aggregates; joins on `voyage_id` / `vessel_imo`.\n"
        "- **Stenaweco Energy** may have **empty IMO** in Mongo → slot validation uses **document-by-name** so `vessel_name` is kept; "
        "`entity.vessel` resolves by name when IMO is missing.\n"
        "- **Imperial / Primorsk** in this audit: present in both stores with doc-aligned IMOs; if you still see 'No metadata', prioritize "
        "**slot extraction / cleaning** over 'database empty'.\n"
        "- **Data quality**: e.g. Stena Imperial tag `Age/Design: Scrubber` vs field `scrubber: No` can confuse narrative; reconcile in ETL or prompts."
    )
    print(sep)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
