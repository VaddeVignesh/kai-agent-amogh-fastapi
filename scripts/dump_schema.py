from __future__ import annotations

from pathlib import Path
import sys

import psycopg2
from psycopg2.extras import RealDictCursor


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.adapters.postgres_adapter import PostgresConfig  # noqa: E402


OUT_PATH = ROOT / "scripts" / "schema_dump.txt"


def _format_rows(rows: list[dict]) -> str:
    if not rows:
        return "(no rows)"
    cols = list(rows[0].keys())
    col_widths = {c: len(c) for c in cols}
    for row in rows:
        for c in cols:
            val = row.get(c)
            sval = "NULL" if val is None else str(val)
            if len(sval) > col_widths[c]:
                col_widths[c] = len(sval)

    header = " | ".join(c.ljust(col_widths[c]) for c in cols)
    sep = "-+-".join("-" * col_widths[c] for c in cols)
    body = []
    for row in rows:
        body.append(
            " | ".join(
                ("NULL" if row.get(c) is None else str(row.get(c))).ljust(col_widths[c])
                for c in cols
            )
        )
    return "\n".join([header, sep, *body])


def _run_section(cur, title: str, sql: str, out_chunks: list[str]) -> None:
    header = f"=== {title} ==="
    print(header)
    out_chunks.append(header)
    cur.execute(sql)
    rows = [dict(r) for r in cur.fetchall()]
    block = _format_rows(rows)
    print(block)
    print()
    out_chunks.append(block)
    out_chunks.append("")


def main() -> None:
    cfg = PostgresConfig.from_env()
    out_chunks: list[str] = []

    queries = [
        (
            "finance_voyage_kpi columns",
            """
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'finance_voyage_kpi'
ORDER BY ordinal_position;
""".strip(),
        ),
        (
            "ops_voyage_summary columns",
            """
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'ops_voyage_summary'
ORDER BY ordinal_position;
""".strip(),
        ),
        (
            "sample finance row",
            """
SELECT * FROM finance_voyage_kpi LIMIT 3;
""".strip(),
        ),
        (
            "sample ops row",
            """
SELECT * FROM ops_voyage_summary LIMIT 3;
""".strip(),
        ),
        (
            "ports_json keys",
            """
SELECT DISTINCT jsonb_object_keys(ports_json::jsonb)
FROM ops_voyage_summary
WHERE ports_json IS NOT NULL LIMIT 50;
""".strip(),
        ),
        (
            "grades_json keys",
            """
SELECT DISTINCT jsonb_object_keys(grades_json::jsonb)
FROM ops_voyage_summary
WHERE grades_json IS NOT NULL LIMIT 50;
""".strip(),
        ),
        (
            "remarks_json keys",
            """
SELECT DISTINCT jsonb_object_keys(remarks_json::jsonb)
FROM ops_voyage_summary
WHERE remarks_json IS NOT NULL LIMIT 50;
""".strip(),
        ),
    ]

    with psycopg2.connect(cfg.dsn) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            for title, sql in queries:
                try:
                    _run_section(cur, title, sql, out_chunks)
                except Exception as exc:
                    err = f"ERROR: {exc}"
                    print(err)
                    print()
                    out_chunks.append(err)
                    out_chunks.append("")
                    conn.rollback()

    OUT_PATH.write_text("\n".join(out_chunks), encoding="utf-8")
    print(f"Wrote dump to: {OUT_PATH}")


if __name__ == "__main__":
    main()
