from __future__ import annotations

from typing import Any, Dict


def compact_payload(merged: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove bulky fields and keep only what's needed for summarization.
    """
    out: Dict[str, Any] = {}

    def _has_joined_rows(artifacts: Any) -> bool:
        return isinstance(artifacts, dict) and isinstance(artifacts.get("merged_rows"), list) and len(artifacts.get("merged_rows") or []) > 0

    def _cap_list(v: Any, n: int) -> Any:
        return v[:n] if isinstance(v, list) else v

    def _light_merged_row(r: Any) -> Any:
        """
        Keep ONLY the flattened fields the summarizer needs.
        This avoids confusing the LLM with deeply nested finance/ops/mongo rows and cuts tokens.
        """
        if not isinstance(r, dict):
            return r

        kp = r.get("key_ports")
        if isinstance(kp, list):
            kp_clean = []
            for x in kp:
                if x in (None, "", [], {}):
                    continue
                if isinstance(x, dict):
                    kp_clean.append({"portName": x.get("portName"), "activityType": x.get("activityType")})
                else:
                    kp_clean.append(str(x))
            kp = _cap_list(kp_clean, 10)

        cg = r.get("cargo_grades")
        if isinstance(cg, list):
            cg = _cap_list([str(x) for x in cg if x not in (None, "", [], {})], 10)

        rem = r.get("remarks")
        if isinstance(rem, list):
            rem = _cap_list([str(x) for x in rem if x not in (None, "", [], {})], 5)
        elif rem not in (None, "", [], {}):
            rem = str(rem)

        # Prefer top-level KPIs; fallback to nested finance (so summarizer always sees pnl/revenue for ranking)
        fin = r.get("finance") if isinstance(r.get("finance"), dict) else {}
        def _k(key: str, *alt_keys: str) -> Any:
            v = r.get(key)
            if v is not None and v != "":
                return v
            for a in alt_keys:
                w = fin.get(a)
                if w is not None and w != "":
                    return w
            return fin.get(key)
        pnl = _k("pnl", "PnL")
        revenue = _k("revenue", "Revenue")
        total_expense = _k("total_expense", "Total_expense", "total expense")
        tce = _k("tce", "TCE")
        total_commission = _k("total_commission", "Total_commission", "total commission")

        out_row = {
            "voyage_id": r.get("voyage_id"),
            "voyage_number": r.get("voyage_number"),
            "pnl": pnl,
            "revenue": revenue,
            "total_expense": total_expense,
            "tce": tce,
            "total_commission": total_commission,
            "key_ports": kp or [],
            "cargo_grades": cg or [],
            "remarks": rem,
        }
        # Vessel-level rows (e.g. ranking.vessels) have vessel_imo, vessel_name, voyage_count
        if r.get("vessel_imo") is not None or r.get("vessel_name") is not None:
            out_row["vessel_imo"] = r.get("vessel_imo")
            out_row["vessel_name"] = r.get("vessel_name")
            out_row["voyage_count"] = r.get("voyage_count")
            out_row["avg_pnl"] = r.get("avg_pnl") or r.get("pnl")
        return out_row

    fin = merged.get("finance")
    if isinstance(fin, dict):
        out["finance"] = {
            "mode": fin.get("mode"),
            "rows": (fin.get("rows") or [])[:50],
        }

    ops = merged.get("ops")
    if isinstance(ops, dict):
        out["ops"] = {
            "mode": ops.get("mode"),
            "rows": (ops.get("rows") or [])[:50],
        }

    mongo = merged.get("mongo")
    if isinstance(mongo, dict):
        out["mongo"] = {
            "mode": mongo.get("mode"),
            "collection": mongo.get("collection"),
            "rows": (mongo.get("rows") or [])[:50],
        }

    artifacts = merged.get("artifacts")
    if isinstance(artifacts, dict):
        compact_artifacts: Dict[str, Any] = {}
        if isinstance(artifacts.get("merged_rows"), list):
            compact_artifacts["merged_rows"] = [
                _light_merged_row(r) for r in (artifacts.get("merged_rows", [])[:50] or [])
            ]
        if isinstance(artifacts.get("voyage_ids"), list):
            compact_artifacts["voyage_ids"] = artifacts.get("voyage_ids", [])[:50]
        # Optional: lightweight coverage hints help the summarizer avoid false "Not available" claims.
        if isinstance(artifacts.get("coverage"), dict):
            compact_artifacts["coverage"] = artifacts.get("coverage") or {}
        if compact_artifacts:
            out["artifacts"] = compact_artifacts

    # If we already have joined rows, drop redundant raw sections to keep payload small.
    # The LLM is instructed to prefer `data.artifacts.merged_rows` as the primary dataset.
    if _has_joined_rows(out.get("artifacts")):
        # Keep a tiny sample of finance rows (helps formatting KPIs) but drop the rest.
        if isinstance(out.get("finance"), dict) and isinstance(out["finance"].get("rows"), list):
            out["finance"]["rows"] = out["finance"]["rows"][:5]
        if isinstance(out.get("ops"), dict):
            out["ops"]["rows"] = []
        if isinstance(out.get("mongo"), dict):
            out["mongo"]["rows"] = []

    out["dynamic_sql_used"] = merged.get("dynamic_sql_used", False)
    out["dynamic_sql_agents"] = merged.get("dynamic_sql_agents", [])

    return out

