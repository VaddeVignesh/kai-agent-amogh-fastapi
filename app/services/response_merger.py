from __future__ import annotations

import ast
from typing import Any, Dict

from app.config.response_rules_loader import (
    get_compact_cargo_grades_limit,
    get_compact_finance_sample_rows_when_joined,
    get_compact_key_ports_limit,
    get_compact_merged_rows_limit,
    get_compact_raw_section_row_limit,
    get_compact_remarks_limit,
    get_compact_voyage_ids_limit,
    get_imo_prefix,
    get_null_equivalent_grade_values,
    get_unknown_vessel_label,
)


def compact_payload(merged: Dict[str, Any]) -> Dict[str, Any]:
    """
    Remove bulky fields and keep only what's needed for summarization.
    """
    out: Dict[str, Any] = {}
    raw_section_row_limit = get_compact_raw_section_row_limit()
    merged_rows_limit = get_compact_merged_rows_limit()
    voyage_ids_limit = get_compact_voyage_ids_limit()
    finance_sample_rows_limit = get_compact_finance_sample_rows_when_joined()
    key_ports_limit = get_compact_key_ports_limit()
    cargo_grades_limit = get_compact_cargo_grades_limit()
    remarks_limit = get_compact_remarks_limit()
    unknown_vessel_label = get_unknown_vessel_label()
    imo_prefix = get_imo_prefix()
    null_equivalent_grade_values = get_null_equivalent_grade_values()

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

        def _grade_text(v: Any) -> str:
            if v in (None, "", [], {}):
                return ""
            if isinstance(v, dict):
                g = v.get("grade_name") or v.get("gradeName") or v.get("name") or v.get("grade")
                return str(g).strip() if g not in (None, "", [], {}) else ""
            if isinstance(v, str):
                s = v.strip()
                if s.startswith("{") and s.endswith("}"):
                    try:
                        obj = ast.literal_eval(s)
                        if isinstance(obj, dict):
                            g = obj.get("grade_name") or obj.get("gradeName") or obj.get("name") or obj.get("grade")
                            return str(g).strip() if g not in (None, "", [], {}) else ""
                    except Exception:
                        pass
                return s
            return str(v).strip()

        kp = r.get("key_ports")
        if isinstance(kp, list):
            kp_clean = []
            for x in kp:
                if x in (None, "", [], {}):
                    continue
                if isinstance(x, dict):
                    kp_clean.append(
                        {
                            "portName": x.get("portName") or x.get("port_name") or x.get("name"),
                            "activityType": x.get("activityType") or x.get("activity_type"),
                        }
                    )
                else:
                    kp_clean.append(str(x))
            kp = _cap_list(kp_clean, key_ports_limit)

        cg = r.get("cargo_grades")
        if isinstance(cg, list):
            cg_clean = []
            seen_cg = set()
            for x in cg:
                s = _grade_text(x)
                sn = s.lower()
                if not s or sn in null_equivalent_grade_values:
                    continue
                if sn in seen_cg:
                    continue
                seen_cg.add(sn)
                cg_clean.append(s)
            cg = _cap_list(cg_clean, cargo_grades_limit)

        rem = r.get("remarks")
        if isinstance(rem, list):
            rem = _cap_list([str(x) for x in rem if x not in (None, "", [], {})], remarks_limit)
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
        total_pnl = _k("total_pnl")
        avg_tce = _k("avg_tce")
        avg_total_expense = _k("avg_total_expense")
        total_revenue = _k("total_revenue")
        actual_avg_pnl = _k("actual_avg_pnl")
        when_fixed_avg_pnl = _k("when_fixed_avg_pnl")
        variance_diff = _k("variance_diff")
        # Aggregate intents may use avg_* fields instead of top-level KPI names.
        if pnl in (None, ""):
            pnl = _k("avg_pnl")
        if revenue in (None, ""):
            revenue = _k("avg_revenue")

        out_row = {
            "voyage_id": r.get("voyage_id"),
            "voyage_number": r.get("voyage_number"),
            "pnl": pnl,
            "revenue": revenue,
            "total_expense": total_expense,
            "tce": tce,
            "total_commission": total_commission,
            "total_pnl": total_pnl,
            "avg_tce": avg_tce,
            "avg_total_expense": avg_total_expense,
            "total_revenue": total_revenue,
            "actual_avg_pnl": actual_avg_pnl,
            "when_fixed_avg_pnl": when_fixed_avg_pnl,
            "variance_diff": variance_diff,
            "port_calls": (
                r.get("port_calls")
                if r.get("port_calls") not in (None, "")
                else fin.get("port_count")
            ),
            "key_ports": kp or [],
            "cargo_grades": cg or [],
            "remarks": rem,
            "voyage_count": (
                r.get("voyage_count")
                if r.get("voyage_count") not in (None, "")
                else fin.get("voyage_count")
            ),
            "is_delayed": r.get("is_delayed"),
        }
        if isinstance(r.get("commissions"), list) and r.get("commissions"):
            out_row["commissions"] = r.get("commissions")
        # Vessel-level rows (e.g. ranking.vessels) have vessel_imo, vessel_name, voyage_count
        if r.get("vessel_imo") is not None or r.get("vessel_name") is not None:
            out_row["vessel_imo"] = r.get("vessel_imo")
            _vname = r.get("vessel_name") or fin.get("vessel_name")
            _vimo = r.get("vessel_imo") or fin.get("vessel_imo")
            out_row["vessel_name"] = (
                _vname if _vname and str(_vname).strip()
                else f"{imo_prefix}{_vimo}" if _vimo
                else unknown_vessel_label
            )
            out_row["voyage_count"] = r.get("voyage_count")
            out_row["avg_pnl"] = r.get("avg_pnl") or r.get("pnl")
        else:
            _vname = r.get("vessel_name") or fin.get("vessel_name")
            _vimo = r.get("vessel_imo") or fin.get("vessel_imo")
            if _vname is not None or _vimo is not None:
                out_row["vessel_imo"] = _vimo
                out_row["vessel_name"] = (
                    _vname if _vname and str(_vname).strip()
                    else f"{imo_prefix}{_vimo}" if _vimo
                    else unknown_vessel_label
                )
        return out_row

    fin = merged.get("finance")
    if isinstance(fin, dict):
        out["finance"] = {
            "mode": fin.get("mode"),
            "rows": (fin.get("rows") or [])[:raw_section_row_limit],
        }

    ops = merged.get("ops")
    if isinstance(ops, dict):
        out["ops"] = {
            "mode": ops.get("mode"),
            "rows": (ops.get("rows") or [])[:raw_section_row_limit],
        }

    mongo = merged.get("mongo")
    if isinstance(mongo, dict):
        out["mongo"] = {
            "mode": mongo.get("mode"),
            "collection": mongo.get("collection"),
            "rows": (mongo.get("rows") or [])[:raw_section_row_limit],
        }

    artifacts = merged.get("artifacts")
    if isinstance(artifacts, dict):
        compact_artifacts: Dict[str, Any] = {}
        if isinstance(artifacts.get("merged_rows"), list):
            compact_artifacts["merged_rows"] = [
                _light_merged_row(r) for r in (artifacts.get("merged_rows", [])[:merged_rows_limit] or [])
            ]
        if isinstance(artifacts.get("voyage_ids"), list):
            compact_artifacts["voyage_ids"] = artifacts.get("voyage_ids", [])[:voyage_ids_limit]
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
            out["finance"]["rows"] = out["finance"]["rows"][:finance_sample_rows_limit]
        if isinstance(out.get("ops"), dict):
            out["ops"]["rows"] = []
        if isinstance(out.get("mongo"), dict):
            out["mongo"]["rows"] = []

    out["dynamic_sql_used"] = merged.get("dynamic_sql_used", False)
    out["dynamic_sql_agents"] = merged.get("dynamic_sql_agents", [])

    return out

