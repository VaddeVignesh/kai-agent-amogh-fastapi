from __future__ import annotations

import ast
from dataclasses import dataclass
import os
from typing import Any, Dict, List, Optional

from app.adapters.postgres_adapter import PostgresAdapter
from app.registries.intent_registry import INTENT_REGISTRY
from app.sql.sql_allowlist import DEFAULT_ALLOWLIST
from app.sql.sql_guard import validate_and_prepare_sql
from app.sql.sql_generator import SQLGenerator


@dataclass(frozen=True)
class FinanceAgentResult:
    intent_key: str
    query_key: str
    params: Dict[str, Any]
    rows: List[Dict[str, Any]]
    mode: str = "registry_sql"
    sql: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "intent_key": self.intent_key,
            "query_key": self.query_key,
            "params": self.params,
            "rows": self.rows,
            "mode": self.mode,
            "sql": self.sql,
        }


class FinanceAgent:
    DEFAULT_LIMIT = 10
    MAX_LIMIT = 200

    def __init__(self, pg: PostgresAdapter, llm_client=None, sql_generator=None, allowlist=None):
        self.pg = pg
        self.llm = llm_client
        self.allowlist = allowlist or DEFAULT_ALLOWLIST
        self.sql_generator = sql_generator or (SQLGenerator(llm_client) if llm_client else None)

    @staticmethod
    def _normalize_slots(slots: Dict[str, Any] | None) -> Dict[str, Any]:
        """
        Accept both flat slot dicts and nested step inputs carrying a `slots` object.
        Top-level keys win; nested `slots` provides fallback values.
        """
        if not isinstance(slots, dict):
            return {}
        nested = slots.get("slots")
        if isinstance(nested, dict):
            merged = {**nested, **slots}
            merged.pop("slots", None)
            return merged
        return dict(slots)

    # =========================================================
    # ENTRY
    # =========================================================

    def run(self, *, intent_key, slots, session_context=None, user_input=None):
        session_context = session_context or {}
        slots = self._normalize_slots(slots)

        try:
            query_key, params = self._map_intent(intent_key, slots)
            rows = self.pg.fetch_all(query_key, params)
            return {
                "mode": "registry_sql",
                "intent_key": intent_key,
                "query_key": query_key,
                "params": params,
                "rows": rows,
            }
        except Exception as e:
            return {
                "mode": "registry_sql",
                "intent_key": intent_key,
                "rows": [],
                "fallback_reason": str(e),
            }

    # =========================================================
    # DYNAMIC SQL
    # =========================================================

    def run_dynamic(self, *, question, intent_key, slots, enforce_limit=200):

        if not self.sql_generator:
            raise RuntimeError("FinanceAgent.run_dynamic requires sql_generator")

        slots = self._normalize_slots(slots)
        # Composite intent hygiene: keep only registry-declared slots plus
        # core execution keys to avoid stale entity filters leaking into SQL.
        _cfg = INTENT_REGISTRY.get(intent_key, {})
        if _cfg.get("route") == "composite":
            _allowed = set((_cfg.get("required_slots") or []) + (_cfg.get("optional_slots") or []))
            _allowed.update({"scenario", "limit", "voyage_ids", "voyage_numbers", "voyage_number", "voyage_id", "cargo_grades", "vessel_imos"})
            slots = {k: v for k, v in slots.items() if k in _allowed}
        limit_val = min(int(slots.get("limit") or self.DEFAULT_LIMIT), self.MAX_LIMIT)
        slots = {**slots, "limit": limit_val}
        guardrails = INTENT_REGISTRY.get(intent_key, {}).get("guardrails", {})

        MAX_SQL_RETRIES = int(os.getenv("SQL_MAX_RETRIES", "2"))

        def _generate_once(q: str, error_hint: str = "") -> tuple[str, Dict[str, Any]]:
            q_eff = q
            if error_hint:
                q_eff = (
                    f"{q}\n\n"
                    "PREVIOUS ATTEMPT FAILED WITH THIS ERROR:\n"
                    f"{error_hint}\n"
                    "Fix the SQL. The most common cause is a column in SELECT "
                    "that is missing from GROUP BY. Check every non-aggregated "
                    "column and ensure it appears in GROUP BY."
                )
            gen = self.sql_generator.generate(
                question=q_eff,
                intent_key=intent_key,
                slots=slots,
                agent="finance",
            )
            return gen.sql.strip(), (gen.params or {})

        def _is_repairable_sql_error(msg: str) -> bool:
            m = (msg or "").lower()
            patterns = (
                "must appear in the group by",
                "aggregate function",
                "column",
                "does not exist",
                "ambiguous",
                "set-returning function",
                "aggregate function calls cannot contain set-returning function calls",
                "syntax error",
                "forbidden sql pattern",
                "placeholder",
            )
            return any(p in m for p in patterns)

        sql, params = _generate_once(question)

        if guardrails.get("inject_voyage_numbers_param"):
            if "%(voyage_numbers)s" in (sql or "") and "voyage_numbers" not in params:
                if slots.get("voyage_numbers") is not None:
                    try:
                        params["voyage_numbers"] = [int(v) for v in (slots.get("voyage_numbers") or [])]
                    except Exception:
                        pass

        if guardrails.get("inject_filter_port_param"):
            port_name = (slots.get("port_name") or "").strip()
            if port_name and "filter_port" not in params:
                params["filter_port"] = port_name

        if "%(scenario)s" in sql and "scenario" not in params:
            params["scenario"] = slots.get("scenario") or "ACTUAL"

        if not sql:
            raise RuntimeError("Empty SQL generated")

        if guardrails.get("finance_no_ops_join") and "ops_voyage_summary" in sql.lower():
            repair_q = (
                f"{question}\n\n"
                "IMPORTANT FIX: Do NOT join ops tables. Query ONLY finance_voyage_kpi. "
                "Return voyage_id, voyage_number, pnl, revenue, total_expense, tce, total_commission. "
                "Filter scenario = COALESCE(%(scenario)s,'ACTUAL'). Order by pnl DESC. LIMIT %(limit)s."
            )
            sql, params = _generate_once(repair_q)

        guard = None
        rows = []
        max_attempts = MAX_SQL_RETRIES + 1
        last_err = ""
        for attempt in range(max_attempts):
            guard = validate_and_prepare_sql(sql=sql, params=params, allowlist=self.allowlist, enforce_limit=True)
            if not guard.ok:
                if (guard.reason or "").lower().find("forbidden sql pattern") >= 0:
                    repair_q = (
                        f"{question}\n\n"
                        "IMPORTANT FIX: Return plain SQL only with no comments and no extra statements. "
                        "Use a single SELECT query and avoid any forbidden tokens."
                    )
                    sql, params = _generate_once(repair_q)
                    continue
                if guardrails.get("finance_no_ops_join"):
                    repair_q = (
                        f"{question}\n\n"
                        "IMPORTANT FIX: Generate SQL on finance_voyage_kpi ONLY (no joins). "
                        "Return voyage_id, voyage_number, pnl, revenue, total_expense, tce, total_commission. "
                        "Filter scenario = COALESCE(%(scenario)s,'ACTUAL'). Use LIMIT %(limit)s."
                    )
                    sql, params = _generate_once(repair_q)
                    continue
                last_err = guard.reason or "validation failed"
                if attempt < max_attempts - 1 and _is_repairable_sql_error(last_err):
                    sql, params = _generate_once(question, error_hint=last_err)
                    continue
                raise RuntimeError(f"Finance SQL validation failed: {guard.reason}")

            try:
                rows = self.pg.execute_dynamic_select(guard.sql, guard.params)
                break
            except Exception as e:
                last_err = str(e)
                if attempt < max_attempts - 1 and _is_repairable_sql_error(last_err):
                    sql, params = _generate_once(question, error_hint=last_err)
                    continue
                raise

        if guardrails.get("require_kpi_columns") and rows:
            first = rows[0] if isinstance(rows[0], dict) else {}
            required_cols = {"voyage_id", "voyage_number", "pnl", "revenue", "total_expense"}
            if not required_cols.issubset(set(first.keys())):
                repair_q = (
                    f"{question}\n\n"
                    "IMPORTANT FIX: Your last SQL did not return the required KPI columns. "
                    "Return voyage_id, voyage_number, pnl, revenue, total_expense "
                    "(and optionally tce, total_commission). "
                    "Use finance_voyage_kpi only and alias columns exactly as named."
                )
                sql2, params2 = _generate_once(repair_q)
                guard2 = validate_and_prepare_sql(sql=sql2, params=params2, allowlist=self.allowlist, enforce_limit=True)
                if guard2.ok:
                    rows = self.pg.execute_dynamic_select(guard2.sql, guard2.params)
                    guard = guard2

        if guardrails.get("verify_scenario_variance_columns") and rows:
            req_cols = {
                "voyage_number",
                "pnl_actual", "pnl_when_fixed", "pnl_variance",
                "tce_actual", "tce_when_fixed", "tce_variance",
            }
            requested_vnums = []
            if isinstance(slots.get("voyage_numbers"), list):
                try:
                    requested_vnums = [int(v) for v in slots.get("voyage_numbers") if v is not None]
                except Exception:
                    requested_vnums = []
            requested_set = set(requested_vnums)

            def _needs_repair(rows_in):
                if not rows_in:
                    return False
                first = rows_in[0] if isinstance(rows_in[0], dict) else {}
                if not req_cols.issubset(set(first.keys())):
                    return True
                got = set()
                for r in rows_in:
                    if isinstance(r, dict):
                        try:
                            got.add(int(r.get("voyage_number")))
                        except Exception:
                            continue
                if requested_set and not requested_set.issubset(got):
                    return True
                if len(rows_in) > len(got):
                    return True
                return False

            if _needs_repair(rows):
                repair_q = (
                    f"{question}\n\n"
                    "IMPORTANT FIX (scenario comparison): "
                    "Use TWO CTEs aggregated to one row per (voyage_number, normalized vessel_imo) "
                    "for ACTUAL and WHEN_FIXED, then join on BOTH and aggregate to one row per voyage_number. "
                    "Filter voyage_number = ANY(%(voyage_numbers)s). "
                    "Return exactly: voyage_number, pnl_actual, pnl_when_fixed, pnl_variance, "
                    "tce_actual, tce_when_fixed, tce_variance. "
                    "Use SUM for pnl fields and AVG for tce fields."
                )
                sql2, params2 = _generate_once(repair_q)
                if "%(voyage_numbers)s" in (sql2 or "") and "voyage_numbers" not in params2:
                    if slots.get("voyage_numbers") is not None:
                        try:
                            params2["voyage_numbers"] = [int(v) for v in (slots.get("voyage_numbers") or [])]
                        except Exception:
                            pass
                guard2 = validate_and_prepare_sql(sql=sql2, params=params2, allowlist=self.allowlist, enforce_limit=True)
                if guard2.ok:
                    rows2 = self.pg.execute_dynamic_select(guard2.sql, guard2.params)
                    if rows2 and not _needs_repair(rows2):
                        rows = rows2
                        guard = guard2

        if guardrails.get("segment_performance_fallback"):
            has_voyage_id = any(isinstance(r, dict) and r.get("voyage_id") for r in (rows or []))
            if (not rows) or (not has_voyage_id):
                voyage_numbers = slots.get("voyage_numbers")
                if voyage_numbers is None and slots.get("voyage_number") is not None:
                    voyage_numbers = [slots.get("voyage_number")]
                if voyage_numbers is not None and not isinstance(voyage_numbers, list):
                    voyage_numbers = [voyage_numbers]
                try:
                    voyage_numbers = [int(v) for v in (voyage_numbers or []) if v is not None]
                except Exception:
                    voyage_numbers = []

                if voyage_numbers:
                    fallback_sql = """
                        SELECT
                          f.voyage_id,
                          f.voyage_number,
                          f.vessel_imo,
                          f.revenue,
                          f.total_expense,
                          f.pnl,
                          f.tce,
                          f.total_commission,
                          f.voyage_start_date,
                          f.voyage_end_date,
                          o.vessel_name,
                          o.module_type,
                          o.ports_json,
                          o.grades_json,
                          o.activities_json,
                          o.remarks_json
                        FROM finance_voyage_kpi f
                        LEFT JOIN ops_voyage_summary o
                          ON f.voyage_number = o.voyage_number
                          AND REPLACE(f.vessel_imo::TEXT, '.0', '') = REPLACE(o.vessel_imo::TEXT, '.0', '')
                        WHERE f.scenario = COALESCE(%(scenario)s, 'ACTUAL')
                          AND f.voyage_number = ANY(%(voyage_numbers)s)
                        ORDER BY f.voyage_end_date DESC
                        LIMIT %(limit)s
                    """
                else:
                    fallback_sql = """
                        SELECT
                          f.voyage_id,
                          f.voyage_number,
                          f.vessel_imo,
                          f.revenue,
                          f.total_expense,
                          f.pnl,
                          f.tce,
                          f.total_commission,
                          f.voyage_start_date,
                          f.voyage_end_date,
                          o.vessel_name,
                          o.module_type,
                          o.ports_json,
                          o.grades_json,
                          o.activities_json,
                          o.remarks_json
                        FROM finance_voyage_kpi f
                        LEFT JOIN ops_voyage_summary o
                          ON f.voyage_number = o.voyage_number
                          AND REPLACE(f.vessel_imo::TEXT, '.0', '') = REPLACE(o.vessel_imo::TEXT, '.0', '')
                        WHERE f.scenario = COALESCE(%(scenario)s, 'ACTUAL')
                          AND f.pnl < 0
                        ORDER BY f.pnl ASC
                        LIMIT %(limit)s
                    """
                fallback_params = {
                    "scenario": slots.get("scenario") or "ACTUAL",
                    "limit": min(int(slots.get("limit") or self.DEFAULT_LIMIT), self.MAX_LIMIT),
                    "voyage_numbers": voyage_numbers,
                }
                fallback_guard = validate_and_prepare_sql(
                    sql=fallback_sql,
                    params=fallback_params,
                    allowlist=self.allowlist,
                    enforce_limit=True,
                )
                if fallback_guard.ok:
                    rows_fb = self.pg.execute_dynamic_select(fallback_guard.sql, fallback_guard.params)
                    if isinstance(rows_fb, list):
                        rows = rows_fb
                        guard = fallback_guard

        return FinanceAgentResult(
            intent_key=intent_key,
            query_key="dynamic.sql",
            params=guard.params if guard else {},
            rows=rows,
            mode="dynamic_sql",
            sql=guard.sql if guard else sql,
        )

    def _try_dynamic(self, *, question, intent_key, slots):
        try:
            q = question or f"Generate SQL for finance intent {intent_key}"
            return self.run_dynamic(question=q, intent_key=intent_key, slots=slots)
        except Exception:
            return None

    # =========================================================
    # INTENT MAPPING
    # =========================================================

    def _map_intent(self, intent_key: str, slots: Dict[str, Any]):

        s = self._normalize_slots(slots)
        limit = min(int(s.get("limit") or self.DEFAULT_LIMIT), self.MAX_LIMIT)
        scenario = s.get("scenario") or "ACTUAL"

        # ------------------------------------------------------
        # VOYAGE SUMMARY
        # ------------------------------------------------------

        if intent_key == "voyage.summary":
            voyage_number = s.get("voyage_number")
            voyage_id = s.get("voyage_id")
            vessel_imo = s.get("imo") or s.get("vessel_imo")
            if not voyage_number and not voyage_id:
                raise ValueError("voyage.summary requires voyage_number or voyage_id")

            return "kpi.voyage_by_reference", {
                "voyage_number": str(int(voyage_number)) if voyage_number is not None else None,
                "voyage_id": str(voyage_id) if voyage_id is not None else None,
                "vessel_imo": str(vessel_imo) if vessel_imo is not None else None,
                "scenario": scenario,
            }

        # ------------------------------------------------------
        # VESSEL SUMMARY
        # ------------------------------------------------------

        if intent_key == "vessel.summary":
            vessel_ref = s.get("vessel_name") or s.get("imo")
            if not vessel_ref:
                raise ValueError("vessel.summary requires vessel_name or imo")

            return "kpi.vessel_voyages_by_reference", {
                "vessel_ref": str(vessel_ref),
                "limit": limit,
                "scenario": scenario,
            }

        # ------------------------------------------------------
        # SCENARIO COMPARISON (FIXED — AGGREGATED)
        # ------------------------------------------------------

        if intent_key == "analysis.scenario_comparison":

            voyage_numbers = (
                s.get("voyage_numbers")
                or s.get("voyages")
                or s.get("voyage_number")
            )

            if not voyage_numbers:
                raise ValueError("analysis.scenario_comparison requires voyage_numbers")

            if isinstance(voyage_numbers, str):
                voyage_numbers = ast.literal_eval(voyage_numbers)

            voyage_numbers = [int(v) for v in voyage_numbers]

            return "finance.compare_scenarios", {
                "voyage_numbers": voyage_numbers
            }

        # ------------------------------------------------------
        # RANKING (SAFE METRIC)
        # ------------------------------------------------------

        if intent_key == "ranking.voyages":

            allowed_metrics = {
                "pnl": "pnl",
                "revenue": "revenue",
                "total_expense": "total_expense",
                "total_commission": "total_commission",
                "tce": "tce",
            }

            metric = allowed_metrics.get(s.get("metric"), "pnl")
            direction = (s.get("direction") or "desc").strip().lower()
            if direction not in ("asc", "desc"):
                direction = "desc"

            return "finance.rank_voyages_safe", {
                "limit": limit,
                "scenario": scenario,
                "metric": metric,
                "direction": direction,
            }

        if intent_key == "ranking.voyages_by_pnl":
            return "finance.rank_voyages_safe", {"limit": limit, "scenario": scenario, "metric": "pnl", "direction": (s.get("direction") or "desc")}

        if intent_key == "ranking.voyages_by_revenue":
            return "finance.rank_voyages_safe", {"limit": limit, "scenario": scenario, "metric": "revenue", "direction": (s.get("direction") or "desc")}

        if intent_key == "ranking.voyages_by_commission":
            return "finance.rank_voyages_safe", {"limit": limit, "scenario": scenario, "metric": "total_commission", "direction": (s.get("direction") or "desc")}

        # ------------------------------------------------------
        # SEGMENT PERFORMANCE
        # ------------------------------------------------------

        if intent_key in ("analysis.segment_performance", "analysis.segmentperformance"):
            return "finance.vessel_segment_performance", {
                "limit": limit,
                "scenario": scenario,
            }

        # ------------------------------------------------------
        # RANKING VESSELS (vessel-level aggregate for merge)
        # ------------------------------------------------------

        if intent_key == "ranking.vessels":
            return "kpi.vessel_performance_summary", {
                "limit": limit,
                "scenario": scenario,
                "min_voyage_count": s.get("min_voyage_count"),
            }

        # ------------------------------------------------------
        # CARGO PROFITABILITY
        # ------------------------------------------------------

        if intent_key in ("analysis.cargo_profitability", "analysis.cargoprofitability"):
            return "kpi.cargo_profitability_analysis", {
                "limit": limit,
                "scenario": scenario,
            }

        # ------------------------------------------------------
        # HIGH REVENUE LOW PNL (registry only — no bunker_cost)
        # ------------------------------------------------------

        if intent_key == "analysis.high_revenue_low_pnl":
            return "finance.high_revenue_low_pnl", {
                "limit": limit,
                "scenario": scenario,
            }

        # ------------------------------------------------------
        # DEFAULT
        # ------------------------------------------------------

        if intent_key == "ops.voyages_by_cargo_grade":
            cargo_grade = (s.get("cargo_grade") or "").strip()
            if not cargo_grade:
                raise ValueError("ops.voyages_by_cargo_grade requires cargo_grade")
            return "kpi.voyages_by_cargo_grade", {
                "cargo_grade": cargo_grade,
                "limit": limit,
                "scenario": scenario,
            }

        if intent_key == "ops.offhire_ranking":
            return "kpi.offhire_ranking", {"limit": limit, "scenario": scenario}

        return "kpi.voyages_by_flexible_filters", {
            "limit": limit,
            "scenario": scenario,
            "voyage_number": None,
        }

    # =========================================================
    # UTIL
    # =========================================================

    @staticmethod
    def _to_numeric_or_none(val):
        if val is None or val == "":
            return None
        if isinstance(val, (int, float)):
            return val
        if isinstance(val, str):
            try:
                return float(val.replace(",", "").strip())
            except Exception:
                return None
        return None


__all__ = ["FinanceAgent", "FinanceAgentResult"]
