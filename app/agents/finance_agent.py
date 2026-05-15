from __future__ import annotations

import ast
from dataclasses import dataclass
import os
import time
from typing import Any, Dict, List, Optional

from app.adapters.postgres_adapter import PostgresAdapter
from app.config.agent_rules_loader import (
    get_finance_allowed_directions,
    get_finance_composite_allowed_extra_slots,
    get_finance_intent_metric_overrides,
    get_finance_max_limit,
    get_finance_ranking_default_direction,
    get_finance_ranking_default_metric,
    get_finance_repair_prompt,
    get_finance_repairable_error_patterns,
    get_finance_safe_metrics,
    get_finance_segment_performance_fallback_sql,
    get_finance_simple_intent_mappings,
    get_finance_validation_message,
)
from app.config.domain_loader import get_default_limit, get_default_scenario, get_min_voyage_count_fallback
from app.core.logger import get_logger
from app.config.response_rules_loader import get_finance_kpi_scope_restricted_user_message
from app.auth import session_may_access_finance_kpi
from app.registries.intent_loader import get_yaml_registry_facade
from app.sql.registry_role_access import is_registry_query_allowed_for_session
from app.sql.sql_allowlist import DEFAULT_ALLOWLIST, build_allowlist_for_session
from app.sql.sql_guard import validate_and_prepare_sql
from app.sql.sql_generator import SQLGenerator

logger = get_logger("finance_agent")
INTENT_REGISTRY = get_yaml_registry_facade(validate_parity=True)["INTENT_REGISTRY"]


@dataclass(frozen=True)
class FinanceAgentResult:
    intent_key: str
    query_key: str
    params: Dict[str, Any]
    rows: List[Dict[str, Any]]
    mode: str = "registry_sql"
    sql: Optional[str] = None
    fallback_reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        out = {
            "intent_key": self.intent_key,
            "query_key": self.query_key,
            "params": self.params,
            "rows": self.rows,
            "mode": self.mode,
            "sql": self.sql,
        }
        if self.fallback_reason:
            out["fallback_reason"] = self.fallback_reason
        return out


class FinanceAgent:
    DEFAULT_LIMIT = get_default_limit()
    MAX_LIMIT = get_finance_max_limit()

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
        start = time.time()
        logger.info(
            "AGENT_START | agent=finance | mode=registry_sql | intent=%s | query_chars=%s",
            intent_key,
            len(user_input or ""),
        )

        try:
            if not session_may_access_finance_kpi(session_context):
                return {
                    "mode": "registry_sql",
                    "intent_key": intent_key,
                    "query_key": "access_denied",
                    "params": {},
                    "rows": [],
                    "fallback_reason": get_finance_kpi_scope_restricted_user_message(),
                }
            query_key, params = self._map_intent(intent_key, slots)
            if not is_registry_query_allowed_for_session(query_key, session_context):
                return {
                    "mode": "registry_sql",
                    "intent_key": intent_key,
                    "query_key": query_key,
                    "params": params,
                    "rows": [],
                    "fallback_reason": (
                        "This query requires database tables that are not enabled for your account role."
                    ),
                }
            rows = self.pg.fetch_all(query_key, params)
            elapsed = round(time.time() - start, 3)
            logger.info(
                "AGENT_END | agent=finance | mode=registry_sql | intent=%s | query_key=%s | rows=%s | latency=%ss",
                intent_key,
                query_key,
                len(rows or []),
                elapsed,
            )
            return {
                "mode": "registry_sql",
                "intent_key": intent_key,
                "query_key": query_key,
                "params": params,
                "rows": rows,
            }
        except Exception as e:
            elapsed = round(time.time() - start, 3)
            logger.error(
                "AGENT_ERROR | agent=finance | mode=registry_sql | intent=%s | latency=%ss | error=%s",
                intent_key,
                elapsed,
                str(e)[:200],
            )
            return {
                "mode": "registry_sql",
                "intent_key": intent_key,
                "rows": [],
                "fallback_reason": str(e),
            }

    # =========================================================
    # DYNAMIC SQL
    # =========================================================

    def run_dynamic(self, *, question, intent_key, slots, enforce_limit=200, session_context=None):
        start = time.time()
        logger.info(
            "AGENT_START | agent=finance | mode=dynamic_sql | intent=%s | query_chars=%s",
            intent_key,
            len(question or ""),
        )

        if not self.sql_generator:
            raise RuntimeError("FinanceAgent.run_dynamic requires sql_generator")

        session_context = session_context or {}
        allowlist = build_allowlist_for_session(session_context, self.allowlist)
        if not session_may_access_finance_kpi(session_context):
            elapsed = round(time.time() - start, 3)
            logger.info(
                "AGENT_END | agent=finance | mode=dynamic_sql | intent=%s | rows=0 | latency=%ss | note=access_denied",
                intent_key,
                elapsed,
            )
            return FinanceAgentResult(
                intent_key=intent_key,
                query_key="access_denied",
                params={},
                rows=[],
                mode="dynamic_sql",
                sql=None,
                fallback_reason=get_finance_kpi_scope_restricted_user_message(),
            )

        slots = self._normalize_slots(slots)
        # Composite intent hygiene: keep only registry-declared slots plus
        # core execution keys to avoid stale entity filters leaking into SQL.
        _cfg = INTENT_REGISTRY.get(intent_key, {})
        if _cfg.get("route") == "composite":
            _allowed = set((_cfg.get("required_slots") or []) + (_cfg.get("optional_slots") or []))
            _allowed.update(get_finance_composite_allowed_extra_slots())
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
            return any(p in m for p in get_finance_repairable_error_patterns())

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
            params["scenario"] = slots.get("scenario") or get_default_scenario()

        if not sql:
            raise RuntimeError("Empty SQL generated")

        if guardrails.get("finance_no_ops_join") and "ops_voyage_summary" in sql.lower():
            repair_q = get_finance_repair_prompt("finance_no_ops_join_initial").format(question=question)
            sql, params = _generate_once(repair_q)

        guard = None
        rows = []
        max_attempts = MAX_SQL_RETRIES + 1
        last_err = ""
        for attempt in range(max_attempts):
            guard = validate_and_prepare_sql(sql=sql, params=params, allowlist=allowlist, enforce_limit=True)
            if not guard.ok:
                if (guard.reason or "").lower().find("forbidden sql pattern") >= 0:
                    repair_q = get_finance_repair_prompt("forbidden_sql_pattern").format(question=question)
                    sql, params = _generate_once(repair_q)
                    continue
                if guardrails.get("finance_no_ops_join"):
                    repair_q = get_finance_repair_prompt("finance_no_ops_join_validation").format(question=question)
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
                repair_q = get_finance_repair_prompt("require_kpi_columns").format(question=question)
                sql2, params2 = _generate_once(repair_q)
                guard2 = validate_and_prepare_sql(sql=sql2, params=params2, allowlist=allowlist, enforce_limit=True)
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
                repair_q = get_finance_repair_prompt("scenario_variance_columns").format(question=question)
                sql2, params2 = _generate_once(repair_q)
                if "%(voyage_numbers)s" in (sql2 or "") and "voyage_numbers" not in params2:
                    if slots.get("voyage_numbers") is not None:
                        try:
                            params2["voyage_numbers"] = [int(v) for v in (slots.get("voyage_numbers") or [])]
                        except Exception:
                            pass
                guard2 = validate_and_prepare_sql(sql=sql2, params=params2, allowlist=allowlist, enforce_limit=True)
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

                fallback_sql = get_finance_segment_performance_fallback_sql(
                    "with_voyage_numbers" if voyage_numbers else "default_negative_pnl"
                )
                fallback_params = {
                    "scenario": slots.get("scenario") or get_default_scenario(),
                    "limit": min(int(slots.get("limit") or self.DEFAULT_LIMIT), self.MAX_LIMIT),
                    "voyage_numbers": voyage_numbers,
                }
                fallback_guard = validate_and_prepare_sql(
                    sql=fallback_sql,
                    params=fallback_params,
                    allowlist=allowlist,
                    enforce_limit=True,
                )
                if fallback_guard.ok:
                    rows_fb = self.pg.execute_dynamic_select(fallback_guard.sql, fallback_guard.params)
                    if isinstance(rows_fb, list):
                        rows = rows_fb
                        guard = fallback_guard

        elapsed = round(time.time() - start, 3)
        logger.info(
            "AGENT_END | agent=finance | mode=dynamic_sql | intent=%s | rows=%s | latency=%ss",
            intent_key,
            len(rows or []),
            elapsed,
        )
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
        except Exception as exc:
            logger.error(
                "AGENT_ERROR | agent=finance | mode=dynamic_sql | intent=%s | error=%s",
                intent_key,
                str(exc)[:200],
            )
            return None

    # =========================================================
    # INTENT MAPPING
    # =========================================================

    def _map_intent(self, intent_key: str, slots: Dict[str, Any]):

        s = self._normalize_slots(slots)
        limit = min(int(s.get("limit") or self.DEFAULT_LIMIT), self.MAX_LIMIT)
        scenario = s.get("scenario") or get_default_scenario()

        # ------------------------------------------------------
        # VOYAGE SUMMARY
        # ------------------------------------------------------

        if intent_key == "voyage.summary":
            voyage_number = s.get("voyage_number")
            voyage_id = s.get("voyage_id")
            vessel_imo = s.get("imo") or s.get("vessel_imo")
            if not voyage_number and not voyage_id:
                raise ValueError(get_finance_validation_message("voyage_summary_requires_reference"))

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
                raise ValueError(get_finance_validation_message("vessel_summary_requires_reference"))

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
        # VOYAGE COMPARISON (multiple voyage numbers)
        # ------------------------------------------------------

        if intent_key == "comparison.voyages":
            voyage_numbers = s.get("voyage_numbers") or s.get("voyages") or []
            if not isinstance(voyage_numbers, list):
                voyage_numbers = [voyage_numbers]
            try:
                voyage_numbers = [int(v) for v in voyage_numbers if v not in (None, "", [], {})]
            except Exception:
                voyage_numbers = []
            if len(voyage_numbers) < 2:
                raise ValueError("comparison.voyages requires at least two voyage_numbers")
            return "finance.compare_voyages", {
                "voyage_numbers": voyage_numbers,
                "limit": max(limit, len(voyage_numbers)),
                "scenario": scenario,
            }

        # ------------------------------------------------------
        # RANKING (SAFE METRIC)
        # ------------------------------------------------------

        if intent_key == "ranking.voyages":

            allowed_metrics = get_finance_safe_metrics()

            metric = allowed_metrics.get(s.get("metric"), get_finance_ranking_default_metric())
            direction = (s.get("direction") or get_finance_ranking_default_direction()).strip().lower()
            if direction not in get_finance_allowed_directions():
                direction = get_finance_ranking_default_direction()

            return "finance.rank_voyages_safe", {
                "limit": limit,
                "scenario": scenario,
                "metric": metric,
                "direction": direction,
            }

        metric_overrides = get_finance_intent_metric_overrides()
        if intent_key in metric_overrides:
            return "finance.rank_voyages_safe", {
                "limit": limit,
                "scenario": scenario,
                "metric": metric_overrides[intent_key],
                "direction": (s.get("direction") or get_finance_ranking_default_direction()),
            }

        simple_mappings = get_finance_simple_intent_mappings()
        if intent_key in simple_mappings:
            mapping = simple_mappings[intent_key]
            params: Dict[str, Any] = {}
            include = mapping.get("include") if isinstance(mapping.get("include"), list) else []
            if "limit" in include:
                params["limit"] = limit
            if "scenario" in include:
                params["scenario"] = scenario
            if "min_voyage_count" in include:
                params["min_voyage_count"] = s.get("min_voyage_count") or get_min_voyage_count_fallback()
            return str(mapping["query_key"]), params

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
