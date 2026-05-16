from __future__ import annotations
from dataclasses import dataclass
import time
from typing import Any, Dict, List, Optional

from app.adapters.postgres_adapter import PostgresAdapter
from app.config.agent_rules_loader import (
    get_ops_cargo_grade_max_count,
    get_ops_cargo_profitability_intents,
    get_ops_canonical_sql,
    get_ops_delay_remark_keywords,
    get_ops_delay_remark_filter_empty,
    get_ops_delay_remark_filter_template,
    get_ops_max_limit,
    get_ops_simple_intent_mappings,
    get_ops_validation_message,
)
from app.auth import session_may_access_ops_summary, session_may_access_finance_kpi
from app.config.domain_loader import get_default_limit, get_default_scenario, is_null_equivalent
from app.core.logger import get_logger
from app.sql.registry_role_access import is_registry_query_allowed_for_session
from app.sql.sql_allowlist import DEFAULT_ALLOWLIST, build_allowlist_for_session
from app.sql.sql_guard import validate_and_prepare_sql
from app.sql.sql_generator import SQLGenerator

logger = get_logger("ops_agent")

_OPS_ACCESS_DENIED = (
    "Operational voyage data is not available for your account role."
)


@dataclass(frozen=True)
class OpsAgentResult:
    intent_key: str
    query_key: str
    params: Dict[str, Any]
    rows: List[Dict[str, Any]]
    mode: str
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


class OpsAgent:
    DEFAULT_LIMIT = get_default_limit()
    MAX_LIMIT = get_ops_max_limit()

    def __init__(self, pg: PostgresAdapter, llm_client=None, sql_generator=None, allowlist=None):
        self.pg = pg
        self.llm = llm_client
        self.allowlist = allowlist or DEFAULT_ALLOWLIST
        self.sql_generator = sql_generator or (SQLGenerator(llm_client) if llm_client else None)

    # =========================================================
    # ENTRY
    # =========================================================

    def run(self, *, intent_key, slots, session_context=None, user_input=None):
        session_context = session_context or {}
        start = time.time()
        logger.info(
            "AGENT_START | agent=ops | mode=registry_sql | intent=%s | query_chars=%s",
            intent_key,
            len(user_input or ""),
        )

        try:
            if not session_may_access_ops_summary(session_context):
                return {
                    "mode": "registry_sql",
                    "intent_key": intent_key,
                    "query_key": "access_denied",
                    "params": {},
                    "rows": [],
                    "fallback_reason": _OPS_ACCESS_DENIED,
                }
            query_key, params = self.map_intent(intent_key, slots, session_context)
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
                "AGENT_END | agent=ops | mode=registry_sql | intent=%s | query_key=%s | rows=%s | latency=%ss",
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
                "AGENT_ERROR | agent=ops | mode=registry_sql | intent=%s | latency=%ss | error=%s",
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

    def try_dynamic(self, *, question, intent_key, slots, session_context=None):
        try:
            return self.run_dynamic(
                question=question,
                intent_key=intent_key,
                slots=slots,
                session_context=session_context,
            )
        except Exception as exc:
            logger.error(
                "AGENT_ERROR | agent=ops | mode=dynamic_sql | intent=%s | error=%s",
                intent_key,
                str(exc)[:200],
            )
            return None

    # =========================================================
    # DYNAMIC EXECUTION (STRICT OPS-ONLY)
    # =========================================================

    def run_dynamic(self, *, question, intent_key, slots, enforce_limit=200, session_context=None):
        start = time.time()
        logger.info(
            "AGENT_START | agent=ops | mode=dynamic_sql | intent=%s | query_chars=%s",
            intent_key,
            len(question or ""),
        )

        session_context = session_context or {}
        allowlist = build_allowlist_for_session(session_context, self.allowlist)

        if not session_may_access_ops_summary(session_context):
            elapsed = round(time.time() - start, 3)
            logger.info(
                "AGENT_END | agent=ops | mode=dynamic_sql | intent=%s | rows=0 | latency=%ss | note=access_denied",
                intent_key,
                elapsed,
            )
            return OpsAgentResult(
                intent_key=intent_key,
                query_key="access_denied",
                params={},
                rows=[],
                mode="dynamic_sql",
                sql=None,
            )

        if not self.sql_generator:
            raise RuntimeError("OpsAgent.run_dynamic requires sql_generator")

        # sanitize limit
        limit_val = min(int(slots.get("limit") or self.DEFAULT_LIMIT), self.MAX_LIMIT)
        slots = {**slots, "limit": limit_val}

        voyage_ids = slots.get("voyage_ids")
        cargo_grades = slots.get("cargo_grades")

        # Defensive: never pass placeholder strings to DB (avoids "argument formats can't be mixed")
        if isinstance(voyage_ids, str) or (voyage_ids is not None and not isinstance(voyage_ids, list)):
            voyage_ids = []
        if voyage_ids is None:
            voyage_ids = []
        if cargo_grades is not None and not isinstance(cargo_grades, list):
            cargo_grades = [cargo_grades] if cargo_grades else []

        def _clean_grade_name(raw) -> str | None:
            if raw is None:
                return None
            if isinstance(raw, dict):
                name = raw.get("grade_name") or raw.get("name") or ""
            else:
                name = str(raw)
            name = name.strip()
            if is_null_equivalent(name):
                return None
            # Drop raw JSON object strings
            if name.startswith("{") and "grade_name" in name:
                return None
            return name

        # ----------------------------------------------------------
        # PATH 0: ranking.vessels — most common cargo grades per vessel (no voyage_ids)
        # ----------------------------------------------------------
        vessel_imos = slots.get("vessel_imos")
        if (
            intent_key == "ranking.vessels"
            and isinstance(vessel_imos, list)
            and vessel_imos
            and not (isinstance(voyage_ids, list) and voyage_ids)
        ):
            if not is_registry_query_allowed_for_session("kpi.vessel_most_common_grades", session_context):
                return OpsAgentResult(
                    intent_key=intent_key,
                    query_key="access_denied",
                    params={},
                    rows=[],
                    mode="dynamic_sql",
                    sql=None,
                )
            try:
                vimos = [v for v in vessel_imos if v is not None]
                if vimos:
                    rows = self.pg.fetch_all("kpi.vessel_most_common_grades", {"vessel_imos": vimos})
                    return OpsAgentResult(
                        intent_key=intent_key,
                        query_key="kpi.vessel_most_common_grades",
                        params={"vessel_imos": vimos},
                        rows=rows or [],
                        mode="dynamic_sql",
                        sql=None,
                    )
            except Exception as e:
                return OpsAgentResult(
                    intent_key=intent_key,
                    query_key="kpi.vessel_most_common_grades",
                    params={},
                    rows=[],
                    mode="dynamic_sql",
                    sql=f"ERROR: {e}",
                )

        # ----------------------------------------------------------
        # PATH 1: Canonical voyage_ids lookup
        # ----------------------------------------------------------

        if isinstance(voyage_ids, list) and voyage_ids:

            canonical_sql = get_ops_canonical_sql("voyage_ids_lookup")

            # IMPORTANT: when we already have explicit voyage_ids from upstream,
            # we should fetch ALL matching rows (do not truncate to the user's display limit).
            effective_limit = min(max(len(voyage_ids), limit_val), self.MAX_LIMIT)
            params = {"voyage_ids": voyage_ids, "limit": effective_limit}

            guard = validate_and_prepare_sql(
                sql=canonical_sql,
                params=params,
                allowlist=allowlist,
                enforce_limit=True,
            )

            if guard.ok:
                rows = self.pg.execute_dynamic_select(guard.sql, guard.params)
                return OpsAgentResult(
                    intent_key=intent_key,
                    query_key="canonical.voyage_ids",
                    params=guard.params,
                    rows=rows,
                    mode="dynamic_sql",
                    sql=guard.sql,
                )

        # ----------------------------------------------------------
        # PATH 1b: Cargo profitability support (ports + delay remarks per grade)
        # ----------------------------------------------------------
        if intent_key in get_ops_cargo_profitability_intents() and isinstance(cargo_grades, list) and cargo_grades:

            cargo_grades_norm = []
            for g in cargo_grades:
                cleaned = _clean_grade_name(g)
                if cleaned is None:
                    continue
                s = str(cleaned).strip().lower()
                if s and not is_null_equivalent(s):
                    cargo_grades_norm.append(s)
            cargo_grades_norm = list(dict.fromkeys(cargo_grades_norm))[: get_ops_cargo_grade_max_count()]
            if not cargo_grades_norm:
                cargo_grades_norm = []

            remark_filters = " OR ".join(
                get_ops_delay_remark_filter_template().format(keyword=keyword)
                for keyword in get_ops_delay_remark_keywords()
            ) or get_ops_delay_remark_filter_empty()

            canonical_sql = get_ops_canonical_sql("cargo_profitability_context").format(
                remark_filters=remark_filters
            )

            params = {"cargo_grades": cargo_grades_norm, "limit": limit_val}
            guard = validate_and_prepare_sql(
                sql=canonical_sql,
                params=params,
                allowlist=allowlist,
                enforce_limit=True,
            )
            if guard.ok:
                rows = self.pg.execute_dynamic_select(guard.sql, guard.params)
                return OpsAgentResult(
                    intent_key=intent_key,
                    query_key="canonical.cargo_profitability_context",
                    params=guard.params,
                    rows=rows,
                    mode="dynamic_sql",
                    sql=guard.sql,
                )

        # ----------------------------------------------------------
        # PATH 2: Single voyage lookup
        # ----------------------------------------------------------

        voyage_number = slots.get("voyage_number")
        if voyage_number:

            lookup_sql = get_ops_canonical_sql("voyage_number_lookup")

            lookup_guard = validate_and_prepare_sql(
                lookup_sql,
                {"voyage_number": voyage_number},
                self.allowlist,
            )

            if lookup_guard.ok:
                rows = self.pg.execute_dynamic_select(
                    lookup_guard.sql, lookup_guard.params
                )
                if rows:
                    slots["voyage_ids"] = [rows[0]["voyage_id"]]
                    return self.run_dynamic(
                        question=question,
                        intent_key=intent_key,
                        slots=slots,
                    )

        # ----------------------------------------------------------
        # PATH 2b: Vessel summary lookup (deterministic)
        # ----------------------------------------------------------

        if intent_key == "vessel.summary":
            vessel_name = slots.get("vessel_name")
            imo = slots.get("imo")

            # Use IMO and/or vessel_name. In some datasets vessel_imo may be NULL,
            # so prefer an OR when both are available.
            if imo and vessel_name:
                where = "(vessel_imo = %(imo)s OR vessel_name ILIKE %(vessel_name)s)"
            elif imo:
                where = "vessel_imo = %(imo)s"
            elif vessel_name:
                where = "vessel_name ILIKE %(vessel_name)s"
            else:
                where = ""

            if where:
                params = {
                    "imo": str(imo) if imo else None,
                    "vessel_name": f"%{vessel_name}%" if vessel_name else None,
                    "limit": limit_val,
                }

                vessel_sql = get_ops_canonical_sql("vessel_summary").format(where=where)

                guard = validate_and_prepare_sql(
                    sql=vessel_sql,
                    params=params,
                    allowlist=allowlist,
                    enforce_limit=True,
                )

                if guard.ok:
                    rows = self.pg.execute_dynamic_select(guard.sql, guard.params)
                    return OpsAgentResult(
                        intent_key=intent_key,
                        query_key="canonical.vessel_summary",
                        params=guard.params,
                        rows=rows,
                        mode="dynamic_sql",
                        sql=guard.sql,
                    )

        # ----------------------------------------------------------
        # PATH 3: LLM SQL (OPS TABLE ONLY)
        # ----------------------------------------------------------

        gen = self.sql_generator.generate(
            question=question,
            intent_key=intent_key,
            slots=slots,
            agent="ops",
        )

        sql = gen.sql.strip()
        params = gen.params or {}

        guard = validate_and_prepare_sql(
            sql=sql,
            params=params,
            allowlist=allowlist,
            enforce_limit=True,
        )

        if not guard.ok:
            return OpsAgentResult(
                intent_key=intent_key,
                query_key="validation_failed",
                params={},
                rows=[],
                mode="dynamic_sql",
                sql=f"ERROR: {guard.reason}",
            )

        rows = self.pg.execute_dynamic_select(guard.sql, guard.params)

        elapsed = round(time.time() - start, 3)
        logger.info(
            "AGENT_END | agent=ops | mode=dynamic_sql | intent=%s | rows=%s | latency=%ss",
            intent_key,
            len(rows or []),
            elapsed,
        )
        return OpsAgentResult(
            intent_key=intent_key,
            query_key="dynamic.sql",
            params=guard.params,
            rows=rows,
            mode="dynamic_sql",
            sql=guard.sql,
        )

    # =========================================================
    # REGISTRY FALLBACK
    # =========================================================

    def map_intent(self, intent_key, slots, session_context=None):
        s = dict(slots or {})
        session_context = session_context or {}
        limit = min(int(s.get("limit") or self.DEFAULT_LIMIT), self.MAX_LIMIT)

        if intent_key == "voyage.summary":
            voyage_number = s.get("voyage_number")
            voyage_id = s.get("voyage_id")
            vessel_imo = s.get("imo") or s.get("vessel_imo")
            if not voyage_number and not voyage_id:
                raise ValueError(get_ops_validation_message("voyage_summary_requires_reference"))
            params = {
                "voyage_number": str(int(voyage_number)) if voyage_number is not None and not voyage_id else None,
                "voyage_id": str(voyage_id) if voyage_id is not None else None,
                "vessel_imo": str(vessel_imo) if vessel_imo is not None else None,
                "limit": 1,
            }
            # Tenants without finance KPI still need ops voyage rows; kpi.* SQL joins finance_voyage_kpi.
            if not session_may_access_finance_kpi(session_context):
                return "ops.voyage_by_reference", params
            params["scenario"] = s.get("scenario") or get_default_scenario()
            return "kpi.voyage_by_reference", params

        if intent_key == "vessel.summary":
            vessel_ref = s.get("vessel_name") or s.get("imo")
            if not vessel_ref:
                raise ValueError(get_ops_validation_message("vessel_summary_requires_reference"))
            if not session_may_access_finance_kpi(session_context):
                return "ops.vessel_voyages_by_reference", {"vessel_ref": str(vessel_ref), "limit": limit}
            return "kpi.vessel_voyages_by_reference", {"vessel_ref": str(vessel_ref), "limit": limit}

        simple_mappings = get_ops_simple_intent_mappings()
        if intent_key in simple_mappings:
            mapping = simple_mappings[intent_key]
            required_slot = mapping.get("required_slot")
            if required_slot:
                value = s.get(str(required_slot))
                if isinstance(value, str):
                    value = value.strip()
                if not value:
                    raise ValueError(f"{intent_key} requires {required_slot}")

            params: Dict[str, Any] = {}
            include = mapping.get("include") if isinstance(mapping.get("include"), list) else []
            if "limit" in include:
                params["limit"] = limit
            if "port_name" in include:
                params["port_name"] = str(s.get("port_name"))
            if "cargo_grade" in include:
                params["cargo_grade"] = str(s.get("cargo_grade")).strip()
            return str(mapping["query_key"]), params

        return "kpi.voyages_by_flexible_filters", {
            "limit": limit,
            "vessel_name": s.get("vessel_name"),
            "module_type": s.get("module_type"),
            "is_delayed": s.get("is_delayed"),
        }


__all__ = ["OpsAgent", "OpsAgentResult"]
