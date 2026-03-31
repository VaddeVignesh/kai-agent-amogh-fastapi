from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from app.adapters.postgres_adapter import PostgresAdapter
from app.sql.sql_allowlist import DEFAULT_ALLOWLIST
from app.sql.sql_guard import validate_and_prepare_sql
from app.sql.sql_generator import SQLGenerator


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
    DEFAULT_LIMIT = 10
    MAX_LIMIT = 200

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

        try:
            query_key, params = self.map_intent(intent_key, slots)
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

    def try_dynamic(self, *, question, intent_key, slots):
        try:
            return self.run_dynamic(question=question, intent_key=intent_key, slots=slots)
        except Exception:
            return None

    # =========================================================
    # DYNAMIC EXECUTION (STRICT OPS-ONLY)
    # =========================================================

    def run_dynamic(self, *, question, intent_key, slots, enforce_limit=200):

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
            if not name or name.lower() in ("null", "none", "", "unknown"):
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

            canonical_sql = """
                SELECT voyage_id,
                       voyage_number,
                       vessel_imo,
                       vessel_name,
                       module_type,
                       fixture_count,
                       offhire_days,
                       is_delayed,
                       delay_reason,
                       ports_json,
                       grades_json,
                       remarks_json,
                       voyage_start_date,
                       voyage_end_date
                FROM ops_voyage_summary
                WHERE voyage_id = ANY(%(voyage_ids)s)
                LIMIT %(limit)s
            """

            # IMPORTANT: when we already have explicit voyage_ids from upstream,
            # we should fetch ALL matching rows (do not truncate to the user's display limit).
            effective_limit = min(max(len(voyage_ids), limit_val), self.MAX_LIMIT)
            params = {"voyage_ids": voyage_ids, "limit": effective_limit}

            guard = validate_and_prepare_sql(
                sql=canonical_sql,
                params=params,
                allowlist=self.allowlist,
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
        if intent_key in ("analysis.cargo_profitability", "analysis.cargoprofitability") and isinstance(cargo_grades, list) and cargo_grades:

            cargo_grades_norm = []
            for g in cargo_grades:
                cleaned = _clean_grade_name(g)
                if cleaned is None:
                    continue
                s = str(cleaned).strip().lower()
                if s and s not in ("none", "null", "n/a", "na"):
                    cargo_grades_norm.append(s)
            cargo_grades_norm = list(dict.fromkeys(cargo_grades_norm))[:50]
            if not cargo_grades_norm:
                cargo_grades_norm = []

            canonical_sql = """
                WITH grade_rows AS (
                  SELECT
                    o.voyage_id,
                    o.voyage_number,
                    o.is_delayed,
                    lower(trim(grade.value)) AS cargo_grade,
                    o.ports_json,
                    o.remarks_json
                  FROM ops_voyage_summary o,
                  jsonb_array_elements_text(o.grades_json) AS grade(value)
                  WHERE o.grades_json IS NOT NULL
                    AND o.grades_json::text != '[]'
                    AND grade.value IS NOT NULL
                    AND trim(grade.value) != ''
                ),
                filtered AS (
                  SELECT *
                  FROM grade_rows
                  WHERE cargo_grade = ANY(%(cargo_grades)s)
                ),
                delay_stats AS (
                  SELECT
                    cargo_grade,
                    COUNT(DISTINCT voyage_id) AS voyage_count,
                    COUNT(DISTINCT voyage_id) FILTER (WHERE is_delayed) AS delayed_voyage_count
                  FROM filtered
                  GROUP BY cargo_grade
                ),
                port_counts AS (
                  SELECT
                    cargo_grade,
                    port,
                    COUNT(*) AS cnt
                  FROM (
                    SELECT cargo_grade, jsonb_array_elements_text(ports_json) AS port
                    FROM filtered
                    WHERE ports_json IS NOT NULL
                      AND ports_json::text != '[]'
                  ) p
                  WHERE port IS NOT NULL AND port != ''
                  GROUP BY cargo_grade, port
                ),
                top_ports AS (
                  SELECT cargo_grade,
                         jsonb_agg(port ORDER BY cnt DESC, port) FILTER (WHERE rn <= 8) AS common_ports
                  FROM (
                    SELECT cargo_grade, port, cnt,
                           ROW_NUMBER() OVER (PARTITION BY cargo_grade ORDER BY cnt DESC, port) AS rn
                    FROM port_counts
                  ) t
                  GROUP BY cargo_grade
                ),
                remark_counts AS (
                  SELECT
                    cargo_grade,
                    remark,
                    COUNT(*) AS cnt
                  FROM (
                    SELECT cargo_grade, jsonb_array_elements_text(remarks_json) AS remark
                    FROM filtered
                    WHERE remarks_json IS NOT NULL
                      AND remarks_json::text != '[]'
                  ) r
                  WHERE remark IS NOT NULL AND remark != ''
                    AND (
                      lower(remark) LIKE '%%congest%%'
                      OR lower(remark) LIKE '%%delay%%'
                      OR lower(remark) LIKE '%%waiting%%'
                      OR lower(remark) LIKE '%%queue%%'
                    )
                  GROUP BY cargo_grade, remark
                ),
                top_remarks AS (
                  SELECT cargo_grade,
                         jsonb_agg(remark ORDER BY cnt DESC, remark) FILTER (WHERE rn <= 5) AS congestion_delay_remarks
                  FROM (
                    SELECT cargo_grade, remark, cnt,
                           ROW_NUMBER() OVER (PARTITION BY cargo_grade ORDER BY cnt DESC, remark) AS rn
                    FROM remark_counts
                  ) t
                  GROUP BY cargo_grade
                )
                SELECT
                  ds.cargo_grade,
                  ds.voyage_count,
                  ds.delayed_voyage_count,
                  tp.common_ports,
                  tr.congestion_delay_remarks
                FROM delay_stats ds
                LEFT JOIN top_ports tp ON ds.cargo_grade = tp.cargo_grade
                LEFT JOIN top_remarks tr ON ds.cargo_grade = tr.cargo_grade
                ORDER BY ds.voyage_count DESC, ds.cargo_grade
                LIMIT %(limit)s
            """

            params = {"cargo_grades": cargo_grades_norm, "limit": limit_val}
            guard = validate_and_prepare_sql(
                sql=canonical_sql,
                params=params,
                allowlist=self.allowlist,
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

            lookup_sql = """
                SELECT voyage_id
                FROM ops_voyage_summary
                WHERE voyage_number = %(voyage_number)s
                LIMIT 1
            """

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

                vessel_sql = f"""
                    SELECT voyage_id,
                           voyage_number,
                           vessel_imo,
                           vessel_name,
                           module_type,
                           fixture_count,
                           offhire_days,
                           is_delayed,
                           delay_reason,
                           ports_json,
                           grades_json,
                           remarks_json,
                           voyage_start_date,
                           voyage_end_date
                    FROM ops_voyage_summary
                    WHERE {where}
                    ORDER BY voyage_end_date DESC NULLS LAST
                    LIMIT %(limit)s
                """

                guard = validate_and_prepare_sql(
                    sql=vessel_sql,
                    params=params,
                    allowlist=self.allowlist,
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
            allowlist=self.allowlist,
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

    def map_intent(self, intent_key, slots):
        s = dict(slots or {})
        limit = min(int(s.get("limit") or self.DEFAULT_LIMIT), self.MAX_LIMIT)

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
                "scenario": s.get("scenario") or "ACTUAL",
            }

        if intent_key == "vessel.summary":
            vessel_ref = s.get("vessel_name") or s.get("imo")
            if not vessel_ref:
                raise ValueError("vessel.summary requires vessel_name or imo")
            return "kpi.vessel_voyages_by_reference", {"vessel_ref": str(vessel_ref), "limit": limit}

        if intent_key == "ops.delayed_voyages":
            return "kpi.delayed_voyages_analysis", {"limit": limit}

        if intent_key in ("ops.voyages_by_port", "ops.port_query"):
            return "kpi.port_performance_analysis", {
                "port_name": s.get("port_name"),
                "limit": limit,
            }

        if intent_key == "ops.voyages_by_cargo_grade":
            cargo_grade = (s.get("cargo_grade") or "").strip()
            if not cargo_grade:
                raise ValueError("ops.voyages_by_cargo_grade requires cargo_grade")
            return "kpi.voyages_by_cargo_grade", {
                "cargo_grade": cargo_grade,
                "limit": limit,
            }

        if intent_key == "port.details":
            port_name = s.get("port_name")
            if not port_name:
                raise ValueError("port.details requires port_name")
            return "kpi.port_performance_analysis", {"port_name": str(port_name), "limit": limit}

        return "kpi.voyages_by_flexible_filters", {
            "limit": limit,
            "vessel_name": s.get("vessel_name"),
            "module_type": s.get("module_type"),
            "is_delayed": s.get("is_delayed"),
        }


__all__ = ["OpsAgent", "OpsAgentResult"]
