from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from app.llm.llm_client import LLMClient
from app.sql.sql_allowlist import DEFAULT_ALLOWLIST, SQLAllowlist


# =========================================================
# OUTPUT MODEL
# =========================================================

@dataclass
class SQLGenOutput:
    sql: str
    params: Dict[str, Any]
    tables: List[str]
    confidence: float


# =========================================================
# GENERATOR
# =========================================================

class SQLGenerator:
    def __init__(self, llm: LLMClient):
        self.llm = llm

    @staticmethod
    def _schema_hint_for_agent(*, agent: str, allowlist: SQLAllowlist) -> Dict[str, Any]:
        """
        Convert the allowlist into a JSON-serializable schema hint for the LLM.
        """
        agent = (agent or "").strip().lower()
        tables = sorted(list(allowlist.allowed_tables))
        cols = {t: sorted(list(allowlist.allowed_columns.get(t, set()))) for t in tables}

        # Only include the tables/columns the agent is expected to query.
        if agent == "finance":
            tables = [t for t in tables if "finance_voyage_kpi" in t or "ops_voyage_summary" in t]
        elif agent == "ops":
            tables = [t for t in tables if "ops_voyage_summary" in t]

        cols = {t: cols.get(t, []) for t in tables}

        return {
            "allowed_tables": tables,
            "allowed_columns": cols,
            "join_hints": [
                {
                    "left": "finance_voyage_kpi",
                    "right": "ops_voyage_summary",
                    "keys": [
                        "voyage_id",
                        "voyage_number + vessel_imo (string-normalized)",
                    ],
                }
            ],
            "param_conventions": {
                "named_params": "Use psycopg2 named params like %(limit)s, %(voyage_number)s, %(voyage_id)s",
                "list_params": "For lists use = ANY(%(voyage_ids)s) and pass voyage_ids as a Python list (guard will wrap for psycopg2).",
                "scenario": "Prefer scenario filter: scenario = COALESCE(%(scenario)s, 'ACTUAL') when relevant.",
            },
            "constraints": {
                "select_only": True,
                "no_writes": True,
                "must_have_limit": True,
            },
        }

    # =====================================================
    # MAIN
    # =====================================================

    def generate(
        self,
        question: str,
        agent: str,
        slots: Optional[Dict[str, Any]] = None,
        intent_key: Optional[str] = None,
    ) -> SQLGenOutput:

        slots = slots or {}
        intent_key = (intent_key or "composite.query").strip()
        agent = (agent or "").strip().lower()

        schema_hint = self._schema_hint_for_agent(agent=agent, allowlist=DEFAULT_ALLOWLIST)

        intent_rules = ""
        if agent == "finance" and intent_key.startswith("ranking.voyages"):
            intent_rules = """
INTENT-SPECIFIC RULES (finance ranking.voyages*):
- Query ONLY `finance_voyage_kpi` (do NOT join ops tables here).
- MUST filter scenario: `scenario = COALESCE(%(scenario)s, 'ACTUAL')`.
- MUST return these columns (with these exact aliases) so downstream merge works:
  - voyage_id
  - voyage_number
  - pnl (numeric)
  - revenue (numeric)
  - total_expense (numeric)
  - tce (numeric)  [optional but preferred]
  - total_commission (numeric) [optional but preferred]
- If you aggregate, GROUP BY voyage_id, voyage_number.
- Rank by pnl DESC unless the question asks otherwise.
"""
        elif agent == "finance" and intent_key == "analysis.scenario_comparison":
            intent_rules = """
INTENT-SPECIFIC RULES (analysis.scenario_comparison):
- Query `finance_voyage_kpi` and compare scenario='ACTUAL' vs scenario='WHEN_FIXED'.
- MUST filter target voyages: `voyage_number = ANY(%(voyage_numbers)s)`.
- MUST pair rows using BOTH voyage_number and normalized vessel_imo key:
  `REPLACE(vessel_imo::TEXT, '.0', '')`.
- Avoid many-to-many joins:
  1) pre-aggregate each scenario to one row per (voyage_number, vessel_imo_key),
  2) join ACTUAL and WHEN_FIXED on both keys,
  3) then aggregate to FINAL one row per voyage_number.
- FINAL output must be one row per requested voyage_number (not vessel-level rows).
- Use this final metric shape:
  - `pnl_actual = SUM(pnl_actual_by_pair)`
  - `pnl_when_fixed = SUM(pnl_when_fixed_by_pair)`
  - `pnl_variance = pnl_actual - pnl_when_fixed`
  - `tce_actual = AVG(tce_actual_by_pair)`
  - `tce_when_fixed = AVG(tce_when_fixed_by_pair)`
  - `tce_variance = tce_actual - tce_when_fixed`
- Return these columns (exact aliases):
  - voyage_number
  - pnl_actual
  - pnl_when_fixed
  - pnl_variance
  - tce_actual
  - tce_when_fixed
  - tce_variance
- ORDER BY voyage_number.
- Keep LIMIT %(limit)s.
"""

        system_prompt = f"""
You are a SQL generator for a maritime analytics chatbot.
Return ONLY valid JSON with keys: sql (string), params (object), tables (string[]), confidence (0..1).

HARD RULES:
- Output SQL for PostgreSQL.
- Only generate SELECT / WITH ... SELECT queries. No INSERT/UPDATE/DELETE/DDL.
- Use ONLY allowed tables/columns from schema_hint. Do not invent columns.
- Use psycopg2 named params: %(param)s. Do not use $1/$2 positional params.
- Include a LIMIT (prefer LIMIT %(limit)s if a limit is available).
- Prefer simple queries; return only the columns needed for the question.
- When using CTEs (WITH ... AS) or JOINs between multiple tables, qualify every column reference with its table or alias (e.g. f.voyage_id, o.voyage_number, dv.voyage_id). Never use unqualified column names that exist in more than one table—PostgreSQL will raise "column reference is ambiguous". Always use table_alias.column_name.

AGENT: {agent}
{intent_rules}
"""

        result = self.llm.generate_sql(
            question=question,
            intent_key=intent_key,
            slots=slots,
            schema_hint=schema_hint,
            agent=agent,
            system_prompt=system_prompt.strip(),
        )

        sql = str(result.get("sql") or "").strip()
        params = result.get("params") or {}
        tables = result.get("tables") or []
        confidence = float(result.get("confidence") or 0.0)

        if not sql:
            return self._empty()

        # Convenience: ensure limit param present when model used %(limit)s.
        if "%(limit)s" in sql and "limit" not in params and slots.get("limit") is not None:
            try:
                params["limit"] = int(slots.get("limit"))
            except Exception:
                pass

        return SQLGenOutput(
            sql=sql.strip(),
            params=dict(params) if isinstance(params, dict) else {},
            tables=[str(t) for t in tables] if isinstance(tables, list) else [],
            confidence=confidence,
        )

    # =====================================================
    # EMPTY FALLBACK
    # =====================================================

    def _empty(self) -> SQLGenOutput:
        return SQLGenOutput(
            "SELECT 1 WHERE 1=0 LIMIT 1",
            {},
            [],
            1.0,
        )