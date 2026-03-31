# app/sql/sql_generator.py

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from app.llm.llm_client import LLMClient
from app.registries.intent_registry import INTENT_REGISTRY
from app.sql.sql_allowlist import DEFAULT_ALLOWLIST, SQLAllowlist


FINANCE_SQL_SYSTEM_PROMPT = """
You are a deterministic PostgreSQL query generator for a maritime voyage
analytics system. Return ONLY a single valid SQL SELECT statement.
No explanation. No markdown. No code fences. Just raw SQL.

═══════════════════════════════════════
SCHEMA — exact column names, no variations allowed
═══════════════════════════════════════

Table: finance_voyage_kpi  (alias: f)
  voyage_id       TEXT
  voyage_number   INTEGER
  vessel_imo      NUMERIC  -- normalize: REPLACE(x::TEXT,'.0','')
  scenario        TEXT     -- always filter: WHERE f.scenario = COALESCE(%(scenario)s,'ACTUAL')
  pnl             NUMERIC
  revenue         NUMERIC
  total_expense   NUMERIC
  tce             NUMERIC
  voyage_days     NUMERIC  -- only on f, never on o
  total_commission NUMERIC
  bunker_cost     NUMERIC
  port_cost       NUMERIC
  voyage_start_date DATE
  voyage_end_date DATE

Table: ops_voyage_summary  (alias: o)
  voyage_id       TEXT
  voyage_number   INTEGER
  vessel_imo      NUMERIC  -- normalize same as above
  vessel_id       TEXT
  vessel_name     TEXT
  module_type     TEXT
  fixture_count   INTEGER
  is_delayed      BOOLEAN
  delay_reason    TEXT
  voyage_start_date DATE
  voyage_end_date DATE
  grades_json     JSONB    -- array of objects: [{"grade_name":"nhc", ...}]
  ports_json      JSONB    -- array of objects: [{"port_name":"Rotterdam","activity_type":"L","display_order":0}]
  remarks_json    JSONB    -- array (strings/objects depending on source); safe to select as-is
  activities_json JSONB
  tags            TEXT
  offhire_days    NUMERIC

COLUMNS THAT DO NOT EXIST — never reference these:
  expense_ratio, expense_to_revenue_ratio,
  o.voyage_days,
  demurrage_days, wait_time, activities,
  cargo_grade (derive from grades_json instead)

JOIN condition — always use this exact form:
  f.voyage_number = o.voyage_number
  AND REPLACE(f.vessel_imo::TEXT,'.0','') = REPLACE(o.vessel_imo::TEXT,'.0','')

═══════════════════════════════════════
PARAMETERS — %(name)s placeholders only, NEVER hardcode any value
═══════════════════════════════════════

The following params are ALWAYS injected before execution:
  %(scenario)s  →  always use as: WHERE f.scenario = COALESCE(%(scenario)s,'ACTUAL')
  %(limit)s     →  always use as: LIMIT %(limit)s

The following params are injected only when the slot is present.
If the slot is present, you MUST use the placeholder — never hardcode the value:
  %(voyage_number)s   →  f.voyage_number = %(voyage_number)s
  %(voyage_id)s       →  f.voyage_id = %(voyage_id)s
  %(voyage_ids)s      →  f.voyage_id = ANY(%(voyage_ids)s::TEXT[])
  %(vessel_imo)s      →  REPLACE(f.vessel_imo::TEXT,'.0','') = %(vessel_imo)s
  %(vessel_name)s     →  o.vessel_name ILIKE %(vessel_name)s
  %(start_date)s      →  o.voyage_start_date >= %(start_date)s
  %(end_date)s        →  o.voyage_end_date <= %(end_date)s
  %(cargo_grades)s    →  lower(trim(elem->>'grade_name')) = ANY(%(cargo_grades)s::TEXT[])

CRITICAL: If the user mentions a vessel name, voyage number, date, or any filter
value, it will arrive as a slot. Use %(vessel_name)s, %(voyage_number)s etc.
Writing WHERE o.vessel_name = 'Stena Conquest' is a hardcoding violation.

═══════════════════════════════════════
JSONB EXPANSION
═══════════════════════════════════════

grades_json — array of objects:
  Expand:  LEFT JOIN LATERAL jsonb_array_elements(o.grades_json) AS elem ON TRUE
  Access:  lower(trim(elem->>'grade_name'))
  Guard:   jsonb_typeof(o.grades_json) = 'array'
           AND jsonb_typeof(elem) = 'object'
           AND elem->>'grade_name' IS NOT NULL
           AND trim(elem->>'grade_name') != ''

ports_json — array of objects:
  Expand:  LEFT JOIN LATERAL jsonb_array_elements(o.ports_json) AS p ON TRUE
  Access:  lower(trim(COALESCE(p->>'port_name', p->>'portName')))
  Guard:   jsonb_typeof(o.ports_json) = 'array'

Count without row explosion (use when only count needed):
  jsonb_array_length(o.ports_json)   -- number of ports
  jsonb_array_length(o.grades_json)  -- number of grades

remarks_json:
  SELECT o.remarks_json AS remarks -- select as-is, never unnest it

═══════════════════════════════════════
GROUP BY — MUST NEVER BREAK
═══════════════════════════════════════

Every column in SELECT that is NOT inside an aggregate function
MUST appear in GROUP BY. Postgres will reject it otherwise.

When you LATERAL-expand JSONB, each expanded value is a separate row.
For each expanded column, choose one:
  A) It IS the grouping dimension → put it in GROUP BY
  B) It is NOT the grouping dimension → aggregate it

Wrong:
  SELECT o.module_type, lower(trim(elem->>'grade_name'))
  GROUP BY o.module_type
  -- ERROR: expanded grade column not in GROUP BY

Correct (aggregate non-dimension):
  SELECT o.module_type,
         mode() WITHIN GROUP (ORDER BY lower(trim(elem->>'grade_name'))) AS most_common_grade
  GROUP BY o.module_type

Correct (dimension in GROUP BY):
  SELECT lower(trim(elem->>'grade_name')) AS cargo_grade, AVG(f.pnl)
  GROUP BY lower(trim(elem->>'grade_name'))

Division: always guard against zero: NULLIF(denominator, 0)
  Example: f.total_expense / NULLIF(f.revenue, 0) AS expense_ratio

═══════════════════════════════════════
AGGREGATION REFERENCE
═══════════════════════════════════════

Voyage count:         COUNT(DISTINCT f.voyage_id)
Average PnL:          AVG(f.pnl)
Average revenue:      AVG(f.revenue)
Total PnL:            SUM(f.pnl)
Total expense:        SUM(f.total_expense)
Expense ratio:        SUM(f.total_expense) / NULLIF(SUM(f.revenue), 0)
Average voyage days:  AVG(f.voyage_days)

Port list per group:
  string_agg(DISTINCT lower(trim(port_text)), ', ') AS most_common_ports

Most common grade per group:
  mode() WITHIN GROUP (ORDER BY lower(trim(elem->>'grade_name'))) AS most_common_grade

Grade list per group:
  string_agg(DISTINCT lower(trim(elem->>'grade_name')), ', ') AS grades

═══════════════════════════════════════
QUERY CONSTRUCTION RULES
═══════════════════════════════════════

1. Read the question. Identify the grouping dimension, or confirm no grouping needed.

2. Select only the columns the question asks for. Never SELECT *.

3. JOIN ops_voyage_summary only when ops columns are needed.

4. LATERAL join grades_json only when grade data is needed.
   LATERAL join ports_json only when port data is needed.
   Never join both unless both are explicitly needed.
   When only counting ports/grades, use jsonb_array_length — no LATERAL needed.

5. Apply all provided slot filters as WHERE conditions using %(name)s placeholders.
   Always apply: WHERE f.scenario = COALESCE(%(scenario)s,'ACTUAL')

6. GROUP BY every non-aggregated SELECT column.

7. ORDER BY the most relevant metric descending unless otherwise implied.

8. Always end with LIMIT %(limit)s.

═══════════════════════════════════════
ABSOLUTE PROHIBITIONS
═══════════════════════════════════════

Never write:  WHERE scenario = 'ACTUAL'                  -- hardcoded scenario
Never write:  LIMIT <any number>                         -- hardcoded limit, use LIMIT %(limit)s
Never write:  vessel_name = 'any name'                   -- hardcoded name
Never write:  voyage_number = '2306'                     -- hardcoded voyage
Never write:  IN ('nhc','crude')                         -- hardcoded list
Never write:  start_date >= '2024-01-01'                 -- hardcoded date
Never write:  SELECT *                                   -- name columns explicitly
Never write:  COUNT(*)                                   -- use COUNT(DISTINCT f.voyage_id)
Never write:  f.voyage_id IN (...)                       -- use ANY(%(voyage_ids)s::TEXT[])
Never write:  o.voyage_days                              -- voyage_days is on f only
Never write:  f.expense_to_revenue_ratio  -- do not exist
Never write:  o.start_date or o.end_date   -- use o.voyage_start_date, o.voyage_end_date

Output only the SQL. Nothing else.
"""


OPS_SQL_SYSTEM_PROMPT = """
You are a deterministic PostgreSQL query generator for a maritime voyage
analytics system. Return ONLY a single valid SQL SELECT statement.
No explanation. No markdown. No code fences. Just raw SQL.

═══════════════════════════════════════
SCHEMA — exact column names, no variations allowed
═══════════════════════════════════════

Table: ops_voyage_summary  (alias: o)
  voyage_id       TEXT
  voyage_number   INTEGER
  vessel_imo      NUMERIC
  vessel_id       TEXT
  vessel_name     TEXT
  module_type     TEXT
  fixture_count   INTEGER
  is_delayed      BOOLEAN
  delay_reason    TEXT
  voyage_start_date DATE
  voyage_end_date DATE
  grades_json     JSONB    -- array of objects: [{"grade_name":"nhc",...}]
  ports_json      JSONB    -- array of objects: [{"port_name":"Rotterdam","activity_type":"L","display_order":0}]
  remarks_json    JSONB    -- select as-is, never unnest inline
  activities_json JSONB
  tags            TEXT
  offhire_days    NUMERIC

Table: finance_voyage_kpi  (alias: f) — join only when financial data needed
  voyage_id       TEXT
  voyage_number   INTEGER
  vessel_imo      NUMERIC
  scenario        TEXT
  pnl             NUMERIC
  revenue         NUMERIC
  total_expense   NUMERIC
  tce             NUMERIC
  voyage_days     NUMERIC  -- only on f, not on o
  total_commission NUMERIC
  bunker_cost     NUMERIC
  port_cost       NUMERIC

COLUMNS THAT DO NOT EXIST — never reference these:
  expense_ratio, o.voyage_days,
  demurrage_days, wait_time, activities,
  cargo_grade (derive from grades_json)

JOIN condition:
  f.voyage_number = o.voyage_number
  AND REPLACE(f.vessel_imo::TEXT,'.0','') = REPLACE(o.vessel_imo::TEXT,'.0','')

═══════════════════════════════════════
PARAMETERS — %(name)s placeholders only, never hardcode values
═══════════════════════════════════════

Always present:
  %(scenario)s  →  WHERE f.scenario = COALESCE(%(scenario)s,'ACTUAL')  (only when joining finance)
  %(limit)s     →  LIMIT %(limit)s

Present when slot exists — always use placeholder, never the literal value:
  %(voyage_number)s   →  o.voyage_number = %(voyage_number)s
  %(voyage_id)s       →  o.voyage_id = %(voyage_id)s
  %(voyage_ids)s      →  o.voyage_id = ANY(%(voyage_ids)s::TEXT[])
  %(vessel_name)s     →  o.vessel_name ILIKE %(vessel_name)s
  %(vessel_imo)s      →  REPLACE(o.vessel_imo::TEXT,'.0','') = %(vessel_imo)s
  %(start_date)s      →  o.voyage_start_date >= %(start_date)s
  %(end_date)s        →  o.voyage_end_date <= %(end_date)s
  %(cargo_grades)s    →  lower(trim(elem->>'grade_name')) = ANY(%(cargo_grades)s::TEXT[])

═══════════════════════════════════════
JSONB EXPANSION
═══════════════════════════════════════

grades_json:
  LEFT JOIN LATERAL jsonb_array_elements(o.grades_json) AS elem ON TRUE
  lower(trim(elem->>'grade_name'))
  Guard: jsonb_typeof(o.grades_json)='array' AND jsonb_typeof(elem)='object'
         AND elem->>'grade_name' IS NOT NULL AND trim(elem->>'grade_name')!=''

ports_json:
  LEFT JOIN LATERAL jsonb_array_elements_text(o.ports_json) AS port_text ON TRUE
  Guard: jsonb_typeof(o.ports_json)='array'

Count only (no row explosion):
  jsonb_array_length(o.ports_json)
  jsonb_array_length(o.grades_json)

remarks_json: SELECT o.remarks_json AS remarks — never unnest

═══════════════════════════════════════
GROUP BY RULE
═══════════════════════════════════════

Every non-aggregated SELECT column must appear in GROUP BY.
LATERAL-expanded columns: either group by them, or aggregate them.
Division: always guard with NULLIF(denominator, 0).

═══════════════════════════════════════
QUERY CONSTRUCTION RULES
═══════════════════════════════════════

1. Read the question. Identify the grouping dimension, or confirm no grouping needed.
2. Select only the columns the question asks for. Never SELECT *.
3. JOIN finance_voyage_kpi only when financial columns are needed.
4. LATERAL join grades_json only when grade data is needed.
   LATERAL join ports_json only when port data is needed.
   Use jsonb_array_length when only counting.
5. Apply all slot filters using %(name)s placeholders.
6. GROUP BY every non-aggregated SELECT column.
7. ORDER BY the most relevant metric descending unless otherwise implied.
8. Always end with LIMIT %(limit)s.

═══════════════════════════════════════
ABSOLUTE PROHIBITIONS
═══════════════════════════════════════

Never write:  any literal value where a %(param)s exists
Never write:  LIMIT <any number>            -- use LIMIT %(limit)s
Never write:  SELECT *
Never write:  COUNT(*)                      -- use COUNT(DISTINCT o.voyage_id)
Never write:  o.voyage_days                 -- voyage_days is on f only
Never write:  o.start_date or o.end_date    -- use o.voyage_start_date, o.voyage_end_date

Output only the SQL. Nothing else.
"""


# Postgres execution error patterns that are retryable
_RETRYABLE_PG_ERRORS = (
    "must appear in the GROUP BY",
    "aggregate function",
    "syntax error",
    "does not exist",
    "invalid input syntax",
    "operator does not exist",
    "ambiguous column",
    "column",
)

_DEFAULT_LIMIT = int(os.getenv("SQL_DEFAULT_LIMIT", "25"))

# LIMIT followed by a bare integer — must use %(limit)s instead
_HARDCODED_LIMIT_RE = re.compile(r"\blimit\s+\d+", re.IGNORECASE)

# String literal in a filter position (=, >=, ILIKE, IN, etc.)
_HARDCODED_STRING_RE = re.compile(
    r"(?:=|>=|<=|>|<|ilike|like|in\s*\()\s*'[^']{1,120}'", re.IGNORECASE
)

# These string literals are genuinely structural — not data values
# '.0'  → REPLACE(x::TEXT, '.0', '') normalization
# ''    → trim/empty guard  AND trim(elem->>'grade_name') != ''
# ' '   → whitespace guard
_ALLOWED_LITERALS = {".0", "", " "}


@dataclass
class SQLGenOutput:
    sql: str
    params: Dict[str, Any]
    tables: List[str]
    confidence: float


class SQLGenerator:
    def __init__(self, llm: LLMClient):
        self.llm = llm
        self.sql_max_tokens = int(os.getenv("SQL_MAX_TOKENS", "1024"))
        self.sql_max_retries = int(os.getenv("SQL_MAX_RETRIES", "2"))

    @staticmethod
    def _schema_hint_for_agent(
        *,
        agent: str,
        allowlist: SQLAllowlist,
        intent_key: str = "",
    ) -> Dict[str, Any]:
        agent = (agent or "").strip().lower()
        tables = sorted(list(allowlist.allowed_tables))
        cols = {
            t: sorted(list(allowlist.allowed_columns.get(t, set())))
            for t in tables
        }

        if agent == "finance":
            tables = [
                t for t in tables
                if "finance_voyage_kpi" in t or "ops_voyage_summary" in t
            ]
        elif agent == "ops":
            tables = [t for t in tables if "ops_voyage_summary" in t]

        cols = {t: cols.get(t, []) for t in tables}

        hint: Dict[str, Any] = {
            "allowed_tables": tables,
            "allowed_columns": cols,
            "join_hints": [
                {
                    "left": "finance_voyage_kpi",
                    "right": "ops_voyage_summary",
                    "keys": [
                        "voyage_number + vessel_imo (string-normalized with REPLACE(.0,''))",
                    ],
                }
            ],
            "param_conventions": {
                "named_params": "Use %(param)s placeholders. Never hardcode values.",
                "list_params": "f.voyage_id = ANY(%(voyage_ids)s::TEXT[])",
                "scenario": "COALESCE(%(scenario)s,'ACTUAL') — always injected.",
                "limit": "LIMIT %(limit)s — always injected.",
            },
            "constraints": {
                "select_only": True,
                "no_writes": True,
                "must_have_limit": True,
            },
        }

        intent_cfg = INTENT_REGISTRY.get(intent_key, {})
        if intent_cfg.get("route") == "composite":
            hint["query_nature"] = (
                "Fleet-wide aggregate query. Use GROUP BY + aggregates. "
                "Do not filter to a single voyage unless a slot provides one. "
                "Identify the grouping dimension from the question and build from rules."
            )

        return hint

    def _inject_required_params(
        self, sql: str, params: Dict[str, Any], slots: Dict[str, Any]
    ) -> Dict[str, Any]:
        merged = dict(params)

        slot_param_map = {
            "%(scenario)s":      ("scenario",      lambda s: s.get("scenario")),
            "%(limit)s":         ("limit",          lambda s: int(s["limit"]) if s.get("limit") is not None else _DEFAULT_LIMIT),
            "%(voyage_ids)s":    ("voyage_ids",     lambda s: s.get("voyage_ids") or []),
            "%(vessel_name)s":   ("vessel_name",    lambda s: s.get("vessel_name")),
            "%(voyage_number)s": ("voyage_number",  lambda s: s.get("voyage_number")),
            "%(voyage_id)s":     ("voyage_id",      lambda s: s.get("voyage_id")),
            "%(cargo_grades)s":  ("cargo_grades",   lambda s: s.get("cargo_grades") or []),
            "%(start_date)s":    ("start_date",     lambda s: s.get("start_date")),
            "%(end_date)s":      ("end_date",       lambda s: s.get("end_date")),
            "%(vessel_imo)s":    ("vessel_imo",     lambda s: s.get("vessel_imo")),
        }

        for placeholder, (key, extractor) in slot_param_map.items():
            if placeholder in sql and key not in merged:
                try:
                    merged[key] = extractor(slots)
                except (TypeError, ValueError, KeyError):
                    merged[key] = None

        # Always inject limit and scenario — they are unconditional
        if "limit" not in merged:
            merged["limit"] = int(slots.get("limit") or _DEFAULT_LIMIT)
        if "scenario" not in merged:
            merged["scenario"] = slots.get("scenario")

        return merged

    def _build_system_prompt(
        self,
        agent: str,
        intent_key: str,
        error_hint: Optional[str],
    ) -> str:
        base = FINANCE_SQL_SYSTEM_PROMPT if agent == "finance" else OPS_SQL_SYSTEM_PROMPT

        intent_cfg = INTENT_REGISTRY.get(intent_key, {})
        intent_rules = intent_cfg.get("sql_hints", {}).get(agent, "")
        if intent_rules:
            base = base + f"\n\nINTENT RULES:\n{intent_rules}"

        if error_hint:
            base = base + (
                f"\n\nPREVIOUS ATTEMPT FAILED:\n{error_hint}\n"
                "Fix the SQL. Common causes:\n"
                "1. Non-aggregated column missing from GROUP BY\n"
                "2. Column does not exist — check schema above exactly\n"
                "3. Division by zero — use NULLIF(denominator, 0)\n"
                "4. Wrong table for column (e.g. voyage_days is on f, not o)\n"
                "5. LATERAL-expanded column in SELECT but missing from GROUP BY\n"
                "6. Hardcoded LIMIT number — use LIMIT %(limit)s\n"
                "7. Hardcoded string value in WHERE — use %(param)s placeholder"
            )

        return base

    def _call_llm(
        self,
        question: str,
        intent_key: str,
        slots: Dict[str, Any],
        schema_hint: Dict[str, Any],
        agent: str,
        system_prompt: str,
    ) -> Dict[str, Any]:
        return self.llm.generate_sql(
            question=question,
            intent_key=intent_key,
            slots=slots,
            schema_hint=schema_hint,
            agent=agent,
            system_prompt=system_prompt,
            temperature=0,         # deterministic — never change this
            max_tokens=self.sql_max_tokens,
        )

    def generate(
        self,
        question: str,
        agent: str,
        slots: Optional[Dict[str, Any]] = None,
        intent_key: Optional[str] = None,
        error_hint: Optional[str] = None,
    ) -> SQLGenOutput:
        slots = slots or {}
        intent_key = (intent_key or "composite.query").strip()
        agent = (agent or "").strip().lower()

        schema_hint = self._schema_hint_for_agent(
            agent=agent,
            allowlist=DEFAULT_ALLOWLIST,
            intent_key=intent_key,
        )

        last_error: Optional[str] = error_hint

        for attempt in range(self.sql_max_retries + 1):
            system_prompt = self._build_system_prompt(
                agent=agent,
                intent_key=intent_key,
                error_hint=last_error,
            )

            result = self._call_llm(
                question=question,
                intent_key=intent_key,
                slots=slots,
                schema_hint=schema_hint,
                agent=agent,
                system_prompt=system_prompt,
            )

            sql = str(result.get("sql") or "").strip()
            params = result.get("params") or {}
            tables = result.get("tables") or []
            confidence = float(result.get("confidence") or 0.0)

            if not sql:
                return self._empty()

            params = self._inject_required_params(sql, params, slots)

            output = SQLGenOutput(
                sql=sql,
                params=params,
                tables=[str(t) for t in tables] if isinstance(tables, list) else [],
                confidence=confidence,
            )

            try:
                self._validate_sql(output, slots)
                return output

            except ValueError as exc:
                last_error = str(exc)
                if attempt < self.sql_max_retries:
                    continue
                return output  # best effort after exhausting retries

            except Exception as exc:
                err_msg = str(exc)
                is_retryable = any(k in err_msg for k in _RETRYABLE_PG_ERRORS)
                if is_retryable and attempt < self.sql_max_retries:
                    last_error = err_msg
                    continue
                return output

        return self._empty()

    def _validate_sql(self, output: SQLGenOutput, slots: Dict[str, Any]) -> None:
        sql = output.sql
        sql_lower = sql.lower()

        if not sql_lower.strip().startswith("select"):
            raise ValueError("SQL does not start with SELECT")

        if "limit" not in sql_lower:
            raise ValueError(
                "SQL missing LIMIT clause. Use LIMIT %(limit)s — never a bare number."
            )

        if _HARDCODED_LIMIT_RE.search(sql_lower):
            raise ValueError(
                "Hardcoding violation: LIMIT has a bare integer. "
                "Use LIMIT %(limit)s instead."
            )

        for pattern in ("scenario = 'actual'", 'scenario = "actual"'):
            if pattern in sql_lower:
                raise ValueError(
                    "Hardcoding violation: scenario value is hardcoded. "
                    "Use COALESCE(%(scenario)s,'ACTUAL')."
                )

        # Strip REPLACE('.0','') before checking for hardcoded strings
        # so the structural normalization pattern doesn't trigger
        sql_for_check = re.sub(
            r"REPLACE\s*\([^)]+\)", "", sql, flags=re.IGNORECASE
        )
        # Also strip COALESCE(%(scenario)s,'ACTUAL') — the 'ACTUAL' default
        # is a fallback value, not a hardcoded filter
        sql_for_check = re.sub(
            r"COALESCE\s*\([^)]+\)", "", sql_for_check, flags=re.IGNORECASE
        )
        matches = _HARDCODED_STRING_RE.findall(sql_for_check)
        suspicious = [
            m for m in matches
            if not any(allowed in m for allowed in _ALLOWED_LITERALS)
        ]
        if suspicious:
            raise ValueError(
                f"Hardcoding violation: literal value(s) found in filter: "
                f"{suspicious}. Use %(param)s placeholders instead."
            )

        if re.search(r"select\s+\*", sql_lower):
            raise ValueError("SQL uses SELECT *. Name all columns explicitly.")

        if re.search(r"count\s*\(\s*\*\s*\)", sql_lower):
            raise ValueError(
                "SQL uses COUNT(*). Use COUNT(DISTINCT f.voyage_id) or "
                "COUNT(DISTINCT o.voyage_id)."
            )

        forbidden_columns = [
            ("o.voyage_days",             "voyage_days is on f only"),
            ("expense_to_revenue_ratio",  "column does not exist, compute inline"),
            ("demurrage_days",            "column does not exist"),
        ]
        for col, reason in forbidden_columns:
            if col in sql_lower:
                raise ValueError(
                    f"Non-existent column reference: {col} — {reason}"
                )

        # Optional placeholder hygiene: only allow entity/filter placeholders
        # when the corresponding slot is actually present for this request.
        optional_placeholder_slots = {
            "%(voyage_number)s": "voyage_number",
            "%(voyage_id)s": "voyage_id",
            "%(voyage_ids)s": "voyage_ids",
            "%(vessel_name)s": "vessel_name",
            "%(vessel_imo)s": "vessel_imo",
            "%(cargo_grades)s": "cargo_grades",
            "%(start_date)s": "start_date",
            "%(end_date)s": "end_date",
        }
        for placeholder, slot_key in optional_placeholder_slots.items():
            if placeholder in sql:
                slot_val = slots.get(slot_key)
                if slot_val in (None, "", [], {}):
                    raise ValueError(
                        f"SQL uses placeholder {placeholder} but slot '{slot_key}' "
                        "is not present for this request."
                    )

    def _empty(self) -> SQLGenOutput:
        return SQLGenOutput(
            sql="SELECT 1 WHERE 1=0",
            params={},
            tables=[],
            confidence=0.0,
        )