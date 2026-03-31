"""
SQL Guard - Validates and prepares SQL. Handles SELECT parsing, param sanitization,
ANY/list params, IN→ANY rewrite, JSONB, ORDER BY cleanup, LIMIT enforcement.
"""

from __future__ import annotations
import ast
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, Optional

from app.sql.sql_allowlist import (
    SQLAllowlist,
    DEFAULT_ALLOWLIST,
    is_table_allowed,
)

DEFAULT_ENFORCED_LIMIT = int(os.getenv("SQL_GUARD_DEFAULT_LIMIT", "50"))

# =========================================================
# RESULT MODEL
# =========================================================

@dataclass
class ValidationResult:
    ok: bool
    sql: str
    params: Dict[str, Any]
    reason: Optional[str] = None


# =========================================================
# FIX MAPS
# =========================================================

_COLUMN_FIXES = {
    "costs": "total_expense",
    "commission": "total_commission",
    # Common user/LLM synonyms
    "profit": "pnl",
    "profitability": "pnl",
    "expense": "total_expense",
    "expenses": "total_expense",
    "start_date": "voyage_start_date",
    "end_date": "voyage_end_date",
    "voyage_start": "voyage_start_date",
    "voyage_end": "voyage_end_date",
}

_TABLE_FIXES = {
    "ops_voyage": "ops_voyage_summary",
    "voyage_kpi": "finance_voyage_kpi",
    "finance_voyage": "finance_voyage_kpi",
    "finance_voyage_summary": "finance_voyage_kpi",
}

_INVALID_COLUMNS = {
    "cargo_grade",
    "cargograde",
    "portname",
    "port_name",
    "port",
    "remarks",
    "voyage_profitability",
    "key_ports_visited",
    "cost",
    "imo",
    "total_cost",
    "vessel_id",
}

_FINANCE_ONLY_COLUMNS = {
    "pnl", "revenue", "tce",
    "bunker_cost", "port_cost",
    "total_expense", "total_commission",
    "scenario",
}

_OPS_ONLY_COLUMNS = {
    "offhire_days", "is_delayed",
    "delay_reason", "module_type",
    "fixture_count", "grades_json",
    "ports_json", "remarks_json",
}


# =========================================================
# PARAM SANITIZATION
# =========================================================

def _sanitize_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert string lists to Python lists
    Wrap lists inside tuple for psycopg2 ANY operator
    """
    clean = {}

    for k, v in params.items():

        if isinstance(v, str):
            stripped = v.strip()
            if stripped.startswith("[") and stripped.endswith("]"):
                try:
                    parsed = ast.literal_eval(stripped)
                    if isinstance(parsed, list):
                        clean[k] = (parsed,)
                        continue
                except Exception:
                    pass
            # Normal string param (e.g. port name) — keep as-is
            clean[k] = v

        elif isinstance(v, list):
            clean[k] = (v,)

        else:
            clean[k] = v

    return clean


# =========================================================
# SIMPLE AUTO FIXES
# =========================================================

def _apply_simple_fixes(sql: str) -> str:
    fixed = sql

    # Column renames
    for wrong, right in _COLUMN_FIXES.items():
        fixed = re.sub(rf"\b{wrong}\b", right, fixed, flags=re.IGNORECASE)

    # Table renames
    for wrong, right in _TABLE_FIXES.items():
        fixed = re.sub(rf"\b{wrong}\b", right, fixed, flags=re.IGNORECASE)

    # JSONB @> 'text' → ::text ILIKE
    fixed = re.sub(
        r"(\w+_json)\s*@>\s*'([^']*)'",
        r"\1::text ILIKE '%\2%'",
        fixed,
        flags=re.IGNORECASE,
    )

    # IN %(param)s → = ANY(%(param)s)
    fixed = re.sub(
        r"\bIN\s+(%\(\w+\)s)",
        r"= ANY(\1)",
        fixed,
        flags=re.IGNORECASE,
    )

    return fixed


# =========================================================
# ORDER BY CLEANER
# =========================================================

def _clean_order_by(sql: str, tables: set) -> str:

    has_finance = any("finance" in t for t in tables)
    has_ops = any("ops" in t for t in tables)

    def _strip(match: re.Match) -> str:
        col = match.group(1).split(".")[-1].lower()

        if col in _FINANCE_ONLY_COLUMNS and not has_finance:
            return ""

        if col in _OPS_ONLY_COLUMNS and not has_ops:
            return ""

        return match.group(0)

    return re.sub(
        r"\bORDER\s+BY\s+([\w.]+)(?:\s+(?:ASC|DESC))?",
        _strip,
        sql,
        flags=re.IGNORECASE,
    )


# =========================================================
# MAIN VALIDATOR
# =========================================================

def validate_and_prepare_sql(
    sql: str,
    params: Dict[str, Any],
    allowlist: SQLAllowlist = DEFAULT_ALLOWLIST,
    enforce_limit: Any = True,
) -> ValidationResult:

    try:

        # ---------------------------------------------
        # Apply automatic corrections
        # ---------------------------------------------

        sql = _apply_simple_fixes(sql)
        params = _sanitize_params(params or {})

        lowered = sql.lower()

        # ---------------------------------------------
        # Extract tables
        # ---------------------------------------------

        # Collect CTE names so we don't treat them as real tables.
        # This keeps allowlist enforcement on underlying tables while permitting safe WITH queries.
        cte_names = set(re.findall(r"\bwith\s+([a-zA-Z_][a-zA-Z0-9_]*)\s+as\s*\(", lowered))
        cte_names.update(re.findall(r",\s*([a-zA-Z_][a-zA-Z0-9_]*)\s+as\s*\(", lowered))

        tables = set()

        for m in re.finditer(r"\bfrom\s+([a-zA-Z0-9_.]+)", lowered):
            tname = m.group(1)
            if tname not in cte_names:
                tables.add(tname)

        for m in re.finditer(r"\bjoin\s+([a-zA-Z0-9_.]+)", lowered):
            tname = m.group(1)
            # Ignore join keywords like "lateral" (e.g., CROSS JOIN LATERAL ...)
            if tname == "lateral":
                continue
            if tname not in cte_names:
                tables.add(tname)

        JSONB_FUNCTIONS = {
            "jsonb_array_elements_text",
            "jsonb_array_elements",
            "jsonb_each",
            "jsonb_each_text",
            "jsonb_object_keys",
        }
        for t in tables:
            if t in JSONB_FUNCTIONS:
                continue
            if not is_table_allowed(t, allowlist):
                return ValidationResult(
                    ok=False,
                    sql=sql,
                    params=params,
                    reason=f"Table not allowed: {t}",
                )

        # ---------------------------------------------
        # Clean ORDER BY invalid columns
        # ---------------------------------------------

        sql = _clean_order_by(sql, tables)

        # ---------------------------------------------
        # Safe SELECT parsing (NO CRASH)
        # ---------------------------------------------

        match = re.search(
            r"select\s+(.*?)(?=\s+from|\s*$)",
            sql,
            flags=re.DOTALL | re.IGNORECASE,
        )

        if match:

            cols_raw = match.group(1)

            # Remove trailing comma safely
            cols_raw = cols_raw.rstrip().rstrip(",")

            # Remove empty column fragments
            cols = [c.strip() for c in cols_raw.split(",") if c.strip()]

            for col_expr in cols:

                if col_expr == "*":
                    continue

                if "(" in col_expr and ")" in col_expr:
                    continue

                parts = col_expr.split()

                if not parts:
                    continue  # prevents crash

                # Check source column, not alias
                # e.g. 'port_text AS port_name' → check 'port_text', not 'port_name'
                if len(parts) >= 3 and parts[-2].lower() == "as":
                    col_name = parts[0].split(".")[-1].lower()
                else:
                    col_name = parts[-1].split(".")[-1].lower()

                if col_name in _INVALID_COLUMNS:
                    return ValidationResult(
                        ok=False,
                        sql=sql,
                        params=params,
                        reason=f"Column '{col_name}' does not exist. Use grades_json / ports_json instead.",
                    )

        # ---------------------------------------------
        # Block forbidden patterns
        # ---------------------------------------------

        # A single trailing semicolon is harmless; remove it before pattern checks
        # to avoid false positives from broad ";.*$" rules.
        sql_for_pattern_check = sql.rstrip()
        if sql_for_pattern_check.endswith(";"):
            sql_for_pattern_check = sql_for_pattern_check[:-1]

        for pattern in allowlist.forbidden_patterns:
            if re.search(pattern, sql_for_pattern_check, flags=re.IGNORECASE):
                return ValidationResult(
                    ok=False,
                    sql=sql,
                    params=params,
                    reason="Forbidden SQL pattern detected",
                )

        # ---------------------------------------------
        # Enforce LIMIT
        # ---------------------------------------------

        if enforce_limit:
            if not re.search(r"\blimit\s+(\d+|%\(\w+\)s|\$\d+)", sql.lower()):
                sql = sql.rstrip().rstrip(";") + f"\nLIMIT {DEFAULT_ENFORCED_LIMIT}"

        # ---------------------------------------------
        # Remove unused params
        # ---------------------------------------------

        # Postgres adapter normalizes ":param" → "%(param)s" right before execution.
        # Keep params used in either placeholder style. Avoid matching "::text" casts.
        named_params = set(re.findall(r"%\((\w+)\)s", sql))
        colon_params = set(re.findall(r"(?<!:):(\w+)(?!:)", sql))
        used_param_names = named_params | colon_params

        filtered_params = {k: v for k, v in params.items() if k in used_param_names}

        return ValidationResult(
            ok=True,
            sql=sql,
            params=filtered_params,
        )

    except Exception as e:

        return ValidationResult(
            ok=False,
            sql=sql,
            params=params,
            reason=f"Validation error: {e}",
        )
