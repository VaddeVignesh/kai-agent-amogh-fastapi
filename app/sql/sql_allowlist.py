# app/sql/sql_allowlist.py
"""
SQL Allowlist - Defines allowed tables and columns for dynamic SQL
"""

from dataclasses import dataclass
from typing import Dict, List, Set


@dataclass
class SQLAllowlist:
    """Configuration for SQL query validation"""
    allowed_tables: Set[str]
    allowed_columns: Dict[str, Set[str]]
    forbidden_patterns: List[str]


# Backwards compatibility alias
AllowlistConfig = SQLAllowlist


DEFAULT_ALLOWLIST = SQLAllowlist(
    allowed_tables={
        "finance_voyage_kpi",
        "ops_voyage_summary",
        "jsonb_array_elements_text",
        "jsonb_array_elements",
    },
    allowed_columns={
        "finance_voyage_kpi": {
            "voyage_id",
            "voyage_number",
            "vessel_imo",
            "scenario",
            "revenue",
            "total_expense",
            "pnl",
            "tce",
            "total_commission",
            "bunker_cost",
            "port_cost",
            "voyage_days",
            "voyage_start_date",
            "voyage_end_date",
            "modified_by",
            "modified_date",
            "extracted_at",
        },
        "ops_voyage_summary": {
            "voyage_id",
            "voyage_number",
            "vessel_id",
            "vessel_imo",
            "vessel_name",
            "module_type",
            "fixture_count",
            "offhire_days",
            "is_delayed",
            "delay_reason",
            "voyage_start_date",
            "voyage_end_date",
            "ports_json",
            "grades_json",
            "activities_json",
            "remarks_json",
            "tags",
            "url",
            "extracted_at",
        },
    },
    forbidden_patterns=[
        r"--",
        r"/\*.*\*/",
        r";.*$",
        r"\bEXEC\b",
        r"\bEXECUTE\b",
        r"\bCREATE\b",
        r"\bDROP\b",
        r"\bALTER\b",
        r"\bTRUNCATE\b",
        r"\bRENAME\b",
        r"\bINSERT\b",
        r"\bUPDATE\b",
        r"\bDELETE\b",
        r"\bMERGE\b",
        r"\bxp_cmdshell\b",
        r"\bdbms_\w+\b",
        r"\butl_file\b",
        r"\binformation_schema\b",
        r"\bpg_catalog\b",
    ],
)


def is_table_allowed(table_name: str, allowlist: SQLAllowlist = DEFAULT_ALLOWLIST) -> bool:
    return table_name.lower() in allowlist.allowed_tables


def is_column_allowed(
    table_name: str,
    column_name: str,
    allowlist: SQLAllowlist = DEFAULT_ALLOWLIST,
) -> bool:
    t = table_name.lower()
    c = column_name.lower()
    if t not in allowlist.allowed_columns:
        return False
    return c in allowlist.allowed_columns[t]


def get_allowed_tables(allowlist: SQLAllowlist = DEFAULT_ALLOWLIST) -> Set[str]:
    return allowlist.allowed_tables.copy()


def get_allowed_columns(
    table_name: str,
    allowlist: SQLAllowlist = DEFAULT_ALLOWLIST,
) -> Set[str]:
    return allowlist.allowed_columns.get(table_name.lower(), set()).copy()
