# app/sql/sql_allowlist.py
"""
SQL Allowlist - Defines allowed tables and columns for dynamic SQL
"""

from dataclasses import dataclass
from typing import Dict, List, Set

from app.config.sql_rules_loader import (
    get_allowed_columns as get_config_allowed_columns,
    get_allowed_tables as get_config_allowed_tables,
    get_forbidden_patterns,
)
from app.auth import get_session_postgres_table_allowlist


@dataclass
class SQLAllowlist:
    """Configuration for SQL query validation"""
    allowed_tables: Set[str]
    allowed_columns: Dict[str, Set[str]]
    forbidden_patterns: List[str]


# Backwards compatibility alias
AllowlistConfig = SQLAllowlist


DEFAULT_ALLOWLIST = SQLAllowlist(
    allowed_tables=get_config_allowed_tables(),
    allowed_columns=get_config_allowed_columns(),
    forbidden_patterns=get_forbidden_patterns(),
)


def build_allowlist_for_session(
    session_context: dict | None,
    base: SQLAllowlist = DEFAULT_ALLOWLIST,
) -> SQLAllowlist:
    """
    Intersect the global allowlist with role_access.postgres_tables when present.
    """
    restricted = get_session_postgres_table_allowlist(session_context)
    if not restricted:
        return base
    restrict_set = {str(x).lower() for x in restricted}
    new_tables = {t for t in base.allowed_tables if str(t).lower() in restrict_set}
    if not new_tables:
        new_tables = set(base.allowed_tables) & restrict_set
    allowed_lower = {str(t).lower() for t in new_tables}
    new_cols = {
        k: v for k, v in base.allowed_columns.items() if str(k).lower() in allowed_lower
    }
    return SQLAllowlist(
        allowed_tables=new_tables,
        allowed_columns=new_cols,
        forbidden_patterns=list(base.forbidden_patterns),
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
