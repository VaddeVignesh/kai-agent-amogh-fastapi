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
