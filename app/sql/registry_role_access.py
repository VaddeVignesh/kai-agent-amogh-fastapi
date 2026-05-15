"""
Registry SQL access checks against session role_access.postgres_tables.
"""

from __future__ import annotations

from typing import Any

from app.auth import get_session_postgres_table_allowlist
from app.registries.sql_registry import SQL_REGISTRY
from app.sql.sql_guard import extract_referenced_sql_tables


def is_registry_query_allowed_for_session(query_key: str, session_context: dict[str, Any] | None) -> bool:
    """
    True if every physical table referenced by the registry SQL for query_key
    is allowed for this session, or if the session has no Postgres restriction.
    """
    allowed = get_session_postgres_table_allowlist(session_context)
    if allowed is None:
        return True
    spec = SQL_REGISTRY.get(query_key)
    if not spec or not str(spec.sql or "").strip():
        return True
    tables = extract_referenced_sql_tables(spec.sql)
    for t in tables:
        base = t.split(".")[-1].lower()
        if base not in allowed:
            return False
    return True
