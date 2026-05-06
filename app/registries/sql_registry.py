from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

from app.config.sql_registry_loader import get_sql_registry_entries


@dataclass(frozen=True)
class QuerySpec:
    description: str
    required_params: List[str]
    sql: str


def _build_sql_registry() -> Dict[str, QuerySpec]:
    return {
        query_key: QuerySpec(
            description=str(config.get("description") or ""),
            required_params=list(config.get("required_params") or []),
            sql=str(config.get("sql") or ""),
        )
        for query_key, config in get_sql_registry_entries().items()
    }


SQL_REGISTRY: Dict[str, QuerySpec] = _build_sql_registry()
SUPPORTED_QUERY_KEYS = set(SQL_REGISTRY.keys())
