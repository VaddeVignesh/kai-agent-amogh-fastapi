from __future__ import annotations

from functools import lru_cache
from typing import Any

from app.config.schema_loader import _read_yaml


@lru_cache(maxsize=1)
def load_sql_registry_yaml() -> dict[str, Any]:
    return _read_yaml("sql_registry.yaml")


def get_sql_registry_entries() -> dict[str, dict[str, Any]]:
    values = load_sql_registry_yaml().get("queries", {})
    if not isinstance(values, dict):
        return {}

    entries: dict[str, dict[str, Any]] = {}
    for query_key, config in values.items():
        if not isinstance(config, dict):
            continue

        required_params = config.get("required_params", [])
        if not isinstance(required_params, list):
            required_params = []

        entries[str(query_key)] = {
            "description": str(config.get("description") or ""),
            "required_params": [str(param) for param in required_params],
            "sql": str(config.get("sql") or ""),
        }
    return entries


def get_supported_query_keys() -> set[str]:
    return set(get_sql_registry_entries())


def invalidate_cache() -> None:
    load_sql_registry_yaml.cache_clear()
