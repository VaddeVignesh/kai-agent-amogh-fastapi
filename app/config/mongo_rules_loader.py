from __future__ import annotations

from copy import deepcopy
from functools import lru_cache
from typing import Any

from app.config.schema_loader import _read_yaml


@lru_cache(maxsize=1)
def load_mongo_rules() -> dict[str, Any]:
    return _read_yaml("mongo_rules.yaml")


def get_mongo_schema_hint() -> dict[str, Any]:
    hint = load_mongo_rules().get("schema_hint", {})
    return deepcopy(hint) if isinstance(hint, dict) else {}


def get_mongo_allowed_collections() -> set[str]:
    collections = get_mongo_schema_hint().get("collections", {})
    if not isinstance(collections, dict):
        return set()
    return {str(name).strip() for name in collections if str(name).strip()}


def get_mongo_allowed_operators() -> set[str]:
    operators = get_mongo_schema_hint().get("allowed_operators", [])
    if not isinstance(operators, list):
        return set()
    return {str(op).strip() for op in operators if str(op).strip()}


def get_mongo_guard_default_limit() -> int:
    value = load_mongo_rules().get("guard", {}).get("default_limit", 10)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 10


def get_mongo_guard_max_limit() -> int:
    value = load_mongo_rules().get("guard", {}).get("max_limit", 50)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 50


def get_mongo_regex_options_allowed_value() -> str:
    value = load_mongo_rules().get("guard", {}).get("regex_options_allowed_value", "i")
    return str(value or "i").strip().lower()


def get_mongo_projection(name: str) -> dict[str, int]:
    raw = load_mongo_rules().get("projections", {}).get(name, {})
    if not isinstance(raw, dict):
        return {}
    projection: dict[str, int] = {}
    for key, value in raw.items():
        try:
            projection[str(key)] = int(value)
        except (TypeError, ValueError):
            projection[str(key)] = 0
    return projection


def get_mongo_limit(name: str, fallback: int) -> int:
    value = load_mongo_rules().get("limits", {}).get(name, fallback)
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _mongo_agent_rules() -> dict[str, Any]:
    values = load_mongo_rules().get("agent", {})
    return deepcopy(values) if isinstance(values, dict) else {}


def get_mongo_agent_output_fields(name: str) -> dict[str, Any]:
    values = _mongo_agent_rules().get(name, {})
    return deepcopy(values) if isinstance(values, dict) else {}


def get_mongo_agent_scoring(name: str) -> dict[str, Any]:
    values = _mongo_agent_rules().get(name, {})
    return deepcopy(values) if isinstance(values, dict) else {}


def invalidate_cache() -> None:
    load_mongo_rules.cache_clear()
