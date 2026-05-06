from __future__ import annotations

from functools import lru_cache
from typing import Any

from app.config.schema_loader import _read_yaml


@lru_cache(maxsize=1)
def load_domain() -> dict:
    return _read_yaml("domain.yaml")


def get_default_limit() -> int:
    value = load_domain().get("query_defaults", {}).get("default_limit", 10)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 10


def get_default_scenario() -> str:
    value = load_domain().get("query_defaults", {}).get("default_scenario", "ACTUAL")
    return str(value or "ACTUAL")


def get_min_voyage_count() -> int:
    value = load_domain().get("analytics_thresholds", {}).get("min_voyage_count", 3)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 3


def get_min_voyage_count_fallback() -> int:
    value = load_domain().get("analytics_thresholds", {}).get("min_voyage_count_fallback", 1)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 1


def get_null_equivalents() -> tuple[str, ...]:
    values = load_domain().get("data_quality_filters", {}).get("null_equivalents", [])
    if not isinstance(values, list):
        values = []
    normalized = {str(v).strip().lower() for v in values}
    # Preserve existing behavior while letting config own the base list.
    normalized.update({"na", "unknown"})
    return tuple(sorted(normalized))


def is_null_equivalent(value: Any) -> bool:
    return str(value or "").strip().lower() in set(get_null_equivalents())


def invalidate_cache() -> None:
    load_domain.cache_clear()
