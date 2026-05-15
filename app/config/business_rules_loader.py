from __future__ import annotations

from functools import lru_cache
from typing import Any

from app.config.schema_loader import _read_yaml


@lru_cache(maxsize=1)
def load_business_rules() -> dict[str, Any]:
    return _read_yaml("business_rules.yaml")


def get_derived_metric_rules() -> dict[str, dict[str, Any]]:
    values = load_business_rules().get("derived_metrics", {})
    if not isinstance(values, dict):
        return {}
    return {
        str(name).strip(): rule
        for name, rule in values.items()
        if str(name).strip() and isinstance(rule, dict)
    }


def get_reasoning_signal_rules() -> dict[str, dict[str, Any]]:
    values = load_business_rules().get("reasoning_signals", {})
    if not isinstance(values, dict):
        return {}
    return {
        str(name).strip(): rule
        for name, rule in values.items()
        if str(name).strip() and isinstance(rule, dict)
    }


def get_answer_contract_sections() -> list[str]:
    values = load_business_rules().get("answer_contract", {})
    sections = values.get("sections", []) if isinstance(values, dict) else []
    if not isinstance(sections, list):
        return []
    return [str(section).strip() for section in sections if str(section).strip()]


def get_reconciliation_rules() -> dict[str, Any]:
    values = load_business_rules().get("reconciliation", {})
    return values if isinstance(values, dict) else {}


def invalidate_cache() -> None:
    load_business_rules.cache_clear()
