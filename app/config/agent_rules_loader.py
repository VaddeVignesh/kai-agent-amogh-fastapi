from __future__ import annotations

from functools import lru_cache
from typing import Any

from app.config.schema_loader import _read_yaml


@lru_cache(maxsize=1)
def load_agent_rules() -> dict[str, Any]:
    return _read_yaml("agent_rules.yaml")


def _finance_rules() -> dict[str, Any]:
    rules = load_agent_rules().get("finance", {})
    return rules if isinstance(rules, dict) else {}


def _ops_rules() -> dict[str, Any]:
    rules = load_agent_rules().get("ops", {})
    return rules if isinstance(rules, dict) else {}


def get_finance_max_limit() -> int:
    value = _finance_rules().get("max_limit", 200)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 200


def get_finance_composite_allowed_extra_slots() -> set[str]:
    values = _finance_rules().get("composite_allowed_extra_slots", [])
    if not isinstance(values, list):
        return set()
    return {str(value).strip() for value in values if str(value).strip()}


def get_finance_safe_metrics() -> dict[str, str]:
    values = _finance_rules().get("ranking", {}).get("safe_metrics", {})
    if not isinstance(values, dict):
        return {}
    return {
        str(key).strip(): str(value).strip()
        for key, value in values.items()
        if str(key).strip() and str(value).strip()
    }


def get_finance_ranking_default_metric() -> str:
    value = _finance_rules().get("ranking", {}).get("default_metric", "pnl")
    return str(value or "pnl").strip()


def get_finance_ranking_default_direction() -> str:
    value = _finance_rules().get("ranking", {}).get("default_direction", "desc")
    return str(value or "desc").strip().lower()


def get_finance_allowed_directions() -> set[str]:
    values = _finance_rules().get("ranking", {}).get("allowed_directions", [])
    if not isinstance(values, list):
        return {"asc", "desc"}
    normalized = {str(value).strip().lower() for value in values if str(value).strip()}
    return normalized or {"asc", "desc"}


def get_finance_intent_metric_overrides() -> dict[str, str]:
    values = _finance_rules().get("ranking", {}).get("intent_metric_overrides", {})
    if not isinstance(values, dict):
        return {}
    return {
        str(key).strip(): str(value).strip()
        for key, value in values.items()
        if str(key).strip() and str(value).strip()
    }


def get_finance_simple_intent_mappings() -> dict[str, dict[str, Any]]:
    values = _finance_rules().get("simple_intent_mappings", {})
    if not isinstance(values, dict):
        return {}
    return {
        str(intent_key).strip(): dict(config)
        for intent_key, config in values.items()
        if str(intent_key).strip() and isinstance(config, dict)
    }


def get_finance_validation_message(name: str) -> str:
    values = _finance_rules().get("validation_messages", {})
    if not isinstance(values, dict):
        return ""
    return str(values.get(name) or "")


def _finance_dynamic_sql_rules() -> dict[str, Any]:
    values = _finance_rules().get("dynamic_sql", {})
    return values if isinstance(values, dict) else {}


def get_finance_repairable_error_patterns() -> list[str]:
    values = _finance_dynamic_sql_rules().get("repairable_error_patterns", [])
    if not isinstance(values, list):
        return []
    return [str(value).strip().lower() for value in values if str(value).strip()]


def get_finance_repair_prompt(name: str) -> str:
    values = _finance_dynamic_sql_rules().get("repair_prompts", {})
    if not isinstance(values, dict):
        return ""
    return str(values.get(name) or "")


def get_finance_segment_performance_fallback_sql(name: str) -> str:
    values = _finance_dynamic_sql_rules().get("segment_performance_fallback_sql", {})
    if not isinstance(values, dict):
        return ""
    return str(values.get(name) or "")


def get_ops_max_limit() -> int:
    value = _ops_rules().get("max_limit", 200)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 200


def get_ops_cargo_grade_max_count() -> int:
    value = _ops_rules().get("cargo_grade_max_count", 50)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 50


def get_ops_cargo_profitability_intents() -> set[str]:
    values = _ops_rules().get("cargo_profitability_intents", [])
    if not isinstance(values, list):
        return set()
    return {str(value).strip() for value in values if str(value).strip()}


def get_ops_delay_remark_keywords() -> list[str]:
    values = _ops_rules().get("delay_remark_keywords", [])
    if not isinstance(values, list):
        return []
    return [str(value).strip().lower() for value in values if str(value).strip()]


def get_ops_simple_intent_mappings() -> dict[str, dict[str, Any]]:
    values = _ops_rules().get("simple_intent_mappings", {})
    if not isinstance(values, dict):
        return {}
    return {
        str(intent_key).strip(): dict(config)
        for intent_key, config in values.items()
        if str(intent_key).strip() and isinstance(config, dict)
    }


def get_ops_validation_message(name: str) -> str:
    values = _ops_rules().get("validation_messages", {})
    if not isinstance(values, dict):
        return ""
    return str(values.get(name) or "")


def get_ops_canonical_sql(name: str) -> str:
    values = _ops_rules().get("canonical_sql", {})
    if not isinstance(values, dict):
        return ""
    return str(values.get(name) or "")


def get_ops_delay_remark_filter_template() -> str:
    return str(_ops_rules().get("delay_remark_filter_template") or "lower(remark) LIKE '%%{keyword}%%'")


def get_ops_delay_remark_filter_empty() -> str:
    return str(_ops_rules().get("delay_remark_filter_empty") or "FALSE")


def invalidate_cache() -> None:
    load_agent_rules.cache_clear()
