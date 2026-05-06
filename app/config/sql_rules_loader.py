from __future__ import annotations

from functools import lru_cache
from typing import Any

from app.config.schema_loader import _read_yaml


@lru_cache(maxsize=1)
def load_sql_rules() -> dict[str, Any]:
    return _read_yaml("sql_rules.yaml")


def _as_lower_set(values: Any) -> set[str]:
    if not isinstance(values, list):
        return set()
    return {str(value).strip().lower() for value in values if str(value).strip()}


def _as_string_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    return [str(value) for value in values]


def _as_string_map(values: Any) -> dict[str, str]:
    if not isinstance(values, dict):
        return {}
    return {
        str(key).strip().lower(): str(value).strip()
        for key, value in values.items()
        if str(key).strip() and str(value).strip()
    }


def get_allowed_tables() -> set[str]:
    return _as_lower_set(load_sql_rules().get("allowlist", {}).get("allowed_tables", []))


def get_allowed_columns() -> dict[str, set[str]]:
    raw = load_sql_rules().get("allowlist", {}).get("allowed_columns", {})
    if not isinstance(raw, dict):
        return {}
    return {
        str(table).strip().lower(): _as_lower_set(columns)
        for table, columns in raw.items()
        if str(table).strip()
    }


def get_forbidden_patterns() -> list[str]:
    return _as_string_list(load_sql_rules().get("allowlist", {}).get("forbidden_patterns", []))


def get_sql_guard_default_limit() -> int:
    value = load_sql_rules().get("guard", {}).get("default_enforced_limit", 50)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 50


def get_column_fixes() -> dict[str, str]:
    return _as_string_map(load_sql_rules().get("guard", {}).get("column_fixes", {}))


def get_table_fixes() -> dict[str, str]:
    return _as_string_map(load_sql_rules().get("guard", {}).get("table_fixes", {}))


def get_invalid_columns() -> set[str]:
    return _as_lower_set(load_sql_rules().get("guard", {}).get("invalid_columns", []))


def get_finance_only_columns() -> set[str]:
    return _as_lower_set(load_sql_rules().get("guard", {}).get("finance_only_columns", []))


def get_ops_only_columns() -> set[str]:
    return _as_lower_set(load_sql_rules().get("guard", {}).get("ops_only_columns", []))


def get_jsonb_functions() -> set[str]:
    return _as_lower_set(load_sql_rules().get("guard", {}).get("jsonb_functions", []))


def get_sql_guard_table_domains() -> dict[str, str]:
    return _as_string_map(load_sql_rules().get("guard", {}).get("table_domains", {}))


def get_sql_guard_rewrite_patterns() -> list[dict[str, str]]:
    values = load_sql_rules().get("guard", {}).get("rewrite_patterns", {})
    if not isinstance(values, dict):
        return []
    patterns: list[dict[str, str]] = []
    for config in values.values():
        if not isinstance(config, dict):
            continue
        pattern = str(config.get("pattern") or "")
        replacement = str(config.get("replacement") or "")
        if pattern:
            patterns.append({"pattern": pattern, "replacement": replacement})
    return patterns


def get_sql_guard_invalid_column_message() -> str:
    value = load_sql_rules().get("guard", {}).get(
        "invalid_column_message",
        "Column '{column}' does not exist. Use grades_json / ports_json instead.",
    )
    return str(value)


def _generator_rules() -> dict[str, Any]:
    values = load_sql_rules().get("generator", {})
    return values if isinstance(values, dict) else {}


def get_sql_generator_default_limit() -> int:
    value = _generator_rules().get("default_limit", 25)
    try:
        return int(value)
    except (TypeError, ValueError):
        return 25


def get_sql_generator_default_intent_key() -> str:
    return str(_generator_rules().get("default_intent_key") or "composite.query")


def get_sql_generator_empty_sql() -> str:
    return str(_generator_rules().get("empty_sql") or "SELECT 1 WHERE 1=0")


def get_sql_generator_retryable_pg_errors() -> tuple[str, ...]:
    return tuple(_as_string_list(_generator_rules().get("retryable_pg_errors", [])))


def get_sql_generator_hardcoded_limit_pattern() -> str:
    return str(_generator_rules().get("hardcoded_limit_pattern") or r"\blimit\s+\d+")


def get_sql_generator_hardcoded_string_pattern() -> str:
    return str(
        _generator_rules().get("hardcoded_string_pattern")
        or r"(?:=|>=|<=|>|<|ilike|like|in\s*\()\s*'[^']{1,120}'"
    )


def get_sql_generator_allowed_literals() -> set[str]:
    values = _generator_rules().get("allowed_literals", [])
    if not isinstance(values, list):
        return {".0", "", " "}
    return {str(value) for value in values}


def get_sql_generator_agent_table_scopes() -> dict[str, list[str]]:
    values = _generator_rules().get("agent_table_scopes", {})
    if not isinstance(values, dict):
        return {}
    return {
        str(agent).strip().lower(): _as_string_list(tables)
        for agent, tables in values.items()
        if str(agent).strip()
    }


def get_sql_generator_join_hints() -> list[dict[str, Any]]:
    values = _generator_rules().get("join_hints", [])
    if not isinstance(values, list):
        return []
    return [dict(value) for value in values if isinstance(value, dict)]


def get_sql_generator_param_conventions() -> dict[str, str]:
    values = _generator_rules().get("param_conventions", {})
    return _as_string_map(values)


def get_sql_generator_constraints() -> dict[str, Any]:
    values = _generator_rules().get("constraints", {})
    return dict(values) if isinstance(values, dict) else {}


def get_sql_generator_composite_query_nature() -> str:
    return str(_generator_rules().get("composite_query_nature") or "")


def get_sql_generator_error_retry_suffix() -> str:
    return str(_generator_rules().get("error_retry_suffix") or "")


def get_sql_generator_validation_messages() -> dict[str, str]:
    values = _generator_rules().get("validation_messages", {})
    return _as_string_map(values)


def get_sql_generator_forbidden_columns() -> list[dict[str, str]]:
    values = _generator_rules().get("forbidden_columns", [])
    if not isinstance(values, list):
        return []
    result: list[dict[str, str]] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        column = str(item.get("column") or "").strip().lower()
        reason = str(item.get("reason") or "").strip()
        if column:
            result.append({"column": column, "reason": reason})
    return result


def get_sql_generator_required_param_slots() -> dict[str, dict[str, Any]]:
    values = _generator_rules().get("required_param_slots", {})
    if not isinstance(values, dict):
        return {}
    return {
        str(placeholder): dict(config)
        for placeholder, config in values.items()
        if isinstance(config, dict)
    }


def get_sql_generator_optional_placeholder_slots() -> dict[str, str]:
    values = _generator_rules().get("optional_placeholder_slots", {})
    if not isinstance(values, dict):
        return {}
    return {
        str(placeholder): str(slot_key)
        for placeholder, slot_key in values.items()
        if str(placeholder).strip() and str(slot_key).strip()
    }


def invalidate_cache() -> None:
    load_sql_rules.cache_clear()
