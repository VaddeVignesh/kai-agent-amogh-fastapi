from __future__ import annotations

from functools import lru_cache
from typing import Any

from app.config.schema_loader import _read_yaml


@lru_cache(maxsize=1)
def load_response_rules() -> dict[str, Any]:
    return _read_yaml("response_rules.yaml")


def _compact_payload_rules() -> dict[str, Any]:
    rules = load_response_rules().get("compact_payload", {})
    return rules if isinstance(rules, dict) else {}


def _router_fallback_templates() -> dict[str, Any]:
    rules = load_response_rules().get("router_fallback_templates", {})
    return rules if isinstance(rules, dict) else {}


def _result_set_response_templates() -> dict[str, Any]:
    rules = load_response_rules().get("result_set_response_templates", {})
    return rules if isinstance(rules, dict) else {}


def _get_int(section: str, key: str, default: int) -> int:
    values = _compact_payload_rules().get(section, {})
    if not isinstance(values, dict):
        return default
    try:
        return int(values.get(key, default))
    except (TypeError, ValueError):
        return default


def _get_string(section: str, key: str, default: str) -> str:
    values = _compact_payload_rules().get(section, {})
    if not isinstance(values, dict):
        return default
    value = values.get(key, default)
    return str(value if value is not None else default)


def get_compact_raw_section_row_limit() -> int:
    return _get_int("row_limits", "raw_section_rows", 50)


def get_compact_merged_rows_limit() -> int:
    return _get_int("row_limits", "merged_rows", 50)


def get_compact_voyage_ids_limit() -> int:
    return _get_int("row_limits", "voyage_ids", 50)


def get_compact_finance_sample_rows_when_joined() -> int:
    return _get_int("row_limits", "finance_sample_rows_when_joined", 5)


def get_compact_key_ports_limit() -> int:
    return _get_int("list_limits", "key_ports", 10)


def get_compact_cargo_grades_limit() -> int:
    return _get_int("list_limits", "cargo_grades", 10)


def get_compact_remarks_limit() -> int:
    return _get_int("list_limits", "remarks", 5)


def get_unknown_vessel_label() -> str:
    return _get_string("display", "unknown_vessel_label", "Unknown Vessel")


def get_imo_prefix() -> str:
    return _get_string("display", "imo_prefix", "IMO:")


def get_null_equivalent_grade_values() -> set[str]:
    values = _compact_payload_rules().get("null_equivalent_grade_values", [])
    if not isinstance(values, list):
        return {"none", "null", "n/a", "na"}
    normalized = {str(value).strip().lower() for value in values if str(value).strip()}
    return normalized or {"none", "null", "n/a", "na"}


def get_router_fallback_template(name: str) -> str:
    value = _router_fallback_templates().get(name, "")
    return str(value or "")


def get_result_set_response_template(name: str) -> str:
    value = _result_set_response_templates().get(name, "")
    return str(value or "")


def invalidate_cache() -> None:
    load_response_rules.cache_clear()
