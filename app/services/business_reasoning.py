from __future__ import annotations

from typing import Any

from app.config.business_rules_loader import (
    get_derived_metric_rules,
    get_reasoning_signal_rules,
)


def enrich_row_with_business_reasoning(row: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(row, dict):
        return row

    enriched = dict(row)
    derived_metrics = _derive_metrics(enriched)
    enriched.update(derived_metrics)

    signals = _evaluate_signals(enriched)
    unavailable = [
        name
        for name, value in derived_metrics.items()
        if value in (None, "", [], {})
    ]
    if derived_metrics or signals or unavailable:
        enriched["business_reasoning"] = {
            "derived_metrics": {
                name: value
                for name, value in derived_metrics.items()
                if value not in (None, "", [], {})
            },
            "signals": signals,
            "unavailable_metrics": unavailable,
        }
    return enriched


def _derive_metrics(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for metric_name, rule in get_derived_metric_rules().items():
        numerator = _number(row.get(str(rule.get("numerator") or "")))
        denominator = _number(row.get(str(rule.get("denominator") or "")))
        output_key = str(rule.get("output_key") or metric_name).strip()
        if not output_key:
            continue
        if numerator is None or denominator in (None, 0):
            out[output_key] = None
            continue
        try:
            precision = int(rule.get("precision", 4))
        except (TypeError, ValueError):
            precision = 4
        out[output_key] = round(numerator / denominator, precision)
    return out


def _evaluate_signals(row: dict[str, Any]) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    for signal_name, rule in get_reasoning_signal_rules().items():
        conditions = rule.get("conditions", {})
        if conditions in (None, "", [], {}):
            continue
        if _conditions_match(row, conditions):
            signals.append(
                {
                    "name": signal_name,
                    "label": str(rule.get("label") or signal_name),
                    "interpretation": str(rule.get("interpretation") or ""),
                    "impact": str(rule.get("impact") or ""),
                }
            )
    return signals


def _conditions_match(row: dict[str, Any], conditions: Any) -> bool:
    if isinstance(conditions, list):
        return all(_conditions_match(row, item) for item in conditions)
    if not isinstance(conditions, dict):
        return False
    if isinstance(conditions.get("all"), list):
        return all(_conditions_match(row, item) for item in conditions.get("all", []))
    if isinstance(conditions.get("any"), list):
        return any(_conditions_match(row, item) for item in conditions.get("any", []))
    return _condition_matches(row, conditions)


def _condition_matches(row: dict[str, Any], condition: dict[str, Any]) -> bool:
    field = str(condition.get("field") or "").strip()
    op = str(condition.get("op") or "").strip().lower()
    raw_left = row.get(field)
    if op == "exists":
        return raw_left not in (None, "", [], {})
    if op == "missing":
        return raw_left in (None, "", [], {})
    if op == "is_true":
        return _truthy(raw_left)
    if op == "is_false":
        return not _truthy(raw_left)
    left = _number(raw_left)
    other_field = str(condition.get("other_field") or "").strip()
    right = _number(row.get(other_field)) if other_field else _number(condition.get("value"))
    if left is None or right is None:
        return False
    if op == "gt":
        return left > right
    if op == "gte":
        return left >= right
    if op == "lt":
        return left < right
    if op == "lte":
        return left <= right
    if op == "eq":
        return left == right
    if op == "neq":
        return left != right
    if op == "gt_field":
        return left > right
    if op == "gte_field":
        return left >= right
    if op == "lt_field":
        return left < right
    if op == "lte_field":
        return left <= right
    if op == "eq_field":
        return left == right
    if op == "neq_field":
        return left != right
    return False


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().casefold() in {"true", "yes", "y", "1"}
    return bool(value)


def _number(value: Any) -> float | None:
    if value in (None, "", [], {}):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
