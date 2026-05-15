from __future__ import annotations

from typing import Any

from app.config.business_rules_loader import get_reconciliation_rules


def reconcile_merged_row(row: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(row, dict):
        return row

    enriched = dict(row)
    result = reconcile_sources(enriched)
    if result:
        enriched["source_reconciliation"] = result
    return enriched


def reconcile_sources(row: dict[str, Any]) -> dict[str, Any]:
    rules = get_reconciliation_rules()
    primary_fields = _string_list(rules.get("primary_identity_fields"))
    fallback_fields = _string_list(rules.get("fallback_identity_fields"))
    source_sections = _string_list(rules.get("source_sections"))
    status_labels = rules.get("status_labels", {}) if isinstance(rules.get("status_labels"), dict) else {}
    severity_rules = rules.get("severity", {}) if isinstance(rules.get("severity"), dict) else {}
    caveat_rules = rules.get("caveats", {}) if isinstance(rules.get("caveats"), dict) else {}
    canonical_field_sources = (
        rules.get("canonical_field_sources", {})
        if isinstance(rules.get("canonical_field_sources"), dict)
        else {}
    )

    source_values = _collect_source_values(row, source_sections, primary_fields + fallback_fields)
    mismatches: list[dict[str, Any]] = []
    matched: list[str] = []
    missing: list[str] = []
    canonical_fields: dict[str, Any] = {}

    for field in primary_fields + fallback_fields:
        values = source_values.get(field, {})
        canonical_value = _select_canonical_value(
            field,
            values,
            _string_list(canonical_field_sources.get(field)),
        )
        if canonical_value not in (None, "", [], {}):
            canonical_fields[field] = canonical_value
        normalized = {
            source: _normalize(field, value)
            for source, value in values.items()
            if value not in (None, "", [], {})
        }
        if len(normalized) < 2:
            if len(normalized) == 1:
                missing.append(field)
            continue
        unique_values = {value for value in normalized.values() if value}
        if len(unique_values) > 1:
            mismatches.append({"field": field, "values": normalized})
        else:
            matched.append(field)

    if mismatches:
        status = str(status_labels.get("mismatch") or "mismatch")
    elif matched:
        status = str(status_labels.get("aligned") or "aligned")
    else:
        status = str(status_labels.get("partial") or "partial")

    severity = str(severity_rules.get(status) or severity_rules.get(_status_key(status, status_labels)) or "warning")
    caveat = str(caveat_rules.get(status) or caveat_rules.get(_status_key(status, status_labels)) or "").strip()

    return {
        "status": status,
        "severity": severity,
        "canonical_fields": canonical_fields,
        "caveats": [caveat] if caveat else [],
        "matched_fields": matched,
        "missing_or_single_source_fields": missing,
        "mismatches": mismatches,
    }


def _collect_source_values(
    row: dict[str, Any],
    source_sections: list[str],
    fields: list[str],
) -> dict[str, dict[str, Any]]:
    values: dict[str, dict[str, Any]] = {field: {} for field in fields}
    for field in fields:
        if row.get(field) not in (None, "", [], {}):
            values[field]["merged"] = row.get(field)

    for source in source_sections:
        section = row.get(source)
        sections = section if isinstance(section, list) else [section]
        for idx, item in enumerate(sections):
            if not isinstance(item, dict):
                continue
            source_name = source if len(sections) == 1 else f"{source}_{idx + 1}"
            for field in fields:
                value = item.get(field)
                if value in (None, "", [], {}):
                    continue
                values[field][source_name] = value
    return values


def _select_canonical_value(field: str, values: dict[str, Any], preferred_sources: list[str]) -> Any:
    for source in preferred_sources:
        if source in values and values[source] not in (None, "", [], {}):
            return values[source]
    for source, value in values.items():
        if value not in (None, "", [], {}):
            return value
    return None


def _status_key(status: str, status_labels: dict[str, Any]) -> str:
    for key, label in status_labels.items():
        if str(label) == status:
            return str(key)
    return status


def _normalize(field: str, value: Any) -> str:
    text = str(value or "").strip()
    if field in {"vessel_imo", "imo"}:
        try:
            return str(int(float(text)))
        except (TypeError, ValueError):
            pass
        digits = "".join(ch for ch in text if ch.isdigit())
        return digits or text.casefold()
    return " ".join(text.casefold().split())


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]
