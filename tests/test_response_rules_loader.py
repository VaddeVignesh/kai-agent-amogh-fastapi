from app.config.response_rules_loader import (
    get_compact_cargo_grades_limit,
    get_compact_finance_sample_rows_when_joined,
    get_compact_key_ports_limit,
    get_compact_merged_rows_limit,
    get_compact_raw_section_row_limit,
    get_compact_remarks_limit,
    get_compact_voyage_ids_limit,
    get_imo_prefix,
    get_null_equivalent_grade_values,
    get_result_set_response_template,
    get_router_fallback_template,
    get_unknown_vessel_label,
)


def test_response_compact_payload_rules_load_from_yaml() -> None:
    assert get_compact_raw_section_row_limit() == 50
    assert get_compact_merged_rows_limit() == 50
    assert get_compact_voyage_ids_limit() == 50
    assert get_compact_finance_sample_rows_when_joined() == 5
    assert get_compact_key_ports_limit() == 10
    assert get_compact_cargo_grades_limit() == 10
    assert get_compact_remarks_limit() == 5


def test_response_display_rules_load_from_yaml() -> None:
    assert get_unknown_vessel_label() == "Unknown Vessel"
    assert get_imo_prefix() == "IMO:"
    assert {"none", "null", "n/a", "na"}.issubset(get_null_equivalent_grade_values())


def test_router_fallback_templates_load_from_yaml() -> None:
    assert get_router_fallback_template("no_data_available") == "Not available in dataset."
    backend_template = get_router_fallback_template("backend_unavailable_generic")
    assert "{voyage_ref}" in backend_template
    assert "POSTGRES_DSN" in backend_template

    mismatch_template = get_router_fallback_template("finance_identity_mismatch")
    assert "{candidate_preview}" in mismatch_template
    assert "different vessel identities" in mismatch_template

    not_found = get_router_fallback_template("voyage_reference_ambiguous_or_not_found")
    assert "{voyage_ref}" in not_found
    assert "full identifier" in not_found.lower()

    assert "Voyage metadata is available" in get_router_fallback_template("voyage_metadata_formatting_failed")


def test_result_set_response_templates_load_from_yaml() -> None:
    assert get_result_set_response_template("not_available") == "Not available"
    assert get_result_set_response_template("no_remarks_recorded") == "No remarks recorded."
    assert "{field}" in get_result_set_response_template("no_rows_field_populated")
    assert "{label}" in get_result_set_response_template("remarks_for_label_heading")
    assert "current result set" in get_result_set_response_template("key_ports_current_result_set_heading")
