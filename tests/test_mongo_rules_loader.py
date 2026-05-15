from app.config.mongo_rules_loader import (
    get_mongo_agent_output_fields,
    get_mongo_agent_scoring,
    get_mongo_allowed_collections,
    get_mongo_allowed_operators,
    get_mongo_guard_default_limit,
    get_mongo_guard_max_limit,
    get_mongo_limit,
    get_mongo_projection,
    get_mongo_schema_hint,
)
from app.orchestration.mongo_schema import mongo_schema_hint


def test_mongo_schema_hint_loads_from_yaml() -> None:
    hint = mongo_schema_hint()
    assert hint == get_mongo_schema_hint()
    assert "vessels" in hint["collections"]
    assert "voyages" in hint["collections"]
    assert "voyageNumber" in hint["collections"]["voyages"]["fields"]
    assert "$regex" in hint["allowed_operators"]


def test_mongo_guard_rules_load_from_yaml() -> None:
    assert get_mongo_allowed_collections() == {"vessels", "voyages"}
    assert "$elemMatch" in get_mongo_allowed_operators()
    assert get_mongo_guard_default_limit() == 10
    assert get_mongo_guard_max_limit() == 50


def test_mongo_projection_and_limits_load_from_yaml() -> None:
    full_context = get_mongo_projection("full_voyage_context")
    assert full_context["voyageId"] == 1
    assert full_context["vesselImo"] == 1
    assert full_context["imo"] == 1
    assert full_context["remarkList"] == 1
    assert get_mongo_projection("minimal_document")["_id"] == 0
    assert get_mongo_projection("voyage_metadata_context")["projected_results"] == 1
    assert get_mongo_projection("voyage_metadata_detail")["extracted_at"] == 1
    assert get_mongo_projection("voyage_identity")["vesselImo"] == 1
    assert get_mongo_projection("single_path_mongo_payload")["fixtures"] == 1
    assert get_mongo_projection("voyage_id_lookup")["voyageId"] == 1
    assert get_mongo_limit("full_voyage_context_batch", 0) == 40
    assert get_mongo_limit("fleet_vessel_list", 0) == 200
    assert get_mongo_limit("voyage_metadata_context_batch", 0) == 40
    assert get_mongo_limit("voyage_metadata_detail_batch", 0) == 40
    assert get_mongo_limit("voyage_identity_batch", 0) == 40


def test_mongo_agent_policy_loads_from_yaml() -> None:
    output_fields = get_mongo_agent_output_fields("full_voyage_context_output_fields")
    scoring = get_mongo_agent_scoring("full_voyage_context_scoring")

    assert output_fields["voyage_id"] == "voyageId"
    assert output_fields["vessel_imo"]["first_of"] == ["vesselImo", "vessel_imo", "imo"]
    assert output_fields["remarks"]["first_of"] == ["remarks", "remarkList"]
    assert scoring["recency_fields"] == ["startDateUtc", "extracted_at"]
    assert scoring["id_field"] == "voyageId"
