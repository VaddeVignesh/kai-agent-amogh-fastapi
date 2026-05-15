import json
from pathlib import Path

from app.config.routing_rules_loader import (
    get_llm_metadata_override_blocking_metric_terms,
    get_llm_vessel_metadata_agg_terms,
)


def test_business_decision_golden_category_is_registered() -> None:
    suite = json.loads(Path("scripts/golden_config_suite.json").read_text(encoding="utf-8"))
    business_items = [
        item
        for item in suite["single_turn"]
        if item.get("category") == "business_decision"
    ]

    assert {item["id"] for item in business_items} == {
        "BIZ_001",
        "BIZ_002",
        "BIZ_003",
        "BIZ_004",
        "BIZ_005",
    }
    assert all(item.get("must_contain") for item in business_items)


def test_business_risk_terms_do_not_force_metadata_routing() -> None:
    metadata_terms = set(get_llm_vessel_metadata_agg_terms())

    assert "operational" not in metadata_terms
    assert "risk" in get_llm_metadata_override_blocking_metric_terms()
