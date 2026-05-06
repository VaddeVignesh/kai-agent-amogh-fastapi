from app.registries.intent_loader import (
    compare_yaml_to_python_registry,
    get_yaml_registry_facade,
    resolve_intent_from_yaml,
)
from app.registries.intent_registry import (
    INTENT_REGISTRY,
    SUPPORTED_INTENTS,
    resolve_intent,
)


def test_yaml_intent_registry_matches_python_registry():
    result = compare_yaml_to_python_registry()
    assert result["supported_intents_match"]
    assert result["registry_keys_match"]
    assert result["aliases_match"]
    assert result["mismatched_intents"] == []
    assert result["python_registry_count"] == 0


def test_yaml_intent_facade_matches_python_shape():
    facade = get_yaml_registry_facade(validate_parity=True)
    assert "voyage.summary" in facade["SUPPORTED_INTENTS"]
    assert facade["INTENT_REGISTRY"]["voyage.summary"]["route"] == "single"
    assert facade["resolve_intent"]("analysis.highrevenuelowpnl") == "analysis.high_revenue_low_pnl"


def test_deprecated_python_registry_facade_is_yaml_backed():
    facade = get_yaml_registry_facade(validate_parity=True)
    assert SUPPORTED_INTENTS == facade["SUPPORTED_INTENTS"]
    assert INTENT_REGISTRY == facade["INTENT_REGISTRY"]
    assert resolve_intent("analysis.highrevenuelowpnl") == facade["resolve_intent"]("analysis.highrevenuelowpnl")


def test_yaml_resolver_matches_python_resolver():
    cases = [
        "",
        "voyage.summary",
        "analysis.highrevenuelowpnl",
        "analysis.revenueVsPnl",
        "ops.delayed_voyages",
        "unknown.intent",
    ]
    for intent_key in cases:
        assert resolve_intent_from_yaml(intent_key) == resolve_intent(intent_key)
