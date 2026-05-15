from app.config.business_rules_loader import (
    get_answer_contract_sections,
    get_derived_metric_rules,
    get_reasoning_signal_rules,
)
from app.services.business_reasoning import enrich_row_with_business_reasoning


def test_business_rules_load_from_yaml() -> None:
    assert "margin" in get_derived_metric_rules()
    assert "cost_ratio" in get_derived_metric_rules()
    assert "inefficient_revenue" in get_reasoning_signal_rules()
    assert get_answer_contract_sections() == [
        "What happened",
        "Why it matters",
        "Business impact",
        "Data caveats",
    ]


def test_business_reasoning_derives_metrics_and_signals() -> None:
    row = {
        "revenue": 1000,
        "pnl": 50,
        "total_expense": 900,
        "total_commission": 25,
    }

    enriched = enrich_row_with_business_reasoning(row)

    assert enriched["margin"] == 0.05
    assert enriched["cost_ratio"] == 0.9
    assert enriched["commission_ratio"] == 0.025
    assert enriched["business_reasoning"]["derived_metrics"]["margin"] == 0.05
    assert {
        signal["name"]
        for signal in enriched["business_reasoning"]["signals"]
    } == {"inefficient_revenue"}


def test_business_reasoning_is_null_safe() -> None:
    enriched = enrich_row_with_business_reasoning({"revenue": 0, "pnl": 100})

    assert enriched["margin"] is None
    assert "margin" in enriched["business_reasoning"]["unavailable_metrics"]


def test_business_reasoning_supports_grouped_boolean_and_field_conditions() -> None:
    enriched = enrich_row_with_business_reasoning(
        {
            "revenue": 1000,
            "pnl": 10,
            "total_expense": 1000,
            "total_commission": 10,
            "is_delayed": True,
            "delay_reason": "weather",
        }
    )

    signal_names = {
        signal["name"]
        for signal in enriched["business_reasoning"]["signals"]
    }
    assert "delay_exposure" in signal_names
    assert "profitable_but_operationally_risky" in signal_names
    assert "weak_business_quality" in signal_names
