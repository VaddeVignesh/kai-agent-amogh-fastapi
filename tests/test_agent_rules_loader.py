from app.config.agent_rules_loader import (
    get_finance_allowed_directions,
    get_finance_composite_allowed_extra_slots,
    get_finance_intent_metric_overrides,
    get_finance_max_limit,
    get_finance_ranking_default_direction,
    get_finance_ranking_default_metric,
    get_finance_repair_prompt,
    get_finance_repairable_error_patterns,
    get_finance_safe_metrics,
    get_finance_segment_performance_fallback_sql,
    get_finance_simple_intent_mappings,
    get_finance_validation_message,
    get_ops_cargo_grade_max_count,
    get_ops_cargo_profitability_intents,
    get_ops_canonical_sql,
    get_ops_delay_remark_keywords,
    get_ops_delay_remark_filter_empty,
    get_ops_delay_remark_filter_template,
    get_ops_max_limit,
    get_ops_simple_intent_mappings,
    get_ops_validation_message,
)


def test_finance_agent_rules_load_from_yaml() -> None:
    assert get_finance_max_limit() == 200
    assert "voyage_numbers" in get_finance_composite_allowed_extra_slots()
    assert get_finance_safe_metrics()["total_commission"] == "total_commission"
    assert get_finance_ranking_default_metric() == "pnl"
    assert get_finance_ranking_default_direction() == "desc"
    assert get_finance_allowed_directions() == {"asc", "desc"}
    assert get_finance_validation_message("voyage_summary_requires_reference") == (
        "voyage.summary requires voyage_number or voyage_id"
    )
    assert get_finance_validation_message("vessel_summary_requires_reference") == (
        "vessel.summary requires vessel_name or imo"
    )
    assert "syntax error" in get_finance_repairable_error_patterns()
    assert "finance_voyage_kpi" in get_finance_repair_prompt("finance_no_ops_join_initial")
    assert "LEFT JOIN ops_voyage_summary" in get_finance_segment_performance_fallback_sql("with_voyage_numbers")


def test_finance_mapping_rules_load_from_yaml() -> None:
    overrides = get_finance_intent_metric_overrides()
    assert overrides["ranking.voyages_by_pnl"] == "pnl"
    assert overrides["ranking.voyages_by_revenue"] == "revenue"
    assert overrides["ranking.voyages_by_commission"] == "total_commission"

    mappings = get_finance_simple_intent_mappings()
    assert mappings["analysis.high_revenue_low_pnl"]["query_key"] == "finance.high_revenue_low_pnl"
    assert mappings["ranking.vessels"]["query_key"] == "kpi.vessel_performance_summary"


def test_ops_agent_rules_load_from_yaml() -> None:
    assert get_ops_max_limit() == 200
    assert get_ops_cargo_grade_max_count() == 50
    assert get_ops_cargo_profitability_intents() == {
        "analysis.cargo_profitability",
        "analysis.cargoprofitability",
    }
    assert get_ops_delay_remark_keywords() == ["congest", "delay", "waiting", "queue"]
    assert get_ops_validation_message("voyage_summary_requires_reference") == (
        "voyage.summary requires voyage_number or voyage_id"
    )
    assert get_ops_validation_message("vessel_summary_requires_reference") == (
        "vessel.summary requires vessel_name or imo"
    )
    assert "FROM ops_voyage_summary" in get_ops_canonical_sql("voyage_ids_lookup")
    assert "{where}" in get_ops_canonical_sql("vessel_summary")
    assert get_ops_delay_remark_filter_template() == "lower(remark) LIKE '%%{keyword}%%'"
    assert get_ops_delay_remark_filter_empty() == "FALSE"


def test_ops_mapping_rules_load_from_yaml() -> None:
    mappings = get_ops_simple_intent_mappings()
    assert mappings["ops.delayed_voyages"]["query_key"] == "kpi.delayed_voyages_analysis"
    assert mappings["ops.voyages_by_port"]["required_slot"] == "port_name"
    assert mappings["ops.voyages_by_cargo_grade"]["required_slot"] == "cargo_grade"
    assert mappings["port.details"]["query_key"] == "kpi.port_performance_analysis"
