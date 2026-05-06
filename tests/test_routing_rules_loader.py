from app.config.routing_rules_loader import (
    get_cargo_grade_terms,
    get_cargo_grade_voyage_terms,
    get_concise_followup_keywords,
    get_comparison_followup_terms,
    get_coreference_words,
    get_corpus_guard_result_set_reference_phrases,
    get_direct_thread_reference_phrases,
    get_first_selector_phrases,
    get_generic_followup_markers,
    get_fresh_ranking_pnl_terms,
    get_incomplete_entity_question_markers,
    get_last_selector_phrases,
    get_long_new_question_prefixes,
    get_llm_answer_financial_first_terms,
    get_llm_answer_grade_terms,
    get_llm_answer_narrative_triggers,
    get_llm_answer_port_terms,
    get_llm_answer_remark_terms,
    get_llm_average_voyage_duration_terms,
    get_llm_cargo_aggregate_terms,
    get_llm_cargo_frequency_terms,
    get_llm_cargo_negative_profit_terms,
    get_llm_cargo_non_primary_subject_terms,
    get_llm_cargo_profitability_metric_terms,
    get_llm_cargo_profitability_terms,
    get_llm_cargo_subject_terms,
    get_llm_comparison_terms,
    get_llm_delay_ranking_terms,
    get_llm_delay_terms,
    get_llm_emissions_ranking_terms,
    get_llm_emissions_terms,
    get_llm_entity_summary_terms,
    get_llm_fleet_vessel_terms,
    get_llm_generic_agg_terms,
    get_llm_metadata_fleet_wide_markers,
    get_llm_metadata_keywords,
    get_llm_mongo_only_voyage_fields,
    get_llm_out_of_scope_greeting_exact,
    get_llm_out_of_scope_greeting_prefixes,
    get_llm_out_of_scope_identity_phrases,
    get_llm_out_of_scope_weather_keywords,
    get_llm_per_vessel_terms,
    get_llm_port_broad_visit_terms,
    get_llm_port_call_ranking_terms,
    get_llm_port_call_terms,
    get_llm_port_fleet_signals,
    get_llm_profit_metric_terms,
    get_llm_ranking_order_terms,
    get_llm_scenario_comparison_terms,
    get_llm_vessel_fleet_exclusion_terms,
    get_llm_vessel_metadata_agg_terms,
    get_llm_vessel_metric_signals,
    get_llm_vessel_performance_terms,
    get_llm_vessel_ranking_signals,
    get_llm_voyage_count_per_vessel_terms,
    get_llm_voyage_extreme_phrases,
    get_llm_voyage_performance_phrases,
    get_llm_voyage_port_listing_terms,
    get_llm_voyage_profitability_phrases,
    get_llm_voyage_resolved_vessel_metadata_terms,
    get_fresh_ranking_phrases,
    get_fresh_ranking_trend_context_words,
    get_fresh_ranking_trend_words,
    get_operational_followup_keywords,
    get_planner_default_primary_source,
    get_planner_entity_to_fleet_intent_map,
    get_planner_port_rankingish_terms,
    get_planner_rankingish_terms,
    get_planner_single_vessel_composite_terms,
    get_planner_text_composite_overrides,
    get_planner_voyage_metadata_terms,
    get_polite_followup_prefixes,
    get_proper_name_request_prefixes,
    get_result_set_explain_verbs,
    get_result_set_extreme_keywords,
    get_result_set_fresh_start_prefixes,
    get_result_set_operation_fields,
    get_result_set_operation_verbs,
    get_result_set_corpus_followup_exclusion_terms,
    get_result_set_question_prefixes,
    get_result_set_extreme_metric_aliases,
    get_result_set_list_fields_context_terms,
    get_result_set_list_fields_extra_exclusion_terms,
    get_result_set_low_extreme_terms,
    get_result_set_metric_followup_exclusion_terms,
    get_result_set_projection_field_terms,
    get_result_set_refinement_phrases,
    get_result_set_refinement_extreme_terms,
    get_result_set_scope_phrases,
    get_result_set_selected_metric_aliases,
    get_result_set_selected_metric_context_terms,
    get_result_set_selected_metric_triggers,
    get_result_set_short_projection_field_terms,
    get_result_set_stale_delay_guard_terms,
    get_same_entity_reference_words,
    get_selected_row_reference_phrases,
    get_session_thread_context_markers,
    get_short_contextual_followup_fields,
    get_singular_selection_markers,
    get_metric_followup_override_actions,
    get_structured_followup_operations,
    get_structured_followup_scopes,
    get_thread_override_explicit_entity_markers,
    get_thread_override_fleet_switch_markers,
    get_thread_override_ranking_terms,
    get_thread_override_referential_markers,
    get_thread_override_vessel_keywords,
    get_thread_override_vessel_metadata_terms,
    get_thread_override_voyage_keywords,
    get_thread_override_voyage_metadata_terms,
    get_fresh_ranking_segment_terms,
    get_fresh_ranking_trend_terms,
    get_incomplete_entity_detail_markers,
    get_incomplete_entity_fastpath_queries,
    get_incomplete_entity_fleet_port_aggregate_markers,
    get_incomplete_entity_placeholder_slot_values,
    get_incomplete_entity_topic_phrases,
    get_incomplete_entity_topic_terms,
    get_value_like_fresh_words,
)


def test_result_set_followup_phrase_groups_load_from_yaml() -> None:
    assert "same vessel" in get_session_thread_context_markers()
    assert "explain" in get_result_set_explain_verbs()
    assert "show me" in get_result_set_fresh_start_prefixes()
    assert "highest" in get_result_set_extreme_keywords()
    assert "among these" in get_result_set_scope_phrases()
    assert "which was" in get_result_set_question_prefixes()
    assert "can you " in get_polite_followup_prefixes()
    assert "cargo grade" in get_concise_followup_keywords()
    assert "best voyage" in get_result_set_refinement_phrases()


def test_phase5d_routing_phrase_groups_load_from_yaml() -> None:
    assert "most profitable" in get_fresh_ranking_phrases()
    assert get_fresh_ranking_trend_words() == ["trend"]
    assert "average" in get_fresh_ranking_trend_context_words()
    assert "operational" in get_operational_followup_keywords()
    assert "same" in get_same_entity_reference_words()
    assert "cargo grade" in get_cargo_grade_terms()
    assert "voyages" in get_cargo_grade_voyage_terms()


def test_planner_routing_policy_loads_from_yaml() -> None:
    assert get_planner_default_primary_source() == "postgres"
    assert get_planner_entity_to_fleet_intent_map()["vessel.summary"] == "ranking.vessels"
    assert "include" in get_planner_rankingish_terms()
    assert "variance" in get_planner_port_rankingish_terms()
    assert "fixture" in get_planner_voyage_metadata_terms()
    assert "ports visited" in get_planner_single_vessel_composite_terms()

    overrides = get_planner_text_composite_overrides()
    names = {override["name"] for override in overrides}
    assert "text_offhire_composite" in names
    assert any(override["confidence"] == 0.92 for override in overrides)


def test_phase6g_followup_phrase_groups_load_from_yaml() -> None:
    assert "tell me" in get_incomplete_entity_question_markers()
    assert "tell me about" in get_incomplete_entity_detail_markers()
    assert "port" in get_incomplete_entity_topic_terms()["port"]
    assert "tell me about voyage" in get_incomplete_entity_topic_phrases()["voyage"]
    assert "most visited" in get_incomplete_entity_fleet_port_aggregate_markers()
    assert "port" in get_incomplete_entity_placeholder_slot_values()["vessel_name"]
    assert "tell me about port" in get_incomplete_entity_fastpath_queries()["port.details"]
    assert "show me " in get_proper_name_request_prefixes()
    assert "compare" in get_value_like_fresh_words()
    assert "which " in get_long_new_question_prefixes()
    assert "what about" in get_generic_followup_markers()
    assert "these" in get_coreference_words()
    assert "top" in get_result_set_refinement_extreme_terms()
    assert "filter" in get_result_set_operation_verbs()
    assert "commission" in get_result_set_operation_fields()
    assert "key ports" in get_short_contextual_followup_fields()
    assert "this vessel" in get_direct_thread_reference_phrases()
    assert "above list" in get_corpus_guard_result_set_reference_phrases()
    assert "best voyage" in get_singular_selection_markers()
    assert "that voyage" in get_selected_row_reference_phrases()
    assert "top result" in get_first_selector_phrases()
    assert "worst one" in get_last_selector_phrases()


def test_phase6i_result_set_metric_and_field_groups_load_from_yaml() -> None:
    projection_fields = get_result_set_projection_field_terms()
    assert "remark" in projection_fields["remarks"]
    assert "cargo grade" in projection_fields["cargo_grades"]
    assert "key ports" in projection_fields["key_ports"]

    short_projection_fields = get_result_set_short_projection_field_terms()
    assert "grade" in short_projection_fields["cargo_grades"]

    extreme_aliases = get_result_set_extreme_metric_aliases()
    assert "port calls" in extreme_aliases["port_calls"]
    assert "cost" in extreme_aliases["total_expense"]
    assert "best voyage" in extreme_aliases["pnl"]

    selected_aliases = get_result_set_selected_metric_aliases()
    assert "average revenue" in selected_aliases["avg_revenue"]
    assert "commission" in selected_aliases["total_commission"]
    assert "vessel name" in selected_aliases["vessel_name"]

    assert "bottom" in get_result_set_low_extreme_terms()
    assert "show" in get_result_set_selected_metric_triggers()
    assert "what about" in get_result_set_selected_metric_context_terms()
    assert "highest" in get_result_set_metric_followup_exclusion_terms()
    assert "for each" in get_result_set_corpus_followup_exclusion_terms()
    assert "for them" in get_result_set_list_fields_context_terms()
    assert "variance" in get_result_set_list_fields_extra_exclusion_terms()


def test_phase6i2_thread_override_groups_load_from_yaml() -> None:
    assert "across all" in get_thread_override_fleet_switch_markers()
    assert "variance" in get_thread_override_ranking_terms()
    assert "for this voyage" in get_thread_override_explicit_entity_markers()
    assert "previously" in get_thread_override_referential_markers()
    assert "hire rate" in get_thread_override_vessel_keywords()
    assert "charterer" in get_thread_override_voyage_keywords()
    assert "who added" in get_thread_override_voyage_metadata_terms()
    assert "defaults" in get_thread_override_vessel_metadata_terms()


def test_phase6i3_remaining_graph_router_guard_groups_load_from_yaml() -> None:
    assert "versus" in get_comparison_followup_terms()

    stale_delay_guard = get_result_set_stale_delay_guard_terms()
    assert "delayed" in stale_delay_guard["all"]
    assert "pnl" in stale_delay_guard["any"]
    assert "voyages flagged" in stale_delay_guard["scope"]

    assert "followup" in get_structured_followup_scopes()
    assert "follow_up_filter" in get_structured_followup_operations()
    assert "project_extreme_field" in get_metric_followup_override_actions()
    assert "most profitable" in get_fresh_ranking_pnl_terms()
    assert "loss making" in get_fresh_ranking_segment_terms()
    assert "trend" in get_fresh_ranking_trend_terms()


def test_phase6h_llm_client_keyword_groups_load_from_yaml() -> None:
    assert "charterer" in get_llm_mongo_only_voyage_fields()
    assert "voyage metadata" in get_llm_mongo_only_voyage_fields()
    assert "hello" in get_llm_out_of_scope_greeting_exact()
    assert "hi " in get_llm_out_of_scope_greeting_prefixes()
    assert "what can you do" in get_llm_out_of_scope_identity_phrases()
    assert "forecast" in get_llm_out_of_scope_weather_keywords()
    assert "root cause" in get_llm_answer_narrative_triggers()
    assert "commission" in get_llm_answer_financial_first_terms()
    assert "route" in get_llm_answer_port_terms()
    assert "cargo" in get_llm_answer_grade_terms()
    assert "delay" in get_llm_answer_remark_terms()


def test_phase6h2_llm_deterministic_intent_groups_load_from_yaml() -> None:
    assert "trend" in get_llm_vessel_performance_terms()
    assert "operating status" in get_llm_voyage_resolved_vessel_metadata_terms()
    assert "average voyage duration" in get_llm_average_voyage_duration_terms()
    assert "per vessel" in get_llm_per_vessel_terms()
    assert "cargo grade" in get_llm_cargo_subject_terms()
    assert "profitable" in get_llm_cargo_aggregate_terms()
    assert "most common" in get_llm_cargo_frequency_terms()
    assert "module types" in get_llm_cargo_non_primary_subject_terms()
    assert "average pnl" in get_llm_cargo_profitability_metric_terms()
    assert "profit" in get_llm_cargo_profitability_terms()
    assert "loss-making" in get_llm_cargo_negative_profit_terms()
    assert "when fixed" in get_llm_scenario_comparison_terms()
    assert "which is better" in get_llm_comparison_terms()
    assert "ports visited" in get_llm_voyage_port_listing_terms()
    assert "tell me about" in get_llm_entity_summary_terms()
    assert "which vessel" in get_llm_vessel_fleet_exclusion_terms()
    assert "top vessel" in get_llm_vessel_ranking_signals()
    assert "voyage count" in get_llm_vessel_metric_signals()
    assert "top performing voyages" in get_llm_voyage_profitability_phrases()
    assert "rank" in get_llm_ranking_order_terms()
    assert "pnl" in get_llm_profit_metric_terms()
    assert "eeoi" in get_llm_emissions_terms()
    assert "lowest" in get_llm_emissions_ranking_terms()
    assert "least profitable voyage" in get_llm_voyage_performance_phrases()
    assert "worst voyage" in get_llm_voyage_extreme_phrases()
    assert "port calls" in get_llm_port_call_terms()
    assert "visited" in get_llm_port_call_ranking_terms()
    assert "busiest port" in get_llm_port_fleet_signals()
    assert "busy" in get_llm_port_broad_visit_terms()
    assert "how many voyage" in get_llm_voyage_count_per_vessel_terms()
    assert "expensive to operate" in get_llm_fleet_vessel_terms()
    assert "current contract" in get_llm_vessel_metadata_agg_terms()
    assert "passage type" in get_llm_metadata_keywords()
    assert "fleet-wide" in get_llm_metadata_fleet_wide_markers()
    assert "fastest" in get_llm_generic_agg_terms()
    assert "offhire" in get_llm_delay_terms()
    assert "biggest" in get_llm_delay_ranking_terms()
