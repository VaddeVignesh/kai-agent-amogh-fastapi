from __future__ import annotations

from functools import lru_cache

from app.config.schema_loader import _read_yaml


@lru_cache(maxsize=1)
def load_routing_rules() -> dict:
    return _read_yaml("routing_rules.yaml")


def _planner_source_keywords(source: str) -> frozenset[str]:
    rules = load_routing_rules()
    values = (
        rules.get("planner_source_keywords", {}).get(source, [])
        if isinstance(rules, dict)
        else []
    )
    if not isinstance(values, list):
        values = []
    return frozenset(str(value).strip().lower() for value in values if str(value).strip())


def get_planner_finance_keywords() -> frozenset[str]:
    return _planner_source_keywords("finance")


def get_planner_mongo_keywords() -> frozenset[str]:
    return _planner_source_keywords("mongo")


def get_planner_default_primary_source() -> str:
    rules = load_routing_rules()
    value = rules.get("planner_default_primary_source", "postgres") if isinstance(rules, dict) else "postgres"
    return str(value or "postgres").strip().lower()


def get_planner_entity_to_fleet_intent_map() -> dict[str, str]:
    rules = load_routing_rules()
    values = rules.get("planner_entity_to_fleet_intent_map", {}) if isinstance(rules, dict) else {}
    if not isinstance(values, dict):
        return {}
    return {
        str(intent_key).strip(): str(fleet_intent).strip()
        for intent_key, fleet_intent in values.items()
        if str(intent_key).strip() and str(fleet_intent).strip()
    }


def get_planner_rankingish_terms() -> list[str]:
    return _get_string_list("planner_rankingish_terms")


def get_planner_port_rankingish_terms() -> list[str]:
    return _get_string_list("planner_port_rankingish_terms")


def get_planner_voyage_metadata_terms() -> list[str]:
    return _get_string_list("planner_voyage_metadata_terms")


def get_planner_single_vessel_composite_terms() -> list[str]:
    return _get_string_list("planner_single_vessel_composite_terms")


def get_planner_text_composite_overrides() -> list[dict]:
    rules = load_routing_rules()
    values = rules.get("planner_text_composite_overrides", []) if isinstance(rules, dict) else []
    if not isinstance(values, list):
        return []
    normalized: list[dict] = []
    for value in values:
        if not isinstance(value, dict):
            continue
        normalized.append(
            {
                "name": str(value.get("name") or "").strip(),
                "all": [str(item).strip().lower() for item in (value.get("all") or []) if str(item).strip()],
                "any": [str(item).strip().lower() for item in (value.get("any") or []) if str(item).strip()],
                "confidence": value.get("confidence"),
            }
        )
    return normalized


def get_voyage_topic_keywords() -> dict[str, list[str]]:
    rules = load_routing_rules()
    values = rules.get("voyage_topic_keywords", {}) if isinstance(rules, dict) else {}
    if not isinstance(values, dict):
        return {}
    normalized: dict[str, list[str]] = {}
    for section, keywords in values.items():
        if not isinstance(keywords, list):
            continue
        normalized[str(section)] = [
            str(keyword).lower()
            for keyword in keywords
            if str(keyword)
        ]
    return normalized


def get_scenario_keyword_aliases() -> dict[str, str]:
    rules = load_routing_rules()
    values = rules.get("scenario_keyword_aliases", {}) if isinstance(rules, dict) else {}
    if not isinstance(values, dict):
        return {}
    return {
        str(keyword).lower(): str(scenario).upper()
        for keyword, scenario in values.items()
        if str(keyword).strip() and str(scenario).strip()
    }


def get_scenario_comparison_words() -> list[str]:
    rules = load_routing_rules()
    values = rules.get("scenario_comparison_words", []) if isinstance(rules, dict) else []
    if not isinstance(values, list):
        return []
    return [str(value).lower() for value in values if str(value)]


def _get_string_list(key: str) -> list[str]:
    rules = load_routing_rules()
    values = rules.get(key, []) if isinstance(rules, dict) else []
    if not isinstance(values, list):
        return []
    return [str(value).lower() for value in values if str(value)]


def _get_string_list_map(key: str) -> dict[str, list[str]]:
    rules = load_routing_rules()
    values = rules.get(key, {}) if isinstance(rules, dict) else {}
    if not isinstance(values, dict):
        return {}
    normalized: dict[str, list[str]] = {}
    for name, terms in values.items():
        if not isinstance(terms, list):
            continue
        clean_name = str(name).strip().lower()
        if not clean_name:
            continue
        normalized[clean_name] = [str(term).lower() for term in terms if str(term)]
    return normalized


def get_followup_backward_markers() -> list[str]:
    return _get_string_list("followup_backward_markers")


def get_older_vessel_markers() -> list[str]:
    return _get_string_list("older_vessel_markers")


def get_simple_voyage_analytical_markers() -> list[str]:
    return _get_string_list("simple_voyage_analytical_markers")


def get_vessel_mention_stop_first_words() -> frozenset[str]:
    return frozenset(_get_string_list("vessel_mention_stop_first_words"))


def get_vessel_mention_descriptor_suffixes() -> frozenset[str]:
    return frozenset(_get_string_list("vessel_mention_descriptor_suffixes"))


def get_vessel_mention_false_prefixes() -> frozenset[str]:
    return frozenset(value.upper() for value in _get_string_list("vessel_mention_false_prefixes"))


def get_fresh_fleet_markers() -> list[str]:
    return _get_string_list("fresh_fleet_markers")


def get_explicit_result_set_reference_phrases() -> list[str]:
    return _get_string_list("explicit_result_set_reference_phrases")


def get_new_request_prefixes() -> tuple[str, ...]:
    return tuple(_get_string_list("new_request_prefixes"))


def get_session_thread_context_markers() -> list[str]:
    return _get_string_list("session_thread_context_markers")


def get_fresh_ranking_phrases() -> list[str]:
    return _get_string_list("fresh_ranking_phrases")


def get_fresh_ranking_trend_words() -> list[str]:
    return _get_string_list("fresh_ranking_trend_words")


def get_fresh_ranking_trend_context_words() -> list[str]:
    return _get_string_list("fresh_ranking_trend_context_words")


def get_result_set_explain_verbs() -> tuple[str, ...]:
    return tuple(_get_string_list("result_set_explain_verbs"))


def get_result_set_fresh_start_prefixes() -> tuple[str, ...]:
    return tuple(_get_string_list("result_set_fresh_start_prefixes"))


def get_result_set_extreme_keywords() -> list[str]:
    return _get_string_list("result_set_extreme_keywords")


def get_result_set_scope_phrases() -> list[str]:
    return _get_string_list("result_set_scope_phrases")


def get_result_set_question_prefixes() -> tuple[str, ...]:
    return tuple(_get_string_list("result_set_question_prefixes"))


def get_polite_followup_prefixes() -> tuple[str, ...]:
    return tuple(_get_string_list("polite_followup_prefixes"))


def get_concise_followup_keywords() -> list[str]:
    return _get_string_list("concise_followup_keywords")


def get_result_set_refinement_phrases() -> list[str]:
    return _get_string_list("result_set_refinement_phrases")


def get_operational_followup_keywords() -> list[str]:
    return _get_string_list("operational_followup_keywords")


def get_same_entity_reference_words() -> list[str]:
    return _get_string_list("same_entity_reference_words")


def get_cargo_grade_terms() -> list[str]:
    return _get_string_list("cargo_grade_terms")


def get_cargo_grade_voyage_terms() -> list[str]:
    return _get_string_list("cargo_grade_voyage_terms")


def get_incomplete_entity_question_markers() -> list[str]:
    return _get_string_list("incomplete_entity_question_markers")


def get_incomplete_entity_detail_markers() -> list[str]:
    return _get_string_list("incomplete_entity_detail_markers")


def get_incomplete_entity_topic_terms() -> dict[str, list[str]]:
    return _get_string_list_map("incomplete_entity_topic_terms")


def get_incomplete_entity_topic_phrases() -> dict[str, list[str]]:
    return _get_string_list_map("incomplete_entity_topic_phrases")


def get_incomplete_entity_fleet_port_aggregate_markers() -> list[str]:
    return _get_string_list("incomplete_entity_fleet_port_aggregate_markers")


def get_incomplete_entity_placeholder_slot_values() -> dict[str, list[str]]:
    return _get_string_list_map("incomplete_entity_placeholder_slot_values")


def get_incomplete_entity_fastpath_queries() -> dict[str, list[str]]:
    return _get_string_list_map("incomplete_entity_fastpath_queries")


def get_proper_name_request_prefixes() -> tuple[str, ...]:
    return tuple(_get_string_list("proper_name_request_prefixes"))


def get_value_like_fresh_words() -> list[str]:
    return _get_string_list("value_like_fresh_words")


def get_long_new_question_prefixes() -> tuple[str, ...]:
    return tuple(_get_string_list("long_new_question_prefixes"))


def get_generic_followup_markers() -> list[str]:
    return _get_string_list("generic_followup_markers")


def get_coreference_words() -> list[str]:
    return _get_string_list("coreference_words")


def get_result_set_refinement_extreme_terms() -> list[str]:
    return _get_string_list("result_set_refinement_extreme_terms")


def get_result_set_operation_verbs() -> list[str]:
    return _get_string_list("result_set_operation_verbs")


def get_result_set_operation_fields() -> list[str]:
    return _get_string_list("result_set_operation_fields")


def get_short_contextual_followup_fields() -> list[str]:
    return _get_string_list("short_contextual_followup_fields")


def get_direct_thread_reference_phrases() -> list[str]:
    return _get_string_list("direct_thread_reference_phrases")


def get_corpus_guard_result_set_reference_phrases() -> list[str]:
    return _get_string_list("corpus_guard_result_set_reference_phrases")


def get_singular_selection_markers() -> list[str]:
    return _get_string_list("singular_selection_markers")


def get_selected_row_reference_phrases() -> list[str]:
    return _get_string_list("selected_row_reference_phrases")


def get_first_selector_phrases() -> list[str]:
    return _get_string_list("first_selector_phrases")


def get_last_selector_phrases() -> list[str]:
    return _get_string_list("last_selector_phrases")


def get_result_set_projection_field_terms() -> dict[str, list[str]]:
    return _get_string_list_map("result_set_projection_field_terms")


def get_result_set_short_projection_field_terms() -> dict[str, list[str]]:
    return _get_string_list_map("result_set_short_projection_field_terms")


def get_result_set_extreme_metric_aliases() -> dict[str, list[str]]:
    return _get_string_list_map("result_set_extreme_metric_aliases")


def get_result_set_low_extreme_terms() -> list[str]:
    return _get_string_list("result_set_low_extreme_terms")


def get_result_set_selected_metric_aliases() -> dict[str, list[str]]:
    return _get_string_list_map("result_set_selected_metric_aliases")


def get_result_set_selected_metric_triggers() -> list[str]:
    return _get_string_list("result_set_selected_metric_triggers")


def get_result_set_selected_metric_context_terms() -> list[str]:
    return _get_string_list("result_set_selected_metric_context_terms")


def get_result_set_metric_followup_exclusion_terms() -> list[str]:
    return _get_string_list("result_set_metric_followup_exclusion_terms")


def get_result_set_corpus_followup_exclusion_terms() -> list[str]:
    return _get_string_list("result_set_corpus_followup_exclusion_terms")


def get_result_set_list_fields_context_terms() -> list[str]:
    return _get_string_list("result_set_list_fields_context_terms")


def get_result_set_list_fields_extra_exclusion_terms() -> list[str]:
    return _get_string_list("result_set_list_fields_extra_exclusion_terms")


def get_thread_override_fleet_switch_markers() -> list[str]:
    return _get_string_list("thread_override_fleet_switch_markers")


def get_thread_override_ranking_terms() -> list[str]:
    return _get_string_list("thread_override_ranking_terms")


def get_thread_override_explicit_entity_markers() -> list[str]:
    return _get_string_list("thread_override_explicit_entity_markers")


def get_thread_override_referential_markers() -> list[str]:
    return _get_string_list("thread_override_referential_markers")


def get_thread_override_vessel_keywords() -> list[str]:
    return _get_string_list("thread_override_vessel_keywords")


def get_thread_override_voyage_keywords() -> list[str]:
    return _get_string_list("thread_override_voyage_keywords")


def get_thread_override_voyage_metadata_terms() -> list[str]:
    return _get_string_list("thread_override_voyage_metadata_terms")


def get_thread_override_vessel_metadata_terms() -> list[str]:
    return _get_string_list("thread_override_vessel_metadata_terms")


def get_comparison_followup_terms() -> list[str]:
    return _get_string_list("comparison_followup_terms")


def get_result_set_stale_delay_guard_terms() -> dict[str, list[str]]:
    return _get_string_list_map("result_set_stale_delay_guard_terms")


def get_structured_followup_scopes() -> list[str]:
    return _get_string_list("structured_followup_scopes")


def get_structured_followup_operations() -> list[str]:
    return _get_string_list("structured_followup_operations")


def get_metric_followup_override_actions() -> list[str]:
    return _get_string_list("metric_followup_override_actions")


def get_fresh_ranking_pnl_terms() -> list[str]:
    return _get_string_list("fresh_ranking_pnl_terms")


def get_fresh_ranking_segment_terms() -> list[str]:
    return _get_string_list("fresh_ranking_segment_terms")


def get_fresh_ranking_trend_terms() -> list[str]:
    return _get_string_list("fresh_ranking_trend_terms")


def get_llm_mongo_only_voyage_fields() -> frozenset[str]:
    return frozenset(_get_string_list("llm_mongo_only_voyage_fields"))


def get_llm_out_of_scope_greeting_exact() -> set[str]:
    return set(_get_string_list("llm_out_of_scope_greeting_exact"))


def get_llm_out_of_scope_greeting_prefixes() -> tuple[str, ...]:
    return tuple(_get_string_list("llm_out_of_scope_greeting_prefixes"))


def get_llm_out_of_scope_identity_phrases() -> list[str]:
    return _get_string_list("llm_out_of_scope_identity_phrases")


def get_llm_out_of_scope_weather_keywords() -> list[str]:
    return _get_string_list("llm_out_of_scope_weather_keywords")


def get_llm_answer_narrative_triggers() -> list[str]:
    return _get_string_list("llm_answer_narrative_triggers")


def get_llm_answer_financial_first_terms() -> list[str]:
    return _get_string_list("llm_answer_financial_first_terms")


def get_llm_answer_port_terms() -> list[str]:
    return _get_string_list("llm_answer_port_terms")


def get_llm_answer_grade_terms() -> list[str]:
    return _get_string_list("llm_answer_grade_terms")


def get_llm_answer_remark_terms() -> list[str]:
    return _get_string_list("llm_answer_remark_terms")


def get_llm_vessel_performance_terms() -> list[str]:
    return _get_string_list("llm_vessel_performance_terms")


def get_llm_voyage_resolved_vessel_metadata_terms() -> list[str]:
    return _get_string_list("llm_voyage_resolved_vessel_metadata_terms")


def get_llm_average_voyage_duration_terms() -> list[str]:
    return _get_string_list("llm_average_voyage_duration_terms")


def get_llm_per_vessel_terms() -> list[str]:
    return _get_string_list("llm_per_vessel_terms")


def get_llm_cargo_subject_terms() -> list[str]:
    return _get_string_list("llm_cargo_subject_terms")


def get_llm_cargo_aggregate_terms() -> list[str]:
    return _get_string_list("llm_cargo_aggregate_terms")


def get_llm_cargo_frequency_terms() -> list[str]:
    return _get_string_list("llm_cargo_frequency_terms")


def get_llm_cargo_non_primary_subject_terms() -> list[str]:
    return _get_string_list("llm_cargo_non_primary_subject_terms")


def get_llm_cargo_profitability_metric_terms() -> list[str]:
    return _get_string_list("llm_cargo_profitability_metric_terms")


def get_llm_cargo_profitability_terms() -> list[str]:
    return _get_string_list("llm_cargo_profitability_terms")


def get_llm_cargo_negative_profit_terms() -> list[str]:
    return _get_string_list("llm_cargo_negative_profit_terms")


def get_llm_scenario_comparison_terms() -> list[str]:
    return _get_string_list("llm_scenario_comparison_terms")


def get_llm_comparison_terms() -> list[str]:
    return _get_string_list("llm_comparison_terms")


def get_llm_voyage_port_listing_terms() -> list[str]:
    return _get_string_list("llm_voyage_port_listing_terms")


def get_llm_entity_summary_terms() -> list[str]:
    return _get_string_list("llm_entity_summary_terms")


def get_llm_vessel_fleet_exclusion_terms() -> list[str]:
    return _get_string_list("llm_vessel_fleet_exclusion_terms")


def get_llm_vessel_ranking_signals() -> list[str]:
    return _get_string_list("llm_vessel_ranking_signals")


def get_llm_vessel_metric_signals() -> list[str]:
    return _get_string_list("llm_vessel_metric_signals")


def get_llm_voyage_profitability_phrases() -> list[str]:
    return _get_string_list("llm_voyage_profitability_phrases")


def get_llm_ranking_order_terms() -> list[str]:
    return _get_string_list("llm_ranking_order_terms")


def get_llm_profit_metric_terms() -> list[str]:
    return _get_string_list("llm_profit_metric_terms")


def get_llm_emissions_terms() -> list[str]:
    return _get_string_list("llm_emissions_terms")


def get_llm_emissions_ranking_terms() -> list[str]:
    return _get_string_list("llm_emissions_ranking_terms")


def get_llm_voyage_performance_phrases() -> list[str]:
    return _get_string_list("llm_voyage_performance_phrases")


def get_llm_voyage_extreme_phrases() -> list[str]:
    return _get_string_list("llm_voyage_extreme_phrases")


def get_llm_port_call_terms() -> list[str]:
    return _get_string_list("llm_port_call_terms")


def get_llm_port_call_ranking_terms() -> list[str]:
    return _get_string_list("llm_port_call_ranking_terms")


def get_llm_port_fleet_signals() -> list[str]:
    return _get_string_list("llm_port_fleet_signals")


def get_llm_port_broad_visit_terms() -> list[str]:
    return _get_string_list("llm_port_broad_visit_terms")


def get_llm_voyage_count_per_vessel_terms() -> list[str]:
    return _get_string_list("llm_voyage_count_per_vessel_terms")


def get_llm_fleet_vessel_terms() -> list[str]:
    return _get_string_list("llm_fleet_vessel_terms")


def get_llm_vessel_metadata_agg_terms() -> list[str]:
    return _get_string_list("llm_vessel_metadata_agg_terms")


def get_llm_metadata_keywords() -> list[str]:
    return _get_string_list("llm_metadata_keywords")


def get_llm_metadata_fleet_wide_markers() -> list[str]:
    return _get_string_list("llm_metadata_fleet_wide_markers")


def get_llm_generic_agg_terms() -> list[str]:
    return _get_string_list("llm_generic_agg_terms")


def get_llm_delay_terms() -> list[str]:
    return _get_string_list("llm_delay_terms")


def get_llm_delay_ranking_terms() -> list[str]:
    return _get_string_list("llm_delay_ranking_terms")


def invalidate_cache() -> None:
    load_routing_rules.cache_clear()
