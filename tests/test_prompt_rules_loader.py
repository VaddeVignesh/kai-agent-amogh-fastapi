from app.config.prompt_rules_loader import (
    get_answer_generation_fallback,
    get_answer_polish_system_prompt,
    get_answer_postprocess_replacements,
    get_default_sql_json_prompt,
    get_graph_router_multi_voyage_answer_system_prompt,
    get_graph_router_voyage_metadata_answer_system_prompt,
    get_llm_answer_generation_system_prompt,
    get_llm_conversation_memory_label,
    get_llm_intent_classifier_system_prompt_template,
    get_llm_ops_only_voyage_answer_instruction,
    get_llm_ranking_answer_hint,
    get_mongo_query_builder_system_prompt,
    get_out_of_scope_response_template,
    get_sql_generator_finance_system_prompt,
    get_sql_generator_ops_system_prompt,
    get_structured_intent_prompt_template,
)


def test_structured_intent_prompt_template_loads_from_yaml() -> None:
    prompt = get_structured_intent_prompt_template()
    assert "query understanding engine" in prompt
    assert "{postgres_schema}" in prompt
    assert "{mongo_schema}" in prompt
    assert '"required_sources"' in prompt


def test_mongo_query_builder_prompt_loads_from_yaml() -> None:
    prompt = get_mongo_query_builder_system_prompt()
    assert "SAFE MongoDB find() specs" in prompt
    assert "Use only allowed operators" in prompt
    assert "Limit must be <= 50" in prompt


def test_default_sql_json_prompt_loads_from_yaml() -> None:
    assert get_default_sql_json_prompt() == "Return SQL JSON only."


def test_sql_generator_prompts_load_from_yaml() -> None:
    finance_prompt = get_sql_generator_finance_system_prompt()
    ops_prompt = get_sql_generator_ops_system_prompt()

    assert "finance_voyage_kpi" in finance_prompt
    assert "ops_voyage_summary" in finance_prompt
    assert "ops_voyage_summary" in ops_prompt
    assert "Output only the SQL" in ops_prompt


def test_llm_response_prompts_load_from_yaml() -> None:
    assert "merged_rows has numeric fields" in get_llm_ranking_answer_hint()
    ops_only = get_llm_ops_only_voyage_answer_instruction()
    assert "HARD CONSTRAINT" in ops_only
    assert "fixtures" in ops_only.lower()
    assert "Digital Sales Agent" in get_out_of_scope_response_template("greeting")
    assert "About Digital Sales Agent" in get_out_of_scope_response_template("identity")
    assert "weather/forecast" in get_out_of_scope_response_template("weather")
    assert "outside the supported dataset" in get_out_of_scope_response_template("default")

    polish_prompt = get_answer_polish_system_prompt()
    assert "senior maritime analyst" in polish_prompt
    assert "Use ONLY the provided JSON" in polish_prompt
    assert "DECISION-GRADE RULE" in polish_prompt

    assert get_answer_generation_fallback("no_recent_context") == "No recent conversational context."
    assert get_answer_generation_fallback("empty_answer") == "Not available in dataset."
    assert ("with the exception of", "except for") in get_answer_postprocess_replacements()

    classifier_prompt = get_llm_intent_classifier_system_prompt_template()
    assert "maritime finance intent classifier" in classifier_prompt
    assert "{recent_context}" in classifier_prompt
    assert "{intents_formatted}" in classifier_prompt

    answer_prompt = get_llm_answer_generation_system_prompt()
    assert "flagship-quality maritime analytics assistant" in answer_prompt
    assert "VERDICT FIRST RULE" in answer_prompt
    assert "DECISION-GRADE CONTRACT" in answer_prompt
    assert "business_answer_contract" in answer_prompt
    assert "include Margin" in answer_prompt
    assert "include Cost ratio" in answer_prompt
    assert "Margin column" in answer_prompt
    assert "explicitly name PnL" in answer_prompt
    assert "Data caveat" in answer_prompt
    assert get_llm_conversation_memory_label("hot_context_empty") == "HOT CONTEXT: none"


def test_graph_router_prompts_load_from_yaml() -> None:
    multi_voyage_prompt = get_graph_router_multi_voyage_answer_system_prompt()
    voyage_metadata_prompt = get_graph_router_voyage_metadata_answer_system_prompt()

    assert "TWO sources" in multi_voyage_prompt
    assert "FINANCIAL DATA" in multi_voyage_prompt
    assert "Voyage Data" not in voyage_metadata_prompt
    assert "Format money fields" in voyage_metadata_prompt
    assert "modifiedByFull" in voyage_metadata_prompt
