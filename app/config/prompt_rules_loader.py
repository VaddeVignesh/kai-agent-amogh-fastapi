from __future__ import annotations

from functools import lru_cache
from typing import Any

from app.config.schema_loader import _read_yaml


@lru_cache(maxsize=1)
def load_prompt_rules() -> dict[str, Any]:
    return _read_yaml("prompt_rules.yaml")


def _get_section(section: str) -> dict[str, Any]:
    values = load_prompt_rules().get(section, {})
    return values if isinstance(values, dict) else {}


def _get_string(section: str, key: str, default: str) -> str:
    value = _get_section(section).get(key, default)
    return str(value if value is not None else default)


def _get_nested_string(section: str, group: str, key: str, default: str) -> str:
    values = _get_section(section).get(group, {})
    if not isinstance(values, dict):
        return default
    value = values.get(key, default)
    return str(value if value is not None else default)


def get_structured_intent_prompt_template() -> str:
    return _get_string("structured_intent", "prompt_template", "")


def get_mongo_query_builder_system_prompt() -> str:
    return _get_string("mongo_query_builder", "system_prompt", "")


def get_default_sql_json_prompt() -> str:
    return _get_string("llm_client", "default_sql_json_prompt", "Return SQL JSON only.")


def get_llm_ranking_answer_hint() -> str:
    return _get_string("llm_client", "ranking_answer_hint", "")


def get_llm_ops_only_voyage_answer_instruction() -> str:
    return _get_string("llm_client", "ops_only_voyage_answer_instruction", "")


def get_out_of_scope_response_template(name: str) -> str:
    return _get_nested_string("llm_client", "out_of_scope_responses", name, "")


def get_answer_polish_system_prompt() -> str:
    return _get_string("llm_client", "answer_polish_system_prompt", "")


def get_answer_generation_fallback(name: str) -> str:
    return _get_nested_string("llm_client", "answer_generation_fallbacks", name, "")


def get_llm_intent_classifier_system_prompt_template() -> str:
    return _get_string("llm_client", "intent_classifier_system_prompt", "")


def get_llm_answer_generation_system_prompt() -> str:
    return _get_string("llm_client", "answer_generation_system_prompt", "")


def get_llm_conversation_memory_label(name: str) -> str:
    return _get_nested_string("llm_client", "conversation_memory_labels", name, "")


def get_answer_postprocess_replacements() -> list[tuple[str, str]]:
    values = _get_section("llm_client").get("answer_postprocess_replacements", {})
    if not isinstance(values, dict):
        return []
    replacements: list[tuple[str, str]] = []
    for raw in values.values():
        parts = str(raw or "").split("|", 1)
        if len(parts) == 2 and parts[0]:
            replacements.append((parts[0], parts[1]))
    return replacements


def get_graph_router_multi_voyage_answer_system_prompt() -> str:
    return _get_string("graph_router", "multi_voyage_answer_system_prompt", "")


def get_graph_router_voyage_metadata_answer_system_prompt() -> str:
    return _get_string("graph_router", "voyage_metadata_answer_system_prompt", "")


def get_sql_generator_finance_system_prompt() -> str:
    return _get_string("sql_generator", "finance_system_prompt", "")


def get_sql_generator_ops_system_prompt() -> str:
    return _get_string("sql_generator", "ops_system_prompt", "")


def invalidate_cache() -> None:
    load_prompt_rules.cache_clear()
