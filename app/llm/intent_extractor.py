import json
import logging
from typing import Optional, Callable

from app.config.schema_loader import (
    get_postgres_schema_str,
    get_mongo_schema_str,
    get_source_map_str,
    get_intent_catalog_str,
    get_entity_catalog_str,
    get_all_schema_field_names,
)

logger = logging.getLogger(__name__)

_INTENT_PROMPT = """You are a query understanding engine for a multi-source system.
Map the user query to structured schema elements.

RULES:
- Use ONLY field names that appear in the schema sections below.
- Do NOT invent field names.
- Derive required_sources from which source group owns each requested field.
- If scope is follow_up, set follow_up_action to: filter, sort, slice, or rerank.
- scenario defaults to ACTUAL if not specified by user.

## Postgres Schema
{postgres_schema}

## Document Store Schema
{mongo_schema}

## Field to Source Mapping
{source_map}

## Available Operations
{intent_catalog}

## Entity Definitions and Cross-Source Rules
{entity_catalog}

## Conversation History (last 3 turns)
{conversation_context}

## Previous Result Summary
{last_result_meta}

## User Query
{query}

Return ONLY valid JSON. No explanation. No markdown fences.

{{
  "operation": "<operation id from Available Operations>",
  "entities": [
    {{
      "type": "<entity type from Entity Definitions>",
      "identifier_field": "<exact field name from schema>",
      "identifier_value": "<value the user provided or null>"
    }}
  ],
  "requested_fields": ["<exact field names from schema only>"],
  "filters": [
    {{
      "field": "<exact schema field>",
      "operator": "<eq|in|gt|lt|gte|lte|between|ilike>",
      "value": "<value>"
    }}
  ],
  "aggregation": {{
    "function": "<avg|sum|count|min|max|mode|rank|null>",
    "group_by": ["<exact schema field or null>"],
    "order_by": "<field ASC|DESC or null>",
    "limit": <integer or null>
  }},
  "scenario": "<ACTUAL|WHEN_FIXED or null — default ACTUAL>",
  "required_sources": ["<postgres|mongo>"],
  "scope": "<fresh|follow_up>",
  "follow_up_action": "<filter|sort|slice|rerank|null>",
  "confidence": "<high|medium|low>",
  "reasoning": "<one sentence explaining the operation and source choice>"
}}
"""


def _format_context(history: list) -> str:
    if not history:
        return "No prior conversation."
    recent = history[-3:]
    parts: list[str] = []
    for i, turn in enumerate(recent):
        role = str(turn.get("role", "?"))
        content = str(turn.get("content", ""))[:200]
        parts.append(f"[{i+1}] {role}: {content}")
    return "\n".join(parts)


def _strip_code_fences(text: str) -> str:
    t = (text or "").strip()
    if t.startswith("```"):
        lines = t.splitlines()
        t = "\n".join(lines[1:])
    if t.endswith("```"):
        lines = t.splitlines()
        t = "\n".join(lines[:-1])
    return t.strip()


def _validate_fields(intent: dict, known_fields: set) -> dict:
    original = list(intent.get("requested_fields", []) or [])
    valid = [f for f in original if f in known_fields]
    removed = [f for f in original if f not in set(valid)]
    if removed:
        logger.warning(f"IntentExtractor: removed unknown fields: {removed}")
    intent["requested_fields"] = valid
    intent["filters"] = [
        f for f in (intent.get("filters", []) or [])
        if isinstance(f, dict) and f.get("field") in known_fields
    ]
    return intent


def extract_structured_intent(
    query: str,
    conversation_history: list,
    llm_caller: Callable[[str], str],
    last_result_meta: Optional[dict] = None,
) -> Optional[dict]:
    try:
        prompt = _INTENT_PROMPT.format(
            postgres_schema=get_postgres_schema_str(),
            mongo_schema=get_mongo_schema_str(),
            source_map=get_source_map_str(),
            intent_catalog=get_intent_catalog_str(),
            entity_catalog=get_entity_catalog_str(),
            conversation_context=_format_context(conversation_history),
            last_result_meta=json.dumps(last_result_meta or {}, default=str)[:600],
            query=query,
        )

        raw = llm_caller(prompt)
        clean = _strip_code_fences(raw)
        intent = json.loads(clean)
        intent = _validate_fields(intent, get_all_schema_field_names())
        logger.info(
            f"IntentExtractor | op={intent.get('operation')} | "
            f"confidence={intent.get('confidence')} | "
            f"sources={intent.get('required_sources')} | "
            f"scope={intent.get('scope')}"
        )
        return intent

    except json.JSONDecodeError as e:
        logger.warning(f"IntentExtractor: JSON parse failed: {e}")
        return None
    except Exception as e:
        logger.error(f"IntentExtractor: error: {e}", exc_info=True)
        return None

