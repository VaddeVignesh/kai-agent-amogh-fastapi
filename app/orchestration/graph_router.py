from __future__ import annotations

import ast
import copy
import json
import logging
import os
import time
import traceback
import uuid
from dataclasses import asdict, is_dataclass
from typing import Any, Dict, Optional, TypedDict

from langgraph.graph import StateGraph, END
import re
import sys

from app.orchestration.planner import Planner, ExecutionPlan
from app.config.mongo_rules_loader import get_mongo_limit, get_mongo_projection
from app.config.prompt_rules_loader import (
    get_graph_router_multi_voyage_answer_system_prompt,
    get_graph_router_voyage_metadata_answer_system_prompt,
)
from app.config.response_rules_loader import (
    get_finance_kpi_scope_restricted_user_message,
    get_result_set_response_template,
    get_router_fallback_template,
)
from app.registries.intent_loader import get_yaml_registry_facade
from app.registries.sql_registry import SQL_REGISTRY
from app.services.response_merger import compact_payload
from app.utils.ops_llm_shrink import shrink_ops_row_json_fields
from app.utils.mongo_ops_slice import (
    build_mongo_llm_row_for_ops_tenant,
    build_voyage_summary_merged_row,
)
from app.utils.voyage_slot_extract import merge_voyage_slot_fields
from app.adapters.mongo_adapter import narrow_voyage_rows_by_entity_slots
from app.auth import session_may_access_finance_kpi
from app.config.routing_rules_loader import (
    get_cargo_grade_terms,
    get_cargo_grade_voyage_terms,
    get_concise_followup_keywords,
    get_comparison_followup_terms,
    get_coreference_words,
    get_corpus_guard_result_set_reference_phrases,
    get_direct_thread_reference_phrases,
    get_explicit_result_set_reference_phrases,
    get_first_selector_phrases,
    get_followup_backward_markers,
    get_fresh_fleet_markers,
    get_fresh_ranking_pnl_terms,
    get_fresh_ranking_phrases,
    get_fresh_ranking_segment_terms,
    get_fresh_ranking_trend_terms,
    get_fresh_ranking_trend_context_words,
    get_fresh_ranking_trend_words,
    get_generic_followup_markers,
    get_incomplete_entity_detail_markers,
    get_incomplete_entity_fastpath_queries,
    get_incomplete_entity_fleet_port_aggregate_markers,
    get_incomplete_entity_placeholder_slot_values,
    get_incomplete_entity_question_markers,
    get_incomplete_entity_topic_phrases,
    get_incomplete_entity_topic_terms,
    get_last_selector_phrases,
    get_long_new_question_prefixes,
    get_new_request_prefixes,
    get_operational_followup_keywords,
    get_older_vessel_markers,
    get_polite_followup_prefixes,
    get_proper_name_request_prefixes,
    get_result_set_operation_fields,
    get_result_set_operation_verbs,
    get_result_set_corpus_followup_exclusion_terms,
    get_result_set_explain_verbs,
    get_result_set_extreme_keywords,
    get_result_set_extreme_metric_aliases,
    get_result_set_fresh_start_prefixes,
    get_result_set_list_fields_context_terms,
    get_result_set_list_fields_extra_exclusion_terms,
    get_result_set_low_extreme_terms,
    get_result_set_metric_followup_exclusion_terms,
    get_result_set_projection_field_terms,
    get_result_set_question_prefixes,
    get_result_set_refinement_phrases,
    get_result_set_refinement_extreme_terms,
    get_result_set_scope_phrases,
    get_result_set_selected_metric_aliases,
    get_result_set_selected_metric_context_terms,
    get_result_set_selected_metric_triggers,
    get_result_set_short_projection_field_terms,
    get_result_set_stale_delay_guard_terms,
    get_scenario_comparison_words,
    get_scenario_keyword_aliases,
    get_selected_row_reference_phrases,
    get_simple_voyage_analytical_markers,
    get_same_entity_reference_words,
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
    get_value_like_fresh_words,
    get_vessel_mention_descriptor_suffixes,
    get_vessel_mention_false_prefixes,
    get_vessel_mention_stop_first_words,
    get_voyage_topic_keywords,
    is_read_only_mutation_request,
    is_voyage_composite_profile_query,
    is_voyage_operational_profile_query,
)
from app.core.logger import get_logger

_INTENT_FACADE = get_yaml_registry_facade(validate_parity=True)
INTENT_REGISTRY = _INTENT_FACADE["INTENT_REGISTRY"]
SUPPORTED_INTENTS = _INTENT_FACADE["SUPPORTED_INTENTS"]
resolve_intent = _INTENT_FACADE["resolve_intent"]
_SUGGESTION_CACHE_TTL_SECONDS = 300.0

from app.llm.intent_extractor import extract_structured_intent
from app.orchestration.source_router import resolve_required_sources
from app.orchestration.followup_resolver import resolve_followup
import logging as _sil_logging

_sil_logger = _sil_logging.getLogger("shadow_intent_layer")
if not _sil_logger.handlers:
    _handler = _sil_logging.StreamHandler()
    _handler.setFormatter(_sil_logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
    _sil_logger.addHandler(_handler)
_sil_logger.setLevel(_sil_logging.INFO)

# =========================================================
# Debug logging
# =========================================================

def _debug_enabled() -> bool:
    return (os.getenv("KAI_DEBUG") or "").strip().lower() in ("1", "true", "yes", "y", "on")


def _dprint(*args: Any, **kwargs: Any) -> None:
    if _debug_enabled():
        try:
            print(*args, **kwargs)
        except UnicodeEncodeError:
            # Windows consoles may use legacy encodings (cp1252). Fall back to an ASCII-safe render.
            sep = kwargs.get("sep", " ")
            end = kwargs.get("end", "\n")
            safe_parts = []
            for a in args:
                s = str(a)
                safe_parts.append(s.encode("ascii", errors="backslashreplace").decode("ascii"))
            try:
                print(sep.join(safe_parts), end=end)
            except Exception:
                # Last-resort: don't crash debug logging.
                return

logger = get_logger("graph_router")


def _router_fallback(name: str, **values: Any) -> str:
    template = get_router_fallback_template(name)
    return template.format(**values) if template else ""


def _result_set_text(name: str, **values: Any) -> str:
    template = get_result_set_response_template(name)
    return template.format(**values) if template else ""


def _shadow_extract(query: str, session: dict, llm_fn) -> dict | None:
    """Run structured intent extraction defensively for routing diagnostics."""
    try:
        last_rows = session.get("last_result_set", [])
        if isinstance(last_rows, dict):
            last_rows = last_rows.get("rows", []) if isinstance(last_rows.get("rows"), list) else []
        intent = extract_structured_intent(
            query=query,
            conversation_history=session.get("turn_history", []),
            llm_caller=llm_fn,
            last_result_meta={
                "row_count": len(last_rows),
                "last_intent": session.get("last_intent"),
                "available_fields": (
                    list(last_rows[0].keys()) if last_rows else []
                ),
            },
        )
        return intent
    except Exception as e:
        _sil_logger.error(f"ShadowMode: failed — {e}")
        return None


def _first_fixture(doc: dict) -> dict:
    raw = doc.get("fixtures")
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                return item
        return {}
    if isinstance(raw, dict):
        for key in ("fixtureList", "fixtures", "list"):
            val = raw.get(key)
            if isinstance(val, list):
                for item in val:
                    if isinstance(item, dict):
                        return item
        return raw
    return {}


VOYAGE_SECTION_MAP = {
    "commissions": lambda d: _first_fixture(d).get("fixtureCommissions", []),
    "cargo":       lambda d: _first_fixture(d).get("fixtureBillsOfLading", []),
    "fixture":     lambda d: {k: _first_fixture(d).get(k) for k in
                       ["cpDate", "cpQuantity", "demurrage", "laytime",
                        "timeBar", "overage", "grades", "fixtureRemark"]},
    "ports":       lambda d: _first_fixture(d).get("fixturePorts", []),
    "legs":        lambda d: d.get("legs", []),
    "revenues":    lambda d: d.get("revenues", []),
    "expenses":    lambda d: d.get("expenses", []),
    "bunkers":     lambda d: d.get("bunkers", []),
    "emissions":   lambda d: d.get("emissions", {}),
    "projected":   lambda d: d.get("projected_results", {}),
    "remarks":     lambda d: d.get("remarks", []),
}

TOPIC_KEYWORDS = get_voyage_topic_keywords()


def select_voyage_sections(
    user_input: str,
    doc: dict,
    session_ctx: Optional[Dict[str, Any]] = None,
) -> dict:
    """
    Picks only the doc sections relevant to the user's question.
    Returns a dict of {section_name: section_data}.
    Falls back to fixture + basic identity fields if nothing matches.
    """
    text = (user_input or "").lower()
    selected: dict = {}

    # Prefer structured intent fields when selecting document sections.
    _si_4d = session_ctx.get("_structured_intent") if isinstance(session_ctx, dict) else None
    _schema_sections = None

    if isinstance(_si_4d, dict) and _si_4d.get("confidence") in ("high", "medium"):
        _req_fields = _si_4d.get("requested_fields") or []
        if _req_fields:
            from app.config.schema_loader import load_schema

            _mongo_top = set(
                (
                    load_schema()
                    .get("mongo", {})
                    .get("collections", {})
                    .get("voyages", {})
                    .get("fields", {})
                    or {}
                ).keys()
            )
            _schema_sections = [f for f in _req_fields if f in _mongo_top]

    if _schema_sections:
        sections_to_project = _schema_sections
    else:
        sections_to_project = None
    # --- end Phase 4D ---

    if sections_to_project:
        for section in sections_to_project:
            if section in VOYAGE_SECTION_MAP:
                selected[section] = VOYAGE_SECTION_MAP[section](doc)
    else:
        for section, keywords in TOPIC_KEYWORDS.items():
            if any(kw in text for kw in keywords):
                selected[section] = VOYAGE_SECTION_MAP[section](doc)

    # Always include basic identity
    selected["voyage_info"] = {
        "voyageNumber": doc.get("voyageNumber"),
        "vesselName":   doc.get("vesselName"),
        "voyageId":     doc.get("voyageId"),
        "startDateUtc": doc.get("startDateUtc"),
    }

    # Fallback: if nothing matched, send fixture summary
    if len(selected) == 1:
        selected["fixture"] = VOYAGE_SECTION_MAP["fixture"](doc)

    return selected

# =========================================================
# Helpers
# =========================================================

def _json_safe(obj: Any) -> Any:
    """Convert any object to JSON-safe format"""
    if obj is None:
        return None
    if hasattr(obj, "to_dict") and callable(getattr(obj, "to_dict")):
        return obj.to_dict()
    if is_dataclass(obj):
        return asdict(obj)
    if isinstance(obj, (dict, list, str, int, float, bool)):
        return obj
    return {"value": str(obj)}

# =========================================================
# Graph State
# =========================================================

class GraphState(TypedDict, total=False):
    session_id: str
    # user_input is the effective query used for planning/execution.
    # raw_user_input is what the user actually typed this turn (e.g. "2301" answering a clarification).
    user_input: str
    raw_user_input: str
    session_ctx: Dict[str, Any]

    intent_key: str
    slots: Dict[str, Any]
    missing_keys: list[str]
    clarification: str

    plan_type: str
    plan: Dict[str, Any]
    step_index: int
    artifacts: Dict[str, Any]

    mongo: Any
    finance: Any
    ops: Any
    
    data: Dict[str, Any]
    merged: Dict[str, Any]
    answer: str

# =========================================================
# Router
# =========================================================

class GraphRouter:
    """
    Hybrid router with composite query support.
    Handles slot merging, composite null safety, scenario detection, token optimization, ranking intents.
    """

    def __init__(self, *, llm, redis_store, mongo_agent, finance_agent, ops_agent):
        self.llm = llm
        self.redis = redis_store
        self.mongo_agent = mongo_agent
        self.finance_agent = finance_agent
        self.ops_agent = ops_agent
        self._suggestion_cache: Dict[str, tuple[float, list[Any]]] = {}

        self.planner = Planner(llm)
        self.graph = self._build_graph()

    # =========================================================
    # Trace (UI-facing debug, not chain-of-thought)
    # =========================================================

    @staticmethod
    def _step_goal_text(*, intent_key: str, agent: str, op: str, step_inputs: Dict[str, Any], slots: Dict[str, Any]) -> str:
        """
        Human-readable "goal" per step for UI trace.
        Keep it short, concrete, and step-specific.
        """
        intent_key = (intent_key or "out_of_scope").strip()
        agent = (agent or "").strip().lower()
        op = (op or "").strip().lower()

        if agent == "mongo" and op == "resolve_anchor":
            return "Resolve/confirm voyage/vessel anchors in MongoDB (so downstream ranking uses the right entity filters)."

        if agent == "finance" and op == "dynamic_sql":
            limit = step_inputs.get("limit") or slots.get("limit")
            metric = slots.get("metric")
            if intent_key.startswith("ranking.") and metric:
                return f"Fetch top {limit} voyages by {metric} from finance KPIs (Postgres), returning voyage_ids for downstream steps."
            if intent_key.startswith("ranking."):
                return f"Fetch top {limit} voyages for the ranking intent (Postgres finance KPIs), returning voyage_ids for downstream steps."
            return f"Run a finance analysis query in Postgres (dynamic SQL) and return voyage_ids for downstream steps (limit={limit})."

        if agent == "ops" and op == "dynamic_sql":
            return "Fetch ops summaries (ports/grades/remarks/delay/offhire) for selected voyage_ids from Postgres when available; skip if finance aggregate result is self-contained."

        if agent == "mongo" and op == "fetch_remarks":
            return "Fetch voyage remarks + minimal fixture context (ports/grades/commissions) for the selected voyage_ids from MongoDB."

        if agent == "llm" and op == "merge":
            return "Deterministically merge finance + ops + mongo context into a single joined dataset for summarization."

        return f"Execute step: {agent}.{op}"

    @staticmethod
    def _compact_for_trace(val: Any, *, max_str: int = 6000, max_list: int = 12, max_dict: int = 50, _depth: int = 0) -> Any:
        """
        Compact large structures so trace doesn't explode:
        - strings: truncated
        - lists: keep first N
        - dicts: keep first N keys (stable order)
        """
        if val is None:
            return None
        if isinstance(val, str):
            s = val.strip()
            if len(s) <= max_str:
                return s
            return s[:max_str].rstrip() + "\n-- (truncated) --"
        if isinstance(val, (int, float, bool)):
            return val
        # Only depth-cap complex nested structures. Always preserve primitive leaf values
        # (e.g. voyageIds inside $in lists) so the UI trace remains informative.
        if _depth > 4:
            return "...(depth cap)..."
        if isinstance(val, list):
            if len(val) <= max_list:
                return [GraphRouter._compact_for_trace(v, max_str=max_str, max_list=max_list, max_dict=max_dict, _depth=_depth + 1) for v in val]
            head = val[:max_list]
            return [
                *[GraphRouter._compact_for_trace(v, max_str=max_str, max_list=max_list, max_dict=max_dict, _depth=_depth + 1) for v in head],
                f"...({len(val) - max_list} more)",
            ]
        if isinstance(val, dict):
            items = list(val.items())
            if len(items) > max_dict:
                items = items[:max_dict]
                out = {k: GraphRouter._compact_for_trace(v, max_str=max_str, max_list=max_list, max_dict=max_dict, _depth=_depth + 1) for k, v in items}
                out["..."] = f"({len(val) - max_dict} more keys)"
                return out
            return {k: GraphRouter._compact_for_trace(v, max_str=max_str, max_list=max_list, max_dict=max_dict, _depth=_depth + 1) for k, v in val.items()}
        # Fallback
        return str(val)

    @staticmethod
    def _trace(state: GraphState, event: Dict[str, Any]) -> None:
        """
        Append a structured execution trace event into state.artifacts.trace.
        This is intended for UI debug panels and MUST NOT contain private model reasoning.
        """
        artifacts = state.get("artifacts")
        if not isinstance(artifacts, dict):
            artifacts = {}
        trace = artifacts.get("trace")
        if not isinstance(trace, list):
            trace = []
        # Compact potentially large values (voyage_id lists, mongo specs, generated SQL, etc.)
        safe_event: Dict[str, Any] = GraphRouter._compact_for_trace(event or {})
        trace.append(safe_event)
        # hard cap
        if len(trace) > 200:
            trace = trace[-200:]
        artifacts["trace"] = trace
        state["artifacts"] = artifacts

    @staticmethod
    def _mongo_projection_for_trace(projection: Dict[str, Any] | None) -> Dict[str, Any]:
        """
        Mirror adapter-side projection shaping so the trace shows the effective Mongo spec.
        """
        proj = dict(projection or {"_id": 0})
        if proj.get("remarks") == 1:
            proj["remarks"] = {"$slice": 5}
        if proj.get("remarkList") == 1:
            proj["remarkList"] = {"$slice": 5}
        return proj

    @staticmethod
    def _trace_single_mongo_query(
        state: GraphState,
        *,
        intent_key: str,
        operation: str,
        collection: str,
        filt: Dict[str, Any],
        projection: Dict[str, Any] | None,
        rows: int | None,
        summary: str,
        limit: int | None = None,
        sort: Any = None,
        mongo_queries: list[Dict[str, Any]] | None = None,
    ) -> None:
        mongo_query = {
            "collection": collection,
            "filter": filt,
            "projection": GraphRouter._mongo_projection_for_trace(projection),
            "sort": sort,
            "limit": limit,
            "pipeline": None,
        }
        GraphRouter._trace(
            state,
            {
                "phase": "single_step_result",
                "intent_key": intent_key,
                "agent": "mongo",
                "operation": operation,
                "ok": True,
                "mode": "mongo_metadata",
                "collection": collection,
                "limit": limit,
                "mongo_query": mongo_query,
                "mongo_queries": mongo_queries,
                "rows": rows,
                "summary": summary,
            },
        )

    # =========================================================
    # Pattern Detection for Scenario Comparisons
    # =========================================================
    
    @staticmethod
    def _detect_scenario_comparison(
        user_input: str,
        extracted_slots: Dict[str, Any],
        session_ctx: Optional[Dict[str, Any]] = None,
    ) -> tuple[bool, Dict[str, Any]]:
        """
        Pattern-based detection for scenario comparison queries.
        
        Detects queries like:
        - "Compare ACTUAL vs WHEN_FIXED"
        - "ACTUAL versus BUDGET"
        - "Show me ACTUAL and WHEN_FIXED"
        - "Variance between ACTUAL and BUDGET"
        
        Returns: (is_scenario_comparison, updated_slots)
        """
        user_lower = user_input.lower()

        found_scenarios: list[str] = []

        # Prefer structured intent scenario hints, then fall back to configured aliases.
        _si_4b = session_ctx.get("_structured_intent") if isinstance(session_ctx, dict) else None
        if isinstance(_si_4b, dict) and _si_4b.get("confidence") in ("high", "medium"):
            _sc_raw = str(_si_4b.get("scenario") or "").strip().upper()
            if _sc_raw:
                for part in re.split(r"[\s,/|]+", _sc_raw):
                    p = part.strip()
                    if p and p not in found_scenarios:
                        found_scenarios.append(p)

        from app.config.schema_loader import build_scenario_alias_map

        _alias_map = build_scenario_alias_map()
        _ul_lower = user_lower
        for alias, canonical in sorted(_alias_map.items(), key=lambda kv: -len(kv[0])):
            if alias in _ul_lower and canonical not in found_scenarios:
                found_scenarios.append(canonical)

        # FALLBACK 2: routing_rules.yaml scenario keyword aliases.
        scenario_keywords = get_scenario_keyword_aliases()
        for keyword, scenario_name in scenario_keywords.items():
            if keyword in user_lower and scenario_name not in found_scenarios:
                found_scenarios.append(scenario_name)
        # --- end Phase 4B ---
        
        # Comparison indicators
        comparison_words = get_scenario_comparison_words()
        
        # Check for comparison words
        has_comparison = any(word in user_lower for word in comparison_words)
        
        # If 2+ scenarios + comparison word = scenario comparison
        if len(found_scenarios) >= 2 and has_comparison:
            updated_slots = dict(extracted_slots)
            updated_slots["scenario"] = found_scenarios
            
            _dprint("   Pattern detected: Scenario comparison")
            _dprint(f"      Scenarios found: {found_scenarios}")
            
            return True, updated_slots
        
        # Also detect if "scenario" word is present with comparison
        if "scenario" in user_lower and has_comparison and len(found_scenarios) >= 1:
            updated_slots = dict(extracted_slots)
            if "scenario" not in updated_slots:
                updated_slots["scenario"] = found_scenarios if found_scenarios else ["ACTUAL", "WHEN_FIXED"]
            return True, updated_slots
        
        return False, extracted_slots

    # =========================================================
    # Nodes
    # =========================================================

    @staticmethod
    def _has_entity_anchor(slots: Dict[str, Any]) -> bool:
        if not isinstance(slots, dict):
            return False
        for key in ("voyage_number", "voyage_numbers", "voyage_id", "vessel_name", "imo", "port_name"):
            value = slots.get(key)
            if value not in (None, "", [], {}):
                return True
        return False

    @staticmethod
    def _entity_slot_keys() -> tuple[str, ...]:
        return ("voyage_number", "voyage_numbers", "voyage_id", "vessel_name", "imo", "port_name")

    @staticmethod
    def _explicit_vessel_mentions(
        user_input: str,
        session_ctx: Optional[Dict[str, Any]] = None,
    ) -> list[str]:
        """
        Extract likely vessel names spelled out in the user message (proper-noun phrases).
        Used to override stale session anchors when the user names a different ship than memory.
        """
        ui = (user_input or "").strip()
        if not ui:
            return []
        found: list[str] = []
        seen: set[str] = set()

        def _add(raw: str) -> None:
            s = (raw or "").strip().strip("\"'")
            if len(s) < 3 or len(s) > 80:
                return
            key = s.lower()
            if key in seen:
                return
            seen.add(key)
            found.append(s)

        stop_first = get_vessel_mention_stop_first_words()
        descriptor_suffixes = get_vessel_mention_descriptor_suffixes()
        false_prefixes = get_vessel_mention_false_prefixes()

        def _ok_group(s: str) -> bool:
            parts = s.split()
            if not parts:
                return False
            if parts[0].lower() in stop_first:
                return False
            if len(parts) == 2 and parts[1].lower() in descriptor_suffixes:
                if parts[0].upper() in false_prefixes:
                    return False
            return True

        # Title-case or acronym-led multi-token names (e.g. Stena Imperial, MTM Potomac).
        np = r"(?:[A-Z][a-z0-9]+|[A-Z]{2,})"
        name = np + r"(?:\s+" + np + r"){0,4}"
        # Case-sensitive capture: re.I would let [A-Z] match lowercase and swallow "including ...".
        name_cs = "(?-i:" + name + ")"
        patterns: list[tuple[re.Pattern[str], tuple[int, ...]]] = [
            (re.compile(rf"\b(?:vessel|ship|vessels|ships)\s+({name})(?=\s|[.,!?]|'s|$)"), (1,)),
            (re.compile(rf"\bfor\s+(?:the\s+)?(?:vessel|ship)\s+({name})(?=\s|[.,!?]|'s|$)"), (1,)),
            (re.compile(rf"(?i)\b(?:profile|details)\s+for\s+(?:the\s+)?(?:vessel|ship)\s+({name_cs})(?=\s|[.,!?]|'s|$)"), (1,)),
            (re.compile(rf"\b(?:linked|tied)\s+to\s+({name})(?:'s|\s+)"), (1,)),
            (re.compile(rf"\bwith\s+({name})\s*\?"), (1,)),
            (re.compile(rf"\bfor\s+({name})\s*\?\s*$"), (1,)),
            (re.compile(rf"(?i)\bfor\s+({name_cs})\s+voyage\b"), (1,)),
            (re.compile(rf"\b({name})'s\b"), (1,)),
            (re.compile(rf"\bbetween\s+({name})\s+and\s+({name})\b"), (1, 2)),
            (
                re.compile(
                    rf"(?i)\b(?:Does|Do|Is|Are|Has|Have|Can|Will)\s+({name_cs})\s+"
                    rf"(?:have|has|still|include|operate|operating|carry|carrying|belong|need|use|using)\b",
                ),
                (1,),
            ),
            (re.compile(rf"(?i)\bdoes\s+({name_cs})\s+belong\b"), (1,)),
        ]
        for rx, groups in patterns:
            for m in rx.finditer(ui):
                for gi in groups:
                    g = (m.group(gi) or "").strip()
                    if _ok_group(g):
                        _add(g)
        return found

    @staticmethod
    def _normalize_vessel_anchor_from_query(
        slots: Dict[str, Any],
        user_input: str,
        session_ctx: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        When the user names exactly one vessel in free text, treat that as the authoritative
        vessel anchor for this turn (overrides Redis-inherited slots and many LLM misses).
        """
        mentions = GraphRouter._explicit_vessel_mentions(user_input, session_ctx)
        out = dict(slots or {})
        if len(mentions) != 1:
            return out
        out["vessel_name"] = mentions[0]
        if not re.search(r"\b\d{7}\b", user_input or ""):
            out.pop("imo", None)
        return out

    @staticmethod
    def _pick_voyage_doc_aligned_to_finance(
        candidates: list[Dict[str, Any]],
        fin: Dict[str, Any],
    ) -> Dict[str, Any] | None:
        """Pick one Mongo voyage row when Postgres finance row anchors identity."""
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]
        f = fin if isinstance(fin, dict) else {}
        fid = str(f.get("voyage_id") or "").strip()
        if fid:
            for c in candidates:
                if str(c.get("voyageId") or "") == fid:
                    return c
        fn = str(f.get("vessel_name") or "").strip().casefold()
        if fn:
            for c in candidates:
                vn = str(c.get("vesselName") or "").strip().casefold()
                if vn and (fn == vn or fn in vn or vn in fn):
                    return c
        raw_imo = str(f.get("vessel_imo") or f.get("imo") or "").strip()
        imo_d = "".join(ch for ch in raw_imo if ch.isdigit())
        if imo_d:
            for c in candidates:
                cr = str(c.get("vesselImo") or c.get("vessel_imo") or "").strip()
                cd = "".join(ch for ch in cr if ch.isdigit())
                if cd == imo_d:
                    return c
        return None

    def _port_exists(self, port_name: str) -> bool | None:
        pg = getattr(self.ops_agent, "pg", None)
        if pg is None or not hasattr(pg, "execute_dynamic_select"):
            value = str(port_name or "").strip()
            return True if value else False
        sql = """
            SELECT 1
            FROM ops_voyage_summary ovs,
                 LATERAL jsonb_array_elements(ovs.ports_json) AS p
            WHERE LOWER(COALESCE(p->>'port_name', '')) = LOWER(%(port_name)s)
            LIMIT 1
        """
        try:
            rows = pg.execute_dynamic_select(sql, {"port_name": str(port_name or "").strip()})
            return bool(rows)
        except Exception:
            return None

    def _validate_entity_slots(self, slots: Dict[str, Any]) -> Dict[str, Any]:
        cleaned = dict(slots or {})
        adapter = getattr(self.mongo_agent, "adapter", None)

        def _voyage_exists(vn: Any) -> bool | None:
            if adapter is None:
                return None
            try:
                value = int(float(vn))
            except Exception:
                return False
            try:
                return adapter.count_voyages_by_number(value) > 0
            except Exception:
                return None

        def _voyage_id_exists(voyage_id: Any) -> bool | None:
            if adapter is None:
                return None
            try:
                value = str(voyage_id or "").strip()
                if not value:
                    return False
                return bool(adapter.fetch_voyage(value, projection=get_mongo_projection("voyage_id_lookup")))
            except TypeError:
                return bool(adapter.fetch_voyage(str(voyage_id or "").strip()))
            except Exception:
                return None

        def _vessel_name_exists(vessel_name: Any) -> bool | None:
            if adapter is None:
                return None
            value = str(vessel_name or "").strip()
            if not value:
                return False
            try:
                doc = adapter.fetch_vessel_by_name(value, projection={"_id": 1})
                return bool(doc)
            except Exception:
                return None

        def _imo_exists(imo: Any) -> bool | None:
            if adapter is None:
                return None
            value = str(imo or "").strip()
            if not value:
                return False
            try:
                return bool(adapter.fetch_vessel(value, projection={"imo": 1}))
            except TypeError:
                return bool(adapter.fetch_vessel(value))
            except Exception:
                return None

        voyage_number_validity = _voyage_exists(cleaned.get("voyage_number")) if "voyage_number" in cleaned else None
        if "voyage_number" in cleaned and voyage_number_validity is False:
            cleaned.pop("voyage_number", None)

        if "voyage_numbers" in cleaned:
            values = cleaned.get("voyage_numbers")
            values = values if isinstance(values, list) else [values]
            valid_voyages = []
            unknown_voyage_validation = False
            for item in values:
                item_validity = _voyage_exists(item)
                if item_validity is True:
                    try:
                        valid_voyages.append(int(float(item)))
                    except Exception:
                        continue
                elif item_validity is None:
                    unknown_voyage_validation = True
            if valid_voyages:
                cleaned["voyage_numbers"] = valid_voyages
            elif unknown_voyage_validation:
                pass
            else:
                cleaned.pop("voyage_numbers", None)

        voyage_id_validity = _voyage_id_exists(cleaned.get("voyage_id")) if "voyage_id" in cleaned else None
        if "voyage_id" in cleaned and voyage_id_validity is False:
            cleaned.pop("voyage_id", None)

        vessel_name_validity = _vessel_name_exists(cleaned.get("vessel_name")) if "vessel_name" in cleaned else None
        if "vessel_name" in cleaned and vessel_name_validity is False:
            cleaned.pop("vessel_name", None)

        imo_validity = _imo_exists(cleaned.get("imo")) if "imo" in cleaned else None
        if "imo" in cleaned and imo_validity is False:
            cleaned.pop("imo", None)

        port_validity = self._port_exists(str(cleaned.get("port_name") or "").strip()) if "port_name" in cleaned else None
        if "port_name" in cleaned and port_validity is False:
            cleaned.pop("port_name", None)

        return cleaned

    @staticmethod
    def _find_slot_in_turn_history(
        *,
        session_ctx: Dict[str, Any],
        slot_key: str,
        prefer_older: bool,
    ) -> Any:
        history = (session_ctx or {}).get("turn_history") if isinstance(session_ctx, dict) else None
        if not isinstance(history, list) or not history:
            return None
        search_space = history[:-1] if prefer_older and len(history) > 1 else history
        for item in reversed(search_space):
            if not isinstance(item, dict):
                continue
            slots = item.get("slots")
            if not isinstance(slots, dict):
                continue
            value = slots.get(slot_key)
            if value not in (None, "", [], {}):
                return value
        return None

    @staticmethod
    def _normalize_memory_value(value: Any) -> str:
        if value in (None, "", [], {}):
            return ""
        if isinstance(value, list):
            parts = [GraphRouter._normalize_memory_value(v) for v in value if v not in (None, "", [], {})]
            parts = [p for p in parts if p]
            return "|".join(parts)
        return str(value).strip().lower()

    @staticmethod
    def _find_distinct_slot_in_turn_history(
        *,
        session_ctx: Dict[str, Any],
        slot_key: str,
        prefer_older: bool,
        exclude_values: list[Any] | None = None,
    ) -> Any:
        history = (session_ctx or {}).get("turn_history") if isinstance(session_ctx, dict) else None
        if not isinstance(history, list) or not history:
            return None
        search_space = history[:-1] if prefer_older and len(history) > 1 else history
        excluded = {
            GraphRouter._normalize_memory_value(v)
            for v in (exclude_values or [])
            if v not in (None, "", [], {})
        }
        for item in reversed(search_space):
            if not isinstance(item, dict):
                continue
            slots = item.get("slots")
            if not isinstance(slots, dict):
                continue
            value = slots.get(slot_key)
            if value in (None, "", [], {}):
                continue
            if GraphRouter._normalize_memory_value(value) in excluded:
                continue
            return value
        return None

    def n_load_session(self, state: GraphState) -> GraphState:
        from app.orchestration.handlers.session_handler import handle_load_session

        return handle_load_session(self, state)

    def _comparison_followup_override(
        self,
        *,
        session_ctx: Dict[str, Any],
        user_input: str,
        intent_key: str,
        extracted_slots: Dict[str, Any],
        backward_reference: bool,
    ) -> tuple[str, Dict[str, Any], str] | None:
        ui = (user_input or "").strip()
        ul = ui.lower()
        if not ui or not any(k in ul for k in get_comparison_followup_terms()):
            return None
        if not isinstance(session_ctx, dict):
            return None

        slots = dict(extracted_slots or {})
        sess_slots = session_ctx.get("memory_slots") or session_ctx.get("slots") or {}
        if not isinstance(sess_slots, dict):
            sess_slots = {}

        backward_markers = get_followup_backward_markers()
        prefer_older = backward_reference or any(marker in ul for marker in backward_markers)
        last_intent = str(session_ctx.get("last_intent") or session_ctx.get("last_intent_key") or "").strip().lower()

        session_family = None
        if last_intent.startswith("voyage."):
            session_family = "voyage"
        elif last_intent.startswith("vessel.") or last_intent == "ranking.vessel_metadata":
            session_family = "vessel"
        elif sess_slots.get("voyage_number") not in (None, "", [], {}) or sess_slots.get("voyage_numbers") not in (None, "", [], {}):
            session_family = "voyage"
        elif sess_slots.get("vessel_name") not in (None, "", [], {}) or sess_slots.get("imo") not in (None, "", [], {}):
            session_family = "vessel"

        if session_family == "voyage":
            explicit_numbers = slots.get("voyage_numbers")
            if not isinstance(explicit_numbers, list):
                explicit_numbers = []
            if not explicit_numbers and slots.get("voyage_number") not in (None, "", [], {}):
                explicit_numbers = [slots.get("voyage_number")]
            if not explicit_numbers:
                explicit_numbers = re.findall(r"\b\d{3,5}\b", ui)

            clean_numbers: list[int] = []
            for value in explicit_numbers:
                try:
                    ivalue = int(float(value))
                except Exception:
                    continue
                if ivalue not in clean_numbers:
                    clean_numbers.append(ivalue)

            current_voyage = sess_slots.get("voyage_number")
            if current_voyage in (None, "", [], {}) and isinstance(sess_slots.get("voyage_numbers"), list) and sess_slots.get("voyage_numbers"):
                current_voyage = sess_slots.get("voyage_numbers")[0]
            if current_voyage in (None, "", [], {}):
                current_voyage = self._find_slot_in_turn_history(
                    session_ctx=session_ctx,
                    slot_key="voyage_number",
                    prefer_older=False,
                )
            try:
                current_voyage = int(float(current_voyage)) if current_voyage not in (None, "", [], {}) else None
            except Exception:
                current_voyage = None

            previous_voyage = self._find_distinct_slot_in_turn_history(
                session_ctx=session_ctx,
                slot_key="voyage_number",
                prefer_older=True,
                exclude_values=[current_voyage],
            )
            try:
                previous_voyage = int(float(previous_voyage)) if previous_voyage not in (None, "", [], {}) else None
            except Exception:
                previous_voyage = None

            if len(clean_numbers) == 1 and current_voyage is not None and clean_numbers[0] != current_voyage:
                pair = [current_voyage, clean_numbers[0]]
                new_slots = dict(slots)
                new_slots.pop("voyage_number", None)
                new_slots["voyage_numbers"] = pair
                if sess_slots.get("scenario") not in (None, "", [], {}) and new_slots.get("scenario") in (None, "", [], {}):
                    new_slots["scenario"] = sess_slots.get("scenario")
                rewritten = f"Compare voyage {pair[0]} with voyage {pair[1]}. Original request: {ui}"
                return "comparison.voyages", new_slots, rewritten

            if not clean_numbers and current_voyage is not None and previous_voyage is not None:
                pair = [current_voyage, previous_voyage]
                new_slots = dict(slots)
                new_slots["voyage_numbers"] = pair
                if sess_slots.get("scenario") not in (None, "", [], {}) and new_slots.get("scenario") in (None, "", [], {}):
                    new_slots["scenario"] = sess_slots.get("scenario")
                rewritten = f"Compare voyage {pair[0]} with previously discussed voyage {pair[1]}. Original request: {ui}"
                return "comparison.voyages", new_slots, rewritten

        if session_family == "vessel":
            explicit_vessel = slots.get("vessel_name") or slots.get("imo")
            current_vessel = sess_slots.get("vessel_name") or sess_slots.get("imo")
            if current_vessel in (None, "", [], {}):
                current_vessel = self._find_slot_in_turn_history(
                    session_ctx=session_ctx,
                    slot_key="vessel_name",
                    prefer_older=False,
                ) or self._find_slot_in_turn_history(
                    session_ctx=session_ctx,
                    slot_key="imo",
                    prefer_older=False,
                )

            if explicit_vessel not in (None, "", [], {}):
                if (
                    current_vessel in (None, "", [], {})
                    or self._normalize_memory_value(current_vessel) == self._normalize_memory_value(explicit_vessel)
                    or prefer_older
                ):
                    current_vessel = self._find_distinct_slot_in_turn_history(
                        session_ctx=session_ctx,
                        slot_key="vessel_name",
                        prefer_older=prefer_older,
                        exclude_values=[explicit_vessel],
                    ) or self._find_distinct_slot_in_turn_history(
                        session_ctx=session_ctx,
                        slot_key="imo",
                        prefer_older=prefer_older,
                        exclude_values=[explicit_vessel],
                    )

                if (
                    current_vessel not in (None, "", [], {})
                    and self._normalize_memory_value(current_vessel) != self._normalize_memory_value(explicit_vessel)
                ):
                    rewritten = f"Compare vessel {current_vessel} with vessel {explicit_vessel}. Original request: {ui}"
                    return "comparison.vessels", dict(slots), rewritten

            older_vessel_markers = get_older_vessel_markers()
            if prefer_older and current_vessel not in (None, "", [], {}):
                previous_vessel = self._find_distinct_slot_in_turn_history(
                    session_ctx=session_ctx,
                    slot_key="vessel_name",
                    prefer_older=True,
                    exclude_values=[current_vessel],
                ) or self._find_distinct_slot_in_turn_history(
                    session_ctx=session_ctx,
                    slot_key="imo",
                    prefer_older=True,
                    exclude_values=[current_vessel],
                )
                if previous_vessel not in (None, "", [], {}):
                    rewritten = f"Compare vessel {current_vessel} with previously discussed vessel {previous_vessel}. Original request: {ui}"
                    return "comparison.vessels", dict(slots), rewritten
            if current_vessel not in (None, "", [], {}) and any(marker in ul for marker in older_vessel_markers):
                previous_vessel = self._find_distinct_slot_in_turn_history(
                    session_ctx=session_ctx,
                    slot_key="vessel_name",
                    prefer_older=True,
                    exclude_values=[current_vessel],
                ) or self._find_distinct_slot_in_turn_history(
                    session_ctx=session_ctx,
                    slot_key="imo",
                    prefer_older=True,
                    exclude_values=[current_vessel],
                )
                if previous_vessel not in (None, "", [], {}):
                    rewritten = f"Compare vessel {current_vessel} with previously discussed vessel {previous_vessel}. Original request: {ui}"
                    return "comparison.vessels", dict(slots), rewritten

        return None

    def n_extract_intent(self, state: GraphState) -> GraphState:
        from app.orchestration.handlers.intent_extraction_handler import handle_extract_intent

        return handle_extract_intent(self, state)

    def _maybe_override_incomplete_entity_intent(
        self,
        *,
        intent_key: str,
        extracted_slots: Dict[str, Any],
        user_input: str,
    ) -> tuple[str, Dict[str, Any]]:
        ui = (user_input or "").strip()
        ul = ui.lower()
        slots = dict(extracted_slots or {})
        placeholder_values = get_incomplete_entity_placeholder_slot_values()

        for slot_key, placeholders in placeholder_values.items():
            value = slots.get(slot_key)
            if isinstance(value, str) and value.strip().lower() in set(placeholders):
                slots.pop(slot_key, None)

        wants_details = any(
            p in ul
            for p in get_incomplete_entity_detail_markers()
        )

        has_voyage_anchor = bool(slots.get("voyage_number") or slots.get("voyage_numbers") or slots.get("voyage_id"))
        has_vessel_anchor = bool(slots.get("vessel_name") or slots.get("imo"))
        has_port_anchor = bool(slots.get("port_name"))
        is_fleet_port_aggregate = any(p in ul for p in get_incomplete_entity_fleet_port_aggregate_markers())
        topic_terms = get_incomplete_entity_topic_terms()
        topic_phrases = get_incomplete_entity_topic_phrases()

        # Port details intent ONLY when the user is asking about a port,
        # not when "ports" appears as an attribute of a voyage/vessel summary.
        # Safe because we only trigger it when there is no other entity anchor.
        port_topic = any(re.search(rf"\b{re.escape(term)}\b", ul) for term in topic_terms.get("port", []))
        if (
            port_topic
            and wants_details
            and not is_fleet_port_aggregate
            and not has_port_anchor
            and not has_voyage_anchor
            and not has_vessel_anchor
        ):
            return "port.details", slots

        # Vessel summary intent if user asks about "vessel/ship" without vessel_name/imo.
        vessel_topic = any(re.search(rf"\b{re.escape(term)}\b", ul) for term in topic_terms.get("vessel", [])) and any(
            p in ul for p in topic_phrases.get("vessel", [])
        )
        if vessel_topic and wants_details and not has_vessel_anchor and not has_voyage_anchor and not has_port_anchor:
            return "vessel.summary", slots

        # Voyage summary intent if user asks about "voyage" without voyage_number/id.
        voyage_topic = any(re.search(rf"\b{re.escape(term)}\b", ul) for term in topic_terms.get("voyage", [])) and any(
            p in ul for p in topic_phrases.get("voyage", [])
        )
        if voyage_topic and wants_details and not has_voyage_anchor and not has_vessel_anchor and not has_port_anchor:
            return "voyage.summary", slots

        return intent_key, slots

    @staticmethod
    def _looks_like_new_question(user_input: str) -> bool:
        ui = (user_input or "").strip()
        if not ui:
            return False
        ul = ui.lower()
        if "?" in ui:
            return True
        # Common question/command verbs that should not be treated as a slot value.
        markers = (
            "tell me",
            "what is",
            "what's",
            "who is",
            "how ",
            "show ",
            "list ",
            "find ",
            "top ",
            "rank",
            "compare",
            "summarize",
            "summary",
            "details",
            "information",
            "explain",
        )
        if any(m in ul for m in markers):
            return True
        # Long-ish inputs are likely a question, not just a value.
        if len(ui.split()) > 4:
            return True
        return False

    @staticmethod
    def _try_resolve_missing_from_text(
        *,
        intent_key: str,
        missing_keys: list[str],
        user_input: str,
        options: Dict[str, Any] | None = None,
    ) -> Dict[str, Any] | None:
        ui = (user_input or "").strip()
        if not ui:
            return None

        ul = ui.lower()
        slots: Dict[str, Any] = {}
        opts = options or {}

        # Numeric selection (1-based) from prior suggestions.
        idx = None
        if ui.isdigit() and len(ui) <= 2:
            try:
                idx = int(ui) - 1
            except Exception:
                idx = None

        def _pick(key: str) -> Any:
            if idx is None:
                return None
            arr = opts.get(key)
            if isinstance(arr, list) and 0 <= idx < len(arr):
                return arr[idx]
            return None

        # A user might respond with a full sentence ("Port is Rotterdam").
        # We'll do simple extraction by missing slot type.
        if "voyage_number" in missing_keys:
            picked = _pick("voyage_number") or _pick("voyage_numbers")
            if picked is not None:
                try:
                    slots["voyage_number"] = int(picked)
                except Exception:
                    pass
            m = re.search(r"\b(\d{3,5})\b", ui)
            if "voyage_number" not in slots and not m:
                return None
            if "voyage_number" not in slots and m:
                slots["voyage_number"] = int(m.group(1))

        if "port_name" in missing_keys:
            picked = _pick("port_name")
            if picked:
                slots["port_name"] = str(picked).strip()
                return slots

            # Do not treat an incomplete question as a port name (e.g. "tell me about port").
            if any(p in ul for p in get_incomplete_entity_question_markers()):
                # If they provided "port <name>" inside the sentence, we still accept it.
                m = re.search(r"\bport\s+([A-Za-z].+)$", ui, flags=re.IGNORECASE)
                if not m:
                    return None
                cand = m.group(1)
            else:
                # Try patterns like "port Rotterdam" / "Rotterdam"
                m = re.search(r"\bport\s+([A-Za-z].+)$", ui, flags=re.IGNORECASE)
                cand = (m.group(1) if m else ui)

            cand = (cand or "").strip().strip("\"'").strip()
            # Reject if still looks like the user is asking about "port" with no name.
            if cand.lower() in {"port", "a port", "the port"}:
                return None
            if "port" in cand.lower() and cand.lower().endswith("port"):
                return None
            if not cand or len(cand) > 80:
                return None
            slots["port_name"] = cand

        if "vessel_name" in missing_keys or "imo" in missing_keys:
            picked = _pick("vessel_name")
            if picked:
                slots["vessel_name"] = str(picked).strip()
                return slots

            # If user gave an IMO, accept it; else treat as vessel name.
            m = re.search(r"\b(\d{7})\b", ui)
            if m:
                slots["imo"] = m.group(1)
            else:
                # Avoid absorbing an incomplete question as the vessel name.
                if any(p in ul for p in get_incomplete_entity_question_markers()):
                    # Allow patterns like "vessel Stena Important"
                    m2 = re.search(r"\b(vessel|ship)\s+([A-Za-z].+)$", ui, flags=re.IGNORECASE)
                    if not m2:
                        return None
                    cand2 = (m2.group(2) or "").strip()
                    if not cand2:
                        return None
                    slots["vessel_name"] = cand2.strip().strip("\"'")
                else:
                    if ui.isdigit():
                        return None
                    if not (2 <= len(ui) <= 60):
                        return None
                    slots["vessel_name"] = ui.strip().strip("\"'")

        if "cargo_type" in missing_keys:
            if len(ui) > 60:
                return None
            slots["cargo_type"] = ui.strip()

        return slots if slots else None

    @staticmethod
    def _is_chitchat(user_input: str) -> bool:
        """
        Detect greetings / identity / help messages that should never be treated as
        "follow-ups" inheriting prior entity anchors (voyage/vessel/port).
        """
        ui = (user_input or "").strip()
        if not ui:
            return True
        ul = ui.lower()

        # Common greetings
        greetings = {
            "hi", "hello", "hey", "hiya", "yo",
            "good morning", "good afternoon", "good evening",
        }
        if ul in greetings:
            return True
        if any(ul.startswith(p) for p in ("hi ", "hello ", "hey ")):
            return True

        # Help / onboarding
        if ul in {"help", "start", "menu", "commands"}:
            return True

        # Identity / capability questions
        identity_phrases = (
            "who are you",
            "who r you",
            "who are u",
            "who r u",
            "what are you",
            "what are u",
            "what can you do",
            "what can u do",
            "what do you do",
            "what do u do",
        )
        if any(p in ul for p in identity_phrases):
            return True

        return False

    @staticmethod
    def _looks_like_explicit_fresh_entity_request(user_input: str) -> bool:
        """
        Detect obviously fresh entity-anchored asks so they do not get swallowed by
        result-set follow-up logic.
        """
        ui = (user_input or "").strip()
        if not ui:
            return False
        ul = ui.lower()

        if re.search(r"\b(?:for\s+)?voyage\s+\d{3,5}\b", ul):
            return True
        if re.search(r"\b(?:vessel|ship)\s+[A-Za-z0-9][A-Za-z0-9 .'\-]{1,60}\b", ul):
            return True
        if GraphRouter._explicit_vessel_mentions(user_input):
            return True
        if re.search(r"\bimo\s+\d{6,8}\b", ul):
            return True

        # "Does Stena Imperial have ..." — uppercase ship name after auxiliary (not "Does the fleet").
        if re.match(r"^(does|do|has|have|can|will)\s+[A-Z]", ui.strip()):
            return True

        if re.match(r"^(is|how is)\s+(?!it\b|this\b|that\b|these\b|those\b|them\b|they\b)[a-z0-9][a-z0-9 .'\-]{3,}", ul):
            return True

        if (
            re.match(r"^(which|what|show|list|give|tell|find|compare)\s+(?:voyage|voyages|vessel|vessels|port|ports)\b", ul)
            and not any(p in ul for p in ("among these", "among them", "from above", "in that list", "in the list", "those voyages", "these voyages"))
        ):
            return True
        if (
            re.match(r"^(ports?\s+with|cargo grades?\s+with|cargo grades?\s+|ports?\s+appear|what are the top\s+\d+\s+cargo grades?)", ul)
            and not any(p in ul for p in ("among these", "among them", "from above", "in that list", "in the list", "those voyages", "these voyages"))
        ):
            return True

        # Proper-name style asks such as "Is Stena Superior ...".
        if re.match(r"^(?:is|how\s+(?:is|do|does))\s+[A-Z][A-Za-z0-9&'\-]+(?:\s+[A-Z][A-Za-z0-9&'\-]+){1,3}\b", ui):
            return True
        if ul.startswith(get_proper_name_request_prefixes()):
            name_match = re.search(r"\b([A-Z][a-zA-Z0-9&'\-]+(?:\s+[A-Z][a-zA-Z0-9&'\-]+){1,3})\b", ui)
            if name_match and not re.search(r"\b(What|Which|Show|Give|Tell|Compare|For|Is|How)\b", name_match.group(1)):
                return True

        return False

    @staticmethod
    def _result_row_label(row: Dict[str, Any]) -> str:
        if not isinstance(row, dict):
            return "N/A"
        for key in (
            "voyage_number",
            "vessel_name",
            "module_type",
            "port_name",
            "cargo_grade",
            "segment",
            "time_bucket",
            "month",
            "entity_id",
            "voyage_id",
        ):
            val = row.get(key)
            if val not in (None, "", [], {}):
                return str(val)
        return "N/A"

    @staticmethod
    def _row_focus_slots(row: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(row, dict):
            return {}
        out: Dict[str, Any] = {}
        for key in ("voyage_number", "voyage_id", "vessel_name", "vessel_imo", "imo", "port_name", "module_type", "time_bucket", "month"):
            val = row.get(key)
            if val in (None, "", [], {}):
                continue
            if key == "vessel_imo":
                out["imo"] = val
            else:
                out[key] = val
        return out

    @staticmethod
    def _result_row_metric_value(row: Dict[str, Any], metric: str) -> float | None:
        if not isinstance(row, dict):
            return None

        alias_map = {
            "expense": "total_expense",
            "cost": "total_expense",
            "commission": "total_commission",
            "profit": "pnl",
            "loss": "pnl",
            "average revenue": "avg_revenue",
            "avg_revenue": "avg_revenue",
            "average_revenue": "avg_revenue",
            "average pnl": "avg_pnl",
            "avg_pnl": "avg_pnl",
            "average_pnl": "avg_pnl",
            "average tce": "avg_tce",
            "avg_tce": "avg_tce",
            "average_tce": "avg_tce",
            "total_pnl": "total_pnl",
            "voyage_count": "voyage_count",
            "count": "voyage_count",
            "port_count": "port_calls",
            "port_counts": "port_calls",
            "ratio": "expense_to_revenue_ratio",
            "expense_ratio": "expense_to_revenue_ratio",
            "expense_to_revenue_ratio": "expense_to_revenue_ratio",
            "average_demurrage_wait_time": "avg_offhire_days",
            "demurrage_wait_time": "avg_offhire_days",
        }
        metric = alias_map.get(str(metric or "").strip().lower(), str(metric or "").strip().lower())

        def _num(x: Any) -> float | None:
            try:
                if x is None:
                    return None
                if isinstance(x, (int, float)):
                    return float(x)
                return float(str(x).replace(",", "").strip())
            except Exception:
                return None

        if metric == "expense_to_revenue_ratio":
            exp = _num(row.get("expense_to_revenue_ratio"))
            if exp is not None:
                return exp
            revenue = _num(row.get("revenue") if row.get("revenue") is not None else row.get("avg_revenue"))
            total_expense = _num(
                row.get("total_expense")
                if row.get("total_expense") is not None
                else row.get("avg_total_expense")
            )
            if revenue not in (None, 0) and total_expense is not None:
                return float(total_expense) / float(revenue)
            return None

        if metric == "avg_pnl":
            return _num(row.get("avg_pnl") if row.get("avg_pnl") is not None else row.get("pnl"))
        if metric == "avg_revenue":
            return _num(row.get("avg_revenue") if row.get("avg_revenue") is not None else row.get("revenue"))
        if metric == "avg_tce":
            return _num(row.get("avg_tce") if row.get("avg_tce") is not None else row.get("tce"))
        if metric == "total_pnl":
            return _num(row.get("total_pnl") if row.get("total_pnl") is not None else row.get("pnl"))
        if metric == "voyage_count":
            return _num(
                row.get("voyage_count")
                if row.get("voyage_count") is not None
                else (row.get("finance_voyage_count") if row.get("finance_voyage_count") is not None else row.get("ops_voyage_count"))
            )
        if metric == "avg_offhire_days":
            return _num(
                row.get("avg_offhire_days")
                if row.get("avg_offhire_days") is not None
                else row.get("offhire_days")
            )
        if metric == "variance":
            for key in ("variance_diff", "pnl_variance", "tce_variance"):
                val = _num(row.get(key))
                if val is not None:
                    return abs(val)
            return None

        return _num(row.get(metric))

    @staticmethod
    def _corpus_fresh_guard_flags(ul: str) -> tuple[bool, bool]:
        """
        Returns (phase4a_ranking_or_topn, corpus_broad_guard).

        ``corpus_broad_guard`` is True for fresh-looking fleet/corpus asks (new-request
        prefix plus ranking phrase, top/bottom-N pattern, or a longer question). Used to
        block stale ``last_result_set`` hijacks and spurious structured follow-up scope.

        ``phase4a_ranking_or_topn`` is the narrower guard reused for LLM intent remaps.
        """
        ul = (ul or "").strip().lower()
        words = [w for w in ul.split() if w]
        starts = ul.startswith(get_new_request_prefixes())
        phase4a = starts and (
            any(phrase in ul for phrase in get_fresh_ranking_phrases())
            or (
                any(word in ul for word in get_fresh_ranking_trend_words())
                and any(word in ul for word in get_fresh_ranking_trend_context_words())
            )
            or bool(re.search(r"\b(top|bottom)\s+\d{1,2}\b", ul))
        )
        corpus = phase4a or (starts and len(words) > 10)
        return phase4a, corpus

    @staticmethod
    def _routing_yaml_token_hit(ul: str, term: str) -> bool:
        """Match YAML routing tokens without substring traps (e.g. ``include`` vs ``including``)."""
        t = (term or "").strip().lower()
        if not t:
            return False
        if " " in t:
            return t in ul
        return re.search(rf"(?<![a-z0-9]){re.escape(t)}(?![a-z0-9])", ul, flags=re.I) is not None

    @staticmethod
    def _yaml_term_list_hit(ul: str, terms) -> bool:
        for raw in terms:
            if GraphRouter._routing_yaml_token_hit(ul, str(raw)):
                return True
        return False

    @staticmethod
    def _generic_followup_markers_hit(ul: str) -> bool:
        return GraphRouter._yaml_term_list_hit(ul, get_generic_followup_markers())

    @staticmethod
    def _ops_only_voyage_snapshot_markdown(ops_safe: dict) -> str:
        """Deterministic ops-only lines for RBAC tenants without finance KPI (no LLM)."""

        rows = ops_safe.get("rows") if isinstance(ops_safe, dict) else None
        if not isinstance(rows, list) or not rows:
            return ""
        r0 = rows[0]
        if not isinstance(r0, dict):
            return ""
        r = copy.deepcopy(r0)
        shrink_ops_row_json_fields(r, voyage_summary=True)
        lines: list[str] = ["### Operations snapshot (this voyage)"]
        for label, key in (
            ("Voyage", "voyage_number"),
            ("Vessel", "vessel_name"),
            ("Module type", "module_type"),
            ("Offhire days", "offhire_days"),
            ("Delayed", "is_delayed"),
            ("Delay reason", "delay_reason"),
        ):
            val = r.get(key)
            if val not in (None, "", [], {}):
                lines.append(f"- **{label}:** {val}")
        ports = r.get("ports")
        if isinstance(ports, list) and ports:
            preview = ", ".join(str(p) for p in ports[:15])
            more = len(ports) - 15
            suffix = f" (+{more} more)" if more > 0 else ""
            lines.append(f"- **Key ports:** {preview}{suffix}")
        grades = r.get("cargo_grade_names")
        if isinstance(grades, list) and grades:
            preview = ", ".join(str(g) for g in grades[:20])
            more = len(grades) - 20
            suffix = f" (+{more} more)" if more > 0 else ""
            lines.append(f"- **Cargo grades:** {preview}{suffix}")
        rem = r.get("remarks_preview")
        if isinstance(rem, list) and rem:
            lines.append("- **Remarks (excerpt):**")
            for t in rem[:3]:
                if str(t).strip():
                    lines.append(f"  - {str(t).strip()[:500]}")
        return "\n".join(lines) if len(lines) > 1 else ""

    @staticmethod
    def _classify_turn_type(*, session_ctx: Dict[str, Any], user_input: str) -> str:
        """
        Classify the incoming message as:
          - "new_question": independent query; do NOT inherit prior anchors.
          - "followup": conversational continuation; safe to apply memory.

        Clarification replies are handled earlier in n_extract_intent when pending_* exists.
        """
        ui = (user_input or "").strip()
        ul = ui.lower()
        words = [w for w in re.split(r"\s+", ul) if w]

        if GraphRouter._is_chitchat(ui):
            return "new_question"

        if GraphRouter._looks_like_explicit_fresh_entity_request(ui):
            return "new_question"

        # Explicit references to previous result sets are follow-ups.
        explicit_rs_ref = any(
            p in ul
            for p in (
                "among these",
                "among them",
                "among this",
                "from above",
                "from the above",
                "from this",
                "in that list",
                "in the list",
                "above list",
                "the above",
                "those voyages",
                "these voyages",
            )
        )
        if explicit_rs_ref:
            return "followup"

        fresh_fleet_markers = (
            "across all",
            "across fleet",
            "fleet-wide",
            "all voyages",
            "all vessels",
            "each vessel",
            "each port",
            "each cargo grade",
            "per vessel",
            "per port",
            "per cargo grade",
            "by vessel",
            "by voyage",
            "by port",
            "for all vessels",
            "for all voyages",
            "whole fleet",
            "entire fleet",
        )
        if any(marker in ul for marker in fresh_fleet_markers):
            return "new_question"

        # Detect explicit entity family mention in this turn.
        mentions_vessel = bool(re.search(r"\b(vessel|ship|imo)\b", ul))
        mentions_voyage = bool(re.search(r"\b(voyage|voyages)\b", ul))
        mentions_port = bool(re.search(r"\b(port|ports)\b", ul))
        explicit_family = (
            "vessel" if mentions_vessel else
            "voyage" if mentions_voyage else
            "port" if mentions_port else
            None
        )

        # Detect current session anchor family.
        # Prefer last_intent family because memory slots may contain both voyage + vessel
        # after an enriched summary turn.
        sess_slots = {}
        last_intent = ""
        if isinstance(session_ctx, dict):
            sess_slots = session_ctx.get("memory_slots") or session_ctx.get("slots") or {}
            li = session_ctx.get("last_intent")
            if isinstance(li, str):
                last_intent = li.strip().lower()
        has_result_set = isinstance(session_ctx, dict) and isinstance(session_ctx.get("last_result_set"), dict)
        if not isinstance(sess_slots, dict):
            sess_slots = {}
        rs_rows = []
        if has_result_set:
            rs = session_ctx.get("last_result_set") if isinstance(session_ctx, dict) else {}
            if isinstance(rs, dict) and isinstance(rs.get("rows"), list):
                rs_rows = rs.get("rows") or []

        session_family = None
        if last_intent.startswith("voyage."):
            session_family = "voyage"
        elif last_intent.startswith("vessel."):
            session_family = "vessel"
        elif last_intent in ("ranking.vessels", "ranking.vessel_metadata"):
            session_family = "vessel"
        elif last_intent.startswith("ops.port_") or last_intent.startswith("port."):
            session_family = "port"
        elif sess_slots.get("vessel_name") or sess_slots.get("imo"):
            session_family = "vessel"
        elif sess_slots.get("voyage_number") or sess_slots.get("voyage_numbers") or sess_slots.get("voyage_id"):
            session_family = "voyage"
        elif sess_slots.get("port_name"):
            session_family = "port"
        elif rs_rows:
            first_row = rs_rows[0] if isinstance(rs_rows[0], dict) else {}
            if isinstance(first_row, dict):
                if first_row.get("voyage_number") not in (None, "", [], {}) or first_row.get("voyage_id") not in (None, "", [], {}):
                    session_family = "voyage"
                elif first_row.get("vessel_name") not in (None, "", [], {}) or first_row.get("imo") not in (None, "", [], {}) or first_row.get("vessel_imo") not in (None, "", [], {}):
                    session_family = "vessel"

        # Topic switch guardrail between voyage/vessel families.
        # Port mentions can still be contextual follow-ups on a voyage/vessel thread.
        if (
            explicit_family in ("voyage", "vessel")
            and session_family in ("voyage", "vessel")
            and explicit_family != session_family
        ):
            return "new_question"

        # Value-like turns are usually continuation replies ("2301", "2", "Rotterdam").
        # Keep short text replies follow-up unless they clearly look like a fresh ask.
        is_value_like = bool(
            re.fullmatch(r"\d{1,5}", ui)
            or (1 <= len(words) <= 3 and "?" not in ui and not any(w in ul for w in get_value_like_fresh_words()))
        )
        if is_value_like:
            return "followup"

        # Long imperative asks are usually fresh questions, even if they contain words
        # like "include/explain" that can appear in follow-ups.
        if len(words) > 8 and ul.startswith(get_long_new_question_prefixes()):
            return "new_question"

        _, _corpus_cls = GraphRouter._corpus_fresh_guard_flags(ul)

        has_coref = any(w in words for w in get_coreference_words())
        if GraphRouter._generic_followup_markers_hit(ul) or has_coref:
            return "followup"

        # If we already have a result set, allow concise refinement operations as follow-ups.
        if has_result_set and len(words) <= 10 and any(k in ul for k in get_result_set_refinement_extreme_terms()):
            return "followup"

        # Generic operation asks over an existing result set should remain follow-ups
        # even when phrased as longer natural sentences.
        if (
            has_result_set
            and GraphRouter._yaml_term_list_hit(ul, get_result_set_operation_verbs())
            and GraphRouter._yaml_term_list_hit(ul, get_result_set_operation_fields())
            and len(words) <= 14
            and not _corpus_cls
        ):
            return "followup"

        # Short contextual asks after an entity-scoped turn should be treated as follow-ups
        # even when phrased as commands (e.g. "list all key ports", "show remarks").
        if (
            session_family in ("voyage", "vessel", "port")
            and len(words) <= 7
            and any(k in ul for k in get_short_contextual_followup_fields())
        ):
            return "followup"

        # Fresh ask markers -> new question.
        starts_like_new_request = ul.startswith(get_new_request_prefixes())
        if starts_like_new_request or GraphRouter._looks_like_new_question(ui):
            return "new_question"

        # Default to follow-up only for short context-carrying turns; else new question.
        return "followup" if len(words) <= 6 else "new_question"

    @staticmethod
    def _direct_session_thread_override(*, session_ctx: Dict[str, Any], user_input: str) -> tuple[str, Dict[str, Any]] | None:
        """
        Force clear entity-thread follow-ups to stay on the active voyage/vessel thread
        before generic result-set logic or fresh LLM classification can hijack them.
        """
        if not isinstance(session_ctx, dict):
            return None

        ui = (user_input or "").strip()
        ul = ui.lower()
        if not ul or GraphRouter._is_chitchat(ui):
            return None
        if len(GraphRouter._explicit_vessel_mentions(ui)) >= 2:
            return None
        if GraphRouter._looks_like_explicit_fresh_entity_request(ui):
            # Allow obvious thread references like "this voyage" / "this vessel" to continue.
            if not any(p in ul for p in get_direct_thread_reference_phrases()):
                return None

        sess_slots = session_ctx.get("memory_slots") or session_ctx.get("slots") or {}
        if not isinstance(sess_slots, dict):
            return None

        mentions_one = GraphRouter._explicit_vessel_mentions(ui)
        if len(mentions_one) == 1:
            m0 = mentions_one[0].strip().lower()
            sess_vn = str(sess_slots.get("vessel_name") or "").strip().lower()
            if sess_vn and m0 != sess_vn:
                return None

        family = None
        if sess_slots.get("voyage_number") or sess_slots.get("voyage_id") or sess_slots.get("voyage_numbers"):
            family = "voyage"
        elif sess_slots.get("vessel_name") or sess_slots.get("imo"):
            family = "vessel"
        else:
            last_intent = str(session_ctx.get("last_intent") or session_ctx.get("last_intent_key") or "").strip().lower()
            if last_intent.startswith("voyage."):
                family = "voyage"
            elif last_intent.startswith("vessel.") or last_intent == "ranking.vessel_metadata":
                family = "vessel"
        if family is None:
            return None

        if any(m in ul for m in get_thread_override_fleet_switch_markers()):
            return None

        rankingish = any(k in ul for k in get_thread_override_ranking_terms())
        explicit_entity_thread_markers = get_thread_override_explicit_entity_markers()

        keyword_hit = any(p in ul for p in get_thread_override_referential_markers())
        if family == "voyage":
            keyword_hit = keyword_hit or any(k in ul for k in get_thread_override_voyage_keywords())
        else:
            keyword_hit = keyword_hit or any(k in ul for k in get_thread_override_vessel_keywords())
        if not keyword_hit:
            return None

        if rankingish and not any(m in ul for m in explicit_entity_thread_markers):
            return None

        out_slots: Dict[str, Any] = {}
        if family == "voyage":
            for key in ("voyage_number", "voyage_numbers", "voyage_id"):
                val = sess_slots.get(key)
                if val not in (None, "", [], {}):
                    out_slots[key] = val
            if sess_slots.get("vessel_name") not in (None, "", [], {}):
                out_slots["vessel_name"] = sess_slots.get("vessel_name")
            if sess_slots.get("imo") not in (None, "", [], {}):
                out_slots["imo"] = sess_slots.get("imo")
            return (
                "voyage.metadata"
                if any(k in ul for k in get_thread_override_voyage_metadata_terms())
                else "voyage.summary",
                out_slots,
            )

        for key in ("vessel_name", "imo"):
            val = sess_slots.get(key)
            if val not in (None, "", [], {}):
                out_slots[key] = val
        return (
            "vessel.metadata"
            if any(k in ul for k in get_thread_override_vessel_metadata_terms())
            else "vessel.summary",
            out_slots,
        )

    @staticmethod
    def _parse_result_set_followup_action(*, user_input: str, session_ctx: Dict[str, Any]) -> Dict[str, Any] | None:
        """
        Parse follow-up operations over the previous result set in a query-agnostic way.
        Returns a normalized action payload or None.
        """
        ul = (user_input or "").strip().lower()
        if not ul:
            return None

        wc = len(ul.split())
        if wc > 22:
            return None
        if wc > 12 and ul.startswith(get_new_request_prefixes()):
            return None

        _sctx_rs = session_ctx if isinstance(session_ctx, dict) else {}
        _si_rs = _sctx_rs.get("_structured_intent")
        _skip_corpus_guard = (
            isinstance(_si_rs, dict)
            and _si_rs.get("confidence") in ("high", "medium")
            and str(_si_rs.get("scope") or "").strip().lower() in ("follow_up", "followup")
        )

        # Corpus-wide / distribution questions are not refinements over the last table.
        if not _skip_corpus_guard:
            if re.search(r"\bacross all (voyages|vessels|ports)\b", ul):
                return None
            if "most frequently" in ul or "most often" in ul:
                return None
            if re.search(r"\b(all|every)\s+voyages\b", ul) and not any(
                p in ul for p in get_corpus_guard_result_set_reference_phrases()
            ):
                return None

        # After a multi-row scenario comparison, "TCE variance only" should list every row — not last_focus.
        if ("tce variance" in ul or "pnl variance" in ul) and any(p in ul for p in ("only", "just")):
            if not any(p in ul for p in ("that one", "this one", "that voyage", "this voyage", "that row", "this row")):
                if "pnl variance" in ul and "tce variance" not in ul:
                    return {"action": "compare_metrics", "metrics": ["pnl_variance"]}
                if "tce variance" in ul:
                    return {"action": "compare_metrics", "metrics": ["tce_variance"]}

        rs = (session_ctx or {}).get("last_result_set") if isinstance(session_ctx, dict) else None
        rows = (rs or {}).get("rows") if isinstance(rs, dict) else None
        rows = rows if isinstance(rows, list) else []
        rs_meta = (rs or {}).get("meta") if isinstance(rs, dict) and isinstance((rs or {}).get("meta"), dict) else {}

        def _has_field(field: str) -> bool:
            for r in rows[:20]:
                if isinstance(r, dict) and GraphRouter._result_row_metric_value(r, field) is not None:
                    return True
                if isinstance(r, dict) and r.get(field) not in (None, "", [], {}):
                    return True
                if field == "cargo_grades" and isinstance(r, dict) and r.get("most_common_grade") not in (None, "", [], {}):
                    return True
            return False

        def _result_set_fields() -> set[str]:
            fields: set[str] = set()
            for r in rows[:30]:
                if not isinstance(r, dict):
                    continue
                for k, v in r.items():
                    if k in (None, ""):
                        continue
                    if v in (None, "", [], {}):
                        continue
                    fields.add(str(k))
            return fields

        def _infer_followup_fields() -> list[str]:
            available = _result_set_fields()
            if not available:
                return []
            selected: list[str] = []
            norm_text = " ".join(re.findall(r"[a-z0-9_]+", ul))
            for f in sorted(available):
                if f in {"voyage_id", "entity_id"}:
                    continue
                key = str(f).strip().lower()
                label = key.replace("_", " ")
                if key in norm_text or label in norm_text:
                    selected.append(key)
            return selected

        def _first_matching_config_key(alias_map: dict[str, list[str]]) -> str | None:
            for target, aliases in alias_map.items():
                if any(alias in ul for alias in aliases):
                    return target
            return None

        singular_selection_markers = get_singular_selection_markers()
        if (
            _first_matching_config_key(get_result_set_projection_field_terms()) is not None
            and any(k in ul for k in get_result_set_metric_followup_exclusion_terms())
            and any(marker in ul for marker in singular_selection_markers)
        ):
            field = _first_matching_config_key(get_result_set_projection_field_terms()) or "remarks"
            metric = _first_matching_config_key(get_result_set_extreme_metric_aliases())
            if metric is None:
                preferred = str(rs_meta.get("primary_metric") or "").strip().lower()
                metric = preferred if preferred and _has_field(preferred) else "pnl"
            extreme = "low" if any(k in ul for k in get_result_set_low_extreme_terms()) else "high"
            if field == "remarks" and not _has_field("remarks"):
                return None
            if field == "cargo_grades" and not _has_field("cargo_grades"):
                return None
            if field == "key_ports" and not _has_field("key_ports"):
                return None
            return {"action": "project_extreme_field", "field": field, "metric": metric, "extreme": extreme}

        if (
            any(p in ul for p in get_selected_row_reference_phrases())
            and _first_matching_config_key(get_result_set_projection_field_terms()) is not None
        ):
            selector = "last_focus"
            if any(p in ul for p in get_first_selector_phrases()):
                selector = "first"
            elif any(p in ul for p in get_last_selector_phrases()):
                selector = "last"
            field = _first_matching_config_key(get_result_set_projection_field_terms()) or "remarks"
            return {"action": "project_selected_field", "field": field, "selector": selector}

        if any(p in ul for p in get_selected_row_reference_phrases()):
            selector = "last_focus"
            if any(p in ul for p in get_first_selector_phrases()):
                selector = "first"
            elif any(p in ul for p in get_last_selector_phrases()):
                selector = "last"
            for metric, aliases in get_result_set_selected_metric_aliases().items():
                if any(raw_key in ul for raw_key in aliases) and any(k in ul for k in get_result_set_selected_metric_triggers()):
                    return {"action": "project_selected_metric", "metric": metric, "selector": selector}

        if (
            len(ul.split()) <= 10
            and _first_matching_config_key(get_result_set_selected_metric_aliases()) is not None
            and any(k in ul for k in get_result_set_selected_metric_context_terms())
            and not any(k in ul for k in get_result_set_metric_followup_exclusion_terms())
            and not any(k in ul for k in get_result_set_corpus_followup_exclusion_terms())
        ):
            metric = _first_matching_config_key(get_result_set_selected_metric_aliases()) or "revenue"
            if "variance" in ul and metric not in ("pnl_variance", "tce_variance"):
                preferred = str(rs_meta.get("primary_metric") or "").strip().lower()
                metric = preferred if preferred else "variance_diff"
            return {"action": "project_selected_metric", "metric": metric, "selector": "last_focus"}

        if (
            len(ul.split()) <= 6
            and _first_matching_config_key(get_result_set_short_projection_field_terms()) is not None
            and any(k in ul for k in get_result_set_selected_metric_context_terms())
            and not any(k in ul for k in get_result_set_metric_followup_exclusion_terms())
            and not any(k in ul for k in get_result_set_corpus_followup_exclusion_terms())
        ):
            field = _first_matching_config_key(get_result_set_short_projection_field_terms()) or "remarks"
            return {"action": "project_selected_field", "field": field, "selector": "last_focus"}

        inferred_fields = _infer_followup_fields()
        if (
            inferred_fields
            and any(k in ul for k in get_result_set_list_fields_context_terms())
            and not any(
                k in ul
                for k in get_result_set_metric_followup_exclusion_terms()
                + get_result_set_list_fields_extra_exclusion_terms()
            )
        ):
            return {"action": "list_fields", "fields": inferred_fields[:6]}

        if ("remarks" in ul or "remark" in ul) and any(p in ul for p in ("had remarks", "have remarks", "with remarks", "has remarks")):
            return {"action": "filter_has_field", "field": "remarks"}

        # Per-row remarks projection ("remarks of each", "show/list/give remarks ...").
        if (
            "remark" in ul
            and (
                ul.startswith(("show remarks", "list remarks", "give remarks"))
                or "remarks of each" in ul
                or "remarks for each" in ul
                or "remarks of every" in ul
            )
        ):
            return {"action": "remarks_each"}

        # Per-row key ports projection.
        if "port" in ul and any(k in ul for k in ("all key ports", "key ports", "list ports", "list all ports", "show ports")):
            return {"action": "list_ports"}

        # Per-row cargo grades projection.
        if (
            ("cargo" in ul and ("grade" in ul or "grades" in ul or "gardes" in ul))
            or "cargo grades" in ul
            or "list all cargo grades" in ul
        ):
            if any(k in ul for k in ("list", "show", "give", "each", "every", "all")) and not any(
                k in ul for k in ("top", "bottom", "highest", "lowest", "best", "worst", "most", "least")
            ):
                return {"action": "list_cargo_grades"}

        # Metric-only comparison/projection from current set.
        if (
            "pnl" in ul
            and "tce" in ul
            and any(k in ul for k in ("compare", "only", "just", "show"))
            and not re.search(r"\b\d{3,5}\b", ul)
        ):
            return {"action": "compare_metrics", "metrics": ["pnl", "tce"]}

        # Generic threshold filtering over current result set.
        metric_alias = {
            "pnl": "pnl",
            "tce": "tce",
            "revenue": "revenue",
            "avg revenue": "avg_revenue",
            "average revenue": "avg_revenue",
            "avg pnl": "avg_pnl",
            "average pnl": "avg_pnl",
            "expense": "total_expense",
            "cost": "total_expense",
            "expense ratio": "expense_to_revenue_ratio",
            "expense-to-revenue ratio": "expense_to_revenue_ratio",
            "expense to revenue ratio": "expense_to_revenue_ratio",
            "commission": "total_commission",
            "offhire": "offhire_days",
            "off hire": "offhire_days",
            "offhire days": "offhire_days",
            "voyage count": "voyage_count",
            "count": "voyage_count",
            "variance": "variance",
            "pnl variance": "pnl_variance",
            "tce variance": "tce_variance",
        }
        mth = re.search(
            r"\b(pnl|tce|revenue|avg\s+revenue|average\s+revenue|avg\s+pnl|average\s+pnl|expense(?:\s*to\s*revenue\s*ratio)?|cost|commission|voyage\s+count|count|variance|pnl\s+variance|tce\s+variance|off\s*hire|offhire(?:\s*days?)?)\b[^\n]*?(>=|<=|>|<|=)\s*(-?\d+(?:\.\d+)?)",
            ul,
        )
        if mth:
            raw_metric = re.sub(r"\s+", " ", str(mth.group(1) or "").strip())
            metric = metric_alias.get(raw_metric, metric_alias.get(raw_metric.replace("  ", " "), raw_metric))
            return {"action": "filter_by_metric_threshold", "metric": metric, "operator": mth.group(2), "value": float(mth.group(3))}
        mth_words = re.search(
            r"\b(pnl|tce|revenue|avg\s+revenue|average\s+revenue|avg\s+pnl|average\s+pnl|expense(?:\s*to\s*revenue\s*ratio)?|cost|commission|voyage\s+count|count|variance|pnl\s+variance|tce\s+variance|off\s*hire|offhire(?:\s*days?)?)\b[^\n]*?\b(above|over|greater than|below|under|less than)\b\s*(-?\d+(?:\.\d+)?)",
            ul,
        )
        if mth_words:
            raw_metric = re.sub(r"\s+", " ", str(mth_words.group(1) or "").strip())
            metric = metric_alias.get(raw_metric, metric_alias.get(raw_metric.replace("  ", " "), raw_metric))
            op_word = str(mth_words.group(2) or "").strip()
            operator = ">" if op_word in ("above", "over", "greater than") else "<"
            return {"action": "filter_by_metric_threshold", "metric": metric, "operator": operator, "value": float(mth_words.group(3))}

        if "changed the most" in ul:
            preferred_metric = str(rs_meta.get("primary_metric") or "").strip().lower()
            if preferred_metric in ("pnl_variance", "tce_variance", "variance", "variance_diff") and _has_field(preferred_metric):
                metric = preferred_metric
            elif _has_field("pnl_variance"):
                metric = "pnl_variance"
            elif _has_field("tce_variance"):
                metric = "tce_variance"
            else:
                metric = "variance"
            return {"action": "compare_extremes", "metric": metric}

        # Top/bottom-N refinement.
        n = None
        m = re.search(r"\b(top|bottom)\s*(\d{1,2})\b", ul)
        if m:
            try:
                n = int(m.group(2))
            except Exception:
                n = None
        if n is None:
            m2 = re.search(r"\b(\d{1,2})\b", ul)
            if m2 and any(k in ul for k in ("top", "bottom", "lowest", "highest", "least", "most")):
                try:
                    n = int(m2.group(1))
                except Exception:
                    n = None
        if n is None and any(k in ul for k in ("top", "bottom", "lowest", "highest", "least", "most")):
            # "most frequently" / superlatives over categories are not "top 5 rows" asks.
            if "most frequently" in ul or "most often" in ul:
                n = None
            else:
                n = 5

        if n is not None:
            # Infer metric in a generic way from user wording + available fields.
            metric = None
            preferred = str(rs_meta.get("primary_metric") or "").strip().lower()
            singular_extreme_markers = (
                "which one",
                "which month",
                "which port",
                "which vessel",
                "which voyage",
                "which cargo grade",
                "top one",
                "bottom one",
                "highest one",
                "lowest one",
                "best one",
                "worst one",
            )
            if "this metric" in ul or "that metric" in ul:
                metric = preferred if preferred and _has_field(preferred) else None
            metric_hints = (
                ("avg_offhire_days", ("average demurrage", "demurrage wait", "wait time")),
                ("expense_to_revenue_ratio", ("expense ratio", "expense-to-revenue ratio", "expense to revenue ratio")),
                ("pnl_variance", ("pnl variance",)),
                ("tce_variance", ("tce variance",)),
                ("variance", ("variance", "changed the most")),
                ("voyage_count", ("voyage count", "count")),
                ("total_pnl", ("total pnl",)),
                ("avg_tce", ("avg tce", "average tce")),
                ("avg_revenue", ("avg revenue", "average revenue")),
                ("avg_pnl", ("avg pnl", "average pnl")),
                ("offhire_days", ("offhire", "off hire")),
                ("port_calls", ("port calls", "most called", "least called", "called voyages")),
                ("total_commission", ("commission",)),
                ("revenue", ("revenue",)),
                ("total_expense", ("expense", "cost")),
                ("tce", ("tce",)),
                ("pnl", ("pnl", "profit", "loss")),
            )
            for field, keys in metric_hints:
                if any(k in ul for k in keys):
                    if field == "variance":
                        metric = preferred if preferred and _has_field(preferred) else ("variance_diff" if _has_field("variance_diff") else field)
                    else:
                        metric = field
                    break
            if metric is None:
                if preferred and _has_field(preferred):
                    metric = preferred
                elif _has_field("avg_offhire_days"):
                    metric = "avg_offhire_days"
                elif _has_field("expense_to_revenue_ratio"):
                    metric = "expense_to_revenue_ratio"
                elif _has_field("offhire_days"):
                    metric = "offhire_days"
                elif _has_field("port_calls"):
                    metric = "port_calls"
                elif _has_field("total_pnl"):
                    metric = "total_pnl"
                elif _has_field("avg_tce"):
                    metric = "avg_tce"
                elif _has_field("avg_pnl"):
                    metric = "avg_pnl"
                elif _has_field("pnl"):
                    metric = "pnl"
                elif _has_field("avg_revenue"):
                    metric = "avg_revenue"
                elif _has_field("revenue"):
                    metric = "revenue"
                elif _has_field("voyage_count"):
                    metric = "voyage_count"
                elif _has_field("total_commission"):
                    metric = "total_commission"
                else:
                    metric = "pnl"

            if any(marker in ul for marker in singular_extreme_markers):
                return {"action": "compare_extremes", "metric": metric}
            action = "bottom_n" if any(k in ul for k in ("bottom", "lowest", "least", "worst")) else "top_n"
            return {"action": action, "n": max(1, min(int(n), 20)), "metric": metric}

        if any(k in ul for k in ("higher", "lower", "better", "worse", "best", "worst", "changed the most")):
            metric = "pnl"
            if "expense ratio" in ul or "expense-to-revenue ratio" in ul or "expense to revenue ratio" in ul:
                metric = "expense_to_revenue_ratio"
            elif "avg revenue" in ul or "average revenue" in ul:
                metric = "avg_revenue"
            elif "avg pnl" in ul or "average pnl" in ul:
                metric = "avg_pnl"
            elif "voyage count" in ul or re.search(r"\bcount\b", ul):
                metric = "voyage_count"
            elif "pnl variance" in ul:
                metric = "pnl_variance" if _has_field("pnl_variance") else "variance"
            elif "tce variance" in ul:
                metric = "tce_variance"
            elif "variance" in ul or "changed the most" in ul:
                preferred_metric = str(rs_meta.get("primary_metric") or "").strip().lower()
                if preferred_metric in ("pnl_variance", "tce_variance", "variance", "variance_diff") and _has_field(preferred_metric):
                    metric = preferred_metric
                elif _has_field("pnl_variance"):
                    metric = "pnl_variance"
                elif _has_field("tce_variance"):
                    metric = "tce_variance"
                else:
                    metric = "variance"
            elif "revenue" in ul:
                metric = "revenue"
            elif "expense" in ul or "cost" in ul:
                metric = "total_expense"
            return {"action": "compare_extremes", "metric": metric}

        return None

    def _apply_session_followup(
        self,
        *,
        intent_key: str,
        extracted_slots: Dict[str, Any],
        session_ctx: Dict[str, Any],
        user_input: str,
        inherit_slot_keys: list[str],
        backward_reference: bool,
    ) -> tuple[str, Dict[str, Any], bool]:
        """
        Resolve follow-up questions using Redis session memory.

        When the user asks something like "what about expenses?" right after a voyage/vessel query,
        the intent extractor may return no entity slots. This function injects the last known entity
        into slots and (when appropriate) coerces the intent to voyage.summary or vessel.summary.
        """
        slots = dict(extracted_slots or {})
        ui = (user_input or "").strip()
        ul = ui.lower()
        multi_vessel_ask = len(GraphRouter._explicit_vessel_mentions(ui)) >= 2

        # Never treat greetings / identity / help as follow-ups that inherit anchors.
        if GraphRouter._is_chitchat(ui):
            return intent_key, slots, False

        if self._has_entity_anchor(slots):
            return intent_key, slots, False

        sess_slots = {}
        if isinstance(session_ctx, dict):
            sess_slots = session_ctx.get("memory_slots") or session_ctx.get("slots") or {}
        if not isinstance(sess_slots, dict) or not sess_slots:
            return intent_key, slots, False

        requested_keys = [k for k in inherit_slot_keys if k in self._entity_slot_keys()]
        if not requested_keys:
            requested_keys = [k for k in self._entity_slot_keys() if sess_slots.get(k) not in (None, "", [], {})]

        for key in requested_keys:
            value = self._find_slot_in_turn_history(
                session_ctx=session_ctx,
                slot_key=key,
                prefer_older=backward_reference,
            )
            if value in (None, "", [], {}):
                value = sess_slots.get(key)
            if value in (None, "", [], {}):
                focus_slots = (session_ctx or {}).get("last_focus_slots") if isinstance(session_ctx, dict) else None
                if isinstance(focus_slots, dict):
                    value = focus_slots.get(key)
            if value not in (None, "", [], {}):
                if multi_vessel_ask and key in ("vessel_name", "imo"):
                    continue
                slots.setdefault(key, value)

        return intent_key, slots, self._has_entity_anchor(slots)

    @staticmethod
    def _apply_session_param_memory(
        *,
        extracted_slots: Dict[str, Any],
        session_ctx: Dict[str, Any],
        user_input: str,
        followup_used: bool,
    ) -> Dict[str, Any]:
        """
        Apply safe user-preference parameters from session memory.
        Intended for follow-ups like:
          - "show top 5" -> later "make it top 10"
          - "last 12 months" -> later "same period but for commission"
        """
        slots = dict(extracted_slots or {})
        if not followup_used:
            return slots

        if not isinstance(session_ctx, dict):
            return slots

        # Avoid parameter injection for explicit out-of-domain queries.
        ul = (user_input or "").lower()
        if GraphRouter._is_chitchat(user_input):
            return slots
        if any(k in ul for k in ("weather", "forecast", "temperature", "rain")):
            return slots

        sess_params = session_ctx.get("param_slots") or {}
        if not isinstance(sess_params, dict) or not sess_params:
            return slots

        for k in ("limit", "date_from", "date_to", "scenario", "metric", "group_by", "threshold", "cargo_type", "cargo_grade"):
            if k not in slots and sess_params.get(k) is not None:
                slots[k] = sess_params[k]
        return slots

    @staticmethod
    def _maybe_override_to_port_query(
        *,
        intent_key: str,
        extracted_slots: Dict[str, Any],
        user_input: str,
    ) -> tuple[str, Dict[str, Any]]:
        """
        Fix common miss-routing for port-centric queries.
        If the question is clearly "find voyages with <PORT>" and asks for route/grades/remarks,
        force the intent to ops.port_query and extract port_name if missing.
        """
        ui = (user_input or "").strip()
        ul = ui.lower()
        slots = dict(extracted_slots or {})

        def _looks_like_port_candidate(cand: str) -> bool:
            c = (cand or "").strip().strip("\"'").lower()
            if not c or len(c) > 40:
                return False
            # Reject metric/aggregation phrases commonly mis-read as ports.
            bad_tokens = (
                "most", "least", "highest", "lowest",
                "port call", "port calls", "key ports",
                "voyage", "voyages", "rank", "pnl", "revenue", "expense", "commission",
            )
            if any(t in c for t in bad_tokens):
                return False
            return True

        # If port already present, use it.
        port = slots.get("port_name")
        if isinstance(port, str) and not _looks_like_port_candidate(port):
            slots.pop("port_name", None)
            port = None
        if not port:
            # Heuristic extraction for: "For port Rotterdam, ..."
            m = re.search(r"\bfor\s+port\s+([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)*)\b", ui, flags=re.IGNORECASE)
            if m:
                cand = (m.group(1) or "").strip().rstrip(",")
                if _looks_like_port_candidate(cand):
                    port = cand
                    slots["port_name"] = cand
        if not port and (" with " in ul or " at " in ul or " visited " in ul or " calls " in ul or " called " in ul):
            # Heuristic extraction for: "... with Rotterdam in the route ..."
            m = re.search(r"\bwith\s+([A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z]+)*)\b", ui, flags=re.IGNORECASE)
            if m:
                cand = (m.group(1) or "").strip()
                # Trim common trailing words.
                cand = re.sub(r"\s+(in|on|for|and)$", "", cand, flags=re.IGNORECASE).strip()
                if _looks_like_port_candidate(cand):
                    port = cand
                    slots["port_name"] = cand

        wants_voyage_filter = (
            ("find voyages" in ul)
            or ("voyages with" in ul)
            or ("voyages that" in ul)
            or ("across voyages" in ul)
            or ("for port" in ul)
        )
        wants_route_like = ("route" in ul) or ("in the route" in ul) or ("visited" in ul) or ("called at" in ul) or ("calls at" in ul)
        wants_grades_or_remarks = any(k in ul for k in ("cargo grade", "cargo grades", "grades", "remarks", "remark", "offhire", "delay", "delayed"))
        wants_metric_ranking = any(k in ul for k in (" rank ", "rank by", "top ", "highest", "lowest", "pnl", "revenue", "profit"))

        if port and wants_voyage_filter and wants_grades_or_remarks:
            if wants_metric_ranking:
                return intent_key, slots
            # Do NOT treat metric/finance phrases as port names (e.g. "delayed voyages with negative PnL" → stay loss_due_to_delay)
            pn = str(port).lower()
            if "pnl" in pn or "revenue" in pn or "expense" in pn or ("negative" in pn and "port" not in pn):
                return intent_key, slots
            # Port queries should be independent: drop carried-over voyage/vessel anchors
            # unless the user explicitly mentions them in this query.
            if not re.search(r"\b\d{3,5}\b", ui):
                slots.pop("voyage_number", None)
                slots.pop("voyage_numbers", None)
                slots.pop("voyage_id", None)
            if not ("imo" in ul or "vessel" in ul or "ship" in ul):
                slots.pop("vessel_name", None)
                slots.pop("imo", None)
            return "ops.port_query", slots

        return intent_key, slots

    def _is_single_entity_query(self, intent_key: str, slots: Dict[str, Any], user_input: str) -> bool:
        """Determine if query is for a specific single entity"""
        # Comparison queries are NOT single entity
        if intent_key.startswith("comparison.") or intent_key.startswith("ranking."):
            return False
        
        if intent_key in ["voyage.summary", "voyage.entity", "vessel.summary", 
                          "vessel.entity", "cargo.details", "port.details"]:
            return True
        
        # Check if specific identifiers present
        has_specific_id = bool(
            slots.get("voyage_number") or 
            slots.get("voyage_id") or 
            slots.get("imo") or
            (slots.get("vessel_name") and "tell me about" in user_input.lower())
        )
        
        # Ranking/filtering keywords indicate composite
        ranking_keywords = ["top", "best", "worst", "highest", "lowest", "most", "least", 
                            "which", "find", "show all", "list", "compare", "vs", "versus"]
        has_ranking = any(kw in user_input.lower() for kw in ranking_keywords)
        
        return has_specific_id and not has_ranking

    @staticmethod
    def _unique_voyage_numbers_from_slots(slots: Dict[str, Any], *, max_count: int = 3) -> list[int]:
        vnums = slots.get("voyage_numbers")
        if not isinstance(vnums, list):
            vn = slots.get("voyage_number")
            vnums = [vn] if vn not in (None, "", [], {}) else []
        unique: list[int] = []
        for v in vnums:
            try:
                iv = int(v)
                if iv not in unique:
                    unique.append(iv)
            except Exception:
                continue
        return unique[:max_count]

    def _collect_voyage_summary_for_number(
        self,
        *,
        voyage_number: int,
        base_slots: Dict[str, Any],
        session_context: Dict[str, Any],
        user_input: str,
        intent_key: str = "voyage.summary",
    ) -> Dict[str, Any]:
        """Mongo-led finance/ops bundle for one voyage number (single canonical vessel)."""
        per_slots = dict(base_slots or {})
        per_slots["voyage_number"] = voyage_number
        per_slots.pop("voyage_id", None)

        def _norm_imo(v: Any) -> str:
            if v in (None, ""):
                return ""
            s = str(v).strip()
            if s.endswith(".0"):
                s = s[:-2]
            return s

        try:
            mongo_data = self.mongo_agent.fetch_full_voyage_context(
                voyage_number=voyage_number,
                voyage_id=None,
                entity_slots=per_slots,
            )
        except Exception:
            mongo_data = {}

        if isinstance(mongo_data, dict) and mongo_data:
            if mongo_data.get("voyage_id"):
                per_slots["voyage_id"] = str(mongo_data.get("voyage_id"))
            if mongo_data.get("vessel_imo"):
                per_slots["imo"] = str(mongo_data.get("vessel_imo"))
                per_slots["vessel_imo"] = str(mongo_data.get("vessel_imo"))
            if mongo_data.get("vessel_name"):
                per_slots["vessel_name"] = str(mongo_data.get("vessel_name"))

        voyage_id = per_slots.get("voyage_id")
        if not voyage_id and not (isinstance(mongo_data, dict) and mongo_data.get("voyage_id")):
            return {
                "ok": False,
                "voyage_number": voyage_number,
                "per_slots": per_slots,
                "error_kind": "not_found",
            }

        try:
            finance_data = self.finance_agent.run(
                intent_key=intent_key,
                slots=per_slots,
                session_context=session_context,
                user_input=user_input,
            )
        except TypeError:
            finance_data = self.finance_agent.run(
                intent_key=intent_key,
                slots=per_slots,
                session_context={**session_context, "user_input": user_input},
            )
        except Exception:
            finance_data = {"mode": "error", "rows": []}

        finance_fr = ""
        if isinstance(finance_data, dict):
            finance_fr = str(finance_data.get("fallback_reason") or "")
        postgres_down = "Postgres is not available" in finance_fr or "connection refused" in finance_fr.lower()
        if postgres_down:
            ops_data = {"mode": "error", "rows": [], "fallback_reason": finance_fr}
        else:
            try:
                ops_data = self.ops_agent.run(
                    intent_key=intent_key,
                    slots=per_slots,
                    session_context=session_context,
                    user_input=user_input,
                )
            except TypeError:
                ops_data = self.ops_agent.run(
                    intent_key=intent_key,
                    slots=per_slots,
                    session_context={**session_context, "user_input": user_input},
                )
            except Exception:
                ops_data = {"mode": "error", "rows": []}

        finance_safe = _json_safe(finance_data)
        ops_safe = _json_safe(ops_data)
        mongo_safe = _json_safe(mongo_data)

        mongo_imo = _norm_imo(mongo_safe.get("vessel_imo")) if isinstance(mongo_safe, dict) else ""
        mongo_vessel_name = str(mongo_safe.get("vessel_name") or "").strip().lower() if isinstance(mongo_safe, dict) else ""

        if (mongo_imo or mongo_vessel_name or voyage_id) and isinstance(finance_safe, dict) and isinstance(finance_safe.get("rows"), list):
            fin_filtered = []
            for r in finance_safe.get("rows") or []:
                if not isinstance(r, dict):
                    continue
                row_imo = _norm_imo(r.get("vessel_imo"))
                row_vname = str(r.get("vessel_name") or "").strip().lower()
                row_vid = str(r.get("voyage_id") or "").strip()
                if voyage_id and row_vid and row_vid == str(voyage_id).strip():
                    fin_filtered.append(r)
                elif (mongo_imo and row_imo == mongo_imo) or (mongo_vessel_name and row_vname == mongo_vessel_name):
                    fin_filtered.append(r)
            if fin_filtered:
                finance_safe = {**finance_safe, "rows": fin_filtered[:1]}

        if (mongo_imo or mongo_vessel_name or voyage_id) and isinstance(ops_safe, dict) and isinstance(ops_safe.get("rows"), list):
            ops_filtered = []
            for r in ops_safe.get("rows") or []:
                if not isinstance(r, dict):
                    continue
                row_imo = _norm_imo(r.get("vessel_imo"))
                row_vname = str(r.get("vessel_name") or "").strip().lower()
                row_vid = str(r.get("voyage_id") or "").strip()
                if voyage_id and row_vid and row_vid == str(voyage_id).strip():
                    ops_filtered.append(r)
                elif (mongo_imo and row_imo == mongo_imo) or (mongo_vessel_name and row_vname == mongo_vessel_name):
                    ops_filtered.append(r)
            if ops_filtered:
                ops_safe = {**ops_safe, "rows": ops_filtered[:1]}

        fin_rows = finance_safe.get("rows") if isinstance(finance_safe, dict) else []
        fin_rows = fin_rows if isinstance(fin_rows, list) else []
        selected_fin = fin_rows[0] if fin_rows and isinstance(fin_rows[0], dict) else {}

        metadata_mongodb: Dict[str, Any] = {}
        doc: Dict[str, Any] = {}
        if per_slots.get("voyage_id"):
            projection = get_mongo_projection("voyage_metadata_context")
            try:
                fd = self.mongo_agent.adapter.fetch_voyage(str(per_slots.get("voyage_id")), projection=projection)
            except Exception:
                fd = None
            if isinstance(fd, dict) and fd:
                doc = fd
                metadata_mongodb = select_voyage_sections(user_input, doc, session_context)

        mongo_vessel_name = None
        if isinstance(doc, dict) and doc.get("vesselName"):
            mongo_vessel_name = doc.get("vesselName")
        elif isinstance(mongo_safe, dict) and mongo_safe.get("vessel_name"):
            mongo_vessel_name = mongo_safe.get("vessel_name")
        if not metadata_mongodb and isinstance(mongo_safe, dict) and mongo_safe:
            metadata_mongodb = select_voyage_sections(
                user_input,
                {
                    "voyageNumber": mongo_safe.get("voyage_number"),
                    "voyageId": mongo_safe.get("voyage_id"),
                    "vesselName": mongo_safe.get("vessel_name"),
                    "remarks": mongo_safe.get("remarks") or [],
                    "fixtures": mongo_safe.get("fixtures") or [],
                    "legs": mongo_safe.get("legs") or [],
                },
                session_context,
            )

        financial_data_postgres = {
            "voyage_id": selected_fin.get("voyage_id") or per_slots.get("voyage_id"),
            "voyage_number": selected_fin.get("voyage_number") or voyage_number,
            "vessel_name": mongo_vessel_name or selected_fin.get("vessel_name") or per_slots.get("vessel_name"),
            "pnl": selected_fin.get("pnl"),
            "revenue": selected_fin.get("revenue"),
            "total_expense": selected_fin.get("total_expense"),
            "tce": selected_fin.get("tce"),
            "total_commission": selected_fin.get("total_commission"),
            "scenario": selected_fin.get("scenario"),
        }

        ops_summary: Dict[str, Any] = {}
        ops_rows = ops_safe.get("rows") if isinstance(ops_safe, dict) else []
        if isinstance(ops_rows, list) and ops_rows and isinstance(ops_rows[0], dict):
            orow = ops_rows[0]
            ops_summary = {
                k: orow.get(k)
                for k in (
                    "offhire_days",
                    "delay_reason",
                    "delay_reasons",
                    "key_ports",
                    "ports",
                    "ports_json",
                    "cargo_grades",
                    "port_calls",
                )
                if orow.get(k) not in (None, "", [], {})
            }

        return {
            "ok": True,
            "voyage_number": voyage_number,
            "per_slots": per_slots,
            "mongo_safe": mongo_safe,
            "finance_safe": finance_safe,
            "ops_safe": ops_safe,
            "financial_data_postgres": financial_data_postgres,
            "metadata_mongodb": metadata_mongodb,
            "ops_summary": ops_summary,
        }

    def n_validate_slots(self, state: GraphState) -> GraphState:
        from app.orchestration.handlers.slot_validation_handler import handle_validate_slots

        return handle_validate_slots(self, state)

    def n_make_clarification(self, state: GraphState) -> GraphState:
        from app.orchestration.handlers.clarification_handler import handle_make_clarification

        return handle_make_clarification(self, state)

    def _build_clarification_message(self, *, intent_key: str, missing_keys: list[str]) -> tuple[str, Dict[str, Any]]:
        missing = [m for m in (missing_keys or []) if isinstance(m, str)]
        if not missing:
            return ("### Quick question\n- What would you like to know?", {})

        m0 = missing[0]
        if m0 == "port_name":
            sugg = self._suggest_ports(limit=8)
            lines = [
                "### Quick question",
                "- You asked about a **port**, but didn’t specify which one.",
                "- Which port do you want to know about?",
            ]
            options: Dict[str, Any] = {}
            if sugg:
                options["port_name"] = sugg
                lines += ["", "### Suggestions"]
                for i, s in enumerate(sugg, start=1):
                    lines.append(f"- {i}. {s}")
                lines += ["", "Reply with a value (or reply with a number from the list)."]
            else:
                lines += ["", "Reply with the port name (e.g. `Rotterdam`)."]
            return ("\n".join(lines).strip(), options)

        if m0 == "voyage_number":
            sugg = self._suggest_voyage_numbers(limit=8)
            lines = [
                "### Quick question",
                "- You asked about a **voyage**, but didn’t specify the voyage number.",
                "- Which **voyage number** do you mean?",
            ]
            options: Dict[str, Any] = {}
            if sugg:
                options["voyage_number"] = sugg
                lines += ["", "### Suggestions"]
                for i, v in enumerate(sugg, start=1):
                    lines.append(f"- {i}. {v}")
                lines += ["", "Reply with a voyage number (or reply with a number from the list)."]
            else:
                lines += ["", "Reply with the voyage number (e.g. `1901`)."]
            return ("\n".join(lines).strip(), options)

        if m0 in ("vessel_name", "imo"):
            sugg = self._suggest_vessels(limit=8)
            lines = [
                "### Quick question",
                "- You asked about a **vessel**, but didn’t specify which one.",
                "- Which vessel are you referring to (name or IMO)?",
            ]
            options: Dict[str, Any] = {}
            if sugg:
                options["vessel_name"] = sugg
                lines += ["", "### Suggestions"]
                for i, s in enumerate(sugg, start=1):
                    lines.append(f"- {i}. {s}")
                lines += ["", "Reply with a value (or reply with a number from the list)."]
            else:
                lines += ["", "Reply with the vessel name or IMO (7 digits)."]
            return ("\n".join(lines).strip(), options)

        if m0 == "cargo_type":
            return (
                "### Quick question\n"
                "- Which **cargo type** do you mean?\n"
                "- Reply with a cargo type/grade name.",
                {},
            )

        return (
            "### Quick question\n"
            f"- I’m missing: **{', '.join(missing)}**.\n"
            "- Reply with the missing value(s), and I’ll run the query.",
            {},
        )

    def _suggest_ports(self, *, limit: int = 8) -> list[str]:
        cached = self._get_cached_suggestions("ports", limit)
        if cached is not None:
            return [str(value) for value in cached[:limit]]
        sql = """
            SELECT p->>'port_name' AS port_name, COUNT(*) AS cnt
            FROM ops_voyage_summary ovs,
                 LATERAL jsonb_array_elements(ovs.ports_json) AS p
            WHERE ovs.ports_json IS NOT NULL
              AND (p->>'port_name') IS NOT NULL
              AND (p->>'port_name') <> ''
            GROUP BY 1
            ORDER BY cnt DESC
            LIMIT 8
        """
        try:
            rows = self.ops_agent.pg.execute_dynamic_select(sql, {})
            out = []
            for r in rows or []:
                if isinstance(r, dict) and r.get("port_name"):
                    out.append(str(r["port_name"]))
            self._set_cached_suggestions("ports", out)
            return out[:limit]
        except Exception:
            return []

    def _suggest_vessels(self, *, limit: int = 8) -> list[str]:
        cached = self._get_cached_suggestions("vessels", limit)
        if cached is not None:
            return [str(value) for value in cached[:limit]]
        sql = """
            SELECT vessel_name, COUNT(*) AS cnt
            FROM ops_voyage_summary
            WHERE vessel_name IS NOT NULL AND vessel_name <> ''
            GROUP BY vessel_name
            ORDER BY cnt DESC
            LIMIT 8
        """
        try:
            rows = self.ops_agent.pg.execute_dynamic_select(sql, {})
            out = []
            for r in rows or []:
                if isinstance(r, dict) and r.get("vessel_name"):
                    out.append(str(r["vessel_name"]))
            self._set_cached_suggestions("vessels", out)
            return out[:limit]
        except Exception:
            return []

    def _suggest_voyage_numbers(self, *, limit: int = 8) -> list[int]:
        cached = self._get_cached_suggestions("voyage_numbers", limit)
        if cached is not None:
            return [int(value) for value in cached[:limit]]
        sql = """
            SELECT DISTINCT voyage_number
            FROM ops_voyage_summary
            WHERE voyage_number IS NOT NULL
            ORDER BY voyage_end_date DESC NULLS LAST
            LIMIT 8
        """
        try:
            rows = self.ops_agent.pg.execute_dynamic_select(sql, {})
            out: list[int] = []
            for r in rows or []:
                if isinstance(r, dict) and r.get("voyage_number") is not None:
                    try:
                        out.append(int(r["voyage_number"]))
                    except Exception:
                        continue
            self._set_cached_suggestions("voyage_numbers", out)
            return out[:limit]
        except Exception:
            return []

    def _get_cached_suggestions(self, name: str, limit: int) -> list[Any] | None:
        cached = self._suggestion_cache.get(name)
        if not cached:
            return None
        expires_at, values = cached
        if time.time() >= expires_at:
            self._suggestion_cache.pop(name, None)
            return None
        return list(values[:limit])

    def _set_cached_suggestions(self, name: str, values: list[Any]) -> None:
        self._suggestion_cache[name] = (
            time.time() + _SUGGESTION_CACHE_TTL_SECONDS,
            list(values or []),
        )

    # =========================================================
    # Planning Node
    # =========================================================

    def n_plan(self, state: GraphState) -> GraphState:
        from app.orchestration.handlers.planning_handler import handle_plan

        return handle_plan(self, state)


    # =========================================================
    # Single Execution (Standard Flow)
    # =========================================================

    def n_run_single(self, state: GraphState) -> GraphState:
        from app.orchestration.handlers.single_run_handler import handle_run_single

        return handle_run_single(self, state)

    def n_execute_step(self, state: GraphState) -> GraphState:
        from app.orchestration.handlers.composite_step_handler import handle_execute_step

        return handle_execute_step(self, state)

    def n_merge(self, state: GraphState) -> GraphState:
        from app.orchestration.handlers.merge_handler import handle_merge

        return handle_merge(self, state)

    def n_summarize(self, state: GraphState) -> GraphState:
        from app.orchestration.handlers.summarize_handler import handle_summarize

        return handle_summarize(self, state)

    @staticmethod
    def _fallback_tabular_answer(*, question: str, intent_key: str, rows: list[Dict[str, Any]]) -> str:
        if not isinstance(rows, list) or not rows:
            return get_router_fallback_template("no_data_available")

        ql = (question or "").lower()
        top = rows[:10]

        def _fmt_num(v: Any, *, money: bool = False) -> str:
            if v in (None, "", "Not available"):
                return "Not available"
            try:
                n = float(v)
                if money:
                    return f"${n:,.2f}"
                if abs(n - int(n)) < 1e-9:
                    return f"{int(n):,}"
                return f"{n:,.2f}"
            except Exception:
                return str(v)

        # Choose primary metric from question wording.
        metric = "pnl"
        if any(k in ql for k in ("revenue", "freight")):
            metric = "revenue"
        elif any(k in ql for k in ("expense", "cost", "bunker", "demurrage")):
            metric = "total_expense"
        elif "tce" in ql:
            metric = "tce"
        elif any(k in ql for k in ("commission",)):
            metric = "total_commission"
        elif any(k in ql for k in ("co2", "emission", "eeoi", "aer")):
            metric = "co2"

        # Build rows sorted by chosen metric when present.
        def _metric_value(r: Dict[str, Any]) -> float:
            try:
                return float(r.get(metric))
            except Exception:
                return float("-inf")

        ranked = sorted([r for r in top if isinstance(r, dict)], key=_metric_value, reverse=True)
        if not ranked:
            ranked = [r for r in top if isinstance(r, dict)]

        winner = ranked[0] if ranked else {}
        winner_voy = winner.get("voyage_number") or "Not available"
        winner_vessel = winner.get("vessel_name") or "Not available"
        winner_metric = _fmt_num(winner.get(metric), money=(metric in ("pnl", "revenue", "total_expense", "total_commission")))

        include_emissions = metric in ("co2", "eeoi", "aer") or any(
            isinstance(r, dict) and (
                r.get("co2") not in (None, "")
                or r.get("eeoi") not in (None, "")
                or r.get("aer") not in (None, "")
            )
            for r in ranked[:10]
        )
        if include_emissions:
            out = [
                "### Summary",
                f"- **Top result**: Voyage {winner_voy} ({winner_vessel}) with {metric} = {winner_metric}",
                "",
                "### Results",
                "| Voyage # | Vessel | CO2 | EEOI | AER | PnL | Revenue | Total expense |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        else:
            out = [
                "### Summary",
                f"- **Top result**: Voyage {winner_voy} ({winner_vessel}) with {metric} = {winner_metric}",
                "",
                "### Results",
                "| Voyage # | Vessel | PnL | Revenue | Total expense | TCE | Total commission |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        for r in ranked[:10]:
            if include_emissions:
                out.append(
                    f"| {r.get('voyage_number') or 'Not available'} | {r.get('vessel_name') or 'Not available'} | "
                    f"{_fmt_num(r.get('co2'))} | {_fmt_num(r.get('eeoi'))} | {_fmt_num(r.get('aer'))} | "
                    f"{_fmt_num(r.get('pnl'), money=True)} | {_fmt_num(r.get('revenue'), money=True)} | "
                    f"{_fmt_num(r.get('total_expense'), money=True)} |"
                )
            else:
                out.append(
                    f"| {r.get('voyage_number') or 'Not available'} | {r.get('vessel_name') or 'Not available'} | "
                    f"{_fmt_num(r.get('pnl'), money=True)} | {_fmt_num(r.get('revenue'), money=True)} | "
                    f"{_fmt_num(r.get('total_expense'), money=True)} | {_fmt_num(r.get('tce'))} | "
                    f"{_fmt_num(r.get('total_commission'), money=True)} |"
                )
        return "\n".join(out)

    # =========================================================
    # Smart slot merging
    # =========================================================

    @staticmethod
    def _merge_slots(
        intent_key: str,
        session_ctx: Dict[str, Any],
        current_slots: Dict[str, Any],
        user_input: str = "",
    ) -> Dict[str, Any]:
        """
        Merge session and current slots based on intent. Use current slots only for independent queries.
        """
        independent_prefixes = ("ranking.", "analysis.", "ops.", "comparison.", "aggregation.", "temporal.")
        independent_intents = {
            # Entity queries (user specified exact entity)
            "voyage.summary", "voyage.entity",
            "vessel.summary", "vessel.entity",
            "cargo.details", "port.details",
            # Composite + fallback
            "composite.query",
            "out_of_scope",
        }

        # Independent intents should NEVER inherit/merge prior entity anchors.
        if intent_key in independent_intents or (intent_key or "").startswith(independent_prefixes):
            return dict(current_slots or {})
        
        # For follow-up questions only
        # Only merge if current_slots is completely empty
        if not current_slots or len(current_slots) == 0:
            mentions = GraphRouter._explicit_vessel_mentions(user_input or "")
            if len(mentions) >= 2:
                # Multi-vessel ask: do not resurrect a stale single-vessel session anchor.
                return {}
            if len(mentions) == 1:
                return {"vessel_name": mentions[0]}
            # True follow-up question like "What about the expenses?"
            session_slots = {}
            if isinstance(session_ctx, dict):
                session_slots = session_ctx.get("memory_slots") or session_ctx.get("slots") or {}
            return dict(session_slots or {})
        
        # Default: use only current slots (don't pollute!)
        return dict(current_slots)

    # =========================================================
    # Session memory helpers (entity + user params)
    # =========================================================

    @staticmethod
    def _extract_memory_slots(slots: Dict[str, Any]) -> Dict[str, Any]:
        """Persist only stable entity/scenario slots (never derived lists like voyage_ids)."""
        if not isinstance(slots, dict):
            return {}
        keep = ("voyage_number", "voyage_numbers", "voyage_id", "vessel_name", "imo", "scenario", "port_name")
        out = {k: slots.get(k) for k in keep if slots.get(k) not in (None, "", [], {})}
        return out

    @staticmethod
    def _extract_param_slots(slots: Dict[str, Any]) -> Dict[str, Any]:
        """Persist only preference-like parameters (limit/date range/metrics)."""
        if not isinstance(slots, dict):
            return {}
        keep = ("limit", "date_from", "date_to", "metric", "group_by", "threshold", "cargo_type", "cargo_grade")
        out = {k: slots.get(k) for k in keep if slots.get(k) not in (None, "", [], {})}
        return out

    @staticmethod
    def _headline_from_answer(answer: str) -> str:
        text = str(answer or "").strip()
        if not text:
            return ""
        lines = []
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            if line.startswith("###"):
                continue
            if line.startswith("|"):
                continue
            if set(line) <= {"|", "-", " "}:
                continue
            if line.startswith("- "):
                line = line[2:].strip()
            lines.append(line)
        if not lines:
            return ""
        first = lines[0]
        sentence = re.split(r"(?<=[.!?])\s+", first, maxsplit=1)[0].strip()
        return sentence[:240]

    def _build_turn_history_entry(
        self,
        *,
        query: str,
        raw_user_input: str,
        intent_key: str,
        slots: Dict[str, Any],
        answer: str,
        plan_type: Any,
    ) -> Dict[str, Any]:
        compact_slots: Dict[str, Any] = {}
        try:
            compact_slots.update(self._extract_memory_slots(slots if isinstance(slots, dict) else {}))
            compact_slots.update(self._extract_param_slots(slots if isinstance(slots, dict) else {}))
        except Exception:
            compact_slots = {}
        entry: Dict[str, Any] = {
            "query": str(query or "").strip(),
            "raw_user_input": str(raw_user_input or "").strip(),
            "intent_key": str(intent_key or "").strip(),
            "slots": compact_slots,
            "answer_headline": self._headline_from_answer(answer),
        }
        if plan_type not in (None, "", [], {}):
            entry["plan_type"] = str(plan_type)
        return entry

    @staticmethod
    def _extract_best_worst_rows_from_answer(*, answer: str, session_ctx: Dict[str, Any]) -> list[Dict[str, Any]]:
        text = str(answer or "")
        if not text:
            return []
        rows: list[Dict[str, Any]] = []
        history = (session_ctx or {}).get("turn_history") if isinstance(session_ctx, dict) else None
        vessel_name = None
        if isinstance(history, list):
            for item in reversed(history):
                if not isinstance(item, dict):
                    continue
                slots = item.get("slots")
                if isinstance(slots, dict) and slots.get("vessel_name"):
                    vessel_name = slots.get("vessel_name")
                    break

        for label in ("best", "worst"):
            m = re.search(
                rf"\b{label}\s+voyage\b[:\s-]*([0-9]{{3,5}}).*?\bPnL\b(?:\s+of)?\s*\$?([0-9,]+(?:\.[0-9]+)?)",
                text,
                flags=re.IGNORECASE | re.DOTALL,
            )
            if not m:
                continue
            try:
                voyage_number = int(m.group(1))
            except Exception:
                continue
            pnl_text = str(m.group(2) or "").replace(",", "").strip()
            try:
                pnl = float(pnl_text)
            except Exception:
                pnl = None
            row: Dict[str, Any] = {"voyage_number": voyage_number, "extreme_label": label}
            if vessel_name:
                row["vessel_name"] = vessel_name
            if pnl is not None:
                row["pnl"] = pnl
            rows.append(row)
        return rows

    @staticmethod
    def _build_persisted_slots(*, base: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge slots for persistence, then drop unsafe/derived keys.
        """
        merged: Dict[str, Any] = {}
        if isinstance(base, dict):
            merged.update(base)
        if isinstance(updates, dict):
            merged.update(updates)

        # Never persist these derived/ephemeral keys (causes slot pollution).
        drop = {"voyage_ids", "finance_rows", "ops_rows", "merged_rows", "include_voyage_ids"}
        for k in list(merged.keys()):
            if k in drop:
                merged.pop(k, None)

        # Anchor exclusivity: keep only ONE active anchor family at a time unless user explicitly provided both.
        # If updates establish a vessel anchor, drop voyage anchor from the old base.
        if (updates.get("vessel_name") or updates.get("imo")) and not (updates.get("voyage_number") or updates.get("voyage_id") or updates.get("voyage_numbers")):
            merged.pop("voyage_number", None)
            merged.pop("voyage_numbers", None)
            merged.pop("voyage_id", None)

        # If updates establish a voyage anchor, drop vessel anchor from the old base.
        if (updates.get("voyage_number") or updates.get("voyage_id") or updates.get("voyage_numbers")) and not (updates.get("vessel_name") or updates.get("imo")):
            merged.pop("vessel_name", None)
            merged.pop("imo", None)

        # If updates establish a port anchor (port queries), drop both voyage/vessel anchors unless explicitly present in updates.
        if updates.get("port_name") and not (
            updates.get("voyage_number")
            or updates.get("voyage_id")
            or updates.get("voyage_numbers")
            or updates.get("vessel_name")
            or updates.get("imo")
        ):
            merged.pop("voyage_number", None)
            merged.pop("voyage_numbers", None)
            merged.pop("voyage_id", None)
            merged.pop("vessel_name", None)
            merged.pop("imo", None)

        # Drop None/empty values
        cleaned: Dict[str, Any] = {}
        for k, v in merged.items():
            if v in (None, "", [], {}):
                continue
            cleaned[k] = v
        return cleaned

    # =========================================================
    # Token optimization helpers
    # =========================================================

    @staticmethod
    def _estimate_tokens(obj: Any) -> int:
        """
        Rough token estimation for debugging.
        Rule of thumb: 1 token ≈ 4 characters
        
        Args:
            obj: Any object to estimate tokens for
        
        Returns:
            Estimated token count
        """
        return len(str(obj)) // 4

    @staticmethod
    def _sanitize_for_llm(merged: Dict[str, Any]) -> Dict[str, Any]:
        """
        Reduce data size before sending to the LLM.
        The runtime merged payload can contain large nested arrays (mongo.rows fixtures, ops ports_json/grades_json/remarks_json,
        and artifacts.merged_rows which embeds finance+ops+remarks). This function caps/truncates those while preserving the
        fields needed for summarization.
        
        Args:
            merged: Full merged data from all agents
        
        Returns:
            Sanitized merged data with reduced size
        """
        import copy

        sanitized = copy.deepcopy(merged)

        intent_raw = sanitized.get("artifacts") if isinstance(sanitized.get("artifacts"), dict) else {}
        intent_k = str((intent_raw or {}).get("intent_key") or "").strip()
        voyage_summary_intent = intent_k == "voyage.summary"

        def _cap_list(v: Any, n: int) -> Any:
            return v[:n] if isinstance(v, list) else v

        def _cap_str(v: Any, n: int) -> Any:
            if isinstance(v, str) and len(v) > n:
                return v[:n] + "...[truncated]"
            return v

        def _compact_dict(d: Any, *, allow_keys: set[str], max_str: int = 300) -> Any:
            if not isinstance(d, dict):
                return d
            out: Dict[str, Any] = {}
            for k in allow_keys:
                if k in d:
                    out[k] = _cap_str(d.get(k), max_str)
            return out

        def _sanitize_remarks(rem: Any) -> Any:
            if isinstance(rem, list):
                rem = rem[:5]
                cleaned = []
                for x in rem:
                    if isinstance(x, dict):
                        cleaned.append(
                            {
                                "remark": _cap_str(x.get("remark"), 300),
                                "modifiedDate": x.get("modifiedDate"),
                                "modifiedByFull": x.get("modifiedByFull"),
                            }
                        )
                    else:
                        cleaned.append(_cap_str(str(x), 300))
                return cleaned
            if isinstance(rem, str):
                return _cap_str(rem, 300)
            return rem
        
        # === Sanitize Mongo rows (LLM find payloads) ===
        if isinstance(sanitized.get("mongo"), dict):
            mongo = sanitized["mongo"]

            # Cap $in filters to avoid huge debug payloads.
            filt = mongo.get("filter")
            if isinstance(filt, dict):
                try:
                    # common pattern: {"voyageId": {"$in": [..]}}
                    for k, v in list(filt.items()):
                        if isinstance(v, dict) and isinstance(v.get("$in"), list) and len(v["$in"]) > 30:
                            v["$in"] = v["$in"][:30]
                            v["_truncated"] = True
                except Exception:
                    pass

            rows = mongo.get("rows")
            if isinstance(rows, list) and rows:
                cleaned_rows: list[dict] = []
                for r in rows[:50]:
                    if not isinstance(r, dict):
                        continue

                    # Keep only the bits we actually summarize against for voyages.
                    keep_top = {"voyageId", "voyageNumber", "remarks", "fixtures", "vesselName", "imo"}
                    rr: Dict[str, Any] = {k: r.get(k) for k in keep_top if k in r}

                    rr["remarks"] = _sanitize_remarks(rr.get("remarks"))

                    fixtures = rr.get("fixtures")
                    if isinstance(fixtures, list):
                        fx_clean: list[dict] = []
                        for fx in fixtures[:3]:
                            if not isinstance(fx, dict):
                                continue
                            fx_clean.append(
                                {
                                    "grades": _cap_list(fx.get("grades"), 10),
                                    "fixtureGrades": _cap_list(
                                        [
                                            _compact_dict(g, allow_keys={"gradeName"}, max_str=80)
                                            for g in (fx.get("fixtureGrades") or [])
                                            if isinstance(g, dict)
                                        ],
                                        10,
                                    ),
                                    "fixturePorts": _cap_list(
                                        [
                                            _compact_dict(p, allow_keys={"portName", "activityType"}, max_str=80)
                                            for p in (fx.get("fixturePorts") or [])
                                            if isinstance(p, dict)
                                        ],
                                        12,
                                    ),
                                    "fixtureCommissions": _cap_list(
                                        [
                                            _compact_dict(
                                                c,
                                                allow_keys={"commissionType", "organizationName", "rate"},
                                                max_str=120,
                                            )
                                            for c in (fx.get("fixtureCommissions") or [])
                                            if isinstance(c, dict)
                                        ],
                                        10,
                                    ),
                                }
                            )
                        rr["fixtures"] = fx_clean

                    cleaned_rows.append(rr)

                mongo["rows"] = cleaned_rows
        
        # === Sanitize Finance Rows (limit to 50) ===
        if isinstance(sanitized.get("finance"), dict):
            rows = sanitized["finance"].get("rows", [])
            if isinstance(rows, list) and len(rows) > 50:
                original_count = len(rows)
                sanitized["finance"]["rows"] = rows[:50]
                sanitized["finance"]["_truncated"] = True
                sanitized["finance"]["_total_rows"] = original_count
                _dprint(f"   🔧 Truncated finance rows: {original_count} → 50")
        
        # === Sanitize Ops Rows (cap + shrink json arrays) ===
        if isinstance(sanitized.get("ops"), dict):
            rows = sanitized["ops"].get("rows", [])
            if isinstance(rows, list) and len(rows) > 50:
                original_count = len(rows)
                sanitized["ops"]["rows"] = rows[:50]
                sanitized["ops"]["_truncated"] = True
                sanitized["ops"]["_total_rows"] = original_count
                _dprint(f"   🔧 Truncated ops rows: {original_count} → 50")

            rows2 = sanitized["ops"].get("rows", [])
            if isinstance(rows2, list):
                for r in rows2:
                    if not isinstance(r, dict):
                        continue
                    shrink_ops_row_json_fields(r, voyage_summary=voyage_summary_intent)
        
        # === Sanitize Artifacts ===
        if isinstance(sanitized.get("artifacts"), dict):
            artifacts = sanitized["artifacts"]
            
            # Truncate merged_rows
            if "merged_rows" in artifacts and isinstance(artifacts["merged_rows"], list):
                if len(artifacts["merged_rows"]) > 50:
                    original_count = len(artifacts["merged_rows"])
                    artifacts["merged_rows"] = artifacts["merged_rows"][:50]
                    artifacts["_merged_rows_truncated"] = True
                    artifacts["_merged_rows_total"] = original_count
                    _dprint(f"   🔧 Truncated merged_rows: {original_count} → 50")

                # Shrink per-row nested fields (remarks + embedded ops json arrays).
                for mr in artifacts["merged_rows"]:
                    if not isinstance(mr, dict):
                        continue

                    mr["cargo_grades"] = _cap_list(mr.get("cargo_grades"), 8)
                    mr["key_ports"] = _cap_list(mr.get("key_ports"), 8)
                    if isinstance(mr.get("key_ports"), list):
                        mr["key_ports"] = [
                            _compact_dict(p, allow_keys={"portName", "activityType"}, max_str=80)
                            for p in (mr.get("key_ports") or [])
                            if isinstance(p, dict)
                        ][:8]

                    mr["commissions"] = _cap_list(mr.get("commissions"), 10)
                    if isinstance(mr.get("commissions"), list):
                        mr["commissions"] = [
                            _compact_dict(c, allow_keys={"commissionType", "organizationName", "rate"}, max_str=120)
                            for c in (mr.get("commissions") or [])
                            if isinstance(c, dict)
                        ][:10]

                    mr["remarks"] = _sanitize_remarks(mr.get("remarks"))

                    # Embedded ops rows can include huge json arrays; cap and shorten.
                    ops_emb = mr.get("ops")
                    if isinstance(ops_emb, list):
                        ops_clean = []
                        for r in ops_emb[:2]:
                            if not isinstance(r, dict):
                                continue
                            rc = dict(r)
                            rc["ports_json"] = _cap_list(rc.get("ports_json"), 10)
                            rc["grades_json"] = _cap_list(rc.get("grades_json"), 10)
                            rc["remarks_json"] = _cap_list(rc.get("remarks_json"), 3)
                            if isinstance(rc.get("remarks_json"), list):
                                rc["remarks_json"] = [_cap_str(str(x), 200) for x in (rc.get("remarks_json") or [])[:3]]
                            ops_clean.append(rc)
                        mr["ops"] = ops_clean
            
            # Truncate individual remarks in remarks_by_voyage_id
            if "remarks_by_voyage_id" in artifacts:
                remarks_dict = artifacts["remarks_by_voyage_id"]
                for vid, remark in list(remarks_dict.items()):
                    if remark:
                        remark_str = str(remark)
                        if len(remark_str) > 1000:
                            remarks_dict[vid] = remark_str[:1000] + "...[truncated]"
            
            # Remove or truncate other large intermediate data
            for key in ["finance_rows", "ops_rows"]:
                if key in artifacts:
                    rows = artifacts[key]
                    if isinstance(rows, list) and len(rows) > 50:
                        artifacts[key] = rows[:50]

            # Lightweight coverage hints for the LLM (helps avoid "Not available" when some rows have data).
            try:
                mrs = artifacts.get("merged_rows")
                if isinstance(mrs, list) and mrs:
                    cg = sum(1 for x in mrs if isinstance(x, dict) and (x.get("cargo_grades") or []))
                    kp = sum(1 for x in mrs if isinstance(x, dict) and (x.get("key_ports") or []))
                    rm = sum(1 for x in mrs if isinstance(x, dict) and (x.get("remarks") not in (None, "", [], {})))
                    artifacts["coverage"] = {
                        "merged_rows": len(mrs),
                        "cargo_grades_available": cg,
                        "key_ports_available": kp,
                        "remarks_available": rm,
                    }
            except Exception:
                pass
        
        return sanitized

    # =========================================================
    # Routing
    # =========================================================

    def r_after_validate(self, state: GraphState) -> str:
        """After validation: clarify, plan, or skip to summarize. Uses resolved intent so aliases (e.g. ops.delayed_voyages) route to plan."""
        resolved = resolve_intent(state.get("intent_key") or "out_of_scope")
        if resolved not in INTENT_REGISTRY:
            return "summarize"
        if state.get("missing_keys"):
            return "clarify"
        return "plan"

    def r_plan_path(self, state: GraphState) -> str:
        """Route based on plan type: single or composite"""
        pt = (state.get("plan_type") or "single").lower()
        return "composite" if pt == "composite" else "single"

    def r_has_more_steps(self, state: GraphState) -> str:
        """Check if composite plan has more steps to execute"""
        plan = state.get("plan") or {}
        steps = plan.get("steps") or []
        idx = int(state.get("step_index") or 0)
        return "more" if idx < len(steps) else "done"

    def r_after_run_single(self, state: GraphState) -> str:
        # Zero-row escalation: plan_type was flipped to "composite" by n_run_single.
        if (state.get("plan_type") or "") == "composite":
            return "escalate"
        """Route to end if answer already generated (e.g. voyage.summary), else merge"""
        # If the single path created a clarification, end immediately (do NOT summarize/overwrite).
        if state.get("clarification"):
            return "done"
        if state.get("answer"):
            return "done"
        return "merge"

    # =========================================================
    # Graph Builder
    # =========================================================

    def _build_graph(self):
        """Build the execution graph"""
        g = StateGraph(GraphState)

        # Define all nodes
        g.add_node("load_session", self.n_load_session)
        g.add_node("extract", self.n_extract_intent)
        g.add_node("validate", self.n_validate_slots)
        g.add_node("clarify", self.n_make_clarification)
        # NOTE: langgraph disallows node names that collide with state keys ("plan" is a state channel).
        g.add_node("build_plan", self.n_plan)
        g.add_node("run_single", self.n_run_single)
        g.add_node("execute_step", self.n_execute_step)
        g.add_node("merge", self.n_merge)
        g.add_node("summarize", self.n_summarize)

        # Set entry point
        g.set_entry_point("load_session")
        
        # Build graph flow
        g.add_edge("load_session", "extract")
        g.add_edge("extract", "validate")
        
        # After validation
        g.add_conditional_edges(
            "validate",
            self.r_after_validate,
            {
                "clarify": "clarify",
                "plan": "build_plan",
                "summarize": "summarize",
            },
        )
        
        g.add_edge("clarify", END)
        
        # After planning
        g.add_conditional_edges(
            "build_plan",
            self.r_plan_path,
            {
                "single": "run_single",
                "composite": "execute_step",
            },
        )
        
        # Composite execution loop
        g.add_conditional_edges(
            "execute_step",
            self.r_has_more_steps,
            {
                "more": "execute_step",
                "done": "merge",
            },
        )
        
        # Single execution
        g.add_conditional_edges(
            "run_single",
            self.r_after_run_single,
            {
                "done": END,
                "merge": "merge",
                "escalate": "execute_step",  # zero-row escalation → composite path
            }
        )
        
        # Final flow
        g.add_edge("merge", "summarize")
        g.add_edge("summarize", END)

        return g.compile()

    # =========================================================
    # Public API
    # =========================================================

    def handle(self, *, session_id: str, user_input: str) -> Dict[str, Any]:
        """Main entry point for query handling."""
        start = time.time()
        role = str(session_id or "").split(":")[0] if ":" in str(session_id or "") else "unknown"
        query_preview = " ".join(str(user_input or "").split())[:90]
        logger.info(
            "QUERY_IN | session=%s | role=%s | query_preview=%s | query_chars=%s",
            session_id,
            role,
            query_preview,
            len(user_input or ""),
        )
        try:
            run_config = {
                "configurable": {"thread_id": session_id},
                "metadata": {"session_id": session_id},
                "run_name": session_id,
            }
            out: GraphState = self.graph.invoke(
                {"session_id": session_id, "user_input": user_input, "raw_user_input": user_input},
                config=run_config,
            )
        except Exception as exc:
            logger.error(
                "QUERY_ERROR | session=%s | role=%s | latency=%ss | error=%s",
                session_id,
                role,
                round(time.time() - start, 3),
                str(exc)[:200],
            )
            raise

        trace = []
        artifacts = out.get("artifacts") or {}
        if isinstance(artifacts, dict) and isinstance(artifacts.get("trace"), list):
            trace = artifacts.get("trace") or []

        if out.get("clarification"):
            logger.info(
                "QUERY_DONE | session=%s | role=%s | intent=%s | clarification=true | latency=%ss",
                session_id,
                role,
                out.get("intent_key"),
                round(time.time() - start, 3),
            )
            return {
                "intent_key": out.get("intent_key"),
                "slots": out.get("slots") or {},
                "clarification": out.get("clarification"),
                "data": {},
                "trace": trace,
            }

        merged = out.get("merged") or {}
        agents = merged.get("dynamic_sql_agents", [])
        logger.info(
            "ROUTING | session=%s | intent=%s | agents=%s",
            session_id,
            out.get("intent_key"),
            agents,
        )
        logger.info(
            "QUERY_DONE | session=%s | role=%s | intent=%s | latency=%ss",
            session_id,
            role,
            out.get("intent_key"),
            round(time.time() - start, 3),
        )

        return {
            "intent_key": out.get("intent_key"),
            "slots": out.get("slots") or {},
            "answer": out.get("answer") or "",
            "data": merged,
            "dynamic_sql_used": merged.get("dynamic_sql_used", False),
            "dynamic_sql_agents": merged.get("dynamic_sql_agents", []),
            "plan": merged.get("plan"),
            "trace": trace,
        }