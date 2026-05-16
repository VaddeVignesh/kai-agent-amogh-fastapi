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
        """Load session context from Redis"""
        state["session_ctx"] = self.redis.load_session(state["session_id"])
        return state

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
        """
        Extract intent and slots from user input.
        Clear previous slots. Pattern-based override for scenario comparisons.
        """
        # Preserve what the user actually typed this turn.
        if not isinstance(state.get("raw_user_input"), str):
            state["raw_user_input"] = state.get("user_input", "") or ""
        raw_user_input = state.get("raw_user_input", "") or ""

        # user_input is the effective query used for intent/planning/execution.
        user_input = state.get("user_input", "") or raw_user_input
        session_id = state.get("session_id", "")
        session_ctx = state.get("session_ctx") or {}
        # LangGraph may populate declared state keys with None. Ensure artifacts is always a dict.
        if not isinstance(state.get("artifacts"), dict):
            state["artifacts"] = {}

        # Fast-path exact incomplete entity questions so clarification does not wait on LLM routing.
        normalized_user_input = re.sub(r"\s+", " ", (user_input or "").strip().lower()).rstrip("?.!")
        for fastpath_intent, phrases in get_incomplete_entity_fastpath_queries().items():
            if normalized_user_input in phrases:
                state["intent_key"] = fastpath_intent
                state["slots"] = {}
                artifacts = state.get("artifacts") or {}
                artifacts["intent_key"] = fastpath_intent
                artifacts["slots"] = {}
                artifacts["user_input"] = user_input
                state["artifacts"] = artifacts
                self._trace(
                    state,
                    {
                        "phase": "intent_extraction",
                        "intent_key": fastpath_intent,
                        "slots": {},
                        "likely_path": "single",
                        "source": "incomplete_entity_fastpath",
                    },
                )
                return state

        # Fast-path obvious voyage lookup requests. This avoids an LLM intent call for
        # simple inputs like "show voyage 2205" while leaving analytical/follow-up
        # questions on the normal router path.
        simple_voyage_match = re.search(r"\bvoyage\s+(?:number\s+|no\.?\s+|#\s*)?(\d{3,5})\b", user_input, flags=re.IGNORECASE)
        if simple_voyage_match:
            ul_fast = user_input.lower()
            analytical_markers = get_simple_voyage_analytical_markers()
            if not any(marker in ul_fast for marker in analytical_markers):
                voyage_number = int(simple_voyage_match.group(1))
                intent_key = "voyage.summary"
                cleaned_slots = {
                    "voyage_number": voyage_number,
                    "voyage_numbers": [voyage_number],
                }

                state["intent_key"] = intent_key
                state["slots"] = cleaned_slots
                artifacts = state.get("artifacts") or {}
                artifacts["intent_key"] = intent_key
                artifacts["slots"] = cleaned_slots
                artifacts["user_input"] = user_input
                state["artifacts"] = artifacts
                self._trace(
                    state,
                    {
                        "phase": "intent_extraction",
                        "intent_key": intent_key,
                        "slots": cleaned_slots,
                        "likely_path": "single",
                        "source": "simple_voyage_fastpath",
                    },
                )
                return state

        # Structured intent extraction feeds routing hints and diagnostic logging.
        def _shadow_llm_fn(prompt: str) -> str:
            try:
                resp = self.llm._call_with_retry(
                    system=prompt,
                    user=json.dumps({"query": user_input}),
                    operation="shadow_intent_extraction",
                )
                if isinstance(resp, str):
                    return resp
                if isinstance(resp, dict):
                    return json.dumps(resp)
                return str(resp)
            except Exception:
                return ""

        _shadow_pre = None
        try:
            _shadow_pre = _shadow_extract(user_input, session_ctx, _shadow_llm_fn)
            if _shadow_pre:
                _sil_logger.info(
                    f"SHADOW | query='{user_input[:80]}' | "
                    f"op={_shadow_pre.get('operation')} | "
                    f"confidence={_shadow_pre.get('confidence')} | "
                    f"sources={_shadow_pre.get('required_sources')} | "
                    f"scope={_shadow_pre.get('scope')} | "
                    f"follow_up_action={_shadow_pre.get('follow_up_action')} | "
                    f"det_intent={session_ctx.get('last_intent')}"
                )
            else:
                _sil_logger.warning(f"SHADOW_FAIL | query='{user_input[:80]}'")
        except Exception:
            pass
        # Persist structured intent for primary routing.
        if isinstance(_shadow_pre, dict) and _shadow_pre.get("confidence") in ("high", "medium"):
            if isinstance(session_ctx, dict):
                session_ctx["_structured_intent"] = _shadow_pre
                state["session_ctx"] = session_ctx
                # Keep aggregate-style operations on their configured primary source.
                _si_check = session_ctx.get("_structured_intent")
                if isinstance(_si_check, dict):
                    _si_op_chk = str(_si_check.get("operation") or "").lower()
                    _si_src = _si_check.get("required_sources") or []
                    if not isinstance(_si_src, list):
                        _si_src = []
                    # Operation ids whose intent_catalog sources are postgres-only (see config/intent_catalog.yaml).
                    _SINGLE_SRC_OPS = {
                        "aggregate_analytics",
                        "aggregation_analytics",
                        "ranked_list",
                        "comparative_analysis",
                    }
                    if len(_si_src) > 1 and _si_op_chk in _SINGLE_SRC_OPS:
                        _si_check["required_sources"] = (
                            ["postgres"] if "postgres" in _si_src else [_si_src[0]]
                        )
                        session_ctx["_structured_intent"] = _si_check
                logger.info(
                    "PRIMARY_STRUCTURED|phase=4a|confidence=%s|scope=%s|follow_up_action=%s",
                    _shadow_pre.get("confidence"),
                    _shadow_pre.get("scope"),
                    _shadow_pre.get("follow_up_action"),
                )
                try:
                    self.redis.save_session(
                        state["session_id"],
                        {"_structured_intent": _shadow_pre},
                    )
                except Exception:
                    pass

        # =========================================================
        # Pending clarification: treat the user's message as a slot value
        # =========================================================
        pending_intent = session_ctx.get("pending_intent") if isinstance(session_ctx, dict) else None
        pending_missing = session_ctx.get("missing_keys") if isinstance(session_ctx, dict) else None
        if isinstance(pending_intent, str) and isinstance(pending_missing, list) and pending_missing:
            options = {}
            if isinstance(session_ctx, dict) and isinstance(session_ctx.get("clarification_options"), dict):
                options = session_ctx.get("clarification_options") or {}
            pending_question = (session_ctx.get("pending_question") or "") if isinstance(session_ctx, dict) else ""
            pending_slots_base = (session_ctx.get("pending_slots") or {}) if isinstance(session_ctx, dict) else {}
            if not isinstance(pending_slots_base, dict):
                pending_slots_base = {}
            resolved_slots = self._try_resolve_missing_from_text(
                intent_key=pending_intent,
                missing_keys=pending_missing,
                user_input=raw_user_input,
                options=options,
            )
            if resolved_slots is not None:
                # Clear pending clarification immediately.
                try:
                    self.redis.save_session(
                        state["session_id"],
                        {
                            **(session_ctx or {}),
                            "pending_intent": None,
                            "missing_keys": None,
                            "clarification_options": {},
                        },
                    )
                except Exception:
                    pass
                # Keep in-memory state aligned with persisted session to prevent stale
                # pending clarification data from being re-written later in this turn.
                if isinstance(session_ctx, dict):
                    session_ctx["pending_intent"] = None
                    session_ctx["missing_keys"] = None
                    session_ctx["clarification_options"] = {}
                    session_ctx["pending_question"] = None
                    session_ctx["pending_slots"] = {}
                    state["session_ctx"] = session_ctx

                intent_key = pending_intent
                extracted_slots = {**pending_slots_base, **resolved_slots}
                followup_used = True

                # Continue through the normal cleaning logic below, but skip the LLM call.
                # Use the ORIGINAL pending question as the effective query for planning/execution.
                if isinstance(pending_question, str) and pending_question.strip():
                    user_input = pending_question.strip()
                    state["user_input"] = user_input
                # ---------------------------------------------------------
                # Deterministic port/vessel/voyage shaping for incomplete queries
                # ---------------------------------------------------------
                intent_key, extracted_slots = self._maybe_override_incomplete_entity_intent(
                    intent_key=intent_key,
                    extracted_slots=extracted_slots,
                    user_input=user_input,
                )

                # Use the same cleaning pipeline section (copied from below)
                cleaned_slots = {}
                user_input_lower = user_input.lower()
                # Registry-driven slot cleanup — no hardcoded intent list.
                _icfg = INTENT_REGISTRY.get(intent_key, {})
                _is_fleet_wide = _icfg.get("route") == "composite"
                _allowed = set(
                    (_icfg.get("required_slots") or [])
                    + (_icfg.get("optional_slots") or [])
                )

                if _is_fleet_wide:
                    for key, val in extracted_slots.items():
                        if key in _allowed:
                            cleaned_slots[key] = val
                else:
                    for key, value in extracted_slots.items():
                        cleaned_slots[key] = value

                state["intent_key"] = intent_key
                state["slots"] = cleaned_slots
                artifacts = state.get("artifacts") or {}
                artifacts["intent_key"] = intent_key
                artifacts["slots"] = cleaned_slots
                artifacts["user_input"] = user_input
                state["artifacts"] = artifacts
                self._trace(
                    state,
                    {
                        "phase": "intent_extraction",
                        "intent_key": intent_key,
                        "slots": cleaned_slots,
                        "source": "clarification_followup",
                        "raw_user_input": raw_user_input,
                        "effective_user_input": user_input,
                    },
                )
                return state
            else:
                # If the user asked a *new question* instead of answering the clarification,
                # drop pending state to avoid polluting the new turn.
                if self._looks_like_new_question(user_input):
                    try:
                        self.redis.save_session(
                            state["session_id"],
                            {**(session_ctx or {}), "pending_intent": None, "missing_keys": None},
                        )
                    except Exception:
                        pass

        # Fast-path for chit-chat: avoid touching external dependencies (LLM/DB/Redis memory)
        # for greetings / identity / help messages. These should always be handled as out_of_scope.
        if self._is_chitchat(user_input):
            intent_key = "out_of_scope"
            cleaned_slots: Dict[str, Any] = {}

            state["intent_key"] = intent_key
            state["slots"] = cleaned_slots

            artifacts = state.get("artifacts") or {}
            artifacts["intent_key"] = intent_key
            artifacts["slots"] = cleaned_slots
            artifacts["user_input"] = user_input
            state["artifacts"] = artifacts
            self._trace(
                state,
                {
                    "phase": "intent_extraction",
                    "intent_key": intent_key,
                    "slots": cleaned_slots,
                    "source": "chitchat_fastpath",
                },
            )
            return state

        # =========================================================
        # Result-set follow-ups ("among these/from above/in that list")
        # If we have a previous multi-row result set stored, route TRUE follow-ups
        # to a deterministic handler. Avoid false-positives on brand-new questions
        # that merely contain words like "remarks" and "explain".
        # =========================================================
        ul = (user_input or "").strip().lower()
        words = [w for w in ul.split() if w]
        has_result_set = isinstance(session_ctx, dict) and isinstance(session_ctx.get("last_result_set"), dict)
        # Composite fleet delay + PnL + offhire queries must not be hijacked by last_result_set.
        stale_delay_guard = get_result_set_stale_delay_guard_terms()
        if (
            has_result_set
            and all(term in ul for term in stale_delay_guard.get("all", []))
            and any(term in ul for term in stale_delay_guard.get("any", []))
            and any(term in ul for term in stale_delay_guard.get("scope", []))
        ):
            has_result_set = False
        turn_type = self._classify_turn_type(session_ctx=session_ctx, user_input=user_input)
        explicit_fresh_entity = self._looks_like_explicit_fresh_entity_request(user_input)
        fresh_fleet_markers = get_fresh_fleet_markers()
        fresh_fleet_turn = any(marker in ul for marker in fresh_fleet_markers)
        explicit_rs_ref = any(
            p in ul
            for p in get_explicit_result_set_reference_phrases()
        )
        starts_like_new_request = ul.startswith(get_new_request_prefixes())
        _phase4a_fresh_ranking_utterance, _corpus_fresh_guard = GraphRouter._corpus_fresh_guard_flags(ul)

        # Use structured follow-up scope only when the operation/action is concrete.
        _si_scope = session_ctx.get("_structured_intent") if isinstance(session_ctx, dict) else None
        _structured_scope_ok = isinstance(_si_scope, dict) and _si_scope.get("confidence") in ("high", "medium")
        if _structured_scope_ok:
            _si_op = str(_si_scope.get("operation") or "").strip().lower()
            _si_fu_act = _si_scope.get("follow_up_action")
            _si_sc_raw = str(_si_scope.get("scope") or "").strip().lower()
            # Only treat as session follow-up when catalog operation is follow_up_filter
            # and the model picked a concrete in-memory action. This avoids ranking /
            # aggregate rows (e.g. T18, T22) being misclassified as followup.result_set
            # when the extractor sets scope=follow_up with a spurious follow_up_action.
            _genuine_followup = (
                _si_sc_raw in get_structured_followup_scopes()
                and _si_op in get_structured_followup_operations()
                and (_si_fu_act is not None and str(_si_fu_act).strip() != "")
            )
            if _genuine_followup and not _corpus_fresh_guard:
                turn_type = "followup"
                fresh_fleet_turn = False
                starts_like_new_request = False

        thread_override = None
        if not explicit_fresh_entity:
            thread_override = self._direct_session_thread_override(
                session_ctx=session_ctx,
                user_input=user_input,
            )

        if (
            thread_override is not None
            and "context" in ul
            and any(p in ul for p in get_session_thread_context_markers())
        ):
            override_intent, override_slots = thread_override
            state["intent_key"] = override_intent
            state["slots"] = override_slots
            artifacts = state.get("artifacts") or {}
            artifacts["intent_key"] = override_intent
            artifacts["slots"] = override_slots
            artifacts["user_input"] = user_input
            state["artifacts"] = artifacts
            self._trace(
                state,
                {
                    "phase": "intent_extraction",
                    "intent_key": override_intent,
                    "slots": override_slots,
                    "source": "session_thread_context_override",
                },
            )
            return state

        if has_result_set and explicit_rs_ref and turn_type == "followup":
            # Explicitly referencing the prior list/result set.
            state["intent_key"] = "followup.result_set"
            state["slots"] = {"action": "compare_extremes"}
            artifacts = state.get("artifacts") or {}
            artifacts["intent_key"] = "followup.result_set"
            artifacts["slots"] = state["slots"]
            artifacts["user_input"] = user_input
            state["artifacts"] = artifacts
            self._trace(
                state,
                {"phase": "intent_extraction", "intent_key": "followup.result_set", "slots": state["slots"], "source": "result_set_followup"},
            )
            return state

        # Explain remarks / what went wrong: only treat as follow-up if it's a short follow-up prompt,
        # NOT a full new request like "Show me the top 5 ... and include remarks that explain why ...".
        is_short_followup = len(words) <= 12
        starts_followup_verb = ul.startswith(get_result_set_explain_verbs())
        mentions_remarks = ("remark" in ul) or ("remarks" in ul)
        if has_result_set and mentions_remarks and is_short_followup and starts_followup_verb and not starts_like_new_request and turn_type == "followup" and not explicit_fresh_entity:
            # Removed brand-specific routing — scope handled by structured intent when available.
            _looks_fresh = any(
                ul.startswith(p) for p in get_result_set_fresh_start_prefixes()
            )
            if _looks_fresh:
                pass  # fall through to normal intent extraction
            else:
                state["intent_key"] = "followup.result_set"
                state["slots"] = {"action": "explain_remarks"}
                artifacts = state.get("artifacts") or {}
                artifacts["intent_key"] = "followup.result_set"
                artifacts["slots"] = state["slots"]
                artifacts["user_input"] = user_input
                state["artifacts"] = artifacts
                self._trace(
                    state,
                    {"phase": "intent_extraction", "intent_key": "followup.result_set", "slots": state["slots"], "source": "result_set_followup"},
                )
                return state

        # Extremes (highest/lowest) as a short follow-up prompt.
        if (
            has_result_set
            and any(k in ul for k in get_result_set_extreme_keywords())
            and is_short_followup
            and not explicit_fresh_entity
            and (
                not starts_like_new_request
                or any(k in ul for k in get_result_set_scope_phrases())
                or ul.startswith(get_result_set_question_prefixes())
            )
            and turn_type == "followup"
            and not any(ul.startswith(p) for p in get_result_set_fresh_start_prefixes())
        ):
            state["intent_key"] = "followup.result_set"
            state["slots"] = {"action": "compare_extremes"}
            artifacts = state.get("artifacts") or {}
            artifacts["intent_key"] = "followup.result_set"
            artifacts["slots"] = state["slots"]
            artifacts["user_input"] = user_input
            state["artifacts"] = artifacts
            self._trace(
                state,
                {"phase": "intent_extraction", "intent_key": "followup.result_set", "slots": state["slots"], "source": "result_set_followup"},
            )
            return state

        # Generic result-set operations (top/bottom/project/compare/filter) across any prior list result.
        # Allow polite command phrasing ("can you list ...") even if turn classifier is conservative.
        if has_result_set:
            op_slots = self._parse_result_set_followup_action(user_input=user_input, session_ctx=session_ctx)
            polite_followup = ul.startswith(get_polite_followup_prefixes())
            concise_followup_override = (not fresh_fleet_turn) and len(words) <= 12 and any(
                k in ul for k in get_concise_followup_keywords()
            )
            # Never let the metric parser hijack fleet-wide or corpus questions; the classifier
            # may mark them new_question but metric_followup_override used to bypass that.
            # Do not let stale result-set memory hijack fresh ranking or trend asks.
            metric_followup_override = (
                (not fresh_fleet_turn)
                and (not _corpus_fresh_guard)
                and isinstance(op_slots, dict)
                and op_slots.get("action") in get_metric_followup_override_actions()
                and len(words) <= 12
            )
            result_set_refinement = concise_followup_override and any(
                k in ul
                for k in get_result_set_refinement_phrases()
            )
            if (
                has_result_set
                and any(k in ul for k in get_operational_followup_keywords())
                and any(k in ul for k in get_same_entity_reference_words())
            ):
                lfs = session_ctx.get("last_focus_slots") if isinstance(session_ctx, dict) else None
                if isinstance(lfs, dict):
                    scoped_slots = {}
                    for key in ("vessel_name", "imo", "vessel_imo", "voyage_number"):
                        if lfs.get(key) not in (None, "", [], {}):
                            scoped_slots[key] = lfs.get(key)
                    if scoped_slots:
                        state["intent_key"] = "vessel.metadata"
                        state["slots"] = scoped_slots
                        artifacts = state.get("artifacts") or {}
                        artifacts["intent_key"] = "vessel.metadata"
                        artifacts["slots"] = scoped_slots
                        artifacts["user_input"] = user_input
                        state["artifacts"] = artifacts
                        self._trace(
                            state,
                            {"phase": "intent_extraction", "intent_key": "vessel.metadata", "slots": scoped_slots, "source": "result_set_operational_followup"},
                        )
                        return state
            if (
                (not _corpus_fresh_guard)
                and isinstance(op_slots, dict)
                and op_slots.get("action")
                and ((not explicit_fresh_entity) or result_set_refinement)
                and (
                    turn_type == "followup"
                    or polite_followup
                    or concise_followup_override
                    or metric_followup_override
                )
            ):
                state["intent_key"] = "followup.result_set"
                state["slots"] = op_slots
                artifacts = state.get("artifacts") or {}
                artifacts["intent_key"] = "followup.result_set"
                artifacts["slots"] = state["slots"]
                artifacts["user_input"] = user_input
                state["artifacts"] = artifacts
                self._trace(
                    state,
                    {"phase": "intent_extraction", "intent_key": "followup.result_set", "slots": state["slots"], "source": "result_set_followup_generic"},
                )
                return state

        if thread_override is not None:
            override_intent, override_slots = thread_override
            state["intent_key"] = override_intent
            state["slots"] = override_slots
            artifacts = state.get("artifacts") or {}
            artifacts["intent_key"] = override_intent
            artifacts["slots"] = override_slots
            artifacts["user_input"] = user_input
            state["artifacts"] = artifacts
            self._trace(
                state,
                {
                    "phase": "intent_extraction",
                    "intent_key": override_intent,
                    "slots": override_slots,
                    "source": "session_thread_override",
                },
            )
            return state
        
        ex = self.llm.extract_intent_slots(
            text=user_input,
            supported_intents=list(SUPPORTED_INTENTS),
            schema_hint={
                "slots": [
                    "vessel_name", "imo",
                    "voyage_number", "voyage_id", "voyage_numbers",
                    "date_from", "date_to",
                    "limit", "port_name", "scenario",
                    "cargo_type", "cargo_grade",
                    "metric", "group_by", "threshold"
                ]
            },
            session_context=session_ctx,
        )

        # Log structured intent output for diagnostics.
        _shadow = _shadow_extract(user_input, session_ctx, _shadow_llm_fn)
        if _shadow:
            _sil_logger.info(
                f"SHADOW | query='{user_input[:80]}' | "
                f"op={_shadow.get('operation')} | "
                f"confidence={_shadow.get('confidence')} | "
                f"sources={_shadow.get('required_sources')} | "
                f"scope={_shadow.get('scope')} | "
                f"follow_up_action={_shadow.get('follow_up_action')} | "
                f"det_intent={ex.get('intent_key')}"
            )
        else:
            _sil_logger.warning(f"SHADOW_FAIL | query='{user_input[:80]}'")
        
        intent_key = ex.get("intent_key", "out_of_scope")
        extracted_slots = ex.get("slots", {}) or {}
        # session_ctx already loaded above

        # The LLM may return followup.result_set for fresh ranking/trend asks when
        # session still holds an unrelated last_result_set — remap to primary fleet intents.
        if _phase4a_fresh_ranking_utterance and intent_key == "followup.result_set":
            if any(term in ul for term in get_fresh_ranking_pnl_terms()):
                intent_key = "ranking.voyages_by_pnl"
            elif any(term in ul for term in get_fresh_ranking_segment_terms()):
                intent_key = "analysis.segment_performance"
            elif any(term in ul for term in get_fresh_ranking_trend_terms()):
                intent_key = "aggregation.trends"
            if isinstance(ex, dict):
                ex["intent_key"] = intent_key

        # Fix common LLM glitch: "tell me about voyage 1901" → vessel_name="voyage 1901"
        try:
            vn = extracted_slots.get("voyage_number")
            vname = extracted_slots.get("vessel_name")
            if isinstance(vname, str):
                vname_norm = vname.strip().lower()
                if vname_norm.startswith("voyage ") and any(ch.isdigit() for ch in vname_norm):
                    # If it looks like a voyage reference, drop it as a vessel name.
                    extracted_slots.pop("vessel_name", None)
                elif vn is not None and vname_norm == f"voyage {int(float(vn))}":
                    extracted_slots.pop("vessel_name", None)
        except Exception:
            pass

        # Pattern-based override for scenario comparisons
        is_scenario_comp, updated_slots = self._detect_scenario_comparison(
            user_input, extracted_slots, session_ctx
        )
        if is_scenario_comp:
            # Override intent if LLM classified incorrectly
            if intent_key in ["out_of_scope", "voyage.summary", "composite.query"]:
                _dprint(f"   🔄 PATTERN OVERRIDE: {intent_key} → comparison.scenario")
                intent_key = "comparison.scenario"
            extracted_slots = updated_slots
        # Fallback: when-fixed + compare/variance but pattern missed → force scenario comparison
        ul = user_input.lower()
        if intent_key == "out_of_scope" and ("when-fixed" in ul or "when fixed" in ul):
            if "compare" in ul or "variance" in ul or "versus" in ul or "vs" in ul:
                intent_key = "analysis.scenario_comparison"
                _dprint(f"   🔄 FALLBACK: out_of_scope → analysis.scenario_comparison")
        
        # Convert voyage_number(s) float to int
        if "voyage_number" in extracted_slots:
            try:
                vn = extracted_slots["voyage_number"]
                if isinstance(vn, list):
                    extracted_slots["voyage_number"] = [int(float(v)) for v in vn]
                else:
                    extracted_slots["voyage_number"] = int(float(vn))
            except (ValueError, TypeError):
                pass
        
        if "voyage_numbers" in extracted_slots:
            try:
                vns = extracted_slots["voyage_numbers"]
                if isinstance(vns, list):
                    extracted_slots["voyage_numbers"] = [int(float(v)) for v in vns]
            except (ValueError, TypeError):
                pass
        
        # Convert limit float to int
        if "limit" in extracted_slots:
            try:
                extracted_slots["limit"] = int(float(extracted_slots["limit"]))
            except (ValueError, TypeError):
                pass
        
        # Convert threshold float to int
        if "threshold" in extracted_slots:
            try:
                extracted_slots["threshold"] = int(float(extracted_slots["threshold"]))
            except (ValueError, TypeError):
                pass

        # Explicit voyage identifiers in the current query must take precedence over
        # previously inferred vessel anchors from session memory.
        explicit_voyage_request = bool(
            re.search(r"\bvoyage\s+\d{3,5}\b", ul)
            or (
                (
                    extracted_slots.get("voyage_number") not in (None, "", [], {})
                    or (isinstance(extracted_slots.get("voyage_numbers"), list) and bool(extracted_slots.get("voyage_numbers")))
                    or extracted_slots.get("voyage_id") not in (None, "", [], {})
                )
                and any(k in ul for k in ("voyage", "voyages", "belongs to", "assigned"))
            )
        )
        explicit_vessel_in_query = len(GraphRouter._explicit_vessel_mentions(user_input)) > 0 or bool(re.search(r"\b\d{7}\b", user_input or ""))
        if explicit_voyage_request and not explicit_vessel_in_query:
            extracted_slots.pop("vessel_name", None)
            extracted_slots.pop("imo", None)
            extracted_slots.pop("vessel_imo", None)

        # =========================================================
        # Deterministic intent for cargo-grade voyage filters
        # "what voyages have NHC as cargo grade" should not go out_of_scope.
        # =========================================================
        if any(k in ul for k in get_cargo_grade_terms()) and any(k in ul for k in get_cargo_grade_voyage_terms()):
            if extracted_slots.get("cargo_grade") or extracted_slots.get("cargo_type"):
                extracted_slots["cargo_grade"] = extracted_slots.get("cargo_grade") or extracted_slots.get("cargo_type")
                intent_key = "ops.voyages_by_cargo_grade"

        # =========================================================
        # Session memory (follow-up resolution)
        # If the user asks a follow-up without repeating the entity,
        # reuse the last known voyage/vessel from Redis session slots.
        # =========================================================
        llm_followup = bool(ex.get("is_followup"))
        backward_reference = bool(ex.get("backward_reference"))
        followup_confidence = str(ex.get("followup_confidence") or "").strip().lower()
        inherit_slot_keys = ex.get("inherit_slots_from_session")
        inherit_slot_keys = inherit_slot_keys if isinstance(inherit_slot_keys, list) else []
        inherit_slot_keys = [str(k).strip() for k in inherit_slot_keys if str(k).strip()]

        extracted_slots = self._validate_entity_slots(extracted_slots)

        sess_mem = session_ctx.get("memory_slots") or session_ctx.get("slots") or {}
        followup_used = False
        if llm_followup or (followup_confidence == "high" and inherit_slot_keys):
            intent_key, extracted_slots, followup_used = self._apply_session_followup(
                intent_key=intent_key,
                extracted_slots=extracted_slots,
                session_ctx=session_ctx,
                user_input=user_input,
                inherit_slot_keys=inherit_slot_keys,
                backward_reference=backward_reference,
            )
            extracted_slots = self._validate_entity_slots(extracted_slots)
            if explicit_voyage_request and not explicit_vessel_in_query:
                extracted_slots.pop("vessel_name", None)
                extracted_slots.pop("imo", None)
                extracted_slots.pop("vessel_imo", None)

        # Session follow-up logic can re-map to
        # followup.result_set for fresh ranking/trend utterances — restore primary intents.
        if _phase4a_fresh_ranking_utterance and intent_key == "followup.result_set":
            if any(term in ul for term in get_fresh_ranking_pnl_terms()):
                intent_key = "ranking.voyages_by_pnl"
            elif any(term in ul for term in get_fresh_ranking_segment_terms()):
                intent_key = "analysis.segment_performance"
            elif any(term in ul for term in get_fresh_ranking_trend_terms()):
                intent_key = "aggregation.trends"

        if intent_key == "cargo.details" and not (
            extracted_slots.get("cargo_type")
            or extracted_slots.get("cargo_grade")
            or extracted_slots.get("cargo_grades")
        ):
            sess_mem_local = session_ctx.get("memory_slots") or session_ctx.get("slots") or {}
            if any((sess_mem_local or {}).get(k) not in (None, "", [], {}) for k in ("voyage_number", "voyage_id", "voyage_numbers")):
                intent_key = "voyage.metadata"

        if intent_key == "cargo.details" and (
            extracted_slots.get("voyage_number")
            or extracted_slots.get("voyage_id")
            or extracted_slots.get("voyage_numbers")
        ):
            intent_key = "voyage.metadata"

        # =========================================================
        # Direct entity-thread rescue
        # Let the LLM see recent turn history first, then rescue weak/new-query
        # classifications back onto the active voyage/vessel thread when the
        # user is clearly continuing that thread.
        # =========================================================
        if llm_followup and not self._has_entity_anchor(extracted_slots):
            skip_v_hist = len(GraphRouter._explicit_vessel_mentions(user_input)) >= 2
            for key in self._entity_slot_keys():
                if skip_v_hist and key in ("vessel_name", "imo"):
                    continue
                inherited_value = self._find_slot_in_turn_history(
                    session_ctx=session_ctx,
                    slot_key=key,
                    prefer_older=backward_reference,
                )
                if inherited_value not in (None, "", [], {}):
                    extracted_slots[key] = inherited_value
            extracted_slots = self._validate_entity_slots(extracted_slots)
            followup_used = self._has_entity_anchor(extracted_slots)

        extracted_slots = self._normalize_vessel_anchor_from_query(
            extracted_slots, user_input, session_ctx
        )
        extracted_slots = self._validate_entity_slots(extracted_slots)

        comparison_override = self._comparison_followup_override(
            session_ctx=session_ctx,
            user_input=user_input,
            intent_key=intent_key,
            extracted_slots=extracted_slots,
            backward_reference=backward_reference,
        )
        if comparison_override is not None:
            intent_key, extracted_slots, effective_user_input = comparison_override
            state["user_input"] = effective_user_input
            user_input = effective_user_input
            followup_used = True

        # =========================================================
        # Lightweight parameter memory (limit/date/scenario/etc.)
        # Only applies on follow-ups and only for safe preference-like keys.
        # =========================================================
        extracted_slots = self._apply_session_param_memory(
            extracted_slots=extracted_slots,
            session_ctx=session_ctx,
            user_input=user_input,
            followup_used=followup_used,
        )

        # =========================================================
        # Deterministic port extraction + intent override for ops port queries
        # Example: "Find voyages with Rotterdam in the route ..."
        # =========================================================
        intent_key, extracted_slots = self._maybe_override_to_port_query(
            intent_key=intent_key,
            extracted_slots=extracted_slots,
            user_input=user_input,
        )

        # Delayed + negative PnL: avoid ops.port_query with port_name="negative PnL" → force finance.loss_due_to_delay
        if intent_key == "ops.port_query" and extracted_slots.get("port_name"):
            pn = str(extracted_slots.get("port_name", "")).lower()
            if "pnl" in pn or ("negative" in pn and "port" not in pn):
                _dprint(f"   🔄 OVERRIDE: ops.port_query (port_name={extracted_slots.get('port_name')}) → finance.loss_due_to_delay")
                intent_key = "finance.loss_due_to_delay"
                extracted_slots.pop("port_name", None)

        # Handle incomplete entity questions ("tell me about port/vessel/voyage") deterministically.
        intent_key, extracted_slots = self._maybe_override_incomplete_entity_intent(
            intent_key=intent_key,
            extracted_slots=extracted_slots,
            user_input=user_input,
        )
        
        # Clean slots based on what's in the query
        cleaned_slots = {}
        user_input_lower = user_input.lower()

        # Registry-driven slot cleanup — no hardcoded intent list.
        # Reads route + allowed slots directly from INTENT_REGISTRY.
        _icfg = INTENT_REGISTRY.get(intent_key, {})
        _is_fleet_wide = _icfg.get("route") == "composite"
        _allowed = set(
            (_icfg.get("required_slots") or [])
            + (_icfg.get("optional_slots") or [])
        )

        if _is_fleet_wide:
            # Fleet-wide intents: keep only registry-declared slots.
            # Prevents entity anchors (voyage_number, vessel_name, imo, etc.)
            # from polluting fleet-wide queries when the LLM hallucinates them.
            for key, val in extracted_slots.items():
                if key in _allowed:
                    cleaned_slots[key] = val

            # Preserve explicit voyage anchors for scoped aggregate prompts such as
            # "Which ports were visited on voyage 2306?" while still dropping
            # hallucinated entity text for generic fleet-wide asks.
            if "voyage_number" in extracted_slots:
                cleaned_slots["voyage_number"] = extracted_slots["voyage_number"]
            if "voyage_numbers" in extracted_slots:
                cleaned_slots["voyage_numbers"] = extracted_slots["voyage_numbers"]
            if "voyage_id" in extracted_slots:
                cleaned_slots["voyage_id"] = extracted_slots["voyage_id"]

            # Keep explicit vessel anchors only when the query text truly contains
            # the candidate value (prevents false anchors like "is fastest on ballast").
            vname = extracted_slots.get("vessel_name")
            if isinstance(vname, str) and vname.strip() and vname.lower() in user_input_lower:
                cleaned_slots["vessel_name"] = vname
            if "imo" in extracted_slots and str(extracted_slots.get("imo") or "").strip():
                cleaned_slots["imo"] = extracted_slots["imo"]
        else:
            # For specific entity queries, validate slots are mentioned in query
            for key, value in extracted_slots.items():
                if key == "voyage_number":
                    # Keep only if voyage number is mentioned in query
                    if followup_used or str(value) in user_input:
                        cleaned_slots[key] = value
                elif key == "voyage_numbers":
                    # Always keep voyage_numbers for comparisons
                    cleaned_slots[key] = value
                elif key == "vessel_name":
                    # Keep only if vessel name is mentioned in query
                    if followup_used or (isinstance(value, str) and value.lower() in user_input_lower):
                        cleaned_slots[key] = value
                elif key == "scenario":
                    # Always keep scenario for comparisons
                    cleaned_slots[key] = value
                else:
                    # Keep other slots
                    cleaned_slots[key] = value

        # =========================================================
        # Normalize voyage_numbers to voyage_number (single)
        # =========================================================
        if "voyage_numbers" in cleaned_slots and isinstance(cleaned_slots["voyage_numbers"], list) and len(cleaned_slots["voyage_numbers"]) == 1:
            cleaned_slots["voyage_number"] = cleaned_slots["voyage_numbers"][0]

        # Final guard: explicit voyage references should not carry stale vessel filters.
        if explicit_voyage_request and not explicit_vessel_in_query:
            cleaned_slots.pop("vessel_name", None)
            cleaned_slots.pop("imo", None)
            cleaned_slots.pop("vessel_imo", None)
        
        _dprint("\nIntent extraction:")
        _dprint(f"   Intent: {intent_key}")
        _dprint(f"   Slots (cleaned): {cleaned_slots}")

        # Final guard before persisting intent: avoids stale-session
        # followup.result_set on fresh fleet ranking / trend asks regardless of upstream overrides.
        _ul_fin = (user_input or "").strip().lower()
        _, _cf_fin = GraphRouter._corpus_fresh_guard_flags(_ul_fin)
        if intent_key == "followup.result_set" and _cf_fin:
            if any(term in _ul_fin for term in get_fresh_ranking_pnl_terms()):
                intent_key = "ranking.voyages_by_pnl"
            elif any(term in _ul_fin for term in get_fresh_ranking_segment_terms()):
                intent_key = "analysis.segment_performance"
            elif "trend" in _ul_fin and ("month" in _ul_fin or "average" in _ul_fin):
                intent_key = "aggregation.trends"
            elif len(_ul_fin.split()) > 12:
                intent_key = "composite.query"
            elif re.search(r"\b(top|bottom)\s+\d{1,2}\b", _ul_fin):
                intent_key = "composite.query"
        elif intent_key == "followup.result_set" and _ul_fin.startswith(
            ("show ", "show me", "list ", "rank ", "find ", "tell ", "what ", "which ")
        ):
            if "most profitable" in _ul_fin:
                intent_key = "ranking.voyages_by_pnl"
            elif "loss-making" in _ul_fin or "loss making" in _ul_fin:
                intent_key = "analysis.segment_performance"
            elif "trend" in _ul_fin and ("month" in _ul_fin or "average" in _ul_fin):
                intent_key = "aggregation.trends"

        # Pre-classify single vs composite
        is_single = self._is_single_entity_query(intent_key, cleaned_slots, user_input)
        if is_single:
            _dprint(f"   🎯 PRE-CLASSIFIED AS SINGLE (specific entity)")
        else:
            _dprint(f"   🎯 LIKELY COMPOSITE (ranking + multi-agent)")
        
        # Update state with cleaned slots
        state["intent_key"] = intent_key
        state["slots"] = cleaned_slots
        
        # Store in artifacts
        artifacts = state.get("artifacts") or {}
        artifacts["intent_key"] = intent_key
        artifacts["slots"] = cleaned_slots
        artifacts["user_input"] = user_input
        state["artifacts"] = artifacts
        
        self._trace(
            state,
            {
                "phase": "intent_extraction",
                "intent_key": intent_key,
                "slots": cleaned_slots,
                "likely_path": "single" if is_single else "composite",
                "source": "llm_extract_intent_slots",
            },
        )
        
        return state

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

        if GraphRouter._looks_like_explicit_fresh_entity_request(ui):
            return "new_question"

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

    def n_validate_slots(self, state: GraphState) -> GraphState:
        """Validate required slots are present"""
        intent_key = state.get("intent_key", "out_of_scope")
        slots = state.get("slots") or {}

        if intent_key == "ops.port_query" and not slots.get("port_name"):
            _ul = (state.get("user_input") or "").lower()
            _is_fleet_port_aggregate = any(
                p in _ul for p in get_incomplete_entity_fleet_port_aggregate_markers()
            )
            if _is_fleet_port_aggregate:
                # Route to aggregate intent, not port lookup
                intent_key = "ranking.ports"
                state["intent_key"] = intent_key
                # Fall through to normal planning — do NOT trigger clarification

        if intent_key not in INTENT_REGISTRY:
            state["missing_keys"] = []
            return state

        required = INTENT_REGISTRY[intent_key].get("required_slots", [])
        def _is_effectively_missing(k: str) -> bool:
            v = slots.get(k)
            if v in (None, "", [], {}):
                return True
            return False

        missing = [k for k in required if _is_effectively_missing(k)]

        # Fleet-wide/composite intents never need an entity anchor — skip clarification.
        if INTENT_REGISTRY.get(intent_key, {}).get("route") == "composite":
            missing = []

        # Heuristic: treat incomplete entity questions as missing their anchor.
        ui = (state.get("user_input") or "").lower()
        if intent_key == "vessel.summary" and not (slots.get("vessel_name") or slots.get("imo")):
            topic_terms = get_incomplete_entity_topic_terms()
            has_vessel_topic = any(
                re.search(rf"\b{re.escape(term)}\b", ui)
                for term in topic_terms.get("vessel", [])
            )
            if has_vessel_topic and any(p in ui for p in get_incomplete_entity_detail_markers()):
                missing = ["vessel_name"]

        # Also treat extracted vessel_name="vessel"/"ship" as missing.
        if intent_key == "vessel.summary":
            vn = slots.get("vessel_name")
            placeholders = set(get_incomplete_entity_placeholder_slot_values().get("vessel_name", []))
            if isinstance(vn, str) and vn.strip().lower() in placeholders and "vessel_name" not in missing:
                missing = ["vessel_name"]

        state["missing_keys"] = missing
        return state

    def n_make_clarification(self, state: GraphState) -> GraphState:
        """Generate clarification question for missing slots"""
        intent_key = state.get("intent_key", "out_of_scope")
        missing = state.get("missing_keys") or []
        clarification, options = self._build_clarification_message(intent_key=intent_key, missing_keys=missing)
        state["clarification"] = clarification
        sess = state.get("session_ctx") or {}

        persisted_slots = self._build_persisted_slots(
            base=(sess.get("slots") or {}),
            updates=(state.get("slots") or {}),
        )
        memory_slots = self._extract_memory_slots(persisted_slots)
        param_slots = self._extract_param_slots(persisted_slots)

        self.redis.save_session(
            state["session_id"],
            {
                **sess,
                "pending_intent": intent_key,
                "missing_keys": missing,
                "clarification_options": options or {},
                # Needed to reliably continue on the next turn (e.g. user replies "2301").
                "pending_question": state.get("user_input") or "",
                "pending_slots": dict(state.get("slots") or {}),
                # Persist only safe memory + preference-like params (no voyage_ids, etc.)
                "memory_slots": memory_slots,
                "param_slots": param_slots,
                "slots": persisted_slots,
            },
        )

        return state

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
        """
        Build execution plan: single or composite. Planner intent overrides extracted intent.
        """

        sess = state.get("session_ctx") or {}
        user_input = state.get("user_input") or ""

        # 1️⃣ Build plan
        plan: ExecutionPlan = self.planner.build_plan(
            text=user_input,
            session_context=sess,
            intent_key=state.get("intent_key"),
            slots=state.get("slots"),
        )

        # Planner overrides extracted intent
        state["intent_key"] = plan.intent_key
        state["plan_type"] = plan.plan_type

        # 3️⃣ Store plan safely
        state["plan"] = {
            "plan_type": plan.plan_type,
            "intent_key": plan.intent_key,
            "required_slots": plan.required_slots or [],
            "confidence": plan.confidence,
            "steps": [asdict(s) for s in (plan.steps or [])],
        }

        state["step_index"] = 0

        # 4️⃣ Initialize artifacts cleanly
        cleaned_slots = dict(state.get("slots") or {})

        prev_artifacts = state.get("artifacts") or {}
        prev_trace = prev_artifacts.get("trace") if isinstance(prev_artifacts, dict) else None
        if not isinstance(prev_trace, list):
            prev_trace = []

        state["artifacts"] = {
            "slots": cleaned_slots,
            "user_input": user_input,
            "intent_key": plan.intent_key,
            "trace": prev_trace,
        }

        self._trace(
            state,
            {
                "phase": "planning",
                "plan_type": plan.plan_type,
                "intent_key": plan.intent_key,
                "steps": [asdict(s) for s in (plan.steps or [])],
            },
        )

        _dprint("\nPlan classification")
        _dprint(f"   Plan Type: {plan.plan_type}")
        _dprint(f"   Final Intent: {plan.intent_key}")
        _dprint(f"   Steps: {len(plan.steps)}")
        _dprint(f"   Slots: {cleaned_slots}")

        return state

    # =========================================================
    # Single Execution (Standard Flow)
    # =========================================================

    def n_run_single(self, state: GraphState) -> GraphState:
        """Standard execution flow using intent registry."""
        intent_key = state.get("intent_key") or state.get("plan", {}).get("intent_key", "out_of_scope")
        cfg = INTENT_REGISTRY.get(intent_key, {})
        session_context = state.get("session_ctx") or {}
        slots = self._merge_slots(
            intent_key,
            session_context,
            state.get("slots") or {},
            state.get("user_input") or state.get("raw_user_input") or "",
        )
        user_input = state.get("user_input") or ""
        plan_type = str(state.get("plan_type") or (state.get("plan") or {}).get("plan_type") or "single").lower()

        _dprint(f"\nSingle execution: {intent_key}")

        def _registry_sql_from_result(res: Any) -> str | None:
            if not isinstance(res, dict):
                return None
            if str(res.get("mode") or "").strip().lower() != "registry_sql":
                return None
            qk = res.get("query_key")
            if not isinstance(qk, str) or not qk.strip():
                return None
            spec = SQL_REGISTRY.get(qk)
            if not spec:
                return None
            try:
                return str(spec.sql).strip()
            except Exception:
                return None

        # =========================================================
        # Result-set follow-ups (deterministic, uses Redis memory)
        # =========================================================
        if intent_key == "followup.result_set":
            rs = (session_context or {}).get("last_result_set") if isinstance(session_context, dict) else None
            rows = (rs or {}).get("rows") if isinstance(rs, dict) else None
            rows = rows if isinstance(rows, list) else []
            # Optional in-memory refinement from structured follow-up before action handlers.
            si_fu = (session_context or {}).get("_structured_intent") if isinstance(session_context, dict) else None
            if isinstance(si_fu, dict) and si_fu.get("confidence") in ("high", "medium"):
                scp = str(si_fu.get("scope") or "").strip().lower()
                if scp in ("follow_up", "followup"):
                    rs_slots = (session_context or {}).get("last_focus_slots") if isinstance(session_context, dict) else {}
                    if not isinstance(rs_slots, dict):
                        rs_slots = {}
                    fr = resolve_followup(si_fu, {"last_result_rows": list(rows), "resolved_slots": rs_slots})
                    if fr and isinstance(fr.get("resolved_rows"), list):
                        rows = fr["resolved_rows"]
                        logger.info(
                            "CACHE_RESOLVED|phase=4a|follow_up_action=%s|row_count=%s",
                            si_fu.get("follow_up_action"),
                            len(rows),
                        )
            action = (slots.get("action") or "").strip().lower()
            ul = (user_input or "").lower()

            def _num(x: Any) -> float | None:
                try:
                    if x is None:
                        return None
                    if isinstance(x, (int, float)):
                        return float(x)
                    s = str(x).replace(",", "").strip()
                    return float(s)
                except Exception:
                    return None

            def _extract_ports(val: Any) -> list[str]:
                if val in (None, "", [], {}):
                    return []
                if isinstance(val, list):
                    out = []
                    for p in val[:30]:
                        if p in (None, "", [], {}):
                            continue
                        if isinstance(p, dict):
                            s = str(
                                p.get("port_name")
                                or p.get("port")
                                or p.get("name")
                                or p.get("portName")
                                or ""
                            ).strip()
                        else:
                            s = str(p).strip()
                        if s:
                            out.append(s)
                    return out
                s = str(val).strip()
                if not s:
                    return []
                parts = [x.strip() for x in re.split(r"\s*[|,;]\s*", s) if x.strip()]
                cleaned = []
                for p in parts[:30]:
                    pl = p.lower()
                    if "grade_name" in pl and "none" in pl:
                        continue
                    cleaned.append(p)
                return cleaned

            def _extract_grades(val: Any) -> list[str]:
                if val in (None, "", [], {}):
                    return []
                if isinstance(val, list):
                    out = []
                    for g in val[:30]:
                        if g in (None, "", [], {}):
                            continue
                        if isinstance(g, dict):
                            s = str(
                                g.get("grade")
                                or g.get("cargo_grade")
                                or g.get("grade_name")
                                or g.get("name")
                                or ""
                            ).strip()
                        else:
                            s = str(g).strip()
                            sl = s.lower()
                            if "grade_name" in sl and "none" in sl:
                                s = ""
                        if s:
                            out.append(s)
                    return out
                s = str(val).strip()
                if not s:
                    return []
                parts = [x.strip() for x in re.split(r"\s*[|,;]\s*", s) if x.strip()]
                return parts[:30]

            def _format_remarks_value(val: Any) -> str:
                if val in (None, "", [], {}):
                    return _result_set_text("no_remarks_recorded")
                items = val if isinstance(val, list) else [val]
                lines: list[str] = []
                for item in items[:10]:
                    if item in (None, "", [], {}):
                        continue
                    if isinstance(item, dict):
                        date = item.get("modifiedDate") or item.get("date") or item.get("createdAt")
                        who = item.get("modifiedByFull") or item.get("modifiedBy") or item.get("user")
                        text = item.get("remark") or item.get("text") or item.get("note")
                    else:
                        parsed = None
                        text_raw = str(item).strip()
                        if text_raw.startswith("{") and text_raw.endswith("}"):
                            try:
                                parsed = ast.literal_eval(text_raw)
                            except Exception:
                                parsed = None
                        if isinstance(parsed, dict):
                            date = parsed.get("modifiedDate") or parsed.get("date") or parsed.get("createdAt")
                            who = parsed.get("modifiedByFull") or parsed.get("modifiedBy") or parsed.get("user")
                            text = parsed.get("remark") or parsed.get("text") or parsed.get("note")
                        else:
                            date = None
                            who = None
                            text = text_raw
                    parts = [str(x).strip() for x in (date, who, text) if x not in (None, "", [], {})]
                    line = " | ".join(parts).strip()
                    if line:
                        lines.append(f"- {line}")
                return "\n".join(lines) if lines else _result_set_text("no_remarks_recorded")

            def _persist_result_set(new_rows: list[dict], *, primary_metric: str | None = None) -> None:
                try:
                    prev_meta = (rs or {}).get("meta") if isinstance(rs, dict) and isinstance((rs or {}).get("meta"), dict) else {}
                    meta = dict(prev_meta)
                    if primary_metric:
                        latest_source_intent = (rs or {}).get("source_intent") if isinstance(rs, dict) else None
                        latest_rs = {
                            "source_intent": latest_source_intent,
                            "rows": new_rows[:50],
                            "meta": meta,
                        }
                        meta["primary_metric"] = primary_metric
                    else:
                        latest_source_intent = (rs or {}).get("source_intent") if isinstance(rs, dict) else None
                        latest_rs = {
                            "source_intent": latest_source_intent,
                            "rows": new_rows[:50],
                            "meta": meta,
                        }
                    self.redis.save_session(
                        state["session_id"],
                        {
                            **(session_context or {}),
                            "last_result_set": latest_rs,
                            "last_user_input": user_input,
                        },
                    )
                    sctx = state.get("session_ctx") if isinstance(state.get("session_ctx"), dict) else {}
                    state["session_ctx"] = {**(sctx or {}), "last_result_set": latest_rs, "last_user_input": user_input}
                except Exception:
                    pass

            def _persist_focus(row: dict | None) -> None:
                if not isinstance(row, dict):
                    return
                focus_slots = self._row_focus_slots(row)
                if not focus_slots:
                    return
                try:
                    self.redis.save_session(
                        state["session_id"],
                        {
                            **(session_context or {}),
                            "last_focus_slots": focus_slots,
                            "last_user_input": user_input,
                        },
                    )
                    sctx = state.get("session_ctx") if isinstance(state.get("session_ctx"), dict) else {}
                    state["session_ctx"] = {**(sctx or {}), "last_focus_slots": focus_slots, "last_user_input": user_input}
                except Exception:
                    pass

            def _format_metric_value(metric: str, row: dict) -> str:
                metric = str(metric or "").strip().lower()
                if metric in ("vessel_name", "port_name"):
                    value = row.get(metric)
                    return str(value).strip() if value not in (None, "", [], {}) else _result_set_text("not_available")
                if metric == "port_calls":
                    value = self._result_row_metric_value(row, metric)
                    return f"{int(value):,}" if value is not None else _result_set_text("not_available")
                value = self._result_row_metric_value(row, metric)
                if value is None:
                    return _result_set_text("not_available")
                return f"{value:,.2f}"

            def _selected_row(selector: str | None = None) -> dict | None:
                selector = str(selector or "").strip().lower()
                if selector == "first":
                    return rows[0] if rows else None
                if selector == "last":
                    return rows[-1] if rows else None
                focus_slots = (session_context or {}).get("last_focus_slots") if isinstance(session_context, dict) else None
                if isinstance(focus_slots, dict) and focus_slots:
                    for r in rows:
                        if not isinstance(r, dict):
                            continue
                        if focus_slots.get("voyage_number") and str(r.get("voyage_number")) == str(focus_slots.get("voyage_number")):
                            return r
                        if focus_slots.get("voyage_id") and str(r.get("voyage_id")) == str(focus_slots.get("voyage_id")):
                            return r
                        if focus_slots.get("vessel_name") and str(r.get("vessel_name")) == str(focus_slots.get("vessel_name")):
                            return r
                        if focus_slots.get("port_name") and str(r.get("port_name")) == str(focus_slots.get("port_name")):
                            return r
                        if focus_slots.get("module_type") and str(r.get("module_type")) == str(focus_slots.get("module_type")):
                            return r
                        if focus_slots.get("time_bucket") and str(r.get("time_bucket")) == str(focus_slots.get("time_bucket")):
                            return r
                        if focus_slots.get("month") and str(r.get("month")) == str(focus_slots.get("month")):
                            return r
                prev_q = str((session_context or {}).get("last_user_input") or "").strip().lower()
                if prev_q and rows:
                    metric = "pnl"
                    if "expense ratio" in prev_q or "expense-to-revenue ratio" in prev_q or "expense to revenue ratio" in prev_q:
                        metric = "expense_to_revenue_ratio"
                    elif "avg revenue" in prev_q or "average revenue" in prev_q:
                        metric = "avg_revenue"
                    elif "avg pnl" in prev_q or "average pnl" in prev_q:
                        metric = "avg_pnl"
                    elif "voyage count" in prev_q or re.search(r"\bcount\b", prev_q):
                        metric = "voyage_count"
                    elif "pnl variance" in prev_q or "changed the most" in prev_q:
                        metric = "pnl_variance" if any(self._result_row_metric_value(r, "pnl_variance") is not None for r in rows if isinstance(r, dict)) else "variance"
                    elif "tce variance" in prev_q:
                        metric = "tce_variance"
                    elif "revenue" in prev_q:
                        metric = "revenue"
                    elif "expense" in prev_q or "cost" in prev_q:
                        metric = "total_expense"
                    scored = []
                    for r in rows:
                        if not isinstance(r, dict):
                            continue
                        val = self._result_row_metric_value(r, metric)
                        if val is None:
                            continue
                        scored.append((val, r))
                    if scored:
                        high_is_worse = metric in {
                            "expense_to_revenue_ratio",
                            "total_expense",
                            "avg_total_expense",
                            "offhire_days",
                            "avg_offhire_days",
                            "pnl_variance",
                            "tce_variance",
                            "variance",
                            "variance_diff",
                        }
                        if "worst" in prev_q:
                            return (max(scored, key=lambda t: t[0]) if high_is_worse else min(scored, key=lambda t: t[0]))[1]
                        if "best" in prev_q:
                            return (min(scored, key=lambda t: t[0]) if high_is_worse else max(scored, key=lambda t: t[0]))[1]
                        if any(k in prev_q for k in ("lowest", "least", "bottom")):
                            return min(scored, key=lambda t: t[0])[1]
                        if any(k in prev_q for k in ("highest", "most", "top")):
                            return max(scored, key=lambda t: t[0])[1]
                return rows[0] if rows else None

            # Generic refinements: top/bottom N by a metric
            if action in ("top_n", "bottom_n"):
                metric = str(slots.get("metric") or "pnl")
                try:
                    n = int(slots.get("n") or 5)
                except Exception:
                    n = 5
                n = max(1, min(n, 20))

                scored = []
                for r in rows:
                    if not isinstance(r, dict):
                        continue
                    label = self._result_row_label(r)
                    if label in (None, "", "N/A"):
                        continue
                    val = self._result_row_metric_value(r, metric)
                    if val is None and metric == "port_calls":
                        ports = _extract_ports(r.get("key_ports"))
                        val = float(len(ports)) if ports else None
                    if val is None:
                        continue
                    scored.append((val, r))

                if not scored:
                    state["answer"] = f"I couldn’t rank the previous result set by **{metric}**. Please ask the base query again with that metric."
                    return state

                scored = sorted(scored, key=lambda t: t[0], reverse=(action == "top_n"))
                picked = [r for _, r in scored[:n]]
                _persist_result_set(picked, primary_metric=metric)

                lines = [f"### {'Top' if action == 'top_n' else 'Bottom'} {len(picked)} by {metric} (from previous results)"]
                for i, r in enumerate(picked, start=1):
                    v = self._result_row_metric_value(r, metric)
                    if v is None and metric == "port_calls":
                        v = float(len(_extract_ports(r.get("key_ports"))))
                    label = self._result_row_label(r)
                    lines.append(f"- {i}. **{label}**: {v:,.2f}" if isinstance(v, float) else f"- {i}. **{label}**")
                _persist_focus(picked[0] if picked else None)
                state["answer"] = "\n".join(lines)
                return state

            # Per-row remarks projection from current result set.
            if action == "remarks_each":
                if not rows:
                    state["answer"] = _result_set_text("no_previous_result_set_read_remarks")
                    return state
                lines = [_result_set_text("remarks_each_heading")]
                for r in rows[:20]:
                    if not isinstance(r, dict):
                        continue
                    vnum = r.get("voyage_number") or "N/A"
                    rem = r.get("remarks")
                    txt = _format_remarks_value(rem)
                    lines.append(f"- **Voyage {vnum}**:")
                    lines.append(txt)
                state["answer"] = "\n".join(lines)
                return state

            if action == "filter_has_field":
                field = str(slots.get("field") or "").strip()
                filtered = [r for r in rows if isinstance(r, dict) and r.get(field) not in (None, "", [], {})]
                if not filtered:
                    state["answer"] = _result_set_text("no_rows_field_populated", field=field)
                    return state
                _persist_result_set(filtered)
                _persist_focus(filtered[0])
                lines = [f"### Rows with {field.replace('_', ' ')} (from previous results)"]
                for r in filtered[:20]:
                    label = self._result_row_label(r)
                    val = r.get(field)
                    preview = str(val).strip() if val not in (None, "", [], {}) else _result_set_text("not_available")
                    lines.append(f"- **{label}**: {preview}")
                state["answer"] = "\n".join(lines)
                return state

            if action == "project_selected_field":
                field = str(slots.get("field") or "").strip()
                chosen = _selected_row(str(slots.get("selector") or "last_focus"))
                if not isinstance(chosen, dict):
                    state["answer"] = _result_set_text("selected_row_not_identified")
                    return state
                _persist_focus(chosen)
                label = self._result_row_label(chosen)
                if field == "remarks":
                    rem = chosen.get("remarks")
                    state["answer"] = (
                        _result_set_text("remarks_for_label_heading", label=label) + "\n"
                        + _format_remarks_value(rem)
                    )
                    return state
                if field == "cargo_grades":
                    grades = _extract_grades(
                        chosen.get("cargo_grades")
                        if chosen.get("cargo_grades") not in (None, "", [], {})
                        else chosen.get("most_common_grade")
                    )
                    state["answer"] = (
                        _result_set_text("cargo_grades_for_label_heading", label=label) + "\n"
                        + (", ".join(grades[:20]) if grades else _result_set_text("no_cargo_grades_previous_result_set"))
                    )
                    return state
                if field == "key_ports":
                    ports = _extract_ports(chosen.get("key_ports"))
                    state["answer"] = (
                        _result_set_text("key_ports_for_label_heading", label=label) + "\n"
                        + (", ".join(ports[:20]) if ports else _result_set_text("no_key_ports_previous_result_set"))
                    )
                    return state
                state["answer"] = _result_set_text("cannot_project_field", field=field)
                return state

            if action == "project_selected_metric":
                metric = str(slots.get("metric") or "revenue").strip().lower()
                chosen = _selected_row(str(slots.get("selector") or "last_focus"))
                if not isinstance(chosen, dict):
                    state["answer"] = _result_set_text("selected_row_not_identified")
                    return state
                _persist_result_set(rows, primary_metric=metric)
                _persist_focus(chosen)
                label = self._result_row_label(chosen)
                pretty_name = metric.replace("_", " ")
                state["answer"] = f"### {pretty_name.title()} for {label}\n{_format_metric_value(metric, chosen)}"
                return state

            if action == "project_extreme_field":
                field = str(slots.get("field") or "").strip()
                metric = str(slots.get("metric") or "pnl").strip().lower()
                extreme = str(slots.get("extreme") or "high").strip().lower()
                scored = []
                for r in rows:
                    if not isinstance(r, dict):
                        continue
                    val = self._result_row_metric_value(r, metric)
                    if val is None:
                        continue
                    scored.append((val, r))
                if not scored:
                    state["answer"] = "I don’t have a previous result set to project from. Please run the base query again."
                    return state
                chosen = min(scored, key=lambda t: t[0])[1] if extreme == "low" else max(scored, key=lambda t: t[0])[1]
                _persist_result_set(rows, primary_metric=metric)
                _persist_focus(chosen)
                label = self._result_row_label(chosen)
                if field == "remarks":
                    rem = chosen.get("remarks")
                    state["answer"] = (
                        _result_set_text("remarks_for_label_heading", label=label) + "\n"
                        + _format_remarks_value(rem)
                    )
                    return state
                if field == "cargo_grades":
                    grades = _extract_grades(
                        chosen.get("cargo_grades")
                        if chosen.get("cargo_grades") not in (None, "", [], {})
                        else chosen.get("most_common_grade")
                    )
                    state["answer"] = (
                        _result_set_text("cargo_grades_for_label_heading", label=label) + "\n"
                        + (", ".join(grades[:20]) if grades else _result_set_text("no_cargo_grades_previous_result_set"))
                    )
                    return state
                if field == "key_ports":
                    ports = _extract_ports(chosen.get("key_ports"))
                    state["answer"] = (
                        _result_set_text("key_ports_for_label_heading", label=label) + "\n"
                        + (", ".join(ports[:20]) if ports else _result_set_text("no_key_ports_previous_result_set"))
                    )
                    return state
                state["answer"] = _result_set_text("cannot_project_selected_field", field=field)
                return state

            # Per-row ports projection from current result set.
            if action == "list_ports":
                if not rows:
                    state["answer"] = _result_set_text("no_previous_result_set_list_ports")
                    return state
                lines = [_result_set_text("key_ports_current_result_set_heading")]
                for r in rows[:20]:
                    if not isinstance(r, dict):
                        continue
                    vnum = r.get("voyage_number") or "N/A"
                    ports = _extract_ports(r.get("key_ports"))
                    if not ports:
                        lines.append(f"- **Voyage {vnum}**: {_result_set_text('no_key_ports_for_row')}")
                    else:
                        preview = ", ".join(ports[:12])
                        suffix = " (+more)" if len(ports) > 12 else ""
                        lines.append(f"- **Voyage {vnum}**: {preview}{suffix}")
                state["answer"] = "\n".join(lines)
                return state

            if action == "list_cargo_grades":
                if not rows:
                    state["answer"] = _result_set_text("no_previous_result_set_list_cargo_grades")
                    return state
                lines = [_result_set_text("cargo_grades_current_result_set_heading")]
                for r in rows[:20]:
                    if not isinstance(r, dict):
                        continue
                    vnum = r.get("voyage_number") or "N/A"
                    grades = _extract_grades(r.get("cargo_grades"))
                    if not grades:
                        lines.append(f"- **Voyage {vnum}**: {_result_set_text('no_cargo_grades_for_row')}")
                    else:
                        preview = ", ".join(grades[:12])
                        suffix = " (+more)" if len(grades) > 12 else ""
                        lines.append(f"- **Voyage {vnum}**: {preview}{suffix}")
                state["answer"] = "\n".join(lines)
                return state

            if action == "list_fields":
                if not rows:
                    state["answer"] = "I don’t have a previous result set to project from. Please run the base query again."
                    return state
                fields_raw = slots.get("fields")
                fields = [str(f).strip().lower() for f in fields_raw if str(f).strip()] if isinstance(fields_raw, list) else []
                if not fields:
                    state["answer"] = "I couldn’t identify which fields to show from the previous result set."
                    return state
                pretty = ", ".join(f.replace("_", " ") for f in fields)
                lines = [f"### {pretty.title()} for current result set"]
                for r in rows[:20]:
                    if not isinstance(r, dict):
                        continue
                    label = self._result_row_label(r)
                    vals = []
                    for f in fields:
                        v = r.get(f)
                        if f == "passage_types" and isinstance(v, list):
                            v = ", ".join(str(x) for x in v if x not in (None, ""))
                        if isinstance(v, list):
                            v = ", ".join(str(x) for x in v[:8]) if v else _result_set_text("not_available")
                        if v in (None, "", [], {}):
                            v = _result_set_text("not_available")
                        vals.append(f"{f.replace('_', ' ')} {v}")
                    lines.append(f"- **{label}** - " + "; ".join(vals))
                state["answer"] = "\n".join(lines)
                return state

            # Generic metrics comparison for current selected set.
            if action == "compare_metrics":
                metrics = slots.get("metrics") if isinstance(slots.get("metrics"), list) else []
                metrics = [str(m).strip() for m in metrics if str(m).strip()]
                if not metrics:
                    metrics = ["pnl", "tce"]
                if not rows:
                    state["answer"] = "I don’t have a previous result set to compare. Please run the base list query again."
                    return state
                lines = [f"### Comparison from previous results ({', '.join(metrics)})"]
                for r in rows[:20]:
                    if not isinstance(r, dict):
                        continue
                    label = self._result_row_label(r)
                    vals = []
                    for m in metrics:
                        raw = self._result_row_metric_value(r, m)
                        v = raw if isinstance(raw, (int, float)) else _num(r.get(m))
                        vals.append(f"{m}={v:,.2f}" if isinstance(v, float) else f"{m}=N/A")
                    lines.append(f"- **{label}**: " + " | ".join(vals))
                state["answer"] = "\n".join(lines)
                return state

            if action == "filter_by_metric_threshold":
                if not rows:
                    state["answer"] = "I don’t have a previous result set to filter. Please run the base query again."
                    return state
                metric = str(slots.get("metric") or "pnl").strip().lower()
                operator = str(slots.get("operator") or ">").strip()
                try:
                    threshold = float(slots.get("value"))
                except Exception:
                    threshold = None
                if threshold is None:
                    state["answer"] = "I couldn’t read the threshold value. Please specify a numeric threshold."
                    return state

                def _passes(val: float) -> bool:
                    if operator == ">":
                        return val > threshold
                    if operator == "<":
                        return val < threshold
                    if operator == ">=":
                        return val >= threshold
                    if operator == "<=":
                        return val <= threshold
                    return abs(val - threshold) < 1e-9

                filtered: list[dict] = []
                for r in rows:
                    if not isinstance(r, dict):
                        continue
                    v = self._result_row_metric_value(r, metric)
                    if v is None:
                        continue
                    if _passes(v):
                        filtered.append(r)

                if not filtered:
                    state["answer"] = f"No rows matched **{metric} {operator} {threshold:g}** in the previous result set."
                    return state

                _persist_result_set(filtered, primary_metric=metric)
                lines = [f"### Filtered results ({metric} {operator} {threshold:g}) from previous results"]
                for r in filtered[:20]:
                    label = self._result_row_label(r)
                    v = self._result_row_metric_value(r, metric)
                    lines.append(f"- **{label}**: {metric}={v:,.2f}" if isinstance(v, float) else f"- **{label}**")
                if len(filtered) > 20:
                    lines.append(f"- ... and {len(filtered) - 20} more rows")
                _persist_focus(filtered[0] if filtered else None)
                state["answer"] = "\n".join(lines)
                return state

            # If user asks for extremes (highest/lowest) among the last list.
            if action == "compare_extremes" or any(k in ul for k in ("highest", "lowest", "max", "min", "best", "worst")):
                metric = str(slots.get("metric") or "pnl").strip().lower()
                explicit_pnl_request = bool(re.search(r"\b(pnl|p&l|profit|loss)\b", ul))
                if not metric or (metric == "pnl" and not explicit_pnl_request):
                    preferred_metric = str(((rs or {}).get("meta") or {}).get("primary_metric") or "").strip().lower()
                    if preferred_metric and not any(
                        k in ul for k in ("expense ratio", "expense-to-revenue ratio", "expense to revenue ratio", "avg revenue", "average revenue", "avg pnl", "average pnl", "avg tce", "average tce", "voyage count", "count", "pnl variance", "tce variance", "variance", "total pnl", "revenue", "expense", "cost", "port count", "port calls", "tce", "commission")
                    ):
                        metric = preferred_metric
                    elif "expense ratio" in ul or "expense-to-revenue ratio" in ul or "expense to revenue ratio" in ul:
                        metric = "expense_to_revenue_ratio"
                    elif "avg revenue" in ul or "average revenue" in ul:
                        metric = "avg_revenue"
                    elif "avg pnl" in ul or "average pnl" in ul:
                        metric = "avg_pnl"
                    elif "avg tce" in ul or "average tce" in ul:
                        metric = "avg_tce"
                    elif "voyage count" in ul or re.search(r"\bcount\b", ul):
                        metric = "voyage_count"
                    elif "pnl variance" in ul or "changed the most" in ul:
                        metric = "pnl_variance" if any(self._result_row_metric_value(r, "pnl_variance") is not None for r in rows if isinstance(r, dict)) else "variance"
                    elif "tce variance" in ul:
                        metric = "tce_variance"
                    elif "variance" in ul:
                        preferred_metric = str(((rs or {}).get("meta") or {}).get("primary_metric") or "").strip().lower()
                        metric = preferred_metric if preferred_metric else "variance"
                    elif "total pnl" in ul:
                        metric = "total_pnl"
                    elif "revenue" in ul:
                        metric = "revenue"
                    elif "expense" in ul or "cost" in ul:
                        metric = "total_expense"
                    elif "port count" in ul or "port calls" in ul:
                        metric = "port_calls"
                    elif "tce" in ul:
                        metric = "tce"
                    elif "commission" in ul:
                        metric = "total_commission"

                scored = []
                for r in rows:
                    if not isinstance(r, dict):
                        continue
                    label = self._result_row_label(r)
                    val = self._result_row_metric_value(r, metric)
                    if label in (None, "", "N/A") or val is None:
                        continue
                    scored.append((val, label, r))
                if not scored:
                    state["answer"] = "I don’t have a previous result set to compare. Please run the list query again."
                    return state
                lo = min(scored, key=lambda t: t[0])
                hi = max(scored, key=lambda t: t[0])
                def _ensure_vessel_anchor(item: tuple[float, str, Dict[str, Any]]) -> tuple[float, str, Dict[str, Any]]:
                    val, lbl, row = item
                    if "vessel" not in ul:
                        return item
                    if not isinstance(row, dict):
                        return item
                    if row.get("vessel_name") not in (None, "", [], {}):
                        return (val, str(row.get("vessel_name")), row)
                    vn = row.get("voyage_number")
                    if vn in (None, "", [], {}):
                        return item
                    try:
                        ctx = self.mongo_agent.fetch_full_voyage_context(voyage_number=int(vn), entity_slots=row)
                    except Exception:
                        ctx = {}
                    vname = (ctx or {}).get("vessel_name")
                    vimo = (ctx or {}).get("vessel_imo")
                    if vname not in (None, "", [], {}):
                        row = dict(row)
                        row["vessel_name"] = vname
                        if vimo not in (None, "", [], {}):
                            row["vessel_imo"] = vimo
                        return (val, str(vname), row)
                    return item
                lo = _ensure_vessel_anchor(lo)
                hi = _ensure_vessel_anchor(hi)
                high_is_worse = metric in {
                    "expense_to_revenue_ratio",
                    "total_expense",
                    "avg_total_expense",
                    "offhire_days",
                    "avg_offhire_days",
                    "pnl_variance",
                    "tce_variance",
                    "variance",
                    "variance_diff",
                }
                asks_high = any(k in ul for k in ("highest", "max", "most", "better", "higher", "changed the most"))
                asks_low = any(k in ul for k in ("lowest", "min", "least", "lower"))
                if metric == "pnl" and "loss" in ul and any(k in ul for k in ("highest", "most", "worst")):
                    # "Highest loss" means the most negative PnL in-context.
                    asks_high = False
                    asks_low = True
                if "best" in ul:
                    if high_is_worse:
                        asks_low = True
                    else:
                        asks_high = True
                if "worst" in ul:
                    if high_is_worse:
                        asks_high = True
                    else:
                        asks_low = True
                if asks_high and not asks_low:
                    _persist_result_set(rows, primary_metric=metric)
                    _persist_focus(hi[2])
                    state["answer"] = f"The highest **{metric}** in the previous results was **{hi[1]}** at **{hi[0]:,.2f}**."
                elif asks_low and not asks_high:
                    _persist_result_set(rows, primary_metric=metric)
                    _persist_focus(lo[2])
                    state["answer"] = f"The lowest **{metric}** in the previous results was **{lo[1]}** at **{lo[0]:,.2f}**."
                else:
                    _persist_result_set(rows, primary_metric=metric)
                    _persist_focus(hi[2])
                    state["answer"] = (
                        "### Among the previous results\n"
                        f"- **Highest {metric.upper()}**: **{hi[1]}** ({hi[0]:,.2f})\n"
                        f"- **Lowest {metric.upper()}**: **{lo[1]}** ({lo[0]:,.2f})"
                    )
                return state

            # Explain remarks: require a voyage selection from the last result set.
            if action == "explain_remarks":
                # If the user already named a voyage number in this message, use it.
                import re as _re
                m = _re.search(r"\b(\d{3,5})\b", user_input or "")
                if m:
                    slots["voyage_number"] = int(m.group(1))
                vnum = slots.get("voyage_number")
                if not vnum:
                    # Ask which voyage they mean, using the last result set as suggestions.
                    sugg = []
                    for r in rows[:8]:
                        if isinstance(r, dict) and r.get("voyage_number") is not None:
                            try:
                                sugg.append(int(r["voyage_number"]))
                            except Exception:
                                continue
                    options = {"voyage_number": sugg} if sugg else {}
                    state["clarification"] = (
                        "### Quick question\\n"
                        "- You said **explain remarks**, but didn’t specify **which voyage** from the previous list.\\n"
                        "- Reply with the voyage number (or pick a number from the suggestions)."
                    )
                    # Persist a pending clarification specifically for this follow-up
                    try:
                        self.redis.save_session(
                            state["session_id"],
                            {
                                **(session_context or {}),
                                "pending_intent": "followup.result_set",
                                "missing_keys": ["voyage_number"],
                                "clarification_options": options,
                                "pending_question": user_input,
                                "pending_slots": {"action": "explain_remarks"},
                            },
                        )
                    except Exception:
                        pass
                    return state

                # Find the matching row and return its remarks.
                chosen = None
                for r in rows:
                    if isinstance(r, dict) and str(r.get("voyage_number")) == str(vnum):
                        chosen = r
                        break
                rem = (chosen or {}).get("remarks") if isinstance(chosen, dict) else None
                _persist_focus(chosen if isinstance(chosen, dict) else None)
                state["answer"] = (
                    f"### Remarks for voyage {vnum}\\n"
                    + (str(rem).strip() if rem not in (None, "", [], {}) else "No remarks were available in the previous result set.")
                )
                return state

        # LangGraph may populate declared state keys with None. Ensure data is always a dict.
        if not isinstance(state.get("data"), dict):
            state["data"] = {}

        # =========================================================
        # Mixed voyage query: PostgreSQL financials + Mongo metadata
        # =========================================================
        if plan_type == "multi" and intent_key in ("voyage.summary", "voyage.metadata"):
            if not slots.get("voyage_number") and isinstance(slots.get("voyage_numbers"), list) and slots.get("voyage_numbers"):
                slots["voyage_number"] = slots.get("voyage_numbers")[0]

            # Canonical voyage resolve before finance call:
            # when voyage_number is reused across vessels, adding canonical voyage_id/imo
            # keeps the finance query anchored to the same voyage identity used by Mongo.
            if slots.get("voyage_number") and (not slots.get("voyage_id") or not (slots.get("vessel_imo") or slots.get("imo"))):
                try:
                    canon = self.mongo_agent.fetch_full_voyage_context(
                        voyage_number=slots.get("voyage_number"),
                        voyage_id=slots.get("voyage_id"),
                        entity_slots=slots,
                    )
                except Exception:
                    canon = {}
                if isinstance(canon, dict) and canon:
                    if canon.get("voyage_id") and not slots.get("voyage_id"):
                        slots["voyage_id"] = str(canon.get("voyage_id"))
                    if canon.get("vessel_imo") and not (slots.get("vessel_imo") or slots.get("imo")):
                        slots["vessel_imo"] = str(canon.get("vessel_imo"))
                    if canon.get("vessel_name") and not slots.get("vessel_name"):
                        slots["vessel_name"] = str(canon.get("vessel_name"))

            # Step 1: financials from Postgres (authoritative for PnL/revenue/expense/TCE/commission)
            self._trace(state, {
                "phase": "multi_step_start",
                "step_index": 1,
                "step_count": 2,
                "intent_key": "voyage.summary",
                "agent": "finance",
                "description": "Fetch actual PnL, revenue, expense, TCE from PostgreSQL",
            })
            try:
                finance_data = self.finance_agent.run(
                    intent_key="voyage.summary",
                    slots=slots,
                    session_context=session_context,
                    user_input=user_input,
                )
            except TypeError:
                finance_data = self.finance_agent.run(
                    intent_key="voyage.summary",
                    slots=slots,
                    session_context={**session_context, "user_input": user_input},
                )
            except Exception as e:
                finance_data = {"mode": "error", "rows": [], "fallback_reason": f"Finance agent error: {e}"}
            finance_safe = _json_safe(finance_data)
            finance_rows = finance_safe.get("rows") if isinstance(finance_safe, dict) else []
            finance_rows = finance_rows if isinstance(finance_rows, list) else []
            finance_query_key = finance_safe.get("query_key") if isinstance(finance_safe, dict) else None
            finance_sql = None
            if finance_query_key in SQL_REGISTRY:
                finance_sql = SQL_REGISTRY[finance_query_key].sql
            self._trace(state, {
                "phase": "multi_step_result",
                "step_index": 1,
                "intent_key": "voyage.summary",
                "agent": "finance",
                "mode": finance_safe.get("mode") if isinstance(finance_safe, dict) else None,
                "query_key": finance_query_key,
                "params": finance_safe.get("params") if isinstance(finance_safe, dict) else None,
                "fallback_reason": finance_safe.get("fallback_reason") if isinstance(finance_safe, dict) else None,
                "sql_present": bool(finance_sql),
                "sql": finance_sql,
                "rows": len(finance_rows),
                "ok": True,
            })

            # Pick finance row first so Mongo can align when voyageNumber is non-unique.
            selected_fin = None
            for r in finance_rows:
                if not isinstance(r, dict):
                    continue
                if slots.get("voyage_number") is None or str(r.get("voyage_number")) == str(slots.get("voyage_number")):
                    selected_fin = r
                    break
            if not selected_fin and finance_rows:
                fr0 = finance_rows[0]
                selected_fin = fr0 if isinstance(fr0, dict) else None
            selected_fin = selected_fin or {}

            # Step 2: metadata from Mongo
            self._trace(state, {
                "phase": "multi_step_start",
                "step_index": 2,
                "step_count": 2,
                "intent_key": "voyage.metadata",
                "agent": "mongo",
                "description": "Fetch remarks, ports, cargo, fixture from MongoDB",
            })
            projection = cfg.get("mongo_projection") or get_mongo_projection("voyage_metadata_context")
            doc: Dict[str, Any] = {}
            mongo_docs: list[Dict[str, Any]] = []
            mongo_filter = None
            mongo_fetch_error = False
            explicit_voyage_only = bool(
                slots.get("voyage_number") not in (None, "", [], {})
                and len(GraphRouter._explicit_vessel_mentions(user_input)) == 0
                and not re.search(r"\b\d{7}\b", user_input or "")
            )
            if slots.get("voyage_id") not in (None, ""):
                mongo_filter = {"voyageId": str(slots.get("voyage_id"))}
                try:
                    fd = self.mongo_agent.adapter.fetch_voyage(str(slots.get("voyage_id")), projection=projection)
                except Exception:
                    fd = None
                    mongo_fetch_error = True
                if isinstance(fd, dict) and fd:
                    doc = fd
                    mongo_docs = [fd]
            if not mongo_docs:
                try:
                    vnum = slots.get("voyage_number")
                    if vnum not in (None, ""):
                        iv = int(vnum)
                        mongo_filter = {"voyageNumber": str(iv)}
                        batch = self.mongo_agent.adapter.list_voyages_by_number(
                            iv,
                            projection=projection,
                            limit=get_mongo_limit("voyage_metadata_context_batch", 40),
                        )
                        merge_sl = {**slots}
                        if selected_fin and not explicit_voyage_only:
                            if selected_fin.get("vessel_name") and not str(merge_sl.get("vessel_name") or "").strip():
                                merge_sl["vessel_name"] = selected_fin.get("vessel_name")
                            vimo = selected_fin.get("vessel_imo") or selected_fin.get("imo")
                            if vimo and not str(merge_sl.get("imo") or merge_sl.get("vessel_imo") or "").strip():
                                merge_sl["imo"] = vimo
                            if selected_fin.get("voyage_id") and not str(merge_sl.get("voyage_id") or "").strip():
                                merge_sl["voyage_id"] = selected_fin.get("voyage_id")
                        narrowed = narrow_voyage_rows_by_entity_slots(batch, merge_sl)
                        picked = None
                        if explicit_voyage_only and narrowed:
                            def _doc_rank(d: Dict[str, Any]) -> tuple[str, str]:
                                if not isinstance(d, dict):
                                    return ("", "")
                                return (
                                    str(d.get("startDateUtc") or d.get("extracted_at") or ""),
                                    str(d.get("voyageId") or ""),
                                )
                            picked = sorted(narrowed, key=_doc_rank, reverse=True)[0]
                        else:
                            picked = self._pick_voyage_doc_aligned_to_finance(narrowed, selected_fin)
                        if picked:
                            doc = picked
                            mongo_docs = [picked]
                        elif len(narrowed) == 1:
                            doc = narrowed[0]
                            mongo_docs = [narrowed[0]]
                        elif narrowed:
                            mongo_docs = narrowed
                            doc = {}
                except Exception:
                    doc = {}
                    mongo_fetch_error = True
            if not isinstance(doc, dict):
                doc = {}
            mongo_row_count = len(mongo_docs) if mongo_docs else (1 if doc else 0)
            self._trace(state, {
                "phase": "multi_step_result",
                "step_index": 2,
                "intent_key": "voyage.metadata",
                "agent": "mongo",
                "mode": "mongo_metadata",
                "collection": "voyages",
                "mongo_query": {
                    "collection": "voyages",
                    "filter": mongo_filter or {},
                    "projection": self._mongo_projection_for_trace(projection),
                    "sort": None,
                    "limit": max(1, mongo_row_count) if mongo_row_count else 1,
                    "pipeline": None,
                },
                "rows": mongo_row_count,
                "ok": True,
            })

            # Assemble source-separated context (financial values MUST come from PostgreSQL).
            mongo_vessel_name = None
            if isinstance(doc, dict) and doc.get("vesselName"):
                mongo_vessel_name = doc.get("vesselName")
            elif mongo_docs:
                mv0 = mongo_docs[0]
                if isinstance(mv0, dict) and mv0.get("vesselName"):
                    mongo_vessel_name = mv0.get("vesselName")
            financial_data_postgres = {
                "voyage_id": selected_fin.get("voyage_id"),
                "voyage_number": selected_fin.get("voyage_number") or slots.get("voyage_number"),
                "vessel_name": (mongo_vessel_name or selected_fin.get("vessel_name")) if explicit_voyage_only else (selected_fin.get("vessel_name") or mongo_vessel_name),
                "pnl": selected_fin.get("pnl"),
                "revenue": selected_fin.get("revenue"),
                "total_expense": selected_fin.get("total_expense"),
                "tce": selected_fin.get("tce"),
                "total_commission": selected_fin.get("total_commission"),
                "scenario": selected_fin.get("scenario"),
            }
            if len(mongo_docs) == 1:
                metadata_mongodb = select_voyage_sections(user_input, mongo_docs[0], session_context)
            elif len(mongo_docs) > 1:
                metadata_mongodb = {
                    "voyages": [select_voyage_sections(user_input, d, session_context) for d in mongo_docs],
                }
            else:
                metadata_mongodb = {}

            finance_fallback_reason = ""
            if isinstance(finance_safe, dict):
                finance_fallback_reason = str(finance_safe.get("fallback_reason") or "")
            mongo_down = not mongo_docs and mongo_fetch_error
            postgres_down = (
                "Postgres is not available" in finance_fallback_reason
                or "connection refused" in finance_fallback_reason.lower()
            )
            if not finance_rows and not mongo_docs and (postgres_down or mongo_down):
                vn = slots.get("voyage_number") or slots.get("voyage_numbers") or "this voyage"
                unavailable_parts = []
                if postgres_down:
                    unavailable_parts.append("Postgres on `localhost:5432`")
                if mongo_down:
                    unavailable_parts.append("MongoDB on `localhost:27017`")
                backend_list = " and ".join(unavailable_parts) if unavailable_parts else "the data backends"
                answer = _router_fallback(
                    "backend_unavailable_with_list",
                    voyage_ref=vn,
                    backend_list=backend_list,
                )
                mongo_safe = {"mode": "mongo_metadata", "ok": False, "rows": mongo_docs}
                merged = {
                    "finance": finance_safe,
                    "ops": {"mode": None, "rows": []},
                    "mongo": mongo_safe,
                    "artifacts": {
                        "intent_key": intent_key,
                        "slots": slots,
                        "financial_data_postgres": financial_data_postgres,
                        "metadata_mongodb": metadata_mongodb,
                    },
                    "dynamic_sql_used": False,
                    "dynamic_sql_agents": [],
                }
                state["merged"] = merged
                state["answer"] = answer
                state["slots"] = slots
                state["finance"] = finance_safe
                state["ops"] = {"mode": None, "rows": []}
                state["mongo"] = mongo_safe
                state["data"]["finance"] = finance_safe
                state["data"]["ops"] = {"mode": None, "rows": []}
                state["data"]["mongo"] = mongo_safe
                state["data"]["artifacts"] = merged.get("artifacts") or {"intent_key": intent_key, "slots": slots}
                return state

            system_prompt = get_graph_router_multi_voyage_answer_system_prompt()
            user_prompt = (
                "Context JSON:\n"
                + json.dumps(
                    {
                        "financial_data_postgres": financial_data_postgres,
                        "metadata_mongodb": metadata_mongodb,
                    },
                    default=str,
                    indent=2,
                )
                + f"\n\nQuestion: {user_input}"
            )
            answer = self.llm._call_with_retry(
                system=system_prompt,
                user=user_prompt,
                operation="multi_voyage_answer",
                return_string=True,
            )
            if not answer:
                answer = _router_fallback("multi_voyage_formatting_failed")

            mongo_safe = {
                "mode": "mongo_metadata",
                "ok": True,
                "rows": mongo_docs if mongo_docs else ([doc] if doc else []),
            }
            merged = {
                "finance": finance_safe,
                "ops": {"mode": None, "rows": []},
                "mongo": mongo_safe,
                "artifacts": {
                    "intent_key": intent_key,
                    "slots": slots,
                    "financial_data_postgres": financial_data_postgres,
                    "metadata_mongodb": metadata_mongodb,
                },
                "dynamic_sql_used": False,
                "dynamic_sql_agents": [],
            }
            state["merged"] = merged
            state["answer"] = answer
            state["slots"] = slots
            state["finance"] = finance_safe
            state["ops"] = {"mode": None, "rows": []}
            state["mongo"] = mongo_safe
            state["data"]["finance"] = finance_safe
            state["data"]["ops"] = {"mode": None, "rows": []}
            state["data"]["mongo"] = mongo_safe
            state["data"]["artifacts"] = merged.get("artifacts") or {"intent_key": intent_key, "slots": slots}
            try:
                persisted_slots = self._build_persisted_slots(
                    base=(session_context.get("slots") or {}),
                    updates=slots,
                )
                remarks_preview = None
                ref_doc = doc if (isinstance(doc, dict) and doc) else (mongo_docs[0] if mongo_docs else {})
                if isinstance(ref_doc, dict):
                    raw_remarks = ref_doc.get("remarks")
                    if isinstance(raw_remarks, list):
                        remarks_preview = [str(x).strip() for x in raw_remarks[:3] if x not in (None, "", [], {})]
                    elif raw_remarks not in (None, "", [], {}):
                        remarks_preview = str(raw_remarks).strip()
                single_result_row = {
                    "voyage_id": persisted_slots.get("voyage_id") or selected_fin.get("voyage_id") or ref_doc.get("voyageId"),
                    "voyage_number": persisted_slots.get("voyage_number") or selected_fin.get("voyage_number") or ref_doc.get("voyageNumber"),
                    "vessel_name": persisted_slots.get("vessel_name") or selected_fin.get("vessel_name") or ref_doc.get("vesselName"),
                    "vessel_imo": persisted_slots.get("vessel_imo") or persisted_slots.get("imo") or selected_fin.get("vessel_imo"),
                    "pnl": selected_fin.get("pnl"),
                    "revenue": selected_fin.get("revenue"),
                    "total_expense": selected_fin.get("total_expense"),
                    "tce": selected_fin.get("tce"),
                    "total_commission": selected_fin.get("total_commission"),
                    "remarks": remarks_preview,
                }
                single_result_row = {k: v for k, v in single_result_row.items() if v not in (None, "", [], {})}
                latest_result_set = {
                    "source_intent": intent_key,
                    "rows": [single_result_row] if single_result_row else [],
                    "meta": {
                        "source_intent": intent_key,
                        "available_metrics": [
                            m for m in ("pnl", "revenue", "total_expense", "tce", "total_commission")
                            if single_result_row.get(m) not in (None, "", [], {})
                        ],
                        "primary_metric": "pnl" if single_result_row.get("pnl") not in (None, "", [], {}) else None,
                    },
                }
                self.redis.save_session(
                    state["session_id"],
                    {
                        **(session_context or {}),
                        "last_intent": intent_key,
                        "last_intent_key": intent_key,
                        "memory_slots": self._extract_memory_slots(persisted_slots),
                        "param_slots": self._extract_param_slots(persisted_slots),
                        "slots": persisted_slots,
                        "last_result_set": latest_result_set,
                        "last_focus_slots": self._row_focus_slots(single_result_row),
                        "last_user_input": user_input,
                        "_turn_marker": uuid.uuid4().hex,
                        "_record_turn": self._build_turn_history_entry(
                            query=user_input,
                            raw_user_input=state.get("raw_user_input") or user_input,
                            intent_key=intent_key,
                            slots=persisted_slots,
                            answer=answer,
                            plan_type=state.get("plan_type") or (state.get("plan") or {}).get("plan_type"),
                        ),
                    },
                )
                state["session_ctx"] = {
                    **(session_context or {}),
                    "last_intent": intent_key,
                    "last_intent_key": intent_key,
                    "memory_slots": self._extract_memory_slots(persisted_slots),
                    "param_slots": self._extract_param_slots(persisted_slots),
                    "slots": persisted_slots,
                    "last_result_set": latest_result_set,
                    "last_focus_slots": self._row_focus_slots(single_result_row),
                    "last_user_input": user_input,
                }
            except Exception:
                pass
            return state

        # =========================================================
        # Voyage summary: full context (finance, ops, mongo)
        # =========================================================
        if intent_key == "voyage.summary":
            voyage_number = slots.get("voyage_number")
            voyage_id = slots.get("voyage_id")

            # 1️⃣ Mongo canonical resolve first (so voyage-number-only queries can be disambiguated)
            try:
                mongo_data = self.mongo_agent.fetch_full_voyage_context(
                    voyage_number=voyage_number,
                    voyage_id=voyage_id,
                    entity_slots=slots,
                )
            except Exception as e:
                _dprint(f"⚠️  Mongo failed: {e}")
                mongo_data = {}

            if isinstance(mongo_data, dict) and mongo_data:
                canonical_vid = mongo_data.get("voyage_id")
                canonical_imo = mongo_data.get("vessel_imo")
                canonical_vname = mongo_data.get("vessel_name")
                if canonical_vid:
                    slots["voyage_id"] = str(canonical_vid)
                if canonical_imo:
                    slots["imo"] = str(canonical_imo)
                    slots["vessel_imo"] = str(canonical_imo)
                if canonical_vname:
                    slots["vessel_name"] = str(canonical_vname)
                voyage_id = slots.get("voyage_id")

            # One voyage → one vessel (Mongo canonical). Do not query finance/ops without Mongo identity.
            if voyage_number and not voyage_id and not (isinstance(mongo_data, dict) and mongo_data.get("voyage_id")):
                vn_disp = str(voyage_number)
                answer = _router_fallback("voyage_reference_ambiguous_or_not_found", voyage_ref=vn_disp)
                state["merged"] = {"finance": {}, "ops": {}, "mongo": {}, "artifacts": {"intent_key": intent_key, "slots": slots}}
                state["answer"] = answer
                state["slots"] = slots
                return state

            # 2️⃣ Finance (scoped to Mongo voyage_id + vessel IMO only)
            self._trace(
                state,
                {
                    "phase": "composite_step_start",
                    "step_index": 1,
                    "step_count": 2,
                    "agent": "finance",
                    "operation": "registry_sql",
                    "inputs": {"intent_key": intent_key},
                    "goal": "Run finance registry SQL in single plan mode.",
                },
            )
            try:
                finance_data = self.finance_agent.run(
                    intent_key=intent_key,
                    slots=slots,
                    session_context=session_context,
                    user_input=user_input,
                )
            except TypeError:
                finance_data = self.finance_agent.run(
                    intent_key=intent_key,
                    slots=slots,
                    session_context={**session_context, "user_input": user_input},
                )
            except Exception as e:
                _dprint(f"⚠️  Finance failed: {e}")
                finance_data = {"mode": "error", "rows": []}
            try:
                fin_rows = finance_data.get("rows") if isinstance(finance_data, dict) else []
                fin_rows = fin_rows if isinstance(fin_rows, list) else []
                fin_vids = [r.get("voyage_id") for r in fin_rows if isinstance(r, dict) and r.get("voyage_id")]
                fin_vids = list(dict.fromkeys(fin_vids))
                fin_sql = _registry_sql_from_result(finance_data)
                self._trace(
                    state,
                    {
                        "phase": "composite_step_result",
                        "step_index": 1,
                        "agent": "finance",
                        "operation": "registry_sql",
                        "ok": True,
                        "mode": finance_data.get("mode") if isinstance(finance_data, dict) else None,
                        "rows": len(fin_rows),
                        "voyage_ids": len(fin_vids),
                        "extracted_voyage_ids": fin_vids,
                        "sql_present": bool(fin_sql),
                        "sql": fin_sql,
                        "summary": f"Finance(single): fetched {len(fin_rows)} rows using registry SQL.",
                    },
                )
            except Exception:
                pass

            # 3️⃣ Ops
            ops_data = None
            finance_fr = ""
            if isinstance(finance_data, dict):
                finance_fr = str(finance_data.get("fallback_reason") or "")
            postgres_down = "Postgres is not available" in finance_fr or "connection refused" in finance_fr.lower()

            if postgres_down:
                ops_data = {"mode": "error", "rows": [], "fallback_reason": finance_fr}
            else:
                self._trace(
                    state,
                    {
                        "phase": "composite_step_start",
                        "step_index": 2,
                        "step_count": 2,
                        "agent": "ops",
                        "operation": "registry_sql",
                        "inputs": {"intent_key": intent_key},
                        "goal": "Run ops registry SQL in single plan mode.",
                    },
                )
                try:
                    ops_data = self.ops_agent.run(
                        intent_key=intent_key,
                        slots=slots,
                        session_context=session_context,
                        user_input=user_input,
                    )
                except TypeError:
                    ops_data = self.ops_agent.run(
                        intent_key=intent_key,
                        slots=slots,
                        session_context={**session_context, "user_input": user_input},
                    )
                except Exception as e:
                    _dprint(f"⚠️  Ops failed: {e}")
                    ops_data = {"mode": "error", "rows": []}
            try:
                ops_rows = ops_data.get("rows") if isinstance(ops_data, dict) else []
                ops_rows = ops_rows if isinstance(ops_rows, list) else []
                ops_vids = [r.get("voyage_id") for r in ops_rows if isinstance(r, dict) and r.get("voyage_id")]
                ops_vids = list(dict.fromkeys(ops_vids))
                ops_sql = _registry_sql_from_result(ops_data)
                self._trace(
                    state,
                    {
                        "phase": "composite_step_result",
                        "step_index": 2,
                        "agent": "ops",
                        "operation": "registry_sql",
                        "ok": True,
                        "mode": ops_data.get("mode") if isinstance(ops_data, dict) else None,
                        "rows": len(ops_rows),
                        "voyage_ids": len(ops_vids),
                        "extracted_voyage_ids": ops_vids,
                        "sql_present": bool(ops_sql),
                        "sql": ops_sql,
                        "summary": f"Ops(single): fetched {len(ops_rows)} rows using registry SQL.",
                    },
                )
            except Exception:
                pass

            finance_safe = _json_safe(finance_data)
            ops_safe = _json_safe(ops_data)
            mongo_safe = _json_safe(mongo_data)
            raw_finance_rows = []
            if isinstance(finance_safe, dict) and isinstance(finance_safe.get("rows"), list):
                raw_finance_rows = list(finance_safe.get("rows") or [])

            # Reconcile single-voyage SQL rows with Mongo's canonical vessel for this voyage_number.
            # In some datasets, voyage_number is reused across vessels; this keeps voyage.summary strict.
            def _norm_imo(v: Any) -> str:
                if v in (None, ""):
                    return ""
                s = str(v).strip()
                if s.endswith(".0"):
                    s = s[:-2]
                return s

            mongo_imo = ""
            mongo_vessel_name = ""
            if isinstance(mongo_safe, dict):
                mongo_imo = _norm_imo(mongo_safe.get("vessel_imo"))
                mongo_vessel_name = str(mongo_safe.get("vessel_name") or "").strip().lower()

            # Strict Mongo-led reconciliation: one voyage → one vessel; never surface multi-vessel finance rows.
            if (mongo_imo or mongo_vessel_name or voyage_id) and isinstance(finance_safe, dict) and isinstance(finance_safe.get("rows"), list):
                fin_rows = finance_safe.get("rows") or []
                fin_filtered = []
                for r in fin_rows:
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
                ops_rows = ops_safe.get("rows") or []
                ops_filtered = []
                for r in ops_rows:
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

            # If all backends are unavailable, avoid a slow/expensive LLM call and return a clear message.
            finance_ok = isinstance(finance_safe, dict) and finance_safe.get("rows")
            ops_ok = isinstance(ops_safe, dict) and ops_safe.get("rows")
            mongo_ok = isinstance(mongo_safe, dict) and (
                mongo_safe.get("remarks") or mongo_safe.get("fixtures") or mongo_safe.get("voyage_id") or mongo_safe.get("voyage_number")
            )
            if not finance_ok and not ops_ok and not mongo_ok:
                vn = voyage_number or slots.get("voyage_numbers") or voyage_id or "this voyage"
                vn_disp = str(vn)
                ops_fr = str((ops_safe or {}).get("fallback_reason") or "")
                fin_fr = str((finance_safe or {}).get("fallback_reason") or "")
                combined = f"{ops_fr} {fin_fr}".lower()
                # Empty SQL/Mongo rows often mean wrong/incomplete voyage ref — do not blame infra unless transport errors show up.
                transport_unavailable = postgres_down or any(
                    needle in combined
                    for needle in (
                        "connection refused",
                        "postgres is not available",
                        "could not connect",
                        "timed out",
                        "timeout",
                        "server selection timed out",
                        "operationalerror",
                        "lost connection",
                        "network is unreachable",
                        "name or service not known",
                    )
                )
                if transport_unavailable:
                    answer = _router_fallback("backend_unavailable_generic", voyage_ref=vn_disp)
                else:
                    answer = _router_fallback("voyage_reference_ambiguous_or_not_found", voyage_ref=vn_disp)
                state["merged"] = {"finance": finance_safe, "ops": ops_safe, "mongo": {}, "artifacts": {"intent_key": intent_key, "slots": slots}}
                state["answer"] = answer
                state["slots"] = slots
                state["finance"] = finance_safe
                state["ops"] = ops_safe
                state["mongo"] = {}
                return state

            # Normalize single-path mongo payload into the same shape as dynamic NoSQL results
            # so validators/summarizer have a consistent contract.
            mongo_llm_like = mongo_safe
            if isinstance(mongo_safe, dict) and mongo_safe and mongo_safe.get("mode") != "mongo_llm":
                vid = mongo_safe.get("voyage_id")
                vnum = mongo_safe.get("voyage_number")
                remarks = mongo_safe.get("remarks") or []
                fixtures = mongo_safe.get("fixtures") or []
                if not session_may_access_finance_kpi(session_context):
                    fixtures = []
                mongo_llm_like = {
                    "mode": "mongo_llm",
                    "ok": True,
                    "collection": "voyages",
                    "filter": {"voyageId": str(vid)} if vid else ({"voyageNumber": str(vnum)} if vnum else {}),
                    "projection": get_mongo_projection("single_path_mongo_payload"),
                    "limit": 1,
                    "rows": [
                        {
                            "voyageId": str(vid) if vid is not None else None,
                            "voyageNumber": str(vnum) if vnum is not None else None,
                            "remarks": remarks,
                            "fixtures": fixtures,
                        }
                    ] if (vid or vnum) else [],
                }

            merged = {
                "finance": finance_safe,
                "ops": ops_safe,
                "mongo": mongo_llm_like,
                "artifacts": {
                    "intent_key": intent_key,
                    "slots": slots,
                    "finance_kpi_unavailable": not session_may_access_finance_kpi(session_context),
                },
                "dynamic_sql_used": isinstance(finance_safe, dict) and finance_safe.get("mode") == "dynamic_sql",
                "dynamic_sql_agents": ["finance", "ops"] if isinstance(finance_safe, dict) and finance_safe.get("mode") == "dynamic_sql" else [],
            }

            # Keep state["data"] aligned with the response payload for validators.
            state["data"]["finance"] = finance_safe
            state["data"]["ops"] = ops_safe
            state["data"]["mongo"] = mongo_llm_like
            state["data"]["artifacts"] = merged.get("artifacts") or {"intent_key": intent_key, "slots": slots}

            # Guardrail: if the user asked for financial metrics on a single voyage but
            # finance has no rows, avoid LLM-generated "best effort" summaries that can
            # fabricate multi-voyage/multi-vessel tables.
            user_input_lower = (user_input or "").lower()
            asked_financials = any(
                k in user_input_lower
                for k in (
                    "financial summary",
                    "financials",
                    "pnl",
                    "profit",
                    "revenue",
                    "expense",
                    "tce",
                    "commission",
                )
            )
            # Ops-only / finance-KPI-disabled tenants: never run the LLM for explicit financial asks
            # when finance rows are empty — ops/mongo payloads can still contain hire/fixture text
            # that models misread as revenue/PnL.
            if asked_financials and not finance_ok and not session_may_access_finance_kpi(session_context):
                answer = get_finance_kpi_scope_restricted_user_message().strip()
                snap = GraphRouter._ops_only_voyage_snapshot_markdown(ops_safe if isinstance(ops_safe, dict) else {})
                if snap:
                    answer = f"{answer}\n\n{snap}"
                state["merged"] = merged
                state["answer"] = answer
                state["slots"] = slots
                state["finance"] = finance_safe
                state["ops"] = ops_safe
                state["mongo"] = mongo_llm_like
                return state

            if asked_financials and not finance_ok and session_may_access_finance_kpi(session_context):
                vn = voyage_number or slots.get("voyage_numbers") or voyage_id or "this voyage"
                # Helpful diagnostics: finance had rows for this voyage number, but none
                # matched Mongo's canonical vessel after strict reconciliation.
                if raw_finance_rows:
                    vessel_labels = []
                    for r in raw_finance_rows:
                        if not isinstance(r, dict):
                            continue
                        imo = _norm_imo(r.get("vessel_imo"))
                        vname = str(r.get("vessel_name") or "").strip()
                        label = f"{vname} ({imo})" if (vname or imo) else ""
                        if label and label not in vessel_labels:
                            vessel_labels.append(label)
                    preview = ", ".join(vessel_labels[:5]) if vessel_labels else "N/A"
                    extra = (len(vessel_labels) - 5) if vessel_labels else 0
                    suffix = f" (+{extra} more)" if extra > 0 else ""
                    answer = _router_fallback(
                        "finance_identity_mismatch",
                        voyage_ref=vn,
                        candidate_preview=preview,
                        candidate_suffix=suffix,
                    )
                    state["merged"] = merged
                    state["answer"] = answer
                    state["slots"] = slots
                    state["finance"] = finance_safe
                    state["ops"] = ops_safe
                    state["mongo"] = mongo_llm_like
                    return state
                remarks_count = 0
                if isinstance(mongo_safe, dict) and isinstance(mongo_safe.get("remarks"), list):
                    remarks_count = len(mongo_safe.get("remarks") or [])
                answer = _router_fallback(
                    "missing_finance_records",
                    voyage_ref=vn,
                    remarks_count=remarks_count,
                )
                state["merged"] = merged
                state["answer"] = answer
                state["slots"] = slots
                state["finance"] = finance_safe
                state["ops"] = ops_safe
                state["mongo"] = mongo_llm_like
                return state

            try:
                merged_llm = GraphRouter._sanitize_for_llm(compact_payload(merged))
                if hasattr(self.llm, "generate_final_answer"):
                    answer = self.llm.generate_final_answer(
                        question=user_input,
                        merged_data=merged_llm,
                        session_context=session_context,
                    )
                else:
                    answer = self.llm.summarize_answer(
                        question=user_input,
                        plan=state.get("plan") or {"plan_type": "single", "intent_key": intent_key},
                        merged=merged_llm,
                        session_context=session_context,
                    )
            except Exception as e:
                _dprint(f"⚠️  LLM generation failed: {e}")
                answer = _router_fallback("llm_summary_error", error=e)

            # Populate state so it naturally bypasses the rest via routing
            state["merged"] = merged
            state["answer"] = answer
            state["slots"] = slots
            state["finance"] = finance_safe
            state["ops"] = ops_safe
            state["mongo"] = mongo_llm_like
            
            # Save session immediately
            persisted_slots = self._build_persisted_slots(base=(session_context.get("slots") or {}), updates=slots)
            fin_rows_now = finance_safe.get("rows") if isinstance(finance_safe, dict) else []
            ops_rows_now = ops_safe.get("rows") if isinstance(ops_safe, dict) else []
            fin_row = fin_rows_now[0] if isinstance(fin_rows_now, list) and fin_rows_now and isinstance(fin_rows_now[0], dict) else {}
            ops_row = ops_rows_now[0] if isinstance(ops_rows_now, list) and ops_rows_now and isinstance(ops_rows_now[0], dict) else {}
            mongo_row = (mongo_llm_like.get("rows") or [None])[0] if isinstance(mongo_llm_like, dict) else None
            remarks_val = []
            if isinstance(mongo_row, dict):
                remarks_val = mongo_row.get("remarks") or []
            remarks_text = None
            if isinstance(remarks_val, list):
                remarks_text = " | ".join([str(x).strip() for x in remarks_val[:4] if x not in (None, "", [], {})]) or None
            elif remarks_val not in (None, "", [], {}):
                remarks_text = str(remarks_val).strip()
            single_result_row = {
                "voyage_id": mongo_safe.get("voyage_id") or slots.get("voyage_id") or fin_row.get("voyage_id") or ops_row.get("voyage_id"),
                "voyage_number": mongo_safe.get("voyage_number") or slots.get("voyage_number") or fin_row.get("voyage_number") or ops_row.get("voyage_number"),
                "vessel_name": mongo_safe.get("vessel_name") or slots.get("vessel_name") or fin_row.get("vessel_name") or ops_row.get("vessel_name"),
                "vessel_imo": mongo_safe.get("vessel_imo") or slots.get("imo") or slots.get("vessel_imo") or fin_row.get("vessel_imo") or ops_row.get("vessel_imo") or ops_row.get("imo"),
                "pnl": fin_row.get("pnl"),
                "revenue": fin_row.get("revenue"),
                "total_expense": fin_row.get("total_expense"),
                "tce": fin_row.get("tce"),
                "total_commission": fin_row.get("total_commission"),
                "offhire_days": ops_row.get("offhire_days"),
                "delay_reason": ops_row.get("delay_reason") or ops_row.get("delay_reasons"),
                "key_ports": ops_row.get("key_ports") or ops_row.get("ports") or ops_row.get("ports_json"),
                "cargo_grades": ops_row.get("cargo_grades") or fin_row.get("cargo_grades"),
                "port_calls": ops_row.get("port_calls") or fin_row.get("port_calls") or fin_row.get("port_count"),
                "remarks": remarks_text,
            }
            single_result_row = {k: v for k, v in single_result_row.items() if v not in (None, "", [], {})}
            single_result_set = {
                "source_intent": intent_key,
                "rows": [single_result_row] if single_result_row else [],
                "meta": {"source_intent": intent_key, "available_metrics": [m for m in ("pnl", "revenue", "total_expense", "tce", "offhire_days", "port_calls") if single_result_row.get(m) not in (None, "", [], {})], "primary_metric": "pnl" if single_result_row.get("pnl") not in (None, "", [], {}) else None},
            }
            self.redis.save_session(
                state["session_id"],
                {
                    **session_context,
                    "last_intent": intent_key,
                    "last_intent_key": intent_key,
                    "memory_slots": self._extract_memory_slots(persisted_slots),
                    "param_slots": self._extract_param_slots(persisted_slots),
                    "slots": persisted_slots,
                    "last_result_set": single_result_set,
                    "last_focus_slots": self._row_focus_slots(single_result_row),
                    "last_user_input": user_input,
                    "_turn_marker": uuid.uuid4().hex,
                    "_record_turn": self._build_turn_history_entry(
                        query=user_input,
                        raw_user_input=state.get("raw_user_input") or user_input,
                        intent_key=intent_key,
                        slots=persisted_slots,
                        answer=answer,
                        plan_type=state.get("plan_type") or (state.get("plan") or {}).get("plan_type"),
                    ),
                },
            )
            state["session_ctx"] = {
                **(session_context or {}),
                "last_intent": intent_key,
                "memory_slots": self._extract_memory_slots(persisted_slots),
                "param_slots": self._extract_param_slots(persisted_slots),
                "slots": persisted_slots,
                "last_result_set": single_result_set,
                "last_focus_slots": self._row_focus_slots(single_result_row),
                "last_user_input": user_input,
            }
            return state

        # =========================================================
        # Fleet-wide vessel metadata ranking/listing from Mongo
        # =========================================================
        if intent_key == "ranking.vessel_metadata":
            def _extract_tag_pairs(doc: Dict[str, Any]) -> list[Dict[str, str]]:
                out: list[Dict[str, str]] = []
                raw_tags = doc.get("tags")
                if not isinstance(raw_tags, list):
                    return out
                for tag in raw_tags:
                    if not isinstance(tag, dict):
                        continue
                    cat = str(tag.get("category") or "").strip()
                    val = str(tag.get("value") or "").strip()
                    if cat or val:
                        out.append({"category": cat, "value": val})
                return out

            def _pool_value(doc: Dict[str, Any]) -> str | None:
                for tag in _extract_tag_pairs(doc):
                    cat = tag.get("category", "").lower()
                    val = tag.get("value")
                    if "pool" in cat and val:
                        return val
                for tag in _extract_tag_pairs(doc):
                    val = tag.get("value", "")
                    if "pool" in val.lower():
                        return val
                return None

            def _normalize_scrubber(val: Any, doc: Dict[str, Any]) -> str | None:
                if val not in (None, ""):
                    return str(val).strip()
                for tag in _extract_tag_pairs(doc):
                    txt = str(tag.get("value") or "").strip()
                    if not txt:
                        continue
                    tl = txt.lower()
                    if "scrubber" in tl:
                        return txt
                return None

            def _default_speed(doc: Dict[str, Any], passage_type: str) -> float | None:
                cps = doc.get("consumption_profiles")
                if not isinstance(cps, list):
                    return None
                wanted = str(passage_type or "").strip().lower()
                fallback: float | None = None
                for cp in cps:
                    if not isinstance(cp, dict):
                        continue
                    pp = cp.get("passageProfile")
                    if not isinstance(pp, list):
                        continue
                    for p in pp:
                        if not isinstance(p, dict):
                            continue
                        if str(p.get("passageType") or "").strip().lower() != wanted:
                            continue
                        cons = p.get("consumption")
                        if not isinstance(cons, list):
                            continue
                        for c in cons:
                            if not isinstance(c, dict):
                                continue
                            try:
                                speed = float(c.get("speed"))
                            except Exception:
                                speed = None
                            if speed is None:
                                continue
                            if c.get("isDefault") is True:
                                return speed
                            if fallback is None or speed > fallback:
                                fallback = speed
                return fallback

            def _current_contract(doc: Dict[str, Any]) -> Dict[str, Any] | None:
                history = (doc.get("contract_history") or {}).get("list")
                if not isinstance(history, list):
                    return None
                for item in history:
                    if isinstance(item, dict) and item.get("isCurrent") is True:
                        return item
                for item in history:
                    if isinstance(item, dict):
                        return item
                return None

            def _contract_duration_days(contract: Dict[str, Any] | None) -> float | None:
                if not isinstance(contract, dict):
                    return None
                try:
                    dur = float(contract.get("duration"))
                except Exception:
                    return None
                dtype = str(contract.get("durationType") or "").strip().lower()
                if "year" in dtype:
                    return dur * 365.0
                if "month" in dtype:
                    return dur * 30.0
                if "week" in dtype:
                    return dur * 7.0
                return dur

            def _contract_duration_label(contract: Dict[str, Any] | None) -> str | None:
                if not isinstance(contract, dict):
                    return None
                dur = contract.get("duration")
                dtype = contract.get("durationType")
                if dur in (None, ""):
                    return None
                return f"{dur} {dtype}".strip() if dtype not in (None, "") else str(dur)

            def _fmt_num(v: Any, *, money: bool = False) -> str:
                if v in (None, "", [], {}):
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

            def _build_table(headers: list[str], body_rows: list[list[str]]) -> str:
                out = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
                for row in body_rows:
                    out.append("| " + " | ".join(row) + " |")
                return "\n".join(out)

            projection = cfg.get("mongo_projection")
            try:
                res = self.mongo_agent.run(
                    intent_key=cfg.get("mongo_intent", "vessel.list_all"),
                    slots={},
                    projection=projection,
                    session_context=session_context,
                )
                mongo_safe = _json_safe(res)
            except Exception as e:
                _dprint(f"⚠️  Mongo failed (ranking.vessel_metadata): {e}")
                mongo_safe = {}

            vessel_docs = []
            if isinstance(mongo_safe, dict):
                doc = mongo_safe.get("document")
                if isinstance(doc, dict) and isinstance(doc.get("vessels"), list):
                    vessel_docs = [v for v in doc.get("vessels") if isinstance(v, dict)]

            self._trace_single_mongo_query(
                state,
                intent_key=intent_key,
                operation="fleet_metadata_lookup",
                collection="vessels",
                filt={},
                projection=projection,
                limit=200,
                rows=len(vessel_docs),
                summary="Mongo: fetched fleet-wide vessel metadata from the `vessels` collection for ranking/listing.",
            )

            ql = (user_input or "").lower()
            wants_count = (
                any(k in ql for k in ("how many", "count", "total how many", "total number"))
                and not any(k in ql for k in ("highest", "lowest", "longest", "fastest", "top", "best", "worst", "least", "most"))
            )
            wants_operating = any(k in ql for k in ("operating", "operational", "active vessel", "active vessels"))
            wants_hire_rate = any(k in ql for k in ("hire rate", "hirerate", "hire_rate", "hire-rate"))
            wants_scrubber = "scrubber" in ql
            wants_non_scrubber = any(k in ql for k in ("non-scrubber", "non scrubber", "without scrubber"))
            wants_ballast_speed = ("ballast" in ql) and ("speed" in ql)
            wants_laden_speed = ("laden" in ql) and ("speed" in ql)
            wants_contract = "contract" in ql and any(k in ql for k in ("longest", "duration", "current", "owner", "delivery"))
            wants_short_pool = "short pool" in ql
            wants_long_pool = "long pool" in ql
            wants_market_type = "market type" in ql

            rows: list[Dict[str, Any]] = []
            seen_vessel_keys: set[str] = set()
            for doc in vessel_docs:
                contract = _current_contract(doc)
                pool = _pool_value(doc)
                scrubber = _normalize_scrubber(doc.get("scrubber"), doc)
                vessel_key = str(doc.get("imo") or doc.get("name") or "").strip().lower()
                if not vessel_key:
                    continue
                if vessel_key in seen_vessel_keys:
                    continue
                seen_vessel_keys.add(vessel_key)
                rows.append(
                    {
                        "vessel_name": doc.get("name"),
                        "imo": doc.get("imo"),
                        "hire_rate": doc.get("hireRate"),
                        "scrubber": scrubber,
                        "market_type": doc.get("marketType"),
                        "is_operating": doc.get("isVesselOperating"),
                        "pool": pool,
                        "ballast_speed": _default_speed(doc, "ballast"),
                        "laden_speed": _default_speed(doc, "laden"),
                        "current_contract_duration_days": _contract_duration_days(contract),
                        "current_contract_duration": _contract_duration_label(contract),
                        "current_contract_owner": (contract or {}).get("owner") if isinstance(contract, dict) else None,
                        "current_contract_delivery": (contract or {}).get("deliveryDatetime") if isinstance(contract, dict) else None,
                    }
                )

            def _scrubber_bucket(val: Any) -> str:
                txt = str(val or "").strip().lower()
                if not txt:
                    return ""
                if "non" in txt and "scrubber" in txt:
                    return "no"
                if txt in ("no", "false", "0"):
                    return "no"
                if "yes" in txt or "scrubber" in txt:
                    return "yes"
                return txt

            filtered = list(rows)
            if wants_operating:
                filtered = [r for r in filtered if r.get("is_operating") is True]
            if wants_non_scrubber:
                filtered = [r for r in filtered if _scrubber_bucket(r.get("scrubber")) == "no"]
            elif wants_scrubber and not wants_hire_rate:
                filtered = [r for r in filtered if _scrubber_bucket(r.get("scrubber")) == "yes"]
            if wants_short_pool:
                filtered = [r for r in filtered if "short pool" in str(r.get("pool") or "").lower()]
            if wants_long_pool:
                filtered = [r for r in filtered if "long pool" in str(r.get("pool") or "").lower()]

            metric = None
            if wants_hire_rate:
                metric = "hire_rate"
            elif wants_ballast_speed:
                metric = "ballast_speed"
            elif wants_laden_speed:
                metric = "laden_speed"
            elif wants_contract:
                metric = "current_contract_duration_days"

            descending = any(k in ql for k in ("highest", "top", "most", "best", "longest", "fastest"))
            ascending = any(k in ql for k in ("lowest", "least", "worst"))
            if metric:
                filtered = sorted(
                    filtered,
                    key=lambda r: (
                        r.get(metric) is None,
                        -(float(r.get(metric))) if (descending and r.get(metric) is not None) else (float(r.get(metric)) if r.get(metric) is not None else float("inf")),
                    ),
                )
                if ascending and not descending:
                    filtered = sorted(
                        filtered,
                        key=lambda r: (r.get(metric) is None, float(r.get(metric)) if r.get(metric) is not None else float("inf")),
                    )
            else:
                filtered = sorted(filtered, key=lambda r: str(r.get("vessel_name") or ""))

            limit = max(1, min(int(slots.get("limit") or 10), 25))
            shown = filtered[:limit]

            if not rows:
                answer = _router_fallback("vessel_metadata_empty")
            elif wants_count:
                if wants_operating:
                    answer = f"There are **{len(filtered)}** vessels currently operating."
                elif wants_non_scrubber:
                    answer = f"There are **{len(filtered)}** non-scrubber vessels."
                elif wants_scrubber:
                    answer = f"There are **{len(filtered)}** scrubber-fitted vessels."
                elif wants_short_pool:
                    answer = f"There are **{len(filtered)}** vessels in the short pool."
                elif wants_long_pool:
                    answer = f"There are **{len(filtered)}** vessels in the long pool."
                else:
                    answer = f"There are **{len(filtered)}** matching vessels."
            else:
                headers = ["Vessel Name", "Vessel IMO"]
                body_rows: list[list[str]] = []
                if wants_hire_rate:
                    headers.append("Hire Rate")
                    for r in shown:
                        body_rows.append([str(r.get("vessel_name") or "Not available"), str(r.get("imo") or "Not available"), _fmt_num(r.get("hire_rate"), money=True)])
                elif wants_ballast_speed:
                    headers.append("Default Ballast Speed")
                    for r in shown:
                        body_rows.append([str(r.get("vessel_name") or "Not available"), str(r.get("imo") or "Not available"), _fmt_num(r.get("ballast_speed"))])
                elif wants_laden_speed:
                    headers.append("Default Laden Speed")
                    for r in shown:
                        body_rows.append([str(r.get("vessel_name") or "Not available"), str(r.get("imo") or "Not available"), _fmt_num(r.get("laden_speed"))])
                elif wants_contract:
                    headers.extend(["Current Contract Duration", "Owner", "Delivery"])
                    for r in shown:
                        body_rows.append([
                            str(r.get("vessel_name") or "Not available"),
                            str(r.get("imo") or "Not available"),
                            str(r.get("current_contract_duration") or "Not available"),
                            str(r.get("current_contract_owner") or "Not available"),
                            str(r.get("current_contract_delivery") or "Not available"),
                        ])
                elif wants_scrubber or wants_non_scrubber:
                    headers.append("Scrubber")
                    for r in shown:
                        body_rows.append([str(r.get("vessel_name") or "Not available"), str(r.get("imo") or "Not available"), str(r.get("scrubber") or "Not available")])
                elif wants_short_pool or wants_long_pool:
                    headers.append("Pool")
                    if wants_market_type:
                        headers.append("Market Type")
                    for r in shown:
                        row = [str(r.get("vessel_name") or "Not available"), str(r.get("imo") or "Not available"), str(r.get("pool") or "Not available")]
                        if wants_market_type:
                            row.append(str(r.get("market_type") or "Not available"))
                        body_rows.append(row)
                elif wants_operating:
                    headers.extend(["Operating", "Market Type"])
                    for r in shown:
                        body_rows.append([
                            str(r.get("vessel_name") or "Not available"),
                            str(r.get("imo") or "Not available"),
                            "Yes" if r.get("is_operating") is True else ("No" if r.get("is_operating") is False else "Not available"),
                            str(r.get("market_type") or "Not available"),
                        ])
                else:
                    headers.extend(["Operating", "Scrubber", "Hire Rate"])
                    for r in shown:
                        body_rows.append([
                            str(r.get("vessel_name") or "Not available"),
                            str(r.get("imo") or "Not available"),
                            "Yes" if r.get("is_operating") is True else ("No" if r.get("is_operating") is False else "Not available"),
                            str(r.get("scrubber") or "Not available"),
                            _fmt_num(r.get("hire_rate"), money=True),
                        ])

                if not shown:
                    answer = "No vessels matched the requested metadata filter."
                else:
                    if wants_hire_rate and shown:
                        lead = f"The vessel with the {'highest' if descending and not ascending else 'lowest'} hire rate is **{shown[0].get('vessel_name') or 'Not available'}**."
                    elif wants_ballast_speed and shown:
                        lead = f"The vessel with the highest default ballast speed is **{shown[0].get('vessel_name') or 'Not available'}**."
                    elif wants_laden_speed and shown:
                        lead = f"The vessel with the highest default laden speed is **{shown[0].get('vessel_name') or 'Not available'}**."
                    elif wants_contract and shown:
                        lead = f"The vessel with the longest current contract duration is **{shown[0].get('vessel_name') or 'Not available'}**."
                    elif wants_operating:
                        lead = f"There are **{len(filtered)}** vessels currently operating; showing the first {len(shown)}."
                    elif wants_non_scrubber:
                        lead = f"There are **{len(filtered)}** non-scrubber vessels; showing the first {len(shown)}."
                    elif wants_scrubber:
                        lead = f"There are **{len(filtered)}** scrubber-fitted vessels; showing the first {len(shown)}."
                    elif wants_short_pool:
                        lead = f"There are **{len(filtered)}** vessels in the short pool; showing the first {len(shown)}."
                    elif wants_long_pool:
                        lead = f"There are **{len(filtered)}** vessels in the long pool; showing the first {len(shown)}."
                    else:
                        lead = f"Showing **{len(shown)}** vessels that match the requested metadata view."
                    answer = lead + "\n\n" + _build_table(headers, body_rows)

            current_trace = []
            if isinstance(state.get("artifacts"), dict) and isinstance((state.get("artifacts") or {}).get("trace"), list):
                current_trace = (state.get("artifacts") or {}).get("trace") or []

            merged_rows = shown if shown else filtered[:limit]
            merged = {
                "finance": {"mode": None, "rows": []},
                "ops": {"mode": None, "rows": []},
                "mongo": mongo_safe if isinstance(mongo_safe, dict) else {},
                "artifacts": {"intent_key": intent_key, "slots": slots, "merged_rows": merged_rows, "trace": current_trace},
                "dynamic_sql_used": False,
                "dynamic_sql_agents": [],
            }

            state["merged"] = merged
            state["answer"] = answer
            state["slots"] = slots
            state["mongo"] = mongo_safe
            state["finance"] = {"mode": None, "rows": []}
            state["ops"] = {"mode": None, "rows": []}
            state["data"]["mongo"] = mongo_safe
            state["data"]["finance"] = {"mode": None, "rows": []}
            state["data"]["ops"] = {"mode": None, "rows": []}
            state["data"]["artifacts"] = {"intent_key": intent_key, "slots": slots, "merged_rows": merged_rows, "trace": current_trace}
            return state

        # =========================================================
        # Vessel metadata: Mongo-only deterministic summary
        # =========================================================
        if intent_key == "vessel.metadata":
            def _extract_passage_types(doc: Dict[str, Any]) -> list[str]:
                passage_types: list[str] = []
                cps = doc.get("consumption_profiles")
                if isinstance(cps, list):
                    seen_pt = set()
                    for cp in cps:
                        if not isinstance(cp, dict):
                            continue
                        pp = cp.get("passageProfile")
                        if not isinstance(pp, list):
                            continue
                        for p in pp:
                            if not isinstance(p, dict):
                                continue
                            pt = p.get("passageType")
                            if pt is None:
                                continue
                            spt = str(pt).strip()
                            if spt and spt not in seen_pt:
                                seen_pt.add(spt)
                                passage_types.append(spt)
                return passage_types

            def _extract_tags(doc: Dict[str, Any]) -> list[str]:
                tags: list[str] = []
                raw_tags = doc.get("tags")
                if isinstance(raw_tags, list):
                    for t in raw_tags:
                        if isinstance(t, dict):
                            v = t.get("value")
                            if v is not None:
                                sv = str(v).strip()
                                if sv:
                                    tags.append(sv)
                return tags[:8]

            def _extract_passage_consumption_rows(doc: Dict[str, Any]) -> list[Dict[str, Any]]:
                rows: list[Dict[str, Any]] = []
                cps = doc.get("consumption_profiles")
                if not isinstance(cps, list):
                    return rows
                for cp in cps:
                    if not isinstance(cp, dict):
                        continue
                    profile_name = cp.get("profileName")
                    pp = cp.get("passageProfile")
                    if not isinstance(pp, list):
                        continue
                    for p in pp:
                        if not isinstance(p, dict):
                            continue
                        ptype = p.get("passageType")
                        cons = p.get("consumption")
                        if not isinstance(cons, list):
                            continue
                        for c in cons:
                            if not isinstance(c, dict):
                                continue
                            rows.append(
                                {
                                    "profile_name": profile_name,
                                    "passage_type": str(ptype).strip() if ptype is not None else None,
                                    "speed": c.get("speed"),
                                    "ifo": c.get("ifo"),
                                    "mgo": c.get("mgo"),
                                    "rpm": c.get("rpm"),
                                    "is_default": bool(c.get("isDefault")) if c.get("isDefault") is not None else False,
                                }
                            )
                return rows

            def _extract_non_passage_consumption(doc: Dict[str, Any]) -> Dict[str, Any] | None:
                cps = doc.get("consumption_profiles")
                if not isinstance(cps, list):
                    return None
                for cp in cps:
                    if not isinstance(cp, dict):
                        continue
                    npp = cp.get("nonPassageProfile")
                    if not isinstance(npp, list):
                        continue
                    for np in npp:
                        if not isinstance(np, dict):
                            continue
                        cons = np.get("consumption")
                        if isinstance(cons, list) and cons and isinstance(cons[0], dict):
                            return cons[0]
                return None

            records: list[Dict[str, Any]] = []
            projection = cfg.get("mongo_projection")
            trace_queries: list[Dict[str, Any]] = []

            # Path A: direct vessel anchor (name/imo)
            if slots.get("vessel_name") or slots.get("imo"):
                try:
                    res = self.mongo_agent.run(
                        intent_key=cfg.get("mongo_intent", "entity.vessel"),
                        slots=slots,
                        projection=projection,
                        session_context=session_context,
                    )
                    mongo_safe = _json_safe(res)
                except Exception as e:
                    _dprint(f"⚠️  Mongo failed (vessel.metadata): {e}")
                    mongo_safe = {}

                doc = (mongo_safe or {}).get("document") if isinstance(mongo_safe, dict) else {}
                if not isinstance(doc, dict):
                    doc = {}
                if not doc and slots.get("vessel_name"):
                    try:
                        doc = self.mongo_agent.adapter.fetch_vessel_by_name(str(slots.get("vessel_name")), projection=projection)
                    except Exception:
                        doc = {}
                    if not isinstance(doc, dict):
                        doc = {}
                if isinstance(doc, dict) and doc:
                    records.append({
                        "voyage_number": slots.get("voyage_number"),
                        "vessel_name": doc.get("name") or slots.get("vessel_name"),
                        "imo": doc.get("imo") or slots.get("imo"),
                        "doc": doc,
                    })
                resolved_imo = (
                    (mongo_safe or {}).get("anchor_id")
                    if isinstance(mongo_safe, dict)
                    else None
                ) or doc.get("imo") or slots.get("imo")
                trace_queries.append(
                    {
                        "collection": "vessels",
                        "filter": {"imo": str(resolved_imo)} if resolved_imo not in (None, "") else {},
                        "projection": self._mongo_projection_for_trace(projection),
                        "sort": None,
                        "limit": 1,
                        "pipeline": None,
                    }
                )
            else:
                # Path B: small voyage-number list (1..3) -> resolve vessel(s) via Mongo voyages
                mongo_safe = {"mode": "mongo_metadata", "ok": True, "rows": []}
                vnums = slots.get("voyage_numbers")
                if not isinstance(vnums, list):
                    vn = slots.get("voyage_number")
                    if vn in (None, "", [], {}):
                        lfs = (session_context or {}).get("last_focus_slots") if isinstance(session_context, dict) else None
                        if isinstance(lfs, dict):
                            vn = lfs.get("voyage_number")
                    vnums = [vn] if vn not in (None, "", [], {}) else []
                unique_vnums: list[int] = []
                for v in vnums:
                    try:
                        iv = int(v)
                        if iv not in unique_vnums:
                            unique_vnums.append(iv)
                    except Exception:
                        continue
                unique_vnums = unique_vnums[:3]

                for vnum in unique_vnums:
                    try:
                        batch = self.mongo_agent.adapter.list_voyages_by_number(
                            vnum,
                            projection=get_mongo_projection("voyage_identity"),
                            limit=get_mongo_limit("voyage_identity_batch", 40),
                        )
                    except Exception:
                        batch = []
                    narrowed = narrow_voyage_rows_by_entity_slots(batch, slots)
                    for vdoc in narrowed:
                        if not isinstance(vdoc, dict) or not vdoc:
                            continue

                        imo = vdoc.get("vesselImo")
                        vessel_name = vdoc.get("vesselName")
                        mdoc = None
                        if imo not in (None, ""):
                            try:
                                mdoc = self.mongo_agent.adapter.fetch_vessel(str(imo), projection=projection)
                            except Exception:
                                mdoc = None
                        if not isinstance(mdoc, dict):
                            mdoc = {}
                        if not mdoc:
                            # Keep minimal voyage-resolved identity even if vessel metadata is missing.
                            mdoc = {"imo": imo, "name": vessel_name}

                        records.append({
                            "voyage_number": vdoc.get("voyageNumber"),
                            "vessel_name": mdoc.get("name") or vessel_name,
                            "imo": mdoc.get("imo") or imo,
                            "doc": mdoc,
                        })
                        mongo_safe["rows"].append(vdoc)

                voyage_filters = [str(v) for v in unique_vnums if v not in (None, "")]
                if voyage_filters:
                    trace_queries.append(
                        {
                            "collection": "voyages",
                            "filter": {"voyageNumber": {"$in": voyage_filters}},
                            "projection": self._mongo_projection_for_trace(
                                get_mongo_projection("voyage_identity")
                            ),
                            "sort": None,
                            "limit": len(voyage_filters),
                            "pipeline": None,
                        }
                    )
                vessel_imos = []
                for rec in records:
                    imo = rec.get("imo")
                    if imo in (None, ""):
                        continue
                    imo_s = str(imo)
                    if imo_s not in vessel_imos:
                        vessel_imos.append(imo_s)
                if vessel_imos:
                    trace_queries.append(
                        {
                            "collection": "vessels",
                            "filter": {"imo": {"$in": vessel_imos}},
                            "projection": self._mongo_projection_for_trace(projection),
                            "sort": None,
                            "limit": len(vessel_imos),
                            "pipeline": None,
                        }
                    )

            ql = (user_input or "").lower()
            ask_identity = any(k in ql for k in ("identity", "imo", "vessel id", "account code", "who is vessel", "vessel details", "name"))
            ask_hire_rate = any(k in ql for k in ("hire rate", "hirerate", "hire_rate", "hire-rate"))
            ask_scrubber = "scrubber" in ql
            ask_market_type = "market type" in ql
            ask_operating = any(k in ql for k in ("is operating", "is vessel operating", "operational", "operating status", "is it operating", "is it operational"))
            ask_commercial = any((ask_hire_rate, ask_scrubber, ask_market_type, ask_operating))
            ask_passage_types = any(k in ql for k in ("passage type", "passage types"))
            ask_passage_consumption = any(k in ql for k in ("consumption profile", "consumption profiles", "consumption", "speed", "ifo", "mgo", "rpm"))
            ask_default_consumption = any(k in ql for k in ("default consumption", "default speed", "defaults", "isdefault", "default"))
            ask_ballast = "ballast" in ql
            ask_laden = "laden" in ql
            ask_non_passage_consumption = any(k in ql for k in ("non passage", "non-passage", "idle", "load", "discharge", "heat", "clean", "inert"))
            if ask_non_passage_consumption and not (ask_ballast or ask_laden):
                ask_passage_consumption = False
            ask_tags = any(k in ql for k in ("tags", "segment", "pool", "commercial tag", "sanction"))
            ask_contract_history = any(k in ql for k in ("contract", "owner", "duration", "cp date", "delivery"))
            ask_contract_owner = "owner" in ql
            ask_contract_duration = "duration" in ql
            ask_contract_cp_date = "cp date" in ql
            ask_contract_delivery = "delivery" in ql
            ask_contract_history_list = any(k in ql for k in ("contract history", "show contract", "list contract", "contracts"))
            ask_contract_fields_only = (
                ask_contract_history
                and not ask_contract_history_list
                and any((ask_contract_owner, ask_contract_duration, ask_contract_cp_date, ask_contract_delivery))
            )
            ask_extracted_at = "extracted at" in ql
            has_specific = any(
                (
                    ask_identity,
                    ask_commercial,
                    ask_passage_types,
                    ask_passage_consumption,
                    ask_default_consumption,
                    ask_ballast,
                    ask_laden,
                    ask_non_passage_consumption,
                    ask_tags,
                    ask_contract_history,
                    ask_extracted_at,
                )
            )

            if not records:
                answer = _router_fallback("voyage_or_vessel_metadata_empty")
            elif len(records) == 1:
                rec = records[0]
                doc = rec.get("doc") if isinstance(rec.get("doc"), dict) else {}
                vessel_name = rec.get("vessel_name") or "this vessel"
                imo = rec.get("imo")
                hire_rate = doc.get("hireRate")
                scrubber = doc.get("scrubber")
                market_type = doc.get("marketType")
                account_code = doc.get("accountCode")
                is_operating = doc.get("isVesselOperating")
                passage_types = _extract_passage_types(doc)
                tags = _extract_tags(doc)
                passage_rows = _extract_passage_consumption_rows(doc)
                non_passage = _extract_non_passage_consumption(doc)
                contract_list = (doc.get("contract_history") or {}).get("list")
                current_contract = None
                if isinstance(contract_list, list):
                    for item in contract_list:
                        if isinstance(item, dict) and item.get("isCurrent") is True:
                            current_contract = item
                            break
                    if current_contract is None:
                        for item in contract_list:
                            if isinstance(item, dict):
                                current_contract = item
                                break
                contract_count = len(contract_list) if isinstance(contract_list, list) else None
                extracted_at = doc.get("extracted_at")

                answer_lines = ["### Summary"]
                answer_lines.append(f"- **Vessel**: {vessel_name}" + (f" (IMO: {imo})" if imo else ""))
                if has_specific:
                    if ask_identity:
                        answer_lines.append(f"- **Vessel ID**: {doc.get('vesselId')}" if doc.get("vesselId") not in (None, "") else "- **Vessel ID**: Not available")
                        answer_lines.append(f"- **Account code**: {account_code}" if account_code not in (None, "") else "- **Account code**: Not available")
                    if ask_commercial:
                        if ask_hire_rate:
                            if hire_rate is not None:
                                try:
                                    answer_lines.append(f"- **Hire rate**: ${float(hire_rate):,.2f}")
                                except Exception:
                                    answer_lines.append(f"- **Hire rate**: {hire_rate}")
                            else:
                                answer_lines.append("- **Hire rate**: Not available")
                        if ask_operating:
                            if is_operating is not None:
                                answer_lines.append(f"- **Operating**: {'Yes' if bool(is_operating) else 'No'}")
                            else:
                                answer_lines.append("- **Operating**: Not available")
                        if ask_scrubber:
                            answer_lines.append(f"- **Scrubber**: {scrubber}" if scrubber not in (None, "") else "- **Scrubber**: Not available")
                        if ask_market_type:
                            answer_lines.append(f"- **Market type**: {market_type}" if market_type not in (None, "") else "- **Market type**: Not available")
                    if ask_passage_types:
                        answer_lines.append(f"- **Passage types**: {', '.join(passage_types)}" if passage_types else "- **Passage types**: Not available")
                    if ask_passage_consumption or ask_default_consumption or ask_ballast or ask_laden:
                        filtered_rows = list(passage_rows)
                        if ask_default_consumption:
                            filtered_rows = [r for r in filtered_rows if r.get("is_default")]
                        if ask_ballast and not ask_laden:
                            filtered_rows = [r for r in filtered_rows if str(r.get("passage_type") or "").lower() == "ballast"]
                        if ask_laden and not ask_ballast:
                            filtered_rows = [r for r in filtered_rows if str(r.get("passage_type") or "").lower() == "laden"]
                        answer_lines.append("")
                        answer_lines.append("### Passage consumption")
                        if filtered_rows:
                            for r in filtered_rows[:8]:
                                ptype = r.get("passage_type") or "Unknown"
                                speed = r.get("speed")
                                ifo = r.get("ifo")
                                mgo = r.get("mgo")
                                dmark = " (default)" if r.get("is_default") else ""
                                answer_lines.append(f"- **{ptype}**{dmark}: speed={speed}, IFO={ifo}, MGO={mgo}")
                        else:
                            answer_lines.append("- Not available")
                    if ask_non_passage_consumption:
                        answer_lines.append("")
                        answer_lines.append("### Non-passage consumption")
                        if isinstance(non_passage, dict) and non_passage:
                            keys = (
                                "ifoLoad", "ifoDischarge", "ifoIdle", "ifoHeat", "ifoClean", "ifoInert",
                                "mgoLoad", "mgoDischarge", "mgoIdle", "mgoHeat", "mgoClean", "mgoInert",
                            )
                            for k in keys:
                                if k in non_passage:
                                    answer_lines.append(f"- **{k}**: {non_passage.get(k)}")
                        else:
                            answer_lines.append("- Not available")
                    if ask_tags:
                        answer_lines.append(f"- **Tags**: {', '.join(tags)}" if tags else "- **Tags**: Not available")
                    if ask_contract_history:
                        if ask_contract_fields_only:
                            if isinstance(current_contract, dict) and current_contract:
                                if ask_contract_owner:
                                    answer_lines.append(f"- **Owner**: {current_contract.get('owner')}" if current_contract.get("owner") not in (None, "") else "- **Owner**: Not available")
                                if ask_contract_duration:
                                    dur = current_contract.get("duration")
                                    dtyp = current_contract.get("durationType")
                                    if dur not in (None, ""):
                                        answer_lines.append(f"- **Duration**: {dur} {dtyp}".strip())
                                    else:
                                        answer_lines.append("- **Duration**: Not available")
                                if ask_contract_cp_date:
                                    answer_lines.append(f"- **CP date**: {current_contract.get('cpDate')}" if current_contract.get("cpDate") not in (None, "") else "- **CP date**: Not available")
                                if ask_contract_delivery:
                                    answer_lines.append(f"- **Delivery**: {current_contract.get('deliveryDatetime')}" if current_contract.get("deliveryDatetime") not in (None, "") else "- **Delivery**: Not available")
                            else:
                                if ask_contract_owner:
                                    answer_lines.append("- **Owner**: Not available")
                                if ask_contract_duration:
                                    answer_lines.append("- **Duration**: Not available")
                                if ask_contract_cp_date:
                                    answer_lines.append("- **CP date**: Not available")
                                if ask_contract_delivery:
                                    answer_lines.append("- **Delivery**: Not available")
                        elif isinstance(contract_list, list) and contract_list:
                            answer_lines.append("")
                            answer_lines.append("### Contract history")
                            for c in contract_list[:5]:
                                if not isinstance(c, dict):
                                    continue
                                cn = c.get("contractNumber")
                                owner = c.get("owner")
                                dur = c.get("duration")
                                dtyp = c.get("durationType")
                                cpd = c.get("cpDate")
                                dd = c.get("deliveryDatetime")
                                answer_lines.append(f"- Contract {cn} | Owner: {owner} | Duration: {dur} {dtyp} | CP Date: {cpd} | Delivery: {dd}")
                        else:
                            answer_lines.append("- **Contract history**: Not available")
                    if ask_extracted_at:
                        answer_lines.append(f"- **Extracted at**: {extracted_at}" if extracted_at not in (None, "") else "- **Extracted at**: Not available")
                else:
                    answer_lines.append(f"- **Passage types**: {', '.join(passage_types)}" if passage_types else "- **Passage types**: Not available")
                    answer_lines.append("")
                    answer_lines.append("### Metadata snapshot")
                    if hire_rate is not None:
                        try:
                            answer_lines.append(f"- **Hire rate**: ${float(hire_rate):,.2f}")
                        except Exception:
                            answer_lines.append(f"- **Hire rate**: {hire_rate}")
                    if scrubber not in (None, ""):
                        answer_lines.append(f"- **Scrubber**: {scrubber}")
                    if market_type not in (None, ""):
                        answer_lines.append(f"- **Market type**: {market_type}")
                    if account_code not in (None, ""):
                        answer_lines.append(f"- **Account code**: {account_code}")
                    if is_operating is not None:
                        answer_lines.append(f"- **Operating**: {'Yes' if bool(is_operating) else 'No'}")
                    if tags:
                        answer_lines.append(f"- **Tags**: {', '.join(tags)}")
                    if extracted_at not in (None, ""):
                        answer_lines.append(f"- **Extracted at**: {extracted_at}")

                answer = "\n".join(answer_lines)
            else:
                # Multi-voyage metadata response (query-specific, compact).
                max_render_rows = 20
                render_records = records[:max_render_rows]
                answer_lines = [
                    "### Summary",
                    f"- Found metadata for {len(records)} voyage-linked vessel records.",
                    (
                        f"- Showing first {len(render_records)} records for readability."
                        if len(records) > max_render_rows
                        else "- Showing all matched records."
                    ),
                    "",
                    "### Results",
                ]
                for rec in render_records:
                    doc = rec.get("doc") if isinstance(rec.get("doc"), dict) else {}
                    vessel_name = rec.get("vessel_name") or "Unknown"
                    imo = rec.get("imo")
                    vnum = rec.get("voyage_number")
                    prefix = f"- Voyage **{vnum}** - **{vessel_name}**" + (f" (IMO: {imo})" if imo else "")
                    details: list[str] = []
                    if ask_identity:
                        if doc.get("vesselId") not in (None, ""):
                            details.append(f"VesselId {doc.get('vesselId')}")
                        if doc.get("accountCode") not in (None, ""):
                            details.append(f"Account {doc.get('accountCode')}")
                    if ask_commercial:
                        hr = doc.get("hireRate")
                        if ask_hire_rate and hr is not None:
                            try:
                                details.append(f"Hire rate ${float(hr):,.2f}")
                            except Exception:
                                details.append(f"Hire rate {hr}")
                        if ask_operating and doc.get("isVesselOperating") is not None:
                            details.append(f"Operating {'Yes' if bool(doc.get('isVesselOperating')) else 'No'}")
                        if ask_scrubber and doc.get("scrubber") not in (None, ""):
                            details.append(f"Scrubber {doc.get('scrubber')}")
                        if ask_market_type and doc.get("marketType") not in (None, ""):
                            details.append(f"Market {doc.get('marketType')}")
                    if ask_passage_types:
                        pts = _extract_passage_types(doc)
                        details.append(f"Passage types {', '.join(pts)}" if pts else "Passage types Not available")
                    if ask_passage_consumption or ask_default_consumption or ask_ballast or ask_laden:
                        p_rows = _extract_passage_consumption_rows(doc)
                        if ask_default_consumption:
                            p_rows = [r for r in p_rows if r.get("is_default")]
                        if ask_ballast and not ask_laden:
                            p_rows = [r for r in p_rows if str(r.get("passage_type") or "").lower() == "ballast"]
                        if ask_laden and not ask_ballast:
                            p_rows = [r for r in p_rows if str(r.get("passage_type") or "").lower() == "laden"]
                        if p_rows:
                            r0 = p_rows[0]
                            details.append(f"{r0.get('passage_type')} speed {r0.get('speed')} IFO {r0.get('ifo')} MGO {r0.get('mgo')}" + (" default" if r0.get("is_default") else ""))
                    if ask_non_passage_consumption:
                        np = _extract_non_passage_consumption(doc)
                        if isinstance(np, dict) and np:
                            details.append("Non-passage available")
                        else:
                            details.append("Non-passage Not available")
                    if ask_tags:
                        tgs = _extract_tags(doc)
                        if tgs:
                            details.append(f"Tags {', '.join(tgs[:4])}")
                    if ask_contract_history:
                        cl = (doc.get("contract_history") or {}).get("list")
                        if isinstance(cl, list):
                            details.append(f"Contracts {len(cl)}")
                    if ask_extracted_at and doc.get("extracted_at") not in (None, ""):
                        details.append(f"Extracted at {doc.get('extracted_at')}")
                    if not details:
                        # No specific asks or missing fields: keep a small default.
                        hr = doc.get("hireRate")
                        pts = _extract_passage_types(doc)
                        if hr is not None:
                            try:
                                details.append(f"Hire rate ${float(hr):,.2f}")
                            except Exception:
                                details.append(f"Hire rate {hr}")
                        if pts:
                            details.append(f"Passage types {', '.join(pts)}")
                    answer_lines.append(prefix + (" - " + "; ".join(details) if details else ""))
                if len(records) > max_render_rows:
                    answer_lines.append(
                        f"- ... {len(records) - max_render_rows} additional matched records omitted for brevity."
                    )
                answer = "\n".join(answer_lines)

            current_trace = []
            if isinstance(state.get("artifacts"), dict) and isinstance((state.get("artifacts") or {}).get("trace"), list):
                current_trace = (state.get("artifacts") or {}).get("trace") or []

            merged = {
                "finance": {"mode": None, "rows": []},
                "ops": {"mode": None, "rows": []},
                "mongo": mongo_safe if isinstance(mongo_safe, dict) else {},
                "artifacts": {"intent_key": intent_key, "slots": slots, "trace": current_trace},
                "dynamic_sql_used": False,
                "dynamic_sql_agents": [],
            }

            if trace_queries:
                self._trace_single_mongo_query(
                    state,
                    intent_key=intent_key,
                    operation="metadata_lookup",
                    collection=str(trace_queries[0].get("collection") or "vessels"),
                    filt=trace_queries[0].get("filter") if isinstance(trace_queries[0].get("filter"), dict) else {},
                    projection=trace_queries[0].get("projection") if isinstance(trace_queries[0].get("projection"), dict) else projection,
                    limit=trace_queries[0].get("limit") if isinstance(trace_queries[0].get("limit"), int) else None,
                    rows=len(records),
                    mongo_queries=trace_queries,
                    summary="Mongo: resolved vessel metadata from voyage/vessel anchors and fetched the requested vessel fields.",
                )

            state["merged"] = merged
            state["answer"] = answer
            state["slots"] = slots
            state["mongo"] = mongo_safe
            state["finance"] = {"mode": None, "rows": []}
            state["ops"] = {"mode": None, "rows": []}
            state["data"]["mongo"] = mongo_safe
            state["data"]["finance"] = {"mode": None, "rows": []}
            state["data"]["ops"] = {"mode": None, "rows": []}
            state["data"]["artifacts"] = {"intent_key": intent_key, "slots": slots, "trace": current_trace}
            persisted_slots = self._build_persisted_slots(base=(session_context.get("slots") or {}), updates=slots)
            meta_focus = records[0] if len(records) == 1 and isinstance(records[0], dict) else None
            focus_slots = {}
            if isinstance(meta_focus, dict):
                focus_slots = self._row_focus_slots(
                    {
                        "voyage_number": meta_focus.get("voyage_number"),
                        "vessel_name": meta_focus.get("vessel_name"),
                        "imo": meta_focus.get("imo"),
                    }
                )
            latest_result_rows: list[Dict[str, Any]] = []
            for rec in records[:200]:
                if not isinstance(rec, dict):
                    continue
                doc = rec.get("doc") if isinstance(rec.get("doc"), dict) else {}
                item = {
                    "voyage_number": rec.get("voyage_number"),
                    "vessel_name": rec.get("vessel_name"),
                    "imo": rec.get("imo"),
                    "account_code": doc.get("accountCode"),
                    "market_type": doc.get("marketType"),
                    "operating": (
                        bool(doc.get("isVesselOperating"))
                        if doc.get("isVesselOperating") is not None
                        else None
                    ),
                    "passage_types": _extract_passage_types(doc),
                }
                item = {k: v for k, v in item.items() if v not in (None, "", [], {})}
                if item:
                    latest_result_rows.append(item)
            latest_result_set = {
                "source_intent": intent_key,
                "rows": latest_result_rows,
                "meta": {
                    "source_intent": intent_key,
                    "available_metrics": [],
                    "primary_metric": None,
                },
            }
            self.redis.save_session(
                state["session_id"],
                {
                    **session_context,
                    "last_intent": intent_key,
                    "last_intent_key": intent_key,
                    "memory_slots": self._extract_memory_slots(persisted_slots),
                    "param_slots": self._extract_param_slots(persisted_slots),
                    "slots": persisted_slots,
                    "last_user_input": user_input,
                    "last_result_set": latest_result_set,
                    "last_focus_slots": focus_slots,
                },
            )
            state["session_ctx"] = {
                **(session_context or {}),
                "last_intent": intent_key,
                "last_intent_key": intent_key,
                "memory_slots": self._extract_memory_slots(persisted_slots),
                "param_slots": self._extract_param_slots(persisted_slots),
                "slots": persisted_slots,
                "last_user_input": user_input,
                "last_result_set": latest_result_set,
                "last_focus_slots": focus_slots,
            }
            return state

        # =========================================================
        # Voyage metadata: Mongo-only deterministic summary
        # =========================================================
        if intent_key == "voyage.metadata":
            projection = cfg.get("mongo_projection") or get_mongo_projection("voyage_metadata_detail")
            records: list[Dict[str, Any]] = []
            mongo_rows: list[Dict[str, Any]] = []
            trace_queries: list[Dict[str, Any]] = []

            # Primary anchors: voyage_id / voyage_number(s)
            if slots.get("voyage_id") not in (None, ""):
                voyage_id = str(slots.get("voyage_id"))
                trace_queries.append(
                    {
                        "collection": "voyages",
                        "filter": {"voyageId": voyage_id},
                        "projection": self._mongo_projection_for_trace(projection),
                        "sort": None,
                        "limit": 1,
                        "pipeline": None,
                    }
                )
                try:
                    doc = self.mongo_agent.adapter.fetch_voyage(voyage_id, projection=projection)
                except Exception:
                    doc = None
                if isinstance(doc, dict) and doc:
                    records.append(doc)
                    mongo_rows.append(doc)

            vnums = slots.get("voyage_numbers")
            if not isinstance(vnums, list):
                vn = slots.get("voyage_number")
                vnums = [vn] if vn not in (None, "", [], {}) else []
            unique_vnums: list[int] = []
            for v in vnums:
                try:
                    iv = int(v)
                    if iv not in unique_vnums:
                        unique_vnums.append(iv)
                except Exception:
                    continue
            unique_vnums = unique_vnums[:3]
            if unique_vnums:
                trace_queries.append(
                    {
                        "collection": "voyages",
                        "filter": {"voyageNumber": {"$in": [str(v) for v in unique_vnums]}},
                        "projection": self._mongo_projection_for_trace(projection),
                        "sort": None,
                        "limit": len(unique_vnums),
                        "pipeline": None,
                    }
                )
            for vnum in unique_vnums:
                try:
                    batch = self.mongo_agent.adapter.list_voyages_by_number(
                        vnum,
                        projection=projection,
                        limit=get_mongo_limit("voyage_metadata_detail_batch", 40),
                    )
                except Exception:
                    batch = []
                narrowed = narrow_voyage_rows_by_entity_slots(batch, slots)
                for doc in narrowed:
                    if isinstance(doc, dict) and doc:
                        records.append(doc)
                        mongo_rows.append(doc)

            # De-duplicate by voyageId if present.
            seen_vid: set[str] = set()
            deduped: list[Dict[str, Any]] = []
            for r in records:
                vid = str(r.get("voyageId") or "")
                if vid and vid in seen_vid:
                    continue
                if vid:
                    seen_vid.add(vid)
                deduped.append(r)
            records = deduped

            if not records:
                answer = _router_fallback("voyage_metadata_empty")
            else:
                if len(records) == 1:
                    sections = select_voyage_sections(user_input, records[0], session_context)
                else:
                    sections = {
                        "voyages": [select_voyage_sections(user_input, d, session_context) for d in records]
                    }

                context_json = json.dumps(sections, default=str, indent=2)
                system_prompt = get_graph_router_voyage_metadata_answer_system_prompt()
                user_prompt = f"""Voyage Data:
{context_json}

Question: {user_input}"""
                try:
                    answer = self.llm._call_with_retry(
                        system=system_prompt,
                        user=user_prompt,
                        operation="voyage_metadata_answer",
                        return_string=True,
                    )
                except Exception:
                    answer = ""
                if not answer:
                    answer = _router_fallback("voyage_metadata_formatting_failed")

            mongo_safe = {"mode": "mongo_metadata", "ok": True, "rows": mongo_rows}
            current_trace = []
            if isinstance(state.get("artifacts"), dict) and isinstance((state.get("artifacts") or {}).get("trace"), list):
                current_trace = (state.get("artifacts") or {}).get("trace") or []

            merged = {
                "finance": {"mode": None, "rows": []},
                "ops": {"mode": None, "rows": []},
                "mongo": mongo_safe,
                "artifacts": {"intent_key": intent_key, "slots": slots, "trace": current_trace},
                "dynamic_sql_used": False,
                "dynamic_sql_agents": [],
            }
            if trace_queries:
                self._trace_single_mongo_query(
                    state,
                    intent_key=intent_key,
                    operation="voyage_metadata_lookup",
                    collection=str(trace_queries[0].get("collection") or "voyages"),
                    filt=trace_queries[0].get("filter") if isinstance(trace_queries[0].get("filter"), dict) else {},
                    projection=trace_queries[0].get("projection") if isinstance(trace_queries[0].get("projection"), dict) else projection,
                    limit=trace_queries[0].get("limit") if isinstance(trace_queries[0].get("limit"), int) else None,
                    rows=len(records),
                    mongo_queries=trace_queries,
                    summary="Mongo: fetched voyage metadata from the `voyages` collection using the resolved voyage anchors.",
                )
            state["merged"] = merged
            state["answer"] = answer
            state["slots"] = slots
            state["mongo"] = mongo_safe
            state["finance"] = {"mode": None, "rows": []}
            state["ops"] = {"mode": None, "rows": []}
            state["data"]["mongo"] = mongo_safe
            state["data"]["finance"] = {"mode": None, "rows": []}
            state["data"]["ops"] = {"mode": None, "rows": []}
            state["data"]["artifacts"] = {"intent_key": intent_key, "slots": slots, "trace": current_trace}
            persisted_slots = self._build_persisted_slots(base=(session_context.get("slots") or {}), updates=slots)
            focus_slots = {}
            if len(records) == 1 and isinstance(records[0], dict):
                focus_slots = self._row_focus_slots(
                    {
                        "voyage_number": records[0].get("voyageNumber"),
                        "voyage_id": records[0].get("voyageId"),
                        "vessel_name": records[0].get("vesselName"),
                    }
                )
            self.redis.save_session(
                state["session_id"],
                {
                    **session_context,
                    "last_intent": intent_key,
                    "last_intent_key": intent_key,
                    "memory_slots": self._extract_memory_slots(persisted_slots),
                    "param_slots": self._extract_param_slots(persisted_slots),
                    "slots": persisted_slots,
                    "last_user_input": user_input,
                    "last_focus_slots": focus_slots,
                },
            )
            state["session_ctx"] = {
                **(session_context or {}),
                "last_intent": intent_key,
                "last_intent_key": intent_key,
                "memory_slots": self._extract_memory_slots(persisted_slots),
                "param_slots": self._extract_param_slots(persisted_slots),
                "slots": persisted_slots,
                "last_user_input": user_input,
                "last_focus_slots": focus_slots,
            }
            return state

        # ---- Standard Execution for generic single intents ----

        # Mongo
        if cfg.get("needs", {}).get("mongo"):
            try:
                res = self.mongo_agent.run(
                    intent_key=cfg.get("mongo_intent", "entity.auto"),
                    slots=slots,
                    projection=cfg.get("mongo_projection"),
                    session_context=session_context,
                )
                state["mongo"] = _json_safe(res)

                # Update session with anchor
                if isinstance(state["mongo"], dict):
                    anchor_type = state["mongo"].get("anchor_type")
                    anchor_id = state["mongo"].get("anchor_id")
                    
                    if anchor_type and anchor_id:
                        persisted_slots = self._build_persisted_slots(base=(session_context.get("slots") or {}), updates=slots)
                        self.redis.save_session(
                            state["session_id"],
                            {
                                **session_context,
                                "anchor_type": anchor_type,
                                "anchor_id": anchor_id,
                                "memory_slots": self._extract_memory_slots(persisted_slots),
                                "param_slots": self._extract_param_slots(persisted_slots),
                                "slots": persisted_slots,
                                "last_user_input": state["user_input"],
                            },
                        )
                        state["session_ctx"] = self.redis.load_session(state["session_id"])

                    # Extract voyage_id or imo from mongo result
                    doc = (state["mongo"] or {}).get("document") or {}
                    if "voyageId" in doc and "voyage_id" not in slots:
                        slots["voyage_id"] = doc["voyageId"]
                    if "imo" in doc and "imo" not in slots:
                        slots["imo"] = doc["imo"]
            except Exception as e:
                _dprint(f"⚠️  Mongo failed: {e}")
                state["mongo"] = None

        # Finance
        if cfg.get("needs", {}).get("finance"):
            self._trace(
                state,
                {
                    "phase": "composite_step_start",
                    "step_index": 1,
                    "step_count": 2 if cfg.get("needs", {}).get("ops") else 1,
                    "agent": "finance",
                    "operation": "registry_sql",
                    "inputs": {"intent_key": intent_key},
                    "goal": "Run finance registry SQL in single plan mode.",
                },
            )
            try:
                res = self.finance_agent.run(
                    intent_key=intent_key,
                    slots=slots,
                    session_context={**session_context, "user_input": state.get("user_input") or ""},
                )
                state["finance"] = _json_safe(res)
            except Exception as e:
                _dprint(f"⚠️  Finance failed: {e}")
                state["finance"] = {
                    "mode": "error",
                    "rows": [],
                    "fallback_reason": f"Finance agent error: {str(e)}"
                }
            try:
                fin_safe = _json_safe(state.get("finance"))
                fin_rows = fin_safe.get("rows") if isinstance(fin_safe, dict) else []
                fin_rows = fin_rows if isinstance(fin_rows, list) else []
                fin_sql = _registry_sql_from_result(fin_safe)
                self._trace(
                    state,
                    {
                        "phase": "composite_step_result",
                        "step_index": 1,
                        "agent": "finance",
                        "operation": "registry_sql",
                        "ok": True,
                        "mode": fin_safe.get("mode") if isinstance(fin_safe, dict) else None,
                        "rows": len(fin_rows),
                        "sql_present": bool(fin_sql),
                        "sql": fin_sql,
                        "summary": f"Finance(single): fetched {len(fin_rows)} rows using registry SQL.",
                    },
                )
            except Exception:
                pass

        # Ops
        if cfg.get("needs", {}).get("ops"):
            self._trace(
                state,
                {
                    "phase": "composite_step_start",
                    "step_index": 2 if cfg.get("needs", {}).get("finance") else 1,
                    "step_count": 2 if cfg.get("needs", {}).get("finance") else 1,
                    "agent": "ops",
                    "operation": "registry_sql",
                    "inputs": {"intent_key": intent_key},
                    "goal": "Run ops registry SQL in single plan mode.",
                },
            )
            try:
                res = self.ops_agent.run(
                    intent_key=intent_key,
                    slots=slots,
                    session_context={**session_context, "user_input": state.get("user_input") or ""},
                )
                state["ops"] = _json_safe(res)
            except Exception as e:
                _dprint(f"⚠️  Ops failed: {e}")
                state["ops"] = {
                    "mode": "error",
                    "rows": [],
                    "fallback_reason": f"Ops agent error: {str(e)}"
                }
            try:
                ops_safe = _json_safe(state.get("ops"))
                ops_rows = ops_safe.get("rows") if isinstance(ops_safe, dict) else []
                ops_rows = ops_rows if isinstance(ops_rows, list) else []
                ops_sql = _registry_sql_from_result(ops_safe)
                self._trace(
                    state,
                    {
                        "phase": "composite_step_result",
                        "step_index": 2 if cfg.get("needs", {}).get("finance") else 1,
                        "agent": "ops",
                        "operation": "registry_sql",
                        "ok": True,
                        "mode": ops_safe.get("mode") if isinstance(ops_safe, dict) else None,
                        "rows": len(ops_rows),
                        "sql_present": bool(ops_sql),
                        "sql": ops_sql,
                        "summary": f"Ops(single): fetched {len(ops_rows)} rows using registry SQL.",
                    },
                )
            except Exception:
                pass

        # =========================================================
        # Optional Dynamic NoSQL enrichment for ops port queries
        # (adds remarks/grades/ports from Mongo when asked)
        # =========================================================
        if intent_key in ("ops.port_query", "ops.voyages_by_port"):
            ui_l = (user_input or "").lower()
            wants_mongo = any(k in ui_l for k in ("remark", "remarks", "grade", "grades", "cargo"))
            if wants_mongo and hasattr(self.mongo_agent, "run_llm_find"):
                try:
                    voyage_ids: list[str] = []
                    fin = state.get("finance")
                    ops = state.get("ops")
                    if isinstance(fin, dict):
                        for r in (fin.get("rows") or [])[:20]:
                            if isinstance(r, dict) and r.get("voyage_id"):
                                voyage_ids.append(str(r["voyage_id"]))
                    if not voyage_ids and isinstance(ops, dict):
                        for r in (ops.get("rows") or [])[:20]:
                            if isinstance(r, dict) and r.get("voyage_id"):
                                voyage_ids.append(str(r["voyage_id"]))
                    voyage_ids = list(dict.fromkeys([v for v in voyage_ids if v]))

                    if voyage_ids:
                        q = (
                            "Fetch remarks + minimal context for these voyage_ids.\n"
                            "Use collection=voyages.\n"
                            "Filter MUST be: {\"voyageId\": {\"$in\": slots.voyage_ids}}.\n"
                            "Projection MUST include: {\"_id\": 0, \"voyageId\": 1, \"voyageNumber\": 1, \"remarks\": 1, "
                            "\"fixtures.grades\": 1, \"fixtures.fixtureGrades.gradeName\": 1, "
                            "\"fixtures.fixturePorts.portName\": 1, \"fixtures.fixturePorts.activityType\": 1}.\n"
                            "Return only the minimal required fields."
                        )
                        mongo_llm = self.mongo_agent.run_llm_find(question=q, slots={"voyage_ids": voyage_ids})
                        state["mongo"] = _json_safe(mongo_llm)
                        if "data" in state and isinstance(state["data"], dict):
                            state["data"]["mongo"] = state["mongo"]
                    elif slots.get("port_name"):
                        # Fallback: query by port name directly if we have no IDs.
                        q = (
                            "Find voyages that called at the given port name (case-insensitive) and return minimal context.\n"
                            "Use collection=voyages.\n"
                            "Filter should use fixtures.fixturePorts.portName with $regex and $options:'i' using slots.port_name.\n"
                            "Projection MUST include: {\"_id\": 0, \"voyageId\": 1, \"voyageNumber\": 1, \"remarks\": 1, "
                            "\"fixtures.grades\": 1, \"fixtures.fixtureGrades.gradeName\": 1, "
                            "\"fixtures.fixturePorts.portName\": 1, \"fixtures.fixturePorts.activityType\": 1}.\n"
                            "Limit <= 50."
                        )
                        mongo_llm = self.mongo_agent.run_llm_find(question=q, slots={"port_name": slots.get("port_name")})
                        state["mongo"] = _json_safe(mongo_llm)
                        if "data" in state and isinstance(state["data"], dict):
                            state["data"]["mongo"] = state["mongo"]
                except Exception as e:
                    _dprint(f"⚠️  Mongo enrichment failed: {e}")

        state["slots"] = slots

        # =========================================================
        # Registry-driven zero-row escalation.
        # If single path returned no rows and no entity anchor exists
        # in slots, the query was fleet-wide but mis-classified as single.
        # Rebuild as composite and signal r_after_run_single to re-route.
        # No hardcoded intent names — driven entirely by registry route.
        # =========================================================
        _has_entity_anchor = bool(
            slots.get("voyage_number") or slots.get("voyage_numbers")
            or slots.get("voyage_id") or slots.get("vessel_name") or slots.get("imo")
        )
        _fin_rows: list = []
        _ops_rows: list = []
        try:
            _f = state.get("finance") or {}
            if isinstance(_f, dict):
                _fin_rows = _f.get("rows") or []
        except Exception:
            pass
        try:
            _o = state.get("ops") or {}
            if isinstance(_o, dict):
                _ops_rows = _o.get("rows") or []
        except Exception:
            pass

        _no_results = (not _fin_rows) and (not _ops_rows)
        _intent_cfg_sr = INTENT_REGISTRY.get(intent_key, {})

        if _no_results and not _has_entity_anchor and _intent_cfg_sr.get("route") == "single":
            _dprint(f"   ⚡ ZERO-ROW ESCALATION: {intent_key} single → composite retry")
            _escalated = self.planner.build_plan(
                text=user_input,
                session_context=session_context,
                intent_key=intent_key,
                slots=slots,
                force_composite=True,
            )
            state["plan"] = {
                "plan_type": _escalated.plan_type,
                "intent_key": _escalated.intent_key,
                "required_slots": _escalated.required_slots or [],
                "confidence": _escalated.confidence,
                "steps": [asdict(s) for s in (_escalated.steps or [])],
            }
            state["plan_type"] = "composite"
            state["intent_key"] = _escalated.intent_key
            state["step_index"] = 0
            if not isinstance(state.get("data"), dict):
                state["data"] = {
                    "finance": {"mode": None, "rows": []},
                    "ops": {"mode": None, "rows": []},
                    "mongo": {},
                    "artifacts": {},
                }
            self._trace(
                state,
                {
                    "phase": "zero_row_escalation",
                    "original_intent": intent_key,
                    "escalated_intent": _escalated.intent_key,
                    "reason": "single path returned zero rows with no entity anchor",
                },
            )

        return state

    # Composite Step Execution
    # =========================================================

    def n_execute_step(self, state: GraphState) -> GraphState:
        """
        Execute one composite step. Caps finance/ops rows and voyage_ids, deduplicates, scenario comparison handling.
        """

        # LangGraph may populate declared state keys with None. Ensure data is always a dict.
        if not isinstance(state.get("data"), dict):
            state["data"] = {
                "finance": {"mode": None, "rows": []},
                "ops": {"mode": None, "rows": []},
                "mongo": {},
                "artifacts": {},
            }

        plan = state.get("plan") or {}
        steps = plan.get("steps") or []
        idx = int(state.get("step_index") or 0)

        artifacts = state.get("artifacts") or {}
        # Merge state slots so port_name etc. are always available for finance/ops
        slots = {**(state.get("slots") or {}), **(artifacts.get("slots") or {})}
        sess = state.get("session_ctx") or {}

        # Composite-step slot hygiene: keep only registry-declared slots plus a
        # small set of derived execution keys. This prevents stale entity slots
        # from leaking into dynamic SQL generation across steps.
        current_intent = state.get("intent_key") or (state.get("plan") or {}).get("intent_key") or ""
        current_cfg = INTENT_REGISTRY.get(current_intent, {})
        if current_cfg.get("route") == "composite":
            _allowed_step_slots = set(
                (current_cfg.get("required_slots") or [])
                + (current_cfg.get("optional_slots") or [])
            )
            _allowed_step_slots.update({
                "scenario", "limit",
                "voyage_ids", "cargo_grades", "vessel_imos",
                "voyage_number", "voyage_numbers", "voyage_id",
            })
            slots = {k: v for k, v in (slots or {}).items() if k in _allowed_step_slots}

        if idx >= len(steps):
            return state

        step = steps[idx]
        agent = (step.get("agent") or "").lower()
        op = step.get("operation") or ""

        op = re.sub(r'([a-z])([A-Z])', r'\1_\2', op).lower()

        _dprint(f"\nComposite step {idx + 1}/{len(steps)}: {agent}.{op}")

        self._trace(
            state,
            {
                "phase": "composite_step_start",
                "step_index": idx + 1,
                "step_count": len(steps),
                "agent": agent,
                "operation": op,
                "inputs": step.get("inputs") or {},
                "goal": self._step_goal_text(
                    intent_key=str(state.get("intent_key") or ""),
                    agent=agent,
                    op=op,
                    step_inputs=step.get("inputs") or {},
                    slots=slots,
                ),
            },
        )

        # Ensure expected sections exist (avoid None/KeyError in error paths).
        data = state.get("data")
        if isinstance(data, dict):
            data.setdefault("finance", {"mode": None, "rows": []})
            data.setdefault("ops", {"mode": None, "rows": []})
            data.setdefault("mongo", {})
            data.setdefault("artifacts", {})
            state["data"] = data

        # =========================================================
        # FINANCE STEP
        # =========================================================
        if agent == "finance" and op in ("dynamic_sql", "registry_sql"):
            # Merge step inputs (e.g. limit from planner) into slots
            step_inputs = step.get("inputs") or {}
            for k, v in step_inputs.items():
                if k not in ("question", "intent_key") and v is not None:
                    slots = {**slots, k: v}
            # Intent from state or plan (plan is authoritative after build_plan)
            intent_key = state.get("intent_key") or (state.get("plan") or {}).get("intent_key") or "composite.query"
            user_q_lower = str(state.get("user_input") or "").strip().lower()
            cargo_frequency_ops_only = (
                intent_key == "ranking.cargo"
                and any(
                    phrase in user_q_lower
                    for phrase in (
                        "appears most frequently across all voyages",
                        "appears most frequently",
                        "most frequent cargo grade",
                        "most common cargo grade",
                        "most commonly carried cargo grade",
                    )
                )
                and not any(
                    phrase in user_q_lower
                    for phrase in (
                        "profitable",
                        "profitability",
                        "average pnl",
                        "avg pnl",
                        "revenue",
                        "margin",
                        "variance",
                        "when fixed",
                        "when-fixed",
                    )
                )
            )
            if cargo_frequency_ops_only and op == "dynamic_sql":
                safe = {
                    "mode": "skipped",
                    "rows": [],
                    "skipped_reason": "cargo_frequency_ops_only",
                }
                artifacts["finance_rows"] = []
                artifacts["voyage_ids"] = []
                state["finance"] = safe
                state["data"]["finance"] = safe
                self._trace(
                    state,
                    {
                        "phase": "composite_step_result",
                        "step_index": idx + 1,
                        "agent": agent,
                        "operation": op,
                        "ok": True,
                        "skipped": True,
                        "mode": "skipped",
                        "rows": 0,
                        "voyage_ids": 0,
                        "reason": (
                            "Cargo-frequency ranking is ops-only for this phrasing; "
                            "finance step skipped to avoid irrelevant SQL generation."
                        ),
                        "summary": "Finance step skipped - cargo frequency is sourced from ops cargo grades.",
                    },
                )
                state["artifacts"] = artifacts
                state["slots"] = slots
                state["step_index"] = idx + 1
                return state
            # Composite always uses dynamic SQL for finance; registry only when plan explicitly says registry_sql (e.g. single-query path)
            use_registry = op == "registry_sql"
            try:
                if (
                    intent_key == "ranking.vessels"
                    and any(
                        phrase in user_q_lower
                        for phrase in (
                            "above-average pnl",
                            "above average pnl",
                        )
                    )
                    and any(
                        phrase in user_q_lower
                        for phrase in (
                            "most common cargo grade",
                            "most common cargo grades",
                            "common cargo grade",
                            "common cargo grades",
                        )
                    )
                ):
                    sql = """
                        SELECT
                          REPLACE(f.vessel_imo::TEXT, '.0', '') AS vessel_imo,
                          MAX(o.vessel_name)                   AS vessel_name,
                          COUNT(DISTINCT f.voyage_id)          AS voyage_count,
                          AVG(f.pnl)                           AS avg_pnl
                        FROM finance_voyage_kpi f
                        JOIN ops_voyage_summary o
                          ON f.voyage_number = o.voyage_number
                          AND REPLACE(f.vessel_imo::TEXT, '.0', '') = REPLACE(o.vessel_imo::TEXT, '.0', '')
                        WHERE f.scenario = COALESCE(%(scenario)s, 'ACTUAL')
                        GROUP BY REPLACE(f.vessel_imo::TEXT, '.0', '')
                        HAVING AVG(f.pnl) > (
                          SELECT AVG(pnl)
                          FROM finance_voyage_kpi
                          WHERE scenario = COALESCE(%(scenario)s, 'ACTUAL')
                        )
                        ORDER BY voyage_count DESC, avg_pnl DESC NULLS LAST
                        LIMIT %(limit)s
                    """
                    params = {
                        "scenario": slots.get("scenario") or "ACTUAL",
                        "limit": min(int(slots.get("limit") or 10), 50),
                    }
                    rows = self.finance_agent.pg.execute_dynamic_select(sql, params)
                    result = {
                        "mode": "dynamic_sql",
                        "intent_key": intent_key,
                        "query_key": "narrow.ranking_vessels_above_avg_pnl",
                        "params": params,
                        "rows": rows,
                        "sql": sql,
                    }
                elif use_registry:
                    result = self.finance_agent.run(
                        intent_key=intent_key,
                        slots=slots,
                        session_context=sess,
                    )
                else:
                    finance_question = state["user_input"]
                    if intent_key == "ranking.vessels" and any(
                        phrase in user_q_lower
                        for phrase in (
                            "most common cargo grade",
                            "most common cargo grades",
                            "common cargo grade",
                            "common cargo grades",
                        )
                    ):
                        finance_question = re.sub(
                            r"(?i)\s*(?:,?\s*and\s+)?most common cargo grades?\b",
                            "",
                            str(finance_question or ""),
                        ).strip(" ,.")
                        if "above-average pnl" in user_q_lower or "above average pnl" in user_q_lower:
                            finance_question = (
                                finance_question.rstrip(" ?.") + "?\n\n"
                                "IMPORTANT: interpret 'above-average PnL' at the vessel aggregate level, "
                                "not at the individual voyage row level. "
                                "Use HAVING AVG(f.pnl) > (SELECT AVG(pnl) FROM finance_voyage_kpi WHERE scenario = COALESCE(%(scenario)s, 'ACTUAL')). "
                                "Do NOT filter voyages with WHERE f.pnl > average. "
                                "Do NOT join or reference cargo grades in the finance SQL."
                            )
                        if finance_question and not finance_question.endswith("?"):
                            finance_question = finance_question + "?"
                    result = self.finance_agent.run_dynamic(
                        question=finance_question,
                        intent_key=intent_key,
                        slots=slots,
                        session_context=sess,
                    )

                safe = _json_safe(result)
                rows = safe.get("rows", []) or []

                # Cap finance rows
                rows = rows[:20]

                safe["rows"] = rows

                state["finance"] = safe
                state["data"]["finance"] = safe

                # Extract voyage_ids safely
                voyage_ids = [
                    r.get("voyage_id")
                    for r in rows
                    if isinstance(r, dict) and r.get("voyage_id")
                ]

                # Fallback for single-voyage composite queries:
                # If finance SQL returned KPI rows without voyage_id (common LLM omission),
                # reuse canonical voyage_id resolved by mongo.resolve_anchor so downstream
                # ops/mongo merge can still produce a coherent single-voyage snapshot.
                if not voyage_ids and rows:
                    include_ids = artifacts.get("include_voyage_ids") or []
                    if isinstance(include_ids, list):
                        include_ids = [x for x in include_ids if x not in (None, "", [], {})]
                    else:
                        include_ids = []
                    if not include_ids and slots.get("voyage_id") not in (None, "", [], {}):
                        include_ids = [slots.get("voyage_id")]
                    if len(include_ids) == 1:
                        canonical_vid = include_ids[0]
                        for r in rows:
                            if isinstance(r, dict) and not r.get("voyage_id"):
                                r["voyage_id"] = canonical_vid
                                if slots.get("voyage_number") and not r.get("voyage_number"):
                                    r["voyage_number"] = slots.get("voyage_number")
                        voyage_ids = [canonical_vid]

                if voyage_ids:
                    # Deduplicate + hard cap
                    unique_ids = list(dict.fromkeys(voyage_ids))

                    # If mongo.resolve_anchor found a specific voyage_id to include,
                    # ensure it is present in the list before downstream ops/mongo steps.
                    include_ids = artifacts.get("include_voyage_ids") or []
                    if isinstance(include_ids, list) and include_ids:
                        unique_ids = list(dict.fromkeys([*include_ids, *unique_ids]))

                    unique_ids = unique_ids[:20]

                    artifacts["voyage_ids"] = unique_ids
                    artifacts["finance_rows"] = rows
                    slots["voyage_ids"] = unique_ids

                    _dprint(f"   ✅ Extracted {len(unique_ids)} voyage_ids (capped)")
                    self._trace(
                        state,
                        {
                            "phase": "composite_step_result",
                            "step_index": idx + 1,
                            "agent": agent,
                            "operation": op,
                            "ok": True,
                            "mode": safe.get("mode"),
                            "rows": len(rows),
                            "voyage_ids": len(unique_ids),
                            "extracted_voyage_ids": unique_ids,
                            "sql_present": bool(safe.get("sql")),
                            "sql": safe.get("sql"),
                            "summary": f"Finance: fetched {len(rows)} rows and extracted {len(unique_ids)} voyage_ids (Postgres).",
                        },
                    )
                else:
                    # No voyage_ids: vessel-level or aggregate (e.g. ranking.vessels). Still store finance_rows for merge.
                    artifacts["finance_rows"] = rows
                    artifacts["voyage_ids"] = []
                    # Some finance aggregate intents return no voyage_ids (e.g., cargo profitability by grade).
                    # For those, carry forward key dimension values so ops can provide context.
                    if state.get("intent_key") in ("analysis.cargo_profitability", "analysis.cargoprofitability"):
                        cargo_grades = []
                        seen_cg = set()
                        for r in rows:
                            if not isinstance(r, dict):
                                continue
                            g = r.get("cargo_grade") or r.get("grade")
                            if not isinstance(g, str):
                                continue
                            s = g.strip()
                            sn = s.lower()
                            if (not s) or sn in ("none", "null", "n/a", "na"):
                                continue
                            if sn in seen_cg:
                                continue
                            seen_cg.add(sn)
                            cargo_grades.append(s)
                        cargo_grades = cargo_grades[:50]
                        if cargo_grades:
                            artifacts["cargo_grades"] = cargo_grades
                            slots["cargo_grades"] = cargo_grades
                    if state.get("intent_key") == "ranking.vessels" and rows:
                        vessel_imos = []
                        for r in rows:
                            if not isinstance(r, dict):
                                continue
                            imo = r.get("vessel_imo")
                            if imo is not None:
                                vessel_imos.append(imo)
                        if vessel_imos:
                            artifacts["vessel_imos"] = vessel_imos[:50]

                    self._trace(
                        state,
                        {
                            "phase": "composite_step_result",
                            "step_index": idx + 1,
                            "agent": agent,
                            "operation": op,
                            "ok": True,
                            "mode": safe.get("mode"),
                            "rows": len(rows),
                            "voyage_ids": 0,
                            "sql_present": bool(safe.get("sql")),
                            "sql": safe.get("sql"),
                            "summary": f"Finance: fetched {len(rows)} rows (Postgres). No voyage_ids extracted.",
                        },
                    )

            except Exception as e:
                err_str = str(e)
                # Retry with registry when dynamic SQL fails due to missing column (e.g. bunker_cost)
                if "bunker_cost" in err_str and (intent_key == "analysis.high_revenue_low_pnl" or (state.get("plan") or {}).get("intent_key") == "analysis.high_revenue_low_pnl"):
                    try:
                        result = self.finance_agent.run(
                            intent_key=intent_key,
                            slots=slots,
                            session_context=sess,
                        )
                        safe = _json_safe(result)
                        rows = safe.get("rows", []) or []
                        rows = rows[:20]
                        safe["rows"] = rows
                        state["finance"] = safe
                        state["data"]["finance"] = safe
                        voyage_ids = [r.get("voyage_id") for r in rows if isinstance(r, dict) and r.get("voyage_id")]
                        voyage_ids = list(dict.fromkeys(voyage_ids))[:20]
                        if voyage_ids:
                            artifacts["voyage_ids"] = voyage_ids
                            artifacts["finance_rows"] = rows
                            slots["voyage_ids"] = voyage_ids
                        self._trace(state, {"phase": "composite_step_result", "step_index": idx + 1, "agent": agent, "operation": op, "ok": True, "mode": safe.get("mode"), "rows": len(rows), "voyage_ids": len(voyage_ids), "extracted_voyage_ids": voyage_ids, "sql_present": bool(safe.get("sql")), "sql": safe.get("sql"), "summary": "Finance: retried with registry SQL (Postgres)."})
                    except Exception as e2:
                        _dprint(f"   ❌ finance (registry retry) failed: {e2}")
                        state["data"]["finance"] = {"mode": "error", "rows": []}
                        self._trace(state, {"phase": "composite_step_result", "step_index": idx + 1, "agent": agent, "operation": op, "ok": False, "error": err_str})
                else:
                    _dprint(f"   ❌ finance.dynamic_sql failed: {e}")
                    state["data"]["finance"] = {"mode": "error", "rows": []}
                    self._trace(
                        state,
                        {
                            "phase": "composite_step_result",
                            "step_index": idx + 1,
                            "agent": agent,
                            "operation": op,
                            "ok": False,
                            "error": err_str,
                        },
                    )

        # =========================================================
        # OPS STEP
        # =========================================================
        elif agent == "ops" and op == "voyage_ids_from_step":
            step_inputs = step.get("inputs") or {}

            # Read voyage_ids from artifacts set by the previous mongo step
            voyage_ids = artifacts.get("voyage_ids") or []

            if not voyage_ids:
                self._trace(state, {
                    "phase": "composite_step_result",
                    "step_index": idx + 1,
                    "agent": "ops",
                    "operation": op,
                    "ok": False,
                    "error": "No voyages found for the specified cargo grade.",
                })
                state["data"]["ops"] = {"mode": "dynamic_sql", "rows": []}

            else:
                enriched_slots = {
                    **slots,
                    "voyage_ids": voyage_ids,
                    "limit": min(25, int(slots.get("limit") or 25)),
                }
                result = self.ops_agent.run_dynamic(
                    question=state["user_input"],
                    intent_key=(
                        state.get("intent_key")
                        or step_inputs.get("intent_key")
                        or "composite.query"
                    ),
                    slots=enriched_slots,
                    session_context=sess,
                )
                safe = _json_safe(result)
                rows = (safe.get("rows") or [])[:25]
                safe["rows"] = rows

                artifacts["ops_rows"] = rows
                state["ops"] = safe
                state["data"]["ops"] = safe

                self._trace(state, {
                    "phase": "composite_step_result",
                    "step_index": idx + 1,
                    "agent": "ops",
                    "operation": op,
                    "ok": True,
                    "rows": len(rows),
                    "voyage_ids": len(voyage_ids),
                    "extracted_voyage_ids": voyage_ids,
                    "sql": safe.get("sql"),
                    "summary": (
                        f"ops voyage_ids_from_step: {len(rows)} rows for {len(voyage_ids)} voyage_ids"
                    ),
                })

        elif agent == "ops" and op == "dynamic_sql":
            # ── Aggregate rows without voyage_ids guard ──────────────────────
            _finance_rows = (state.get("finance") or {}).get("rows") or []
            _voyage_ids_available = bool(
                artifacts.get("voyage_ids") or
                (state.get("finance") or {}).get("voyage_ids")
            )
            _intent_key_ops = state.get("intent_key") or (state.get("plan") or {}).get("intent_key") or "composite.query"
            _user_q_lower = str(state.get("user_input") or "").strip().lower()
            _needs_vessel_grade_enrichment = (
                _intent_key_ops == "ranking.vessels"
                and any(
                    phrase in _user_q_lower
                    for phrase in (
                        "most common cargo grade",
                        "most common cargo grades",
                        "common cargo grade",
                        "common cargo grades",
                    )
                )
            )

            if (not _voyage_ids_available) and _finance_rows and not _needs_vessel_grade_enrichment:
                self._trace(state, {
                    "phase": "composite_step_result",
                    "step_index": idx + 1,
                    "agent": "ops",
                    "operation": op,
                    "ok": True,
                    "skipped": True,
                    "rows": len(_finance_rows),
                    "reason": (
                        "Finance returned aggregate rows (GROUP BY query). "
                        "No voyage_ids to pass — ops step not needed, finance result is self-contained."
                    ),
                    "summary": "Ops step skipped — finance aggregate query is self-contained.",
                })
                state["data"]["ops"] = {
                    "mode": "skipped",
                    "rows": _finance_rows,
                    "skipped_reason": "aggregate_no_voyage_ids",
                }
                artifacts["slots"] = slots
                state["artifacts"] = artifacts
                state["slots"] = slots
                state["step_index"] = idx + 1
                return state

            # Resolve step inputs (e.g. voyage_ids: "$finance.voyage_ids") so we never pass literal placeholders to the agent
            step_inputs = step.get("inputs") or {}
            for k, v in step_inputs.items():
                if k in ("question", "intent_key"):
                    continue
                if v == "$finance.voyage_ids" or (isinstance(v, str) and v.strip() == "$finance.voyage_ids"):
                    v = artifacts.get("voyage_ids") or []
                if v is not None:
                    slots = {**slots, k: v}

            voyage_ids = artifacts.get("voyage_ids") or []
            if not isinstance(voyage_ids, list):
                voyage_ids = []
            if voyage_ids:
                slots["voyage_ids"] = voyage_ids[:20]
            else:
                slots.pop("voyage_ids", None)

            intent_key_ops = state.get("intent_key") or (state.get("plan") or {}).get("intent_key") or "composite.query"
            if intent_key_ops in ("analysis.cargo_profitability", "analysis.cargoprofitability") and (artifacts.get("cargo_grades") or []):
                slots["cargo_grades"] = artifacts["cargo_grades"]
            if intent_key_ops == "ranking.vessels" and not voyage_ids:
                vessel_imos = artifacts.get("vessel_imos") or []
                if not vessel_imos and isinstance(_finance_rows, list):
                    vessel_imos = []
                    for r in _finance_rows:
                        if not isinstance(r, dict):
                            continue
                        imo = r.get("vessel_imo")
                        if imo in (None, "", [], {}):
                            continue
                        imo_s = str(imo)
                        if imo_s not in vessel_imos:
                            vessel_imos.append(imo_s)
                    if vessel_imos:
                        artifacts["vessel_imos"] = vessel_imos[:50]
                if vessel_imos:
                    slots["vessel_imos"] = vessel_imos[:50]

            # Trace resolved inputs (plan "inputs" are placeholders; these are what we actually pass to the agent)
            _resolved_vids = len(slots.get("voyage_ids") or []) if isinstance(slots.get("voyage_ids"), list) else 0
            _resolved_grades = len(slots.get("cargo_grades") or []) if isinstance(slots.get("cargo_grades"), list) else 0
            self._trace(
                state,
                {
                    "phase": "composite_step_inputs_resolved",
                    "step_index": idx + 1,
                    "agent": agent,
                    "resolved_voyage_ids_count": _resolved_vids,
                    "resolved_cargo_grades_count": _resolved_grades,
                    "intent_key_ops": intent_key_ops,
                },
            )

            try:
                result = self.ops_agent.run_dynamic(
                    question=state["user_input"],
                    intent_key=intent_key_ops,
                    slots=slots,
                    session_context=sess,
                )

                safe = _json_safe(result)
                rows = safe.get("rows", []) or []

                # Hard cap ops rows
                rows = rows[:20]
                safe["rows"] = rows

                state["ops"] = safe
                state["data"]["ops"] = safe

                artifacts["ops_rows"] = rows

                _dprint(f"   ✅ Got {len(rows)} ops rows (capped)")
                self._trace(
                    state,
                    {
                        "phase": "composite_step_result",
                        "step_index": idx + 1,
                        "agent": agent,
                        "operation": op,
                        "ok": True,
                        "mode": safe.get("mode"),
                        "rows": len(rows),
                        "voyage_ids": len(slots.get("voyage_ids") or []) if isinstance(slots.get("voyage_ids"), list) else None,
                        "extracted_voyage_ids": (slots.get("voyage_ids") or []) if isinstance(slots.get("voyage_ids"), list) else None,
                        "sql_present": bool(safe.get("sql")),
                        "sql": safe.get("sql"),
                        "summary": f"Ops: fetched {len(rows)} ops rows for voyage_ids (Postgres).",
                    },
                )

            except Exception as e:
                _dprint(f"   ❌ ops.dynamic_sql failed: {e}")
                state["data"]["ops"] = {"mode": "error", "rows": []}
                self._trace(
                    state,
                    {
                        "phase": "composite_step_result",
                        "step_index": idx + 1,
                        "agent": agent,
                        "operation": op,
                        "ok": False,
                        "error": str(e),
                    },
                )

        # =========================================================
        # MONGO STEP
        # =========================================================
        elif agent == "mongo" and op == "cargo_grade_lookup":
            step_inputs = step.get("inputs") or {}
            grades = step_inputs.get("cargo_grades") or slots.get("cargo_grades") or []
            grades = [str(g).strip().lower() for g in grades if g]

            docs = []
            voyage_ids = []

            if grades:
                mongo_resp = self.mongo_agent.run_llm_find(
                    question=state["user_input"],
                    slots={"cargo_grades": grades},
                )
                safe = _json_safe(mongo_resp)
                rows = safe.get("rows") if isinstance(safe, dict) else []
                rows = rows if isinstance(rows, list) else []
                docs = rows[:25]
                voyage_ids = [
                    str(r.get("voyageId"))
                    for r in docs
                    if isinstance(r, dict) and r.get("voyageId")
                ]

            voyage_ids = list(dict.fromkeys(voyage_ids))   # deduplicate, preserve order

            # ── write to artifacts (NOT step_results) ──────────────────────
            artifacts["voyage_ids"] = voyage_ids
            artifacts["mongo_grade_docs"] = docs
            slots["voyage_ids"] = voyage_ids

            state["mongo"] = {"mode": "mongo_llm", "ok": True, "rows": docs}
            state["data"]["mongo"] = state["mongo"]

            self._trace(state, {
                "phase": "composite_step_result",
                "step_index": idx + 1,
                "agent": "mongo",
                "operation": op,
                "ok": True,
                "rows": len(docs),
                "voyage_ids": len(voyage_ids),
                "extracted_voyage_ids": voyage_ids,
                "summary": (
                    f"cargo_grade_lookup: grades={grades} -> {len(voyage_ids)} voyage_ids"
                ),
            })

        elif agent == "mongo" and op == "resolve_anchor":
            q = (step.get("inputs") or {}).get("goal") or state.get("user_input") or ""
            try:
                mongo_resp = self.mongo_agent.run_llm_find(question=q, slots=slots)
                safe = _json_safe(mongo_resp)
                state["mongo"] = safe
                state["data"]["mongo"] = safe

                rows = safe.get("rows") if isinstance(safe, dict) else None
                if isinstance(rows, list) and rows:
                    first = rows[0] if isinstance(rows[0], dict) else {}
                    if first.get("voyageId") and not slots.get("voyage_id"):
                        slots["voyage_id"] = first["voyageId"]
                    imo = first.get("imo") or first.get("vesselImo")
                    if imo and not slots.get("imo"):
                        slots["imo"] = imo
                    if first.get("voyageNumber") and not slots.get("voyage_number"):
                        slots["voyage_number"] = first["voyageNumber"]
                    if first.get("vesselName") and not slots.get("vessel_name"):
                        slots["vessel_name"] = first["vesselName"]

                # Carry resolved voyage_id into artifacts so later steps include it
                if slots.get("voyage_id"):
                    inc = artifacts.get("include_voyage_ids")
                    if not isinstance(inc, list):
                        inc = []
                    if slots["voyage_id"] not in inc:
                        inc.append(slots["voyage_id"])
                    artifacts["include_voyage_ids"] = inc
                self._trace(
                    state,
                    {
                        "phase": "composite_step_result",
                        "step_index": idx + 1,
                        "agent": agent,
                        "operation": op,
                        "ok": True,
                        "mode": safe.get("mode") if isinstance(safe, dict) else None,
                        "mongo_ok": safe.get("ok") if isinstance(safe, dict) else None,
                        "collection": safe.get("collection") if isinstance(safe, dict) else None,
                        "limit": safe.get("limit") if isinstance(safe, dict) else None,
                        "mongo_query": (
                            {
                                "collection": safe.get("collection"),
                                "filter": safe.get("filter"),
                                "projection": safe.get("projection"),
                                "sort": safe.get("sort"),
                                "limit": safe.get("limit"),
                                "pipeline": safe.get("pipeline"),
                            }
                            if isinstance(safe, dict)
                            else None
                        ),
                        "rows": len(safe.get("rows") or []) if isinstance(safe, dict) else None,
                        "summary": "Mongo: resolve anchors/entities for the query (MongoDB).",
                    },
                )
            except Exception as e:
                state["data"]["mongo"] = {"mode": "mongo_llm", "ok": False, "reason": str(e), "rows": []}
                self._trace(
                    state,
                    {
                        "phase": "composite_step_result",
                        "step_index": idx + 1,
                        "agent": agent,
                        "operation": op,
                        "ok": False,
                        "error": str(e),
                    },
                )

        elif agent == "mongo" and op == "fetch_remarks":

            voyage_ids = artifacts.get("voyage_ids") or []
            remarks_by: Dict[str, Any] = {}
            cargo_by: Dict[str, Any] = {}
            ports_by: Dict[str, Any] = {}
            voyage_number_by: Dict[str, Any] = {}
            commissions_by: Dict[str, Any] = {}

            _dprint(f"   📝 Fetching remarks for {len(voyage_ids)} voyages...")

            used_llm = False
            if voyage_ids:
                try:
                    slots["voyage_ids"] = voyage_ids[:20]

                    # For remarks fetch, keep the LLM spec focused ONLY on voyage_ids
                    # to avoid over-filtering (e.g. by port_name) and to ensure voyageId
                    # is always projected for deterministic mapping.
                    slots_for_mongo = {"voyage_ids": slots["voyage_ids"]}
                    user_q = (state.get("user_input") or "").lower()
                    include_commissions = "commission" in user_q
                    q = (step.get("inputs") or {}).get("goal") or (
                        "Fetch remarks + minimal context for these voyage_ids.\n"
                        "Use collection=voyages.\n"
                        "Filter MUST be: {\"voyageId\": {\"$in\": slots.voyage_ids}}.\n"
                        "Projection MUST include: {\"_id\": 0, \"voyageId\": 1, \"voyageNumber\": 1, \"remarks\": 1, "
                        "\"fixtures.grades\": 1, \"fixtures.fixtureGrades.gradeName\": 1, "
                        "\"fixtures.fixturePorts.portName\": 1, \"fixtures.fixturePorts.activityType\": 1"
                        + (", \"fixtures.fixtureCommissions.commissionType\": 1, \"fixtures.fixtureCommissions.organizationName\": 1, \"fixtures.fixtureCommissions.rate\": 1" if include_commissions else "")
                        + "}.\n"
                        "Return only the minimal required fields."
                    )

                    mongo_resp = self.mongo_agent.run_llm_find(question=q, slots=slots_for_mongo)
                    safe = _json_safe(mongo_resp)
                    state["mongo"] = safe
                    state["data"]["mongo"] = safe

                    rows = safe.get("rows") if isinstance(safe, dict) else None
                    if isinstance(rows, list) and rows:
                        for r in rows:
                            if not isinstance(r, dict):
                                continue
                            vid = r.get("voyageId") or r.get("voyage_id")
                            if not vid:
                                continue
                            vid_s = str(vid)
                            voyage_number_by[vid_s] = r.get("voyageNumber") or r.get("voyage_number")

                            remarks = r.get("remarks") or r.get("remarkList") or r.get("remarks_json")
                            # Normalize remarks into a list of short strings (Mongo often stores remarkList as objects).
                            remarks_norm: list[str] = []
                            if isinstance(remarks, str) and remarks.strip():
                                remarks_norm = [remarks.strip()]
                            elif isinstance(remarks, list):
                                for it in remarks:
                                    if isinstance(it, str) and it.strip():
                                        remarks_norm.append(it.strip())
                                    elif isinstance(it, dict):
                                        txt = it.get("remark") or it.get("text") or it.get("message")
                                        if isinstance(txt, str) and txt.strip():
                                            remarks_norm.append(txt.strip())
                            elif isinstance(remarks, dict):
                                txt = remarks.get("remark") or remarks.get("text") or remarks.get("message")
                                if isinstance(txt, str) and txt.strip():
                                    remarks_norm = [txt.strip()]

                            # De-dupe preserve order; keep at most 25 to control payload size.
                            seen_rm = set()
                            remarks_dedup = []
                            for rm in remarks_norm:
                                if rm not in seen_rm:
                                    seen_rm.add(rm)
                                    remarks_dedup.append(rm)
                            remarks_by[vid_s] = remarks_dedup[:25] if remarks_dedup else None

                            # Cargo grade (best-effort): prefer fixtures.grades string, else fixtureGrades.gradeName list
                            grades: list[str] = []
                            comms: list[dict] = []
                            fixtures = r.get("fixtures")
                            fixtures_list = None
                            if isinstance(fixtures, list):
                                fixtures_list = fixtures
                            elif isinstance(fixtures, dict):
                                fl = fixtures.get("fixtureList") or fixtures.get("fixtures") or fixtures.get("list")
                                if isinstance(fl, list):
                                    fixtures_list = fl

                            if isinstance(fixtures_list, list):
                                for fx in fixtures_list:
                                    if not isinstance(fx, dict):
                                        continue
                                    g = fx.get("grades")
                                    if isinstance(g, str) and g.strip():
                                        grades.append(g.strip())
                                    fgs = fx.get("fixtureGrades")
                                    if isinstance(fgs, list):
                                        for fg in fgs:
                                            if isinstance(fg, dict):
                                                gn = fg.get("gradeName")
                                                if isinstance(gn, str) and gn.strip():
                                                    grades.append(gn.strip())

                                    # Bills of lading may contain grade names in some datasets
                                    bols = fx.get("fixtureBillsOfLading") or fx.get("billsOfLading") or fx.get("bills")
                                    if isinstance(bols, list):
                                        for bol in bols:
                                            if not isinstance(bol, dict):
                                                continue
                                            gn = bol.get("fixtureGradeName") or bol.get("gradeName") or bol.get("cargoGrade")
                                            if isinstance(gn, str) and gn.strip():
                                                grades.append(gn.strip())

                                    # Commission details (best-effort)
                                    fcs = fx.get("fixtureCommissions")
                                    if isinstance(fcs, list):
                                        for fc in fcs:
                                            if not isinstance(fc, dict):
                                                continue
                                            ct = fc.get("commissionType")
                                            on = fc.get("organizationName")
                                            rate = fc.get("rate")
                                            if ct or on or rate is not None:
                                                comms.append({
                                                    "commissionType": ct,
                                                    "organizationName": on,
                                                    "rate": rate,
                                                })

                            # de-dupe preserve order
                            seen_g = set()
                            grades_dedup = []
                            for g in grades:
                                if g not in seen_g:
                                    seen_g.add(g)
                                    grades_dedup.append(g)
                            cargo_by[vid_s] = grades_dedup

                            # de-dupe commissions by (type, org, rate)
                            if comms:
                                seen_c = set()
                                comms_dedup = []
                                for c in comms:
                                    key = (c.get("commissionType"), c.get("organizationName"), c.get("rate"))
                                    if key not in seen_c:
                                        seen_c.add(key)
                                        comms_dedup.append(c)
                                commissions_by[vid_s] = comms_dedup[:20]

                            # Key ports from fixturePorts
                            ports: list[dict] = []
                            if isinstance(fixtures_list, list):
                                for fx in fixtures_list:
                                    if not isinstance(fx, dict):
                                        continue
                                    fps = fx.get("fixturePorts")
                                    if not isinstance(fps, list):
                                        continue
                                    for p in fps:
                                        if not isinstance(p, dict):
                                            continue
                                        pn = p.get("portName")
                                        at = p.get("activityType")
                                        if isinstance(pn, str) and pn.strip():
                                            ports.append({"portName": pn.strip(), "activityType": at})

                            # de-dupe ports by (portName, activityType)
                            seen_p = set()
                            ports_dedup = []
                            for p in ports:
                                key = (p.get("portName"), p.get("activityType"))
                                if key not in seen_p:
                                    seen_p.add(key)
                                    ports_dedup.append(p)
                            ports_by[vid_s] = ports_dedup[:20]

                        # Only treat LLM path as successful if we can map at least one voyageId.
                        used_llm = bool(remarks_by)
                except Exception:
                    used_llm = False

            if not used_llm:
                # Fallback: existing anchor logic (safe, but multiple small calls)
                for vid in voyage_ids[:20]:
                    try:
                        resp = self.mongo_agent.run(
                            intent_key="voyage.entity",
                            slots={"voyage_id": vid},
                            projection={
                                "_id": 0,
                                "voyageId": 1,
                                "remarks": 1,
                                "remarkList": 1,
                            },
                            session_context=sess,
                        )

                        doc = resp.document or {}
                        remarks = doc.get("remarks") or doc.get("remarkList")
                        remarks_by[str(vid)] = remarks
                    except Exception:
                        remarks_by[str(vid)] = None

            artifacts["remarks_by_voyage_id"] = remarks_by
            artifacts["cargo_by_voyage_id"] = cargo_by
            artifacts["ports_by_voyage_id"] = ports_by
            artifacts["voyage_number_by_voyage_id"] = voyage_number_by
            artifacts["commissions_by_voyage_id"] = commissions_by
            _dprint(f"   ✅ Fetched remarks for {len(remarks_by)} voyages")
            self._trace(
                state,
                {
                    "phase": "composite_step_result",
                    "step_index": idx + 1,
                    "agent": agent,
                    "operation": op,
                    "ok": True,
                    "mode": (state.get("mongo") or {}).get("mode") if isinstance(state.get("mongo"), dict) else None,
                    "mongo_ok": (state.get("mongo") or {}).get("ok") if isinstance(state.get("mongo"), dict) else None,
                    "mongo_query": (
                        {
                            "collection": (state.get("mongo") or {}).get("collection"),
                            "filter": (state.get("mongo") or {}).get("filter"),
                            "projection": (state.get("mongo") or {}).get("projection"),
                            "sort": (state.get("mongo") or {}).get("sort"),
                            "limit": (state.get("mongo") or {}).get("limit"),
                            "pipeline": (state.get("mongo") or {}).get("pipeline"),
                        }
                        if used_llm and isinstance(state.get("mongo"), dict)
                        else None
                    ),
                    "rows": len(((state.get("mongo") or {}).get("rows") or [])) if isinstance(state.get("mongo"), dict) else None,
                    "voyage_ids": len(voyage_ids or []),
                    "extracted_voyage_ids": (voyage_ids or []),
                    "remarks_by_voyage_id": len(remarks_by or {}),
                    "summary": f"Mongo: fetched remarks + minimal context for {len(voyage_ids or [])} voyages (MongoDB).",
                },
            )

        # =========================================================
        # LLM MERGE STEP
        # =========================================================
        elif agent == "llm" and op == "merge":

            merged_rows = []
            finance_rows = artifacts.get("finance_rows") or []
            ops_rows = artifacts.get("ops_rows") or []
            remarks = artifacts.get("remarks_by_voyage_id") or {}
            cargo_by = artifacts.get("cargo_by_voyage_id") or {}
            ports_by = artifacts.get("ports_by_voyage_id") or {}
            voyage_number_by = artifacts.get("voyage_number_by_voyage_id") or {}
            commissions_by = artifacts.get("commissions_by_voyage_id") or {}
            intent_key_merge = state.get("intent_key") or ""

            def _num(v):
                if v is None or v == "":
                    return None
                try:
                    return float(v)
                except (TypeError, ValueError):
                    return v

            def _grade_key(v):
                if v is None:
                    return ""
                if isinstance(v, dict):
                    v = v.get("grade_name") or v.get("gradeName") or v.get("name") or v.get("grade")
                s = str(v).strip()
                if s.startswith("{") and s.endswith("}"):
                    try:
                        obj = ast.literal_eval(s)
                        if isinstance(obj, dict):
                            g = obj.get("grade_name") or obj.get("gradeName") or obj.get("name") or obj.get("grade")
                            s = str(g).strip() if g not in (None, "", [], {}) else ""
                    except Exception:
                        m = re.search(r"""['"](?:grade_name|gradeName|name|grade)['"]\s*:\s*['"]([^'"]+)['"]""", s)
                        if m:
                            s = m.group(1).strip()
                s = s.lower()
                if s in ("", "none", "null", "n/a", "na"):
                    return ""
                return s

            def _dedup_merged_rows(rows):
                out = []
                seen_rows = set()
                for r in rows:
                    if not isinstance(r, dict):
                        continue
                    vid = r.get("voyage_id")
                    if vid not in (None, "", [], {}):
                        key = ("voyage_id", str(vid))
                    else:
                        cgs = r.get("cargo_grades") if isinstance(r.get("cargo_grades"), list) else []
                        cgs_norm = tuple(sorted({_grade_key(x) for x in cgs if _grade_key(x)}))
                        if cgs_norm:
                            key = ("cargo_grades", cgs_norm)
                        elif r.get("vessel_imo") not in (None, "", [], {}):
                            key = ("vessel_imo", str(r.get("vessel_imo")), str(r.get("voyage_number") or ""))
                        else:
                            key = (
                                "fallback",
                                str(r.get("voyage_number") or ""),
                                str(r.get("pnl") or ""),
                                str(r.get("revenue") or ""),
                                str(r.get("total_expense") or ""),
                            )
                    if key in seen_rows:
                        continue
                    seen_rows.add(key)
                    out.append(r)
                return out

            # Index ops rows by voyage_id
            def _vid_key(v):
                if v in (None, ""):
                    return ""
                return str(v).strip().upper()

            ops_by_vid = {}
            for r in ops_rows:
                if isinstance(r, dict) and r.get("voyage_id"):
                    ops_by_vid.setdefault(_vid_key(r.get("voyage_id")), []).append(r)

            # ranking.vessels: finance returns vessel-level rows (vessel_imo, voyage_count, avg_pnl) without voyage_id
            if intent_key_merge == "ranking.vessels" and finance_rows:
                has_voyage_id = any(isinstance(fr, dict) and fr.get("voyage_id") for fr in finance_rows)
                if not has_voyage_id:
                    def _imo_key(imo):
                        if imo is None:
                            return ""
                        try:
                            return str(int(float(imo)))
                        except (TypeError, ValueError):
                            return str(imo).strip()

                    ops_by_imo = {}
                    for r in ops_rows:
                        if isinstance(r, dict):
                            imo = r.get("vessel_imo")
                            if imo is not None:
                                k = _imo_key(imo)
                                if k:
                                    ops_by_imo.setdefault(k, []).append(r)
                    seen_imo = set()
                    for fr in finance_rows:
                        if not isinstance(fr, dict):
                            continue
                        imo = fr.get("vessel_imo") or fr.get("vessel_imo")
                        if imo is None:
                            continue
                        imo_str = _imo_key(imo)
                        if not imo_str or imo_str in seen_imo:
                            continue
                        seen_imo.add(imo_str)
                        ops_for_imo = ops_by_imo.get(imo_str, [])
                        cargo_grades = []
                        for orow in ops_for_imo:
                            g = orow.get("grades_json")
                            if isinstance(g, (list, tuple)):
                                for x in g:
                                    if x is not None:
                                        s = str(x).strip()
                                        if s and s not in cargo_grades:
                                            cargo_grades.append(s)
                        def _n(v):
                            if v is None or v == "": return None
                            try: return float(v)
                            except (TypeError, ValueError): return v
                        merged_rows.append({
                            "vessel_imo": imo_str,
                            "vessel_name": fr.get("vessel_name"),
                            "voyage_count": fr.get("voyage_count"),
                            "is_operating": fr.get("is_operating"),
                            "scrubber": fr.get("scrubber"),
                            "market_type": fr.get("market_type"),
                            "ballast_speed": _n(fr.get("ballast_speed")),
                            "laden_speed": _n(fr.get("laden_speed")),
                            "contract_duration_days": _n(fr.get("contract_duration_days")),
                            "avg_pnl": _n(fr.get("avg_pnl") or fr.get("total_pnl") or fr.get("pnl")),
                            "total_pnl": _n(fr.get("total_pnl") or fr.get("pnl")),
                            "pnl": _n(fr.get("avg_pnl") or fr.get("pnl") or fr.get("total_pnl")),
                            "revenue": _n(fr.get("revenue") or fr.get("total_revenue") or fr.get("avg_revenue")),
                            "total_expense": _n(fr.get("total_expense") or fr.get("avg_total_expense")),
                            "bunker_cost": _n(fr.get("bunker_cost") or fr.get("total_bunker_cost") or fr.get("avg_bunker_cost")),
                            "tce": _n(fr.get("tce") or fr.get("avg_tce")),
                            "total_commission": _n(fr.get("total_commission")),
                            "cargo_grades": cargo_grades[:15],
                            "key_ports": [],
                            "remarks": None,
                        })
                    merged_rows = _dedup_merged_rows(merged_rows)
                    artifacts["merged_rows"] = merged_rows
                    try:
                        cov = artifacts.get("coverage") if isinstance(artifacts.get("coverage"), dict) else {}
                        if not isinstance(cov, dict):
                            cov = {}
                        cov["merged_rows_total"] = len(merged_rows)
                        cov["pnl_available"] = sum(1 for r in merged_rows if isinstance(r, dict) and r.get("pnl") not in (None, ""))
                        cov["cargo_grades_available"] = sum(1 for r in merged_rows if isinstance(r, dict) and r.get("cargo_grades"))
                        artifacts["coverage"] = cov
                    except Exception:
                        pass
                    _dprint(f"   ✅ Merged {len(merged_rows)} vessel-level rows (ranking.vessels)")
                    self._trace(state, {"phase": "composite_step_result", "step_index": idx + 1, "agent": agent, "operation": op, "ok": True, "merged_rows": len(merged_rows), "summary": f"Merge: joined {len(merged_rows)} vessel-level rows for ranking.vessels."})
                    artifacts["slots"] = slots
                    state["artifacts"] = artifacts
                    state["slots"] = slots
                    state["step_index"] = idx + 1
                    return state

            # Grade-level merge fallback for aggregate intents with no voyage_id (e.g. cargo profitability).
            # This keeps enrichment deterministic when finance rows are grouped by dimensions instead of voyage.
            has_voyage_id = any(isinstance(fr, dict) and fr.get("voyage_id") for fr in finance_rows)
            if (not has_voyage_id) and finance_rows:
                # Avoid finance/ops column collision on voyage_count for grade-level merges.
                for fr in finance_rows:
                    if isinstance(fr, dict) and "voyage_count" in fr and "finance_voyage_count" not in fr:
                        fr["finance_voyage_count"] = fr.pop("voyage_count")
                for orow in ops_rows:
                    if isinstance(orow, dict) and "voyage_count" in orow and "ops_voyage_count" not in orow:
                        orow["ops_voyage_count"] = orow.pop("voyage_count")

                ops_by_grade = {}
                for orow in ops_rows:
                    if not isinstance(orow, dict):
                        continue
                    gk = _grade_key(orow.get("cargo_grade") or orow.get("grade"))
                    if not gk:
                        continue
                    cur = ops_by_grade.get(gk) or {}
                    if not cur.get("common_ports") and isinstance(orow.get("common_ports"), list):
                        cur["common_ports"] = orow.get("common_ports")
                    if not cur.get("remarks") and isinstance(orow.get("congestion_delay_remarks"), list):
                        cur["remarks"] = orow.get("congestion_delay_remarks")
                    if cur.get("ops_voyage_count") in (None, "") and orow.get("ops_voyage_count") not in (None, ""):
                        cur["ops_voyage_count"] = orow.get("ops_voyage_count")
                    ops_by_grade[gk] = cur

                finance_by_grade: Dict[str, Dict[str, Any]] = {}
                for fr in finance_rows:
                    if not isinstance(fr, dict):
                        continue
                    grade_raw = fr.get("cargo_grade") or fr.get("grade")
                    gk = _grade_key(grade_raw)
                    if not gk:
                        continue
                    b = finance_by_grade.get(gk)
                    if not b:
                        display_grade = (
                            (grade_raw.get("grade_name") or grade_raw.get("gradeName") or grade_raw.get("name") or grade_raw.get("grade"))
                            if isinstance(grade_raw, dict)
                            else grade_raw
                        )
                        if isinstance(display_grade, str):
                            ds = display_grade.strip()
                            if ds.startswith("{") and ds.endswith("}"):
                                try:
                                    obj = ast.literal_eval(ds)
                                    if isinstance(obj, dict):
                                        g = obj.get("grade_name") or obj.get("gradeName") or obj.get("name") or obj.get("grade")
                                        display_grade = str(g).strip() if g not in (None, "", [], {}) else ""
                                except Exception:
                                    m = re.search(r"""['"](?:grade_name|gradeName|name|grade)['"]\s*:\s*['"]([^'"]+)['"]""", ds)
                                    if m:
                                        display_grade = m.group(1).strip()
                        b = {
                            "display_grade": str(display_grade).strip() if display_grade not in (None, "", [], {}) else gk,
                            "weight": 0.0,
                            "pnl_wsum": 0.0,
                            "revenue_wsum": 0.0,
                            "tce_wsum": 0.0,
                            "expense_wsum": 0.0,
                            "commission_wsum": 0.0,
                            "has_pnl": False,
                            "has_revenue": False,
                            "has_tce": False,
                            "has_expense": False,
                            "has_commission": False,
                            "voyage_count": 0.0,
                            "has_voyage_count": False,
                            "actual_avg_pnl": None,
                            "when_fixed_avg_pnl": None,
                            "variance_diff": None,
                        }
                        finance_by_grade[gk] = b

                    vc = _num(fr.get("finance_voyage_count"))
                    w = vc if (isinstance(vc, (int, float)) and vc and vc > 0) else 1.0
                    b["weight"] += w
                    if isinstance(vc, (int, float)) and vc >= 0:
                        b["voyage_count"] += float(vc)
                        b["has_voyage_count"] = True

                    pnl_v = _num(fr.get("avg_pnl") if fr.get("avg_pnl") is not None else fr.get("pnl"))
                    if isinstance(pnl_v, (int, float)):
                        b["pnl_wsum"] += float(pnl_v) * w
                        b["has_pnl"] = True

                    rev_v = _num(fr.get("avg_revenue") if fr.get("avg_revenue") is not None else fr.get("revenue"))
                    if isinstance(rev_v, (int, float)):
                        b["revenue_wsum"] += float(rev_v) * w
                        b["has_revenue"] = True

                    tce_v = _num(fr.get("avg_tce") if fr.get("avg_tce") is not None else fr.get("tce"))
                    if isinstance(tce_v, (int, float)):
                        b["tce_wsum"] += float(tce_v) * w
                        b["has_tce"] = True

                    exp_v = _num(fr.get("total_expense"))
                    if isinstance(exp_v, (int, float)):
                        b["expense_wsum"] += float(exp_v) * w
                        b["has_expense"] = True

                    com_v = _num(fr.get("total_commission"))
                    if isinstance(com_v, (int, float)):
                        b["commission_wsum"] += float(com_v) * w
                        b["has_commission"] = True
                    if b.get("actual_avg_pnl") in (None, "") and fr.get("actual_avg_pnl") not in (None, ""):
                        b["actual_avg_pnl"] = _num(fr.get("actual_avg_pnl"))
                    if b.get("when_fixed_avg_pnl") in (None, "") and fr.get("when_fixed_avg_pnl") not in (None, ""):
                        b["when_fixed_avg_pnl"] = _num(fr.get("when_fixed_avg_pnl"))
                    if b.get("variance_diff") in (None, "") and fr.get("variance_diff") not in (None, ""):
                        b["variance_diff"] = _num(fr.get("variance_diff"))

                for gk, b in finance_by_grade.items():
                    w = float(b.get("weight") or 0.0)
                    if w <= 0:
                        continue
                    octx = ops_by_grade.get(gk) or {}
                    ports = []
                    cp = octx.get("common_ports")
                    if isinstance(cp, list):
                        for p in cp:
                            if p is None:
                                continue
                            pn = None
                            at = None
                            if isinstance(p, dict):
                                pn = p.get("portName") or p.get("port_name") or p.get("name")
                                at = p.get("activityType") or p.get("activity_type")
                            else:
                                ps = str(p).strip()
                                if ps.startswith("{") and ps.endswith("}"):
                                    try:
                                        obj = ast.literal_eval(ps)
                                        if isinstance(obj, dict):
                                            pn = obj.get("port_name") or obj.get("portName") or obj.get("name")
                                            at = obj.get("activity_type") or obj.get("activityType")
                                    except Exception:
                                        m_name = re.search(r"""['"](?:port_name|portName|name)['"]\s*:\s*['"]([^'"]+)['"]""", ps)
                                        m_act = re.search(r"""['"](?:activity_type|activityType)['"]\s*:\s*['"]([^'"]+)['"]""", ps)
                                        if m_name:
                                            pn = m_name.group(1).strip()
                                        if m_act:
                                            at = m_act.group(1).strip()
                                else:
                                    pn = ps
                            if pn:
                                ports.append({"portName": str(pn).strip(), "activityType": at})
                    if not ports:
                        mcp = fr.get("most_common_ports")
                        if isinstance(mcp, str) and mcp.strip():
                            for token in [x.strip() for x in mcp.split(",") if str(x).strip()]:
                                ports.append({"portName": token, "activityType": None})
                        elif isinstance(mcp, list):
                            for p in mcp:
                                if isinstance(p, dict):
                                    pn = p.get("portName") or p.get("port_name") or p.get("name")
                                    at = p.get("activityType") or p.get("activity_type")
                                    if pn:
                                        ports.append({"portName": str(pn).strip(), "activityType": at})
                                elif p is not None and str(p).strip():
                                    ports.append({"portName": str(p).strip(), "activityType": None})
                    merged_rows.append({
                        "voyage_id": None,
                        "voyage_number": None,
                        "pnl": (b["pnl_wsum"] / w) if b.get("has_pnl") else None,
                        "revenue": (b["revenue_wsum"] / w) if b.get("has_revenue") else None,
                        "total_expense": (b["expense_wsum"] / w) if b.get("has_expense") else None,
                        "tce": (b["tce_wsum"] / w) if b.get("has_tce") else None,
                        "total_commission": (b["commission_wsum"] / w) if b.get("has_commission") else None,
                        "finance": {"cargo_grade": b.get("display_grade")},
                        "ops": [],
                        "cargo_grades": [b.get("display_grade") or gk],
                        "key_ports": ports[:20],
                        "remarks": octx.get("remarks") or None,
                        "finance_voyage_count": (int(b["voyage_count"]) if b.get("has_voyage_count") else None),
                        "ops_voyage_count": octx.get("ops_voyage_count"),
                        "voyage_count": (int(b["voyage_count"]) if b.get("has_voyage_count") else None),
                        "actual_avg_pnl": b.get("actual_avg_pnl"),
                        "when_fixed_avg_pnl": b.get("when_fixed_avg_pnl"),
                        "variance_diff": b.get("variance_diff"),
                    })

            # Deduplicated merge (by voyage_id)
            seen = set()

            for fr in finance_rows:
                if not isinstance(fr, dict):
                    continue

                vid = fr.get("voyage_id")
                vid_k = _vid_key(vid)

                if not vid_k or vid_k in seen:
                    continue

                seen.add(vid_k)

                # Flatten core finance KPIs at the top-level of merged_rows for deterministic summarization.
                # This avoids the LLM missing KPIs when `finance.rows` is compacted away.
                # Normalize numeric keys (support alternate casing from DB) and coerce to float for JSON.
                pnl = _num(fr.get("pnl") or fr.get("PnL"))
                revenue = _num(fr.get("revenue") or fr.get("Revenue"))
                total_expense = _num(fr.get("total_expense") or fr.get("Total_expense") or fr.get("total expense"))
                tce = _num(fr.get("tce") or fr.get("TCE"))
                total_commission = _num(fr.get("total_commission") or fr.get("Total_commission") or fr.get("total commission"))
                co2 = _num(fr.get("co2") or fr.get("voyageCO2"))
                eeoi = _num(fr.get("eeoi") or fr.get("voyageEEOI"))
                aer = _num(fr.get("aer") or fr.get("voyageAER"))
                # Scenario comparison: use registry columns when present so table shows actual vs when-fixed and variance
                if intent_key_merge == "analysis.scenario_comparison":
                    pnl = _num(fr.get("pnl_actual") or pnl)
                    tce = _num(fr.get("tce_actual") or tce)

                ops_for_vid = ops_by_vid.get(vid_k, [])

                # Fallback extraction (ops → merged fields) when Mongo enrichment isn't present.
                # This is critical for composite intents that skip Mongo (e.g. analysis.cargo_profitability)
                # and for cases where Mongo remarks/fixtures are empty.
                cargo_grades = cargo_by.get(str(vid), [])
                key_ports = ports_by.get(str(vid), [])
                remark_val = remarks.get(str(vid)) if isinstance(remarks, dict) else None

                if not cargo_grades and isinstance(ops_for_vid, list):
                    gs: list[str] = []
                    for orow in ops_for_vid:
                        if not isinstance(orow, dict):
                            continue
                        g = orow.get("grades_json")
                        if isinstance(g, list):
                            for x in g:
                                if x is None:
                                    continue
                                if isinstance(x, dict):
                                    # Common shapes: {"gradeName": "..."} / {"grade_name": "..."} / {"name": "..."}
                                    gv = x.get("gradeName") or x.get("grade_name") or x.get("name")
                                    if gv is None:
                                        continue
                                    s = str(gv).strip()
                                else:
                                    s = str(x).strip()
                                if s and s.lower() not in ("none", "null", "n/a", "na"):
                                    gs.append(s)
                    # de-dupe preserve order
                    seen_g = set()
                    cargo_grades = [x for x in gs if not (x in seen_g or seen_g.add(x))]

                if not key_ports and isinstance(ops_for_vid, list):
                    ps: list[dict] = []
                    for orow in ops_for_vid:
                        if not isinstance(orow, dict):
                            continue
                        p = orow.get("ports_json")
                        if isinstance(p, list):
                            for x in p:
                                if isinstance(x, dict):
                                    pn = x.get("portName") or x.get("port_name") or x.get("name")
                                    at = x.get("activityType") or x.get("activity_type")
                                    if pn:
                                        ps.append({"portName": str(pn).strip(), "activityType": at})
                                elif x is not None:
                                    s = str(x).strip()
                                    if s:
                                        ps.append({"portName": s, "activityType": None})
                    # de-dupe by (portName, activityType)
                    seen_p = set()
                    key_ports = []
                    for p in ps:
                        key = (p.get("portName"), p.get("activityType"))
                        if key in seen_p:
                            continue
                        seen_p.add(key)
                        key_ports.append(p)

                # Aggregate queries may already return a finance-computed common port list.
                # Preserve it when ops-derived key_ports are absent.
                if not key_ports:
                    mcp = fr.get("most_common_ports")
                    if isinstance(mcp, str) and mcp.strip():
                        parsed_ports = []
                        for token in [x.strip() for x in mcp.split(",") if str(x).strip()]:
                            parsed_ports.append({"portName": token, "activityType": None})
                        if parsed_ports:
                            key_ports = parsed_ports[:20]
                    elif isinstance(mcp, list):
                        parsed_ports = []
                        for x in mcp:
                            if isinstance(x, dict):
                                pn = x.get("portName") or x.get("port_name") or x.get("name")
                                at = x.get("activityType") or x.get("activity_type")
                                if pn:
                                    parsed_ports.append({"portName": str(pn).strip(), "activityType": at})
                            elif x is not None and str(x).strip():
                                parsed_ports.append({"portName": str(x).strip(), "activityType": None})
                        if parsed_ports:
                            key_ports = parsed_ports[:20]

                if remark_val in (None, "", [], {}) and isinstance(ops_for_vid, list):
                    rs: list[str] = []
                    for orow in ops_for_vid:
                        if not isinstance(orow, dict):
                            continue
                        rj = orow.get("remarks_json")
                        if isinstance(rj, list):
                            for x in rj:
                                if x is None:
                                    continue
                                s = str(x).strip()
                                if s:
                                    rs.append(s)
                    # keep small; sanitize later anyway
                    if rs:
                        # de-dupe preserve order
                        seen_r = set()
                        remark_val = [x for x in rs if not (x in seen_r or seen_r.add(x))]

                row = {
                    "voyage_id": vid,
                    "voyage_number": voyage_number_by.get(str(vid)) or fr.get("voyage_number"),
                    "vessel_name": (
                        fr.get("vessel_name")
                        or (
                            (ops_for_vid[0].get("vessel_name") if isinstance(ops_for_vid, list) and ops_for_vid and isinstance(ops_for_vid[0], dict) else None)
                        )
                    ),
                    "vessel_imo": (
                        fr.get("vessel_imo")
                        or (
                            (ops_for_vid[0].get("vessel_imo") if isinstance(ops_for_vid, list) and ops_for_vid and isinstance(ops_for_vid[0], dict) else None)
                        )
                    ),
                    "pnl": pnl,
                    "revenue": revenue,
                    "total_expense": total_expense,
                    "tce": tce,
                    "total_commission": total_commission,
                    "co2": co2,
                    "eeoi": eeoi,
                    "aer": aer,
                    "finance": fr,
                    "ops": ops_for_vid,
                    "cargo_grades": cargo_grades,
                    "key_ports": key_ports,
                    "remarks": remark_val,
                    "commissions": commissions_by.get(str(vid), []),
                    "is_delayed": (
                        fr.get("is_delayed")
                        if fr.get("is_delayed") is not None
                        else (
                            (ops_for_vid[0].get("is_delayed") if isinstance(ops_for_vid, list) and ops_for_vid and isinstance(ops_for_vid[0], dict) else None)
                        )
                    ),
                }
                if intent_key_merge == "analysis.scenario_comparison":
                    row["pnl_actual"] = _num(fr.get("pnl_actual"))
                    row["pnl_when_fixed"] = _num(fr.get("pnl_when_fixed"))
                    row["pnl_variance"] = _num(fr.get("pnl_variance"))
                    row["tce_actual"] = _num(fr.get("tce_actual"))
                    row["tce_when_fixed"] = _num(fr.get("tce_when_fixed"))
                    row["tce_variance"] = _num(fr.get("tce_variance"))
                # Offhire ranking: expose offhire_days and delay_reason at top level so table can show them
                if fr.get("offhire_days") is not None or intent_key_merge == "ops.offhire_ranking":
                    offhire_val = fr.get("offhire_days")
                    delay_val = fr.get("delay_reason")
                    if (offhire_val in (None, "")) and isinstance(ops_for_vid, list) and ops_for_vid:
                        first_ops = ops_for_vid[0] if isinstance(ops_for_vid[0], dict) else {}
                        if isinstance(first_ops, dict):
                            offhire_val = first_ops.get("offhire_days")
                            delay_val = delay_val or first_ops.get("delay_reason")
                    row["offhire_days"] = _num(offhire_val)
                    row["delay_reason"] = delay_val
                # Port calls: preserve finance-provided count when available,
                # otherwise fall back to derived key_ports length.
                pc = fr.get("port_count")
                if pc in (None, "") and isinstance(key_ports, list):
                    pc = len(key_ports)
                if pc not in (None, ""):
                    try:
                        row["port_calls"] = int(pc)
                    except Exception:
                        row["port_calls"] = _num(pc)
                merged_rows.append(row)

            merged_rows = _dedup_merged_rows(merged_rows)
            artifacts["merged_rows"] = merged_rows

            # Data coverage hints to prevent false "Not available" claims in summarization.
            try:
                cov = artifacts.get("coverage") if isinstance(artifacts.get("coverage"), dict) else {}
                if not isinstance(cov, dict):
                    cov = {}
                cov["merged_rows_total"] = len(merged_rows)
                cov["pnl_available"] = sum(1 for r in merged_rows if isinstance(r, dict) and r.get("pnl") not in (None, ""))
                cov["cargo_grades_available"] = sum(1 for r in merged_rows if isinstance(r, dict) and r.get("cargo_grades"))
                cov["key_ports_available"] = sum(1 for r in merged_rows if isinstance(r, dict) and r.get("key_ports"))
                cov["remarks_available"] = sum(1 for r in merged_rows if isinstance(r, dict) and r.get("remarks") not in (None, "", [], {}))
                artifacts["coverage"] = cov
            except Exception:
                pass

            _dprint(f"   ✅ Merged {len(merged_rows)} unique rows")
            self._trace(
                state,
                {
                    "phase": "composite_step_result",
                    "step_index": idx + 1,
                    "agent": agent,
                    "operation": op,
                    "ok": True,
                    "merged_rows": len(merged_rows),
                    "summary": f"Merge: joined finance + ops + mongo context into {len(merged_rows)} merged rows.",
                },
            )

        # =========================================================
        # FINAL STATE UPDATE
        # =========================================================
        artifacts["slots"] = slots
        state["artifacts"] = artifacts
        state["slots"] = slots
        state["step_index"] = idx + 1

        return state

    # =========================================================
    # Merge
    # =========================================================

    def n_merge(self, state: GraphState) -> GraphState:
        """Merge results with improved null safety."""
        # Initialize data if missing
        if "data" not in state or state["data"] is None:
            state["data"] = {
                "finance": {"mode": None, "rows": []},
                "ops": {"mode": None, "rows": []},
                "mongo": {},
                "artifacts": {}
            }
        
        data = state["data"]
        
        # Ensure all sections exist
        if "finance" not in data or data["finance"] is None:
            data["finance"] = state.get("finance") or {"mode": None, "rows": []}
        elif state.get("finance"):
            data["finance"] = state["finance"]
            
        if "ops" not in data or data["ops"] is None:
            data["ops"] = state.get("ops") or {"mode": None, "rows": []}
        elif state.get("ops"):
            data["ops"] = state["ops"]
            
        if "mongo" not in data or data["mongo"] is None:
            data["mongo"] = state.get("mongo") or {}
        elif state.get("mongo"):
            data["mongo"] = state["mongo"]
            
        if "artifacts" not in data or data["artifacts"] is None:
            data["artifacts"] = state.get("artifacts") or {}
        elif state.get("artifacts"):
            data["artifacts"] = state["artifacts"]

        # Check for dynamic SQL usage
        dynamic_agents = []
        
        finance_data = data.get("finance")
        if isinstance(finance_data, dict) and finance_data.get("mode") == "dynamic_sql":
            dynamic_agents.append("finance")

        ops_data = data.get("ops")
        if isinstance(ops_data, dict) and ops_data.get("mode") == "dynamic_sql":
            dynamic_agents.append("ops")

        # Build merged structure
        state["merged"] = {
            "mongo": data.get("mongo"),
            "finance": data.get("finance"),
            "ops": data.get("ops"),
            "artifacts": data.get("artifacts"),
            "plan": state.get("plan"),
            "dynamic_sql_used": bool(dynamic_agents),
            "dynamic_sql_agents": dynamic_agents,
        }
        
        # Also update data reference
        state["data"] = data

        return state

    # =========================================================
    # Summarize with Token Optimization
    # =========================================================

    def n_summarize(self, state: GraphState) -> GraphState:
        """
        Generate final response with DATA SANITIZATION and TOKEN TRACKING.
        
        ✅ NEW: Sanitizes data before sending to LLM to prevent rate limits
        ✅ NEW: Tracks token usage for debugging
        """
        intent_key = state.get("intent_key", "out_of_scope")
        merged_full = state.get("merged") or {}
        # For ranking.*, ensure merged_rows have explicit pnl/revenue/total_expense at top level (avoid summarizer saying "not available")
        if str(intent_key).startswith("ranking."):
            artifacts = merged_full.get("artifacts")
            if isinstance(artifacts, dict) and isinstance(artifacts.get("merged_rows"), list):
                # Build lookup from raw finance rows by voyage_id (in case merge step used different key casing)
                finance_rows = (merged_full.get("finance") or {}).get("rows") or []
                fin_by_vid = {}
                for r in finance_rows:
                    if isinstance(r, dict) and r.get("voyage_id"):
                        fin_by_vid[str(r["voyage_id"]).strip().upper()] = r
                def _norm_num(v):
                    if v in (None, "", "Not available", "not available", "N/A", "n/a", "NA", "na"):
                        return None
                    try:
                        return float(v)
                    except Exception:
                        return v
                for mr in artifacts["merged_rows"]:
                    if not isinstance(mr, dict):
                        continue
                    vid = mr.get("voyage_id")
                    fin = mr.get("finance") if isinstance(mr.get("finance"), dict) else {}
                    if not fin and vid is not None:
                        fin = fin_by_vid.get(str(vid).strip().upper()) or {}
                    if (_norm_num(mr.get("pnl")) is None) and fin:
                        mr["pnl"] = _norm_num(fin.get("pnl") or fin.get("PnL"))
                    else:
                        mr["pnl"] = _norm_num(mr.get("pnl"))
                    if (_norm_num(mr.get("revenue")) is None) and fin:
                        mr["revenue"] = _norm_num(fin.get("revenue") or fin.get("Revenue"))
                    else:
                        mr["revenue"] = _norm_num(mr.get("revenue"))
                    if (_norm_num(mr.get("total_expense")) is None) and fin:
                        mr["total_expense"] = _norm_num(fin.get("total_expense") or fin.get("Total_expense") or fin.get("total expense"))
                    else:
                        mr["total_expense"] = _norm_num(mr.get("total_expense"))
        merged = compact_payload(merged_full)
        
        # Ensure merged has all required keys
        if not isinstance(merged.get("finance"), dict):
            merged["finance"] = {"mode": None, "rows": []}
        if not isinstance(merged.get("ops"), dict):
            merged["ops"] = {"mode": None, "rows": []}
        if not isinstance(merged.get("artifacts"), dict):
            merged["artifacts"] = {}
        
        # Track token usage before sanitization
        original_tokens = self._estimate_tokens(merged)
        self._trace(
            state,
            {
                "phase": "token_usage",
                "stage": "pre_sanitize",
                "total_tokens_est": original_tokens,
                "mongo_tokens_est": self._estimate_tokens(merged.get("mongo", {})),
                "finance_tokens_est": self._estimate_tokens(merged.get("finance", {})),
                "ops_tokens_est": self._estimate_tokens(merged.get("ops", {})),
                "artifacts_tokens_est": self._estimate_tokens(merged.get("artifacts", {})),
            },
        )
        if original_tokens > 1000:
            _dprint(f"\n📊 TOKEN USAGE ANALYSIS:")
            _dprint(f"   Original merged data: ~{original_tokens:,} tokens")
            
            # Break down by component
            mongo_tokens = self._estimate_tokens(merged.get("mongo", {}))
            finance_tokens = self._estimate_tokens(merged.get("finance", {}))
            ops_tokens = self._estimate_tokens(merged.get("ops", {}))
            artifacts_tokens = self._estimate_tokens(merged.get("artifacts", {}))
            
            _dprint(f"   ├─ Mongo: ~{mongo_tokens:,} tokens")
            _dprint(f"   ├─ Finance: ~{finance_tokens:,} tokens")
            _dprint(f"   ├─ Ops: ~{ops_tokens:,} tokens")
            _dprint(f"   └─ Artifacts: ~{artifacts_tokens:,} tokens")
        
        # Sanitize data to reduce token usage (after compaction)
        sanitized_merged = self._sanitize_for_llm(merged)
        
        # Show token savings
        sanitized_tokens = self._estimate_tokens(sanitized_merged)
        self._trace(
            state,
            {
                "phase": "token_usage",
                "stage": "post_sanitize",
                "total_tokens_est": sanitized_tokens,
                "saved_tokens_est": original_tokens - sanitized_tokens,
                "saved_pct_est": (100 * (original_tokens - sanitized_tokens) / max(original_tokens, 1)),
            },
        )
        if original_tokens > 1000:
            savings = original_tokens - sanitized_tokens
            savings_pct = 100 * savings / max(original_tokens, 1)
            _dprint(f"\n   After sanitization: ~{sanitized_tokens:,} tokens")
            _dprint(f"   💾 SAVED: ~{savings:,} tokens ({savings_pct:.1f}% reduction)")
            
            if sanitized_tokens < 5000:
                _dprint(f"   ✅ Token usage is now within safe limits!")
            elif sanitized_tokens < 10000:
                _dprint(f"   ⚠️  Token usage is moderate - should be OK")
            else:
                _dprint(f"   🔴 Token usage is still high - may hit rate limits")
        
        # Check if we have any data
        finance_data = sanitized_merged.get("finance", {})
        ops_data = sanitized_merged.get("ops", {})
        artifacts = sanitized_merged.get("artifacts", {})
        
        finance_rows = finance_data.get("rows", []) if isinstance(finance_data, dict) else []
        ops_rows = ops_data.get("rows", []) if isinstance(ops_data, dict) else []
        merged_rows = artifacts.get("merged_rows", []) if isinstance(artifacts, dict) else []
        
        has_data = bool(finance_rows or ops_rows or sanitized_merged.get("mongo") or merged_rows)
        
        try:
            # Try to generate proper response with SANITIZED data
            answer = self.llm.summarize_answer(
                question=state["user_input"],
                plan=state.get("plan") or {"plan_type": "single", "intent_key": intent_key},
                merged=sanitized_merged,
                session_context=state.get("session_ctx") or {},
            )
            
            # Validate answer quality
            if not answer or len(answer) < 20 or answer.startswith("Intent="):
                if merged_rows and (
                    str(intent_key).startswith("ranking.")
                    or str(intent_key).startswith("comparison.")
                    or str(intent_key).startswith("aggregation.")
                ):
                    answer = self._fallback_tabular_answer(
                        question=state.get("user_input") or "",
                        intent_key=str(intent_key or ""),
                        rows=merged_rows,
                    )
                else:
                    raise ValueError("Generated answer is too short or malformed")
                
        except Exception as e:
            # Improved fallback with data context
            error_trace = traceback.format_exc()
            _dprint(f"⚠️ WARNING: summarize_answer failed: {e}")
            _dprint(f"Error trace: {error_trace}")
            
            if has_data:
                if merged_rows and (
                    str(intent_key).startswith("ranking.")
                    or str(intent_key).startswith("comparison.")
                    or str(intent_key).startswith("aggregation.")
                ):
                    answer = self._fallback_tabular_answer(
                        question=state.get("user_input") or "",
                        intent_key=str(intent_key or ""),
                        rows=merged_rows,
                    )
                else:
                    answer = (
                        f"I found {len(finance_rows)} finance records and {len(ops_rows)} ops records "
                        f"for your query, but encountered an error generating the summary. "
                        f"Intent: {intent_key}. Error: {str(e)}"
                    )
            else:
                answer = (
                    f"No data available for this query (Intent: {intent_key}). "
                    f"This could mean the requested information doesn't exist in the database."
                )

        state["answer"] = answer

        # Persist a compact result-set memory for multi-row answers (for "among these" follow-ups).
        try:
            from datetime import date, datetime
            from decimal import Decimal

            def _json_primitive(v: Any) -> Any:
                if v is None:
                    return None
                if isinstance(v, (str, int, float, bool)):
                    return v
                if isinstance(v, Decimal):
                    try:
                        return float(v)
                    except Exception:
                        return str(v)
                if isinstance(v, (datetime, date)):
                    try:
                        return v.isoformat()
                    except Exception:
                        return str(v)
                # Lists/dicts: keep small + stringify safely
                if isinstance(v, list):
                    return [_json_primitive(x) for x in v[:8]]
                if isinstance(v, dict):
                    out: Dict[str, Any] = {}
                    for k, vv in list(v.items())[:25]:
                        out[str(k)] = _json_primitive(vv)
                    return out
                return str(v)

            def _compact_remarks(val: Any, *, max_len: int = 600) -> str | None:
                if val in (None, "", [], {}):
                    return None
                try:
                    if isinstance(val, str):
                        s = val.strip()
                    elif isinstance(val, list):
                        parts = []
                        for x in val[:4]:
                            if x in (None, "", [], {}):
                                continue
                            parts.append(str(x).strip())
                        s = " | ".join([p for p in parts if p])
                    else:
                        s = str(val).strip()
                    if not s:
                        return None
                    if len(s) > max_len:
                        return s[:max_len].rstrip() + "…"
                    return s
                except Exception:
                    return None

            def _infer_result_set_meta(rows_in: list[dict], *, source_intent: str | None, user_query: str | None) -> Dict[str, Any]:
                metrics = (
                    "expense_to_revenue_ratio",
                    "avg_offhire_days",
                    "offhire_days",
                    "port_calls",
                    "voyage_count",
                    "avg_pnl",
                    "pnl",
                    "total_pnl",
                    "avg_revenue",
                    "revenue",
                    "total_expense",
                    "avg_total_expense",
                    "avg_tce",
                    "tce",
                    "total_commission",
                    "pnl_variance",
                    "tce_variance",
                    "variance_diff",
                    "actual_avg_pnl",
                    "when_fixed_avg_pnl",
                )
                available = []
                for m in metrics:
                    ok = False
                    for rr in rows_in[:50]:
                        if isinstance(rr, dict) and (
                            rr.get(m) not in (None, "", [], {})
                            or GraphRouter._result_row_metric_value(rr, m) is not None
                        ):
                            ok = True
                            break
                    if ok:
                        available.append(m)
                primary = None
                source_intent = str(source_intent or "")
                ql = str(user_query or "").strip().lower()
                preferred_by_query = [
                    ("expense ratio", "expense_to_revenue_ratio"),
                    ("expense-to-revenue ratio", "expense_to_revenue_ratio"),
                    ("expense to revenue ratio", "expense_to_revenue_ratio"),
                    ("average demurrage", "avg_offhire_days"),
                    ("demurrage wait", "avg_offhire_days"),
                    ("wait time", "avg_offhire_days"),
                    ("total pnl", "total_pnl"),
                    ("average tce", "avg_tce"),
                    ("avg tce", "avg_tce"),
                    ("pnl variance", "pnl_variance"),
                    ("tce variance", "tce_variance"),
                    ("variance", "variance_diff"),
                    ("average revenue", "avg_revenue"),
                    ("avg revenue", "avg_revenue"),
                    ("average pnl", "avg_pnl"),
                    ("avg pnl", "avg_pnl"),
                    ("port calls", "port_calls"),
                    ("port count", "port_calls"),
                    ("voyage count", "voyage_count"),
                    ("commission", "total_commission"),
                    ("revenue", "revenue"),
                    ("expense", "total_expense"),
                    ("cost", "total_expense"),
                    ("tce", "tce"),
                    ("pnl", "pnl"),
                ]
                for phrase, metric in preferred_by_query:
                    if phrase in ql and metric in available:
                        primary = metric
                        break
                preferred_by_intent = [
                    ("ranking.port_calls", "port_calls"),
                    ("ops.offhire_ranking", "offhire_days"),
                    ("ranking.ports", "avg_offhire_days"),
                    ("analysis.by_module_type", "avg_pnl"),
                    ("analysis.scenario_comparison", "pnl_variance"),
                    ("analysis.high_revenue_low_pnl", "expense_to_revenue_ratio"),
                ]
                if primary is None:
                    for prefix, metric in preferred_by_intent:
                        if source_intent.startswith(prefix) and metric in available:
                            primary = metric
                            break
                if primary is None:
                    primary = available[0] if available else None
                return {
                    "source_intent": source_intent,
                    "available_metrics": available,
                    "primary_metric": primary,
                }

            def _compact_result_row(raw_row: Dict[str, Any]) -> Dict[str, Any]:
                if not isinstance(raw_row, dict):
                    return {}
                out: Dict[str, Any] = {}
                for key, val in list(raw_row.items())[:60]:
                    if key in {"finance", "ops", "mongo", "artifacts", "commissions"}:
                        continue
                    out[str(key)] = _json_primitive(val)
                for nested_key in ("finance", "ops"):
                    nested = raw_row.get(nested_key)
                    if isinstance(nested, dict):
                        for key, val in list(nested.items())[:60]:
                            out.setdefault(str(key), _json_primitive(val))
                if out.get("remarks") in (None, "", [], {}):
                    out["remarks"] = _compact_remarks(raw_row.get("remarks"))
                else:
                    out["remarks"] = _compact_remarks(out.get("remarks"))
                if out.get("delay_reason") in (None, "", [], {}):
                    out["delay_reason"] = _json_primitive(raw_row.get("delay_reason") or raw_row.get("delay_reasons"))
                revenue = GraphRouter._result_row_metric_value(out, "revenue")
                total_expense = GraphRouter._result_row_metric_value(out, "total_expense")
                if out.get("expense_to_revenue_ratio") in (None, "", [], {}) and revenue not in (None, 0) and total_expense is not None:
                    out["expense_to_revenue_ratio"] = float(total_expense) / float(revenue)
                return {k: v for k, v in out.items() if v not in (None, "", [], {})}

            sess = state.get("session_ctx") or {}
            art = state.get("artifacts") or {}
            if (
                str(state.get("intent_key") or "").strip().lower() == "followup.result_set"
                and isinstance(sess, dict)
                and isinstance(sess.get("last_result_set"), dict)
            ):
                latest_result_set = sess.get("last_result_set")
                raise StopIteration
            mrs = art.get("merged_rows") if isinstance(art, dict) else None
            if isinstance(mrs, list) and mrs:
                compact_rows = []
                for r in mrs[:20]:
                    if not isinstance(r, dict):
                        continue
                    compact = _compact_result_row(r)
                    if compact:
                        compact_rows.append(compact)
                latest_result_set = {
                    "source_intent": state.get("intent_key"),
                    "rows": compact_rows,
                    "meta": _infer_result_set_meta(compact_rows, source_intent=state.get("intent_key"), user_query=state.get("user_input")),
                }
                self.redis.save_session(
                    state["session_id"],
                    {
                        **(sess or {}),
                        "last_result_set": latest_result_set,
                    },
                )
                if isinstance(sess, dict):
                    sess["last_result_set"] = latest_result_set
            else:
                # Fallback: if we don't have merged_rows (some single intents), store from finance rows.
                merged = state.get("merged") or {}
                fin = merged.get("finance") if isinstance(merged, dict) else None
                fin_rows = (fin or {}).get("rows") if isinstance(fin, dict) else None
                if isinstance(fin_rows, list) and len(fin_rows) >= 1:
                    compact_rows = []
                    for r in fin_rows[:20]:
                        if not isinstance(r, dict):
                            continue
                        compact = _compact_result_row(r)
                        if compact:
                            compact_rows.append(compact)
                    latest_result_set = {
                        "source_intent": state.get("intent_key"),
                        "rows": compact_rows,
                        "meta": _infer_result_set_meta(compact_rows, source_intent=state.get("intent_key"), user_query=state.get("user_input")),
                    }
                    self.redis.save_session(
                        state["session_id"],
                        {
                            **(sess or {}),
                            "last_result_set": latest_result_set,
                        },
                    )
                    if isinstance(sess, dict):
                        sess["last_result_set"] = latest_result_set
                else:
                    # Fallback for ops-only/composite responses where finance rows may be empty.
                    ops = merged.get("ops") if isinstance(merged, dict) else None
                    ops_rows = (ops or {}).get("rows") if isinstance(ops, dict) else None
                    if isinstance(ops_rows, list) and len(ops_rows) >= 1:
                        compact_rows = []
                        for r in ops_rows[:20]:
                            if not isinstance(r, dict):
                                continue
                            compact = _compact_result_row(r)
                            if compact:
                                compact_rows.append(compact)
                        latest_result_set = {
                            "source_intent": state.get("intent_key"),
                            "rows": compact_rows,
                            "meta": _infer_result_set_meta(compact_rows, source_intent=state.get("intent_key"), user_query=state.get("user_input")),
                        }
                        self.redis.save_session(
                            state["session_id"],
                            {
                                **(sess or {}),
                                "last_result_set": latest_result_set,
                            },
                        )
                        if isinstance(sess, dict):
                            sess["last_result_set"] = latest_result_set
                    else:
                        derived_rows = self._extract_best_worst_rows_from_answer(
                            answer=state.get("answer") or "",
                            session_ctx=sess if isinstance(sess, dict) else {},
                        )
                        latest_result_set = {
                            "source_intent": state.get("intent_key"),
                            "rows": derived_rows,
                            "meta": _infer_result_set_meta(derived_rows, source_intent=state.get("intent_key"), user_query=state.get("user_input")) if derived_rows else {},
                        }
                        self.redis.save_session(
                            state["session_id"],
                            {
                                **(sess or {}),
                                "last_result_set": latest_result_set,
                            },
                        )
                        if isinstance(sess, dict):
                            sess["last_result_set"] = latest_result_set
        except StopIteration:
            pass
        except Exception:
            pass

        # Save to session
        sess = state.get("session_ctx") or {}
        # Never carry stale pending clarification flags across a completed turn.
        # Clarification state is set only in n_make_clarification when needed.
        if isinstance(sess, dict):
            sess.pop("pending_intent", None)
            sess.pop("missing_keys", None)
            sess.pop("clarification_options", None)
            sess.pop("pending_question", None)
            sess.pop("pending_slots", None)
            if latest_result_set is not None:
                sess["last_result_set"] = latest_result_set
        persisted_slots = self._build_persisted_slots(base=(sess.get("slots") or {}), updates=(state.get("slots") or {}))
        last_voyage_ids = None
        try:
            art = state.get("artifacts") or {}
            vids = art.get("voyage_ids") if isinstance(art, dict) else None
            if isinstance(vids, list) and vids:
                # de-dupe + cap
                last_voyage_ids = list(dict.fromkeys([str(v) for v in vids if v]))[:20]
        except Exception:
            last_voyage_ids = None
        self.redis.save_session(
            state["session_id"],
            {
                **sess,
                "last_intent": intent_key,
                "last_intent_key": intent_key,
                "memory_slots": self._extract_memory_slots(persisted_slots),
                "param_slots": self._extract_param_slots(persisted_slots),
                "slots": persisted_slots,
                "last_user_input": state.get("user_input"),
                "last_voyage_ids": last_voyage_ids,
                "last_plan_type": state.get("plan_type") or (state.get("plan") or {}).get("plan_type"),
                "_turn_marker": uuid.uuid4().hex,
                "_record_turn": self._build_turn_history_entry(
                    query=state.get("user_input") or "",
                    raw_user_input=state.get("raw_user_input") or state.get("user_input") or "",
                    intent_key=intent_key,
                    slots=persisted_slots,
                    answer=state.get("answer") or "",
                    plan_type=state.get("plan_type") or (state.get("plan") or {}).get("plan_type"),
                ),
            },
        )

        return state

    # =========================================================
    # Deterministic ranking/comparison fallback
    # =========================================================

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