# app/llm/llm_client.py

"""
LLM Client v8 — Registry-Driven Intent Classification
March 2026

Changes from v7:
- extract_intent_slots: Intent list now includes rich descriptions from
  INTENT_REGISTRY so the LLM can distinguish entity-anchored vs fleet-wide intents.
- _sanitize_slots: Added semantic vessel_name guard — rejects query-phrase
  fragments captured as vessel names (data-driven, not hardcoded patterns).
"""

from __future__ import annotations

import ast
import json
import os
import re
import re as _re
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time
from decimal import Decimal
from typing import Any, Dict, List, Optional

from groq import Groq

from app.core.request_context import get_request_session_id
from app.utils.ops_llm_shrink import shrink_ops_row_json_fields
from app.config.prompt_rules_loader import (
    get_answer_generation_fallback,
    get_answer_polish_system_prompt,
    get_answer_postprocess_replacements,
    get_default_sql_json_prompt,
    get_llm_answer_generation_system_prompt,
    get_llm_conversation_memory_label,
    get_llm_intent_classifier_system_prompt_template,
    get_llm_ops_only_voyage_answer_instruction,
    get_llm_ranking_answer_hint,
    get_out_of_scope_response_template,
)
from app.config.business_rules_loader import get_answer_contract_sections
from app.config.routing_rules_loader import (
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
    get_llm_metadata_override_blocking_metric_terms,
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
)
from app.core.logger import get_logger

logger = get_logger("llm_client")


def _should_use_voyage_metadata(
    user_input: str,
    intent_key: str,
    slots: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Returns True if the query references MongoDB-only voyage fields
    and the current intent is voyage.summary or voyage.detail.
    This override runs AFTER deterministic classification.
    """
    if intent_key not in ("voyage.summary", "voyage.detail"):
        return False
    voyage_numbers = (slots or {}).get("voyage_numbers")
    has_voyage_anchor = bool(
        (slots or {}).get("voyage_number")
        or (slots or {}).get("voyage_id")
        or (isinstance(voyage_numbers, list) and len(voyage_numbers) > 0)
    )
    if not has_voyage_anchor:
        return False
    text_lower = (user_input or "").lower()
    return any(field in text_lower for field in get_llm_mongo_only_voyage_fields())


# =========================================================
# CONFIG
# =========================================================

@dataclass
class LLMConfig:
    api_key: str
    model: str = "openai/oss-gpt-120b"
    temperature: float = 0.0


# =========================================================
# CLIENT
# =========================================================

class LLMClient:

    def __init__(self, config: LLMConfig):
        self.config = config
        self.client = Groq(api_key=config.api_key)
        self.sql_max_tokens = int(os.getenv("SQL_MAX_TOKENS", "1024"))

    # =========================================================
    # Deterministic intent router (before LLM)
    # =========================================================

    def _deterministic_intent_legacy(self, text: str) -> Optional[str]:
        t = text.lower()
        has_specific_voyage_anchor = bool(
            re.search(r"\bvoyage(?:s)?\s+\d{3,5}\b", t)
            or (re.search(r"\bfor\s+voyages?\b", t) and re.search(r"\b\d{3,5}\b", t))
        )
        looks_specific_vessel_performance = bool(
            re.search(r"\bstena\s+[a-z0-9][a-z0-9\-]*\b", t)
            and any(k in t for k in get_llm_vessel_performance_terms())
        )

        if has_specific_voyage_anchor and any(k in t for k in get_llm_voyage_resolved_vessel_metadata_terms()):
            return "vessel.metadata"

        if any(k in t for k in get_llm_average_voyage_duration_terms()) and any(
            k in t for k in get_llm_per_vessel_terms()
        ):
            return "aggregation.average"

        # FIRST: Delayed voyages with negative PnL / loss / root cause → finance.loss_due_to_delay (avoid mis-classification as port_query)
        if "delayed" in t and ("negative" in t and "pnl" in t or "negative pn" in t or "loss" in t or "root cause" in t):
            return "finance.loss_due_to_delay"

        # 1) Cargo-grade profitability (fleet aggregate) — must run before generic ranking
        # so grade-level aggregate asks don't get misrouted to ranking.pnl.
        cargo_grade_terms = get_llm_cargo_subject_terms()
        aggregate_terms = get_llm_cargo_aggregate_terms()
        cargo_frequency_terms = get_llm_cargo_frequency_terms()
        cargo_subject_is_primary = not any(
            k in t for k in get_llm_cargo_non_primary_subject_terms()
        )
        if cargo_subject_is_primary and any(k in t for k in cargo_grade_terms) and ("when-fixed" in t or "when fixed" in t or ("actual" in t and "variance" in t)):
            return "analysis.cargo_profitability"
        if cargo_subject_is_primary and any(k in t for k in cargo_grade_terms) and any(
            k in t for k in get_llm_cargo_profitability_metric_terms()
        ):
            return "analysis.cargo_profitability"
        if (
            cargo_subject_is_primary
            and any(k in t for k in cargo_grade_terms)
            and any(k in t for k in cargo_frequency_terms)
            and not any(k in t for k in get_llm_cargo_profitability_terms())
        ):
            return "ranking.cargo"
        if any(k in t for k in cargo_grade_terms) and any(k in t for k in get_llm_cargo_negative_profit_terms()):
            return "ranking.cargo"
        if cargo_subject_is_primary and any(k in t for k in cargo_grade_terms) and any(k in t for k in aggregate_terms):
            return "analysis.cargo_profitability"

        # Scenario comparison should win over plain voyage-vs-voyage comparison.
        if any(k in t for k in get_llm_scenario_comparison_terms()) or ("actual" in t and "variance" in t):
            return "analysis.scenario_comparison"

        # Explicit voyage-to-voyage comparison with 2+ voyage numbers.
        if any(k in t for k in get_llm_comparison_terms()):
            nums = re.findall(r"\b\d{3,5}\b", t)
            if len(nums) >= 2:
                return "comparison.voyages"

        # Voyage-anchored port listing should stay voyage-scoped.
        if re.search(r"\bvoyage\s+\d{3,5}\b", t) and any(
            k in t for k in get_llm_voyage_port_listing_terms()
        ):
            return "voyage.metadata"

        # 0) "Tell me about voyage 1901" / "voyage 1901 summary" → voyage summary
        if "voyage" in t and any(k in t for k in get_llm_entity_summary_terms()):
            if re.search(r"\bvoyage\s+\d{3,5}\b", t):
                return "voyage.summary"

        # 0) "Tell me about vessel ..." → vessel summary
        if ("vessel" in t or "ship" in t) and (
            any(k in t for k in get_llm_entity_summary_terms())
        ):
            return "vessel.summary"

        # 0a) Explicit entity-anchored vessel asks should stay vessel-scoped,
        # not fleet rankings (e.g., "For vessel X, show voyage-by-voyage trend...").
        # Keep this generic and avoid fleet phrases like "which vessel/per vessel/each vessel".
        if ("for vessel " in t or "for ship " in t) and not any(
            p in t for p in get_llm_vessel_fleet_exclusion_terms()
        ):
            return "vessel.summary"

        # 0) Commission ranking
        if ("commission" in t) and ("top" in t) and ("voyage" in t):
            return "ranking.voyages_by_commission"

        # 0b) Vessel screening: high voyage count + above-average profitability
        if ("high voyage count" in t or "many voyages" in t) and ("above-average" in t or "above average" in t) and ("profit" in t or "pnl" in t or "profitability" in t):
            return "ranking.vessels"

        # 1b) Vessel-level fleet aggregates should stay vessel-scoped, not voyage-scoped.
        if ("vessel" in t or "vessels" in t) and any(k in t for k in get_llm_vessel_ranking_signals()) and any(
            k in t for k in get_llm_vessel_metric_signals()
        ):
            return "ranking.vessels"

        # 2) Voyage-ranking phrasing should stay voyage-scoped even when extra output
        # fields like cargo grade / ports are requested.
        if any(k in t for k in get_llm_voyage_profitability_phrases()):
            return "ranking.voyages_by_pnl"

        # 2) Ranking by PnL
        if any(k in t for k in get_llm_ranking_order_terms()) and any(k in t for k in get_llm_profit_metric_terms()):
            return "ranking.pnl"

        # 2b) Ranking by revenue
        if any(k in t for k in get_llm_ranking_order_terms()) and "revenue" in t:
            return "ranking.revenue"

        # Emissions / climate metric rankings.
        if any(k in t for k in get_llm_emissions_terms()) and any(
            k in t for k in get_llm_emissions_ranking_terms()
        ):
            return "ranking.voyages"

        # Natural language voyage-performance phrasing.
        if any(k in t for k in get_llm_voyage_performance_phrases()):
            if not looks_specific_vessel_performance:
                return "ranking.voyages_by_pnl"
        if any(k in t for k in get_llm_voyage_extreme_phrases()) and not looks_specific_vessel_performance:
            return "ranking.voyages_by_pnl"

        # 4) Port-call ranking (voyages with most port calls / ports visited / port stops)
        if (
            any(k in t for k in get_llm_port_call_terms())
            and any(k in t for k in get_llm_port_call_ranking_terms())
        ) or ("visited the most ports" in t):
            return "ranking.port_calls"

        # 5) Offhire + financial impact
        if "offhire" in t and not has_specific_voyage_anchor:
            return "ops.delayed_voyages"

        # 6) Loss-making
        if "loss-making" in t or "loss making" in t:
            return "analysis.segment_performance"

        # 6b) High revenue but low/negative PnL
        if "high revenue" in t and ("low pnl" in t or "negative pnl" in t or ("low" in t and "pnl" in t)):
            return "analysis.high_revenue_low_pnl"

        # 6c) Module type: average PnL, most common cargo grades/ports
        if "module type" in t and ("average pnl" in t or "most common" in t or "cargo grades" in t or "cargo grade" in t or "ports" in t or "tc voyage" in t or "spot" in t):
            return "analysis.by_module_type"

        # 7) Top voyages (generic)
        if "top" in t and "voyage" in t:
            return "ranking.voyages"

        # 8) Fleet-wide port ranking — catch ALL variants before LLM sees them
        if "port" in t and any(k in t for k in get_llm_port_fleet_signals()):
            return "ranking.ports"
        # Broader catch: any combo of (most/common/frequent/busiest) + (visit*) + port
        if "port" in t and "most" in t and any(k in t for k in get_llm_port_broad_visit_terms()):
            return "ranking.ports"

        # 9) Voyage count per vessel (how many voyages does each/per vessel)
        if "voyage" in t and any(k in t for k in get_llm_voyage_count_per_vessel_terms()):
            return "aggregation.count"

        # 10) Fleet-level vessel aggregate/screening queries.
        explicit_single_vessel = bool(re.search(r"\b(?:vessel|ship)\s+[a-z0-9][a-z0-9\- ]{1,40}\b", t))
        if (
            ("vessel" in t or "vessels" in t)
            and any(k in t for k in get_llm_vessel_metadata_agg_terms())
            and not explicit_single_vessel
        ):
            return "ranking.vessel_metadata"
        if any(k in t for k in get_llm_fleet_vessel_terms()):
            return "ranking.vessels"
        if ("vessel" in t or "vessels" in t) and any(k in t for k in get_llm_generic_agg_terms()) and not explicit_single_vessel:
            return "ranking.vessels"

        # 11) Delay/waiting-centric ranking.
        if any(k in t for k in get_llm_delay_terms()) and any(
            k in t for k in get_llm_delay_ranking_terms()
        ):
            return "ops.offhire_ranking"

        return None

    def _deterministic_intent(self, text: str, session: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Deterministic routing (Phase 5A: session reserved for future structured priming)."""
        return self._deterministic_intent_legacy(text)

    @staticmethod
    def _compact_context_slots(slots: Any) -> Dict[str, Any]:
        if not isinstance(slots, dict):
            return {}
        keep = (
            "voyage_number", "voyage_numbers", "voyage_id",
            "vessel_name", "imo", "port_name",
            "scenario", "limit", "metric", "group_by",
            "cargo_grade", "cargo_grades",
        )
        out: Dict[str, Any] = {}
        for key in keep:
            value = slots.get(key)
            if value in (None, "", [], {}):
                continue
            out[key] = value
        return out

    def _recent_turns_for_context(self, session_context: Optional[Dict[str, Any]], *, limit: int = 3) -> List[Dict[str, Any]]:
        if not isinstance(session_context, dict):
            return []
        history = session_context.get("turn_history")
        if not isinstance(history, list):
            return []
        compact: List[Dict[str, Any]] = []
        for item in history[-limit:]:
            if not isinstance(item, dict):
                continue
            row: Dict[str, Any] = {}
            query = str(item.get("query") or "").strip()
            if query:
                row["query"] = query
            intent_key = str(item.get("intent_key") or "").strip()
            if intent_key:
                row["intent_key"] = intent_key
            slots = self._compact_context_slots(item.get("slots"))
            if slots:
                row["slots"] = slots
            headline = str(item.get("answer_headline") or "").strip()
            if headline:
                row["answer_headline"] = headline
            if row:
                compact.append(row)
        return compact

    def _conversation_memory_windows(self, session_context: Optional[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        turns = self._recent_turns_for_context(session_context, limit=5)
        if not turns:
            return {"hot": [], "warm": []}

        hot = turns[-3:]
        warm_rows: List[Dict[str, Any]] = []
        for item in turns[:-3]:
            row: Dict[str, Any] = {}
            intent_key = str(item.get("intent_key") or "").strip()
            if intent_key:
                row["intent_key"] = intent_key
            slots = self._compact_context_slots(item.get("slots"))
            if slots:
                row["slots"] = slots
            if row:
                warm_rows.append(row)
        return {"hot": hot, "warm": warm_rows}

    def _memory_windows_prompt(self, session_context: Optional[Dict[str, Any]]) -> str:
        windows = self._conversation_memory_windows(session_context)
        hot_turns = windows.get("hot") or []
        warm_turns = windows.get("warm") or []

        lines: List[str] = []
        if hot_turns:
            lines.append(get_llm_conversation_memory_label("hot_context_header"))
            for idx, item in enumerate(hot_turns, start=1):
                lines.append(get_llm_conversation_memory_label("hot_turn_label").format(index=idx))
                lines.append(get_llm_conversation_memory_label("user_label").format(value=item.get("query")))
                if item.get("intent_key"):
                    lines.append(get_llm_conversation_memory_label("intent_label").format(value=item.get("intent_key")))
                if item.get("slots"):
                    lines.append(get_llm_conversation_memory_label("anchors_slots_label").format(value=json.dumps(item.get("slots"), ensure_ascii=True)))
                if item.get("answer_headline"):
                    lines.append(get_llm_conversation_memory_label("prior_answer_summary_label").format(value=item.get("answer_headline")))
        else:
            lines.append(get_llm_conversation_memory_label("hot_context_empty"))

        lines.append("")
        if warm_turns:
            lines.append(get_llm_conversation_memory_label("warm_context_header"))
            for idx, item in enumerate(warm_turns, start=1):
                lines.append(get_llm_conversation_memory_label("warm_turn_label").format(index=idx))
                if item.get("intent_key"):
                    lines.append(get_llm_conversation_memory_label("intent_label").format(value=item.get("intent_key")))
                if item.get("slots"):
                    lines.append(get_llm_conversation_memory_label("anchors_label").format(value=json.dumps(item.get("slots"), ensure_ascii=True)))
        else:
            lines.append(get_llm_conversation_memory_label("warm_context_empty"))

        return "\n".join(lines)

    def _recent_turns_prompt(self, session_context: Optional[Dict[str, Any]], *, limit: int = 3) -> str:
        turns = self._recent_turns_for_context(session_context, limit=limit)
        if not turns:
            return get_answer_generation_fallback("no_recent_context")
        lines = []
        for idx, item in enumerate(turns, start=1):
            lines.append(get_llm_conversation_memory_label("turn_label").format(index=idx))
            lines.append(get_llm_conversation_memory_label("user_label").format(value=item.get("query")))
            if item.get("intent_key"):
                lines.append(get_llm_conversation_memory_label("intent_label").format(value=item.get("intent_key")))
            if item.get("slots"):
                lines.append(get_llm_conversation_memory_label("slots_label").format(value=json.dumps(item.get("slots"), ensure_ascii=True)))
            if item.get("answer_headline"):
                lines.append(get_llm_conversation_memory_label("prior_answer_summary_label").format(value=item.get("answer_headline")))
        return "\n".join(lines)

    # =========================================================
    # Extract intent and slots
    # =========================================================

    def extract_intent_slots(
        self,
        *,
        text: str,
        supported_intents: List[str],
        schema_hint: Optional[Dict[str, Any]] = None,
        session_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:

        # Normalize common apostrophe variants (incl. mojibake) to improve regex reliability.
        text_norm = (text or "")
        text_norm = (
            text_norm.replace("\u2019", "'")
            .replace("\u2018", "'")
            .replace("\u00e2\u20ac\u2122", "'")
            .replace("\u00e2\u20ac\u02dc", "'")
        )

        # 1️⃣ Deterministic override
        deterministic = self._deterministic_intent(text_norm, session_context)

        # 2️⃣ Regex slot extraction (always)
        slots: Dict[str, Any] = {}

        # Voyage numbers
        voyages = re.findall(r"\b\d{3,4}\b", text_norm)
        if voyages:
            slots["voyage_numbers"] = [int(v) for v in voyages]

        # IMO extraction (e.g. "IMO 9667485", "vessel IMO: 9667485")
        imo_match = re.search(r"\b(?:vessel\s+)?imo(?:\s*[:#-]?\s*|\s+)(\d{7,10})\b", text_norm, re.IGNORECASE)
        if imo_match:
            slots["imo"] = imo_match.group(1).strip()

        # Vessel extraction (safer)
        vessel_match = re.search(
            r"(?:vessel|ship)\s+"
            r"("
            r"[A-Za-z0-9][A-Za-z0-9\- ]{2,60}?"
            r")"
            r"(?="
            r"(?:\s+been\b|\s+is\b|\s+has\b|\s+doing\b|\s+performing\b|\s+recently\b|\s+last\b|\s+summary\b|\s+overview\b)"
            r"|[?.!,;:]|$"
            r")",
            text,
            re.IGNORECASE,
        )

        if vessel_match:
            cand = vessel_match.group(1).strip()
            cand = re.sub(r"(?:'s|'s)\s*$", "", cand).strip()
            # Ignore pseudo-names from fleet prompts such as
            # "Which vessel is fastest on ballast?"
            if not re.match(r"^(?:is|are|was|were|has|have|had|does|do|did|can|could|should|would)\b", cand, re.IGNORECASE):
                slots["vessel_name"] = cand
        else:
            phr_patterns = [
                r"(?:how\s+has|how\s+is)\s+([A-Za-z0-9][A-Za-z0-9\- ]{2,60}?)\s+been\b",
                r"\bof\s+([A-Za-z0-9][A-Za-z0-9\- ]{2,60}?)(?:'s|'s)\b",
                r"\bis\s+(?:vessel\s+)?([A-Za-z0-9][A-Za-z0-9\- ]{2,60}?)(?=\s+(?:doing|performing|good|bad)\b)",
                r"(?:quick\s+overview\s+of|overview\s+of)\s+([A-Za-z0-9][A-Za-z0-9\- ]{2,60}?)(?:[:?]|$)",
                r"(?:tell\s+me\s+about|give\s+me\s+details\s+about)\s+([A-Za-z0-9][A-Za-z0-9\- ]{2,60}?)(?:[?.!]|$)",
            ]
            for pat in phr_patterns:
                m = re.search(pat, text_norm, re.IGNORECASE)
                if m:
                    cand = m.group(1).strip().strip("\"'\u201c\u201d")
                    cand = re.sub(r"(?:'s|'s)\s*$", "", cand).strip()
                    if 2 <= len(cand) <= 60:
                        slots["vessel_name"] = cand
                        break

            # Extra heuristic: many vessel names in this dataset start with "Stena <Name>".
            if "vessel_name" not in slots:
                m = re.search(r"\b(stena\s+[A-Za-z0-9][A-Za-z0-9\-]*(?:\s+[A-Za-z0-9][A-Za-z0-9\-]*){0,2})\b", text_norm, re.IGNORECASE)
                if m:
                    slots["vessel_name"] = m.group(1).strip()

        # Limit (top N, or "N voyages")
        limit_match = re.search(r"top\s+(\d+)", text_norm.lower())
        if limit_match:
            slots["limit"] = int(limit_match.group(1))
        if "limit" not in slots:
            n_voyages = re.search(r"(\d+)\s+voyages", text_norm.lower())
            if n_voyages:
                slots["limit"] = min(int(n_voyages.group(1)), 50)

        # Port name from "visited X" / "called at X"
        port_visited = re.search(
            r"(?:visited|called at)\s+([A-Za-z0-9\-]+(?:\s+[A-Za-z0-9\-]+)*)",
            text_norm,
            re.IGNORECASE,
        )
        if port_visited:
            slots["port_name"] = port_visited.group(1).strip()

        # Keep direct port lookups port-scoped, but do not override voyage/vessel rankings
        # just because a port filter is present in the question.
        if not deterministic and slots.get("port_name"):
            tl_port = text_norm.lower()
            rankingish = any(k in tl_port for k in ("top", "rank", "ranking", "compare", "vs", "versus", "pnl", "revenue", "profit"))
            if ("visited" in tl_port or "called at" in tl_port) and not rankingish and not ("voyage" in tl_port or "voyages" in tl_port):
                deterministic = "ops.port_query"

        # Commission rankings
        if not deterministic:
            tl = text_norm.lower()
            if ("commission" in tl) and ("top" in tl) and ("voyage" in tl):
                deterministic = "ranking.voyages_by_commission"

        # Loss-making / what went wrong → segment_performance composite (needs breakdown + remarks)
        if not deterministic:
            tl = text_norm.lower()
            if ("loss-making" in tl) or ("loss making" in tl) or ("went wrong" in tl):
                deterministic = "analysis.segment_performance"

        tl = text_norm.lower()
        metadata_keywords = get_llm_metadata_keywords()

        # Metadata-first routing for vessel/voyage-anchored questions.
        # Allow metadata override when early deterministic pick is summary intent.
        fleet_wide_markers = get_llm_metadata_fleet_wide_markers()
        looks_fleet_wide = any(m in tl for m in fleet_wide_markers)
        blocks_metadata_override = any(k in tl for k in get_llm_metadata_override_blocking_metric_terms())

        if any(k in tl for k in metadata_keywords) and (
            not deterministic or deterministic in ("voyage.summary", "vessel.summary", "ranking.vessels")
        ) and not blocks_metadata_override:
            has_vessel_anchor = bool(slots.get("vessel_name") or slots.get("imo"))
            has_voyage_anchor = bool(slots.get("voyage_number") or slots.get("voyage_id"))
            vnums = slots.get("voyage_numbers")
            has_small_voyage_anchor = isinstance(vnums, list) and 1 <= len(vnums) <= 3
            if looks_fleet_wide:
                deterministic = "ranking.vessel_metadata"
            elif has_vessel_anchor or has_small_voyage_anchor:
                deterministic = "vessel.metadata" if has_vessel_anchor else "voyage.metadata"
            elif has_voyage_anchor:
                deterministic = "voyage.metadata"

        # If user asked about a specific vessel and it's not metadata, route to vessel.summary.
        if not deterministic and slots.get("vessel_name"):
            if any(
                k in tl
                for k in (
                    "profitability",
                    "pnl",
                    "tce",
                    "over time",
                    "trend",
                    "performing",
                    "doing well",
                    "poorly",
                    "overall",
                    "best",
                    "worst",
                    "recent",
                    "recently",
                    "last",
                    "captain",
                    "brief",
                    "route pattern",
                    "cargo pattern",
                    "remarks",
                )
            ) or ("voyage" in tl or "voyages" in tl):
                deterministic = "vessel.summary"

        # 3️⃣ If deterministic intent found → skip LLM
        if deterministic:
            intent_key = deterministic
            # RULE 1 + RULE 4:
            # - voyage.metadata only when a voyage anchor exists AND Mongo-only fields are asked.
            # - never fire voyage.metadata without voyage number/id anchor.
            # RULE 2:
            # - financial-only voyage questions (PnL/revenue/expense/TCE/performance totals) stay on voyage.summary
            #   because they do not match Mongo-only field keywords.
            # RULE 3:
            # - mixed question (metadata + financial) routes to voyage.metadata (single intent, no split).
            if _should_use_voyage_metadata(text_norm, intent_key, slots):
                intent_key = "voyage.metadata"
            # ─────────────────────────────────────────────────────────────────
            logger.debug(f"[intent_routing] final intent: {intent_key}")
            return {
                "intent_key": intent_key,
                "slots": self._sanitize_slots(slots),
                "is_followup": False,
                "inherit_slots_from_session": [],
                "backward_reference": False,
                "followup_confidence": "low",
            }

        # =========================================================
        # 4️⃣ LLM CALL — Build description-rich intent list from INTENT_REGISTRY
        # =========================================================
        # CHANGE 1: Import registry and inject descriptions into the prompt so
        # the LLM can distinguish entity-anchored vs fleet-wide aggregate intents.
        # No hardcoding — descriptions come purely from the registry.
        # =========================================================
        from app.registries.intent_loader import get_yaml_registry_facade

        INTENT_REGISTRY = get_yaml_registry_facade(validate_parity=True)["INTENT_REGISTRY"]

        intent_lines = []
        for i in supported_intents:
            cfg = INTENT_REGISTRY.get(i, {})
            desc = cfg.get("description", "")
            required = cfg.get("required_slots", [])
            route = cfg.get("route", "single")
            line = f"- {i}: {desc}"
            if required:
                line += f" | requires slots: {required}"
            if route == "composite":
                line += " | FLEET-WIDE: no specific entity slot needed or expected"
            intent_lines.append(line)

        intents_formatted = "\n".join(intent_lines)
        recent_context = self._memory_windows_prompt(session_context)

        system = get_llm_intent_classifier_system_prompt_template().format(
            recent_context=recent_context,
            intents_formatted=intents_formatted,
        )

        result = self._call_with_retry(
            system=system,
            user=json.dumps({"query": text_norm}),
            operation="intent_extraction",
        )

        if not result or not isinstance(result, dict):
            return {
                "intent_key": "out_of_scope",
                "slots": slots,
                "is_followup": False,
                "inherit_slots_from_session": [],
                "backward_reference": False,
                "followup_confidence": "low",
            }

        intent = result.get("intent_key", "out_of_scope")
        llm_slots = result.get("slots", {}) or {}
        is_followup = bool(result.get("is_followup"))
        inherit_slots = result.get("inherit_slots_from_session")
        inherit_slots = inherit_slots if isinstance(inherit_slots, list) else []
        inherit_slots = [str(k).strip() for k in inherit_slots if str(k).strip()]
        backward_reference = bool(result.get("backward_reference"))
        followup_confidence = str(result.get("followup_confidence") or "").strip().lower()
        if followup_confidence not in ("high", "medium", "low"):
            followup_confidence = "low"

        # Merge regex + llm slots (regex wins)
        llm_slots.update({k: v for k, v in slots.items()})
        clean_slots = self._sanitize_slots(llm_slots)
        ql = text_norm.lower()
        rankingish_terms = ("top", "rank", "ranking", "compare", "vs", "versus")

        # 4b) Post-LLM correction: ops.port_query with "negative PnL" (or similar) as port_name is wrong
        if intent == "ops.port_query" and clean_slots.get("port_name"):
            pn = str(clean_slots.get("port_name", "")).strip().lower()
            if "pnl" in pn or "negative" in pn or pn in ("revenue", "expense", "tce"):
                intent = "finance.loss_due_to_delay"
                clean_slots = {k: v for k, v in clean_slots.items() if k != "port_name"}
            elif any(k in ql for k in rankingish_terms) and ("voyage" in ql or "voyages" in ql):
                if "commission" in ql:
                    intent = "ranking.voyages_by_commission"
                elif "revenue" in ql and "pnl" not in ql and "profit" not in ql:
                    intent = "ranking.voyages_by_revenue"
                else:
                    intent = "ranking.voyages_by_pnl"

        voyage_numbers_list = clean_slots.get("voyage_numbers") if isinstance(clean_slots.get("voyage_numbers"), list) else []
        has_single_voyage_anchor = bool(clean_slots.get("voyage_number")) or len(voyage_numbers_list) == 1
        asks_voyage_details = any(
            k in ql
            for k in (
                "what does the data show",
                "summary",
                "incident",
                "financial",
                "pnl",
                "revenue",
                "expense",
                "tce",
                "offhire",
                "ports",
                "remarks",
                "route",
                "cargo grade",
            )
        )
        if has_single_voyage_anchor and asks_voyage_details and not any(k in ql for k in rankingish_terms):
            if any(k in ql for k in metadata_keywords):
                intent = "voyage.metadata"
            else:
                intent = "voyage.summary"

        if "module type" in ql and any(k in ql for k in ("average pnl", "average revenue", "voyage count", "most common cargo grade", "most common cargo grades")):
            intent = "analysis.by_module_type"

        if intent == "ranking.cargo" and any(
            k in ql for k in (
                "average pnl", "avg pnl", "average revenue", "avg revenue",
                "most profitable", "profitable overall", "highest average pnl", "best performing cargo",
            )
        ):
            intent = "analysis.cargo_profitability"

        if intent == "ranking.pnl" and any(
            k in ql for k in ("most profitable voyages", "top profitable voyages", "top 5 most profitable voyages", "top 10 most profitable voyages")
        ):
            intent = "ranking.voyages_by_pnl"

        if clean_slots.get("vessel_name") and any(
            k in ql for k in ("overall", "performing", "voyage history", "over time", "trend", "best voyage", "worst voyage")
        ) and not any(k in ql for k in ("top vessels", "which vessels", "across all vessels", "fleet")):
            intent = "vessel.summary"

        # 5️⃣ Recovery for common "false out_of_scope" cases.
        # If we have strong entity slots, do not allow out_of_scope to block a valid answer.
        if intent == "out_of_scope":
            if clean_slots.get("vessel_name"):
                ql = text_norm.lower()
                intent = "vessel.metadata" if any(k in ql for k in metadata_keywords) else "vessel.summary"
            elif clean_slots.get("voyage_number"):
                ql = text_norm.lower()
                intent = "voyage.metadata" if any(k in ql for k in metadata_keywords) else "voyage.summary"
            elif clean_slots.get("voyage_numbers"):
                ql = text_norm.lower()
                intent = "voyage.metadata" if any(k in ql for k in metadata_keywords) else "voyage.summary"

        # cargo_grades post-processing — no hardcoding, LLM extracted these
        if clean_slots.get("cargo_grade") and not clean_slots.get("cargo_grades"):
            clean_slots["cargo_grades"] = [str(clean_slots.get("cargo_grade")).strip().lower()]

        if clean_slots.get("cargo_grades"):
            clean_slots["cargo_grades"] = [
                str(g).strip().lower()
                for g in clean_slots["cargo_grades"]
                if g
            ]
            if clean_slots["cargo_grades"] and not clean_slots.get("cargo_grade"):
                clean_slots["cargo_grade"] = clean_slots["cargo_grades"][0]

        if (
            clean_slots.get("cargo_grades")
            and not clean_slots.get("voyage_number")
            and not clean_slots.get("voyage_numbers")
            and not clean_slots.get("voyage_id")
        ):
            clean_slots["likely_path"] = "cargo_grade_ops"

        intent_key = intent
        # RULE 1 + RULE 4:
        # - voyage.metadata only when a voyage anchor exists AND Mongo-only fields are asked.
        # - never fire voyage.metadata without voyage number/id anchor.
        # RULE 2:
        # - financial-only voyage questions stay as voyage.summary when no Mongo-only field keywords are present.
        # RULE 3:
        # - if both metadata and financial keywords exist, choose voyage.metadata (single route).
        if _should_use_voyage_metadata(text_norm, intent_key, clean_slots):
            intent_key = "voyage.metadata"
        # ─────────────────────────────────────────────────────────────────
        logger.debug(f"[intent_routing] final intent: {intent_key}")
        return {
            "intent_key": intent_key,
            "slots": clean_slots,
            "is_followup": is_followup,
            "inherit_slots_from_session": inherit_slots,
            "backward_reference": backward_reference,
            "followup_confidence": followup_confidence,
        }

    # =========================================================
    # Slot sanitization
    # =========================================================

    def _sanitize_slots(self, slots: Dict[str, Any]) -> Dict[str, Any]:

        clean: Dict[str, Any] = {}

        # voyage_numbers
        if "voyage_numbers" in slots:
            try:
                vns = slots["voyage_numbers"]
                if not isinstance(vns, list):
                    vns = [vns]
                clean["voyage_numbers"] = [
                    int(float(v)) for v in vns if str(v).isdigit()
                ]
            except Exception:
                pass

        # limit
        if "limit" in slots:
            try:
                limit = int(float(slots["limit"]))
                clean["limit"] = max(1, min(limit, 50))
            except Exception:
                pass

        # vessel_name
        if "vessel_name" in slots:
            name = str(slots["vessel_name"]).strip()
            # Trim trailing query phrases accidentally captured as part of vessel name.
            name = re.sub(
                r"\b(?:operating status|operational status|operational|operating|is operating|is operational|status|passage type|passage types|hire rate|hirerate|account code|market type|scrubber|tags|contract history|highest|lowest|top|most|least|best|worst|fastest|longest|average|avg|voyage count|on ballast|on laden)\b.*$",
                "",
                name,
                flags=re.IGNORECASE,
            ).strip()
            name = re.sub(r"\s{2,}", " ", name).strip()
            if 2 <= len(name) <= 60:
                generic_placeholders = {
                    "name", "names", "vessel", "ship", "vessel name", "ship name",
                    "cargo", "cargo grade", "grade", "module", "module type",
                }
                if name.lower() in generic_placeholders:
                    pass
                else:
                    # =========================================================
                    # CHANGE 2: Registry-driven semantic guard for vessel_name.
                    # Rejects names that are clearly metric/query phrases, not
                    # real vessel names. Driven by INTENT_REGISTRY allowed_slots —
                    # if the active intent doesn't expect vessel_name, this acts as
                    # a last-resort safety net. No hardcoded phrase lists.
                    # =========================================================
                    name_lower = name.lower()
                    # Pull all slot keys that ranking/aggregation/fleet-wide intents
                    # declare as their metrics — anything matching those patterns
                    # in a vessel_name value means it was mis-extracted.
                    from app.registries.intent_loader import get_yaml_registry_facade

                    INTENT_REGISTRY = get_yaml_registry_facade(validate_parity=True)["INTENT_REGISTRY"]
                    fleet_intents_with_metric = [
                        cfg for cfg in INTENT_REGISTRY.values()
                        if cfg.get("route") == "composite"
                        and "metric" in cfg.get("optional_slots", [])
                    ]
                    # Build a set of common metric-related terms from all optional_slot
                    # keys across fleet-wide intents (e.g. "metric", "group_by", "filter")
                    fleet_slot_keys = set()
                    for cfg in fleet_intents_with_metric:
                        fleet_slot_keys.update(cfg.get("optional_slots", []))

                    # If the vessel_name value contains words that are metric slot
                    # keys or common aggregation phrasing, reject it.
                    # This is data-driven: as you add more optional_slot keys to
                    # fleet-wide intents in the registry, they're automatically covered.
                    aggregation_terms = fleet_slot_keys | {
                        "most", "highest", "lowest", "best", "worst",
                        "average", "total", "count", "number of",
                        "frequent", "common", "active", "profitable",
                        "earning", "earned", "performing", "ranked",
                        "fastest", "longest", "ballast", "laden", "scrubber",
                        "operating", "contract duration", "voyage count",
                    }
                    if any(term in name_lower for term in aggregation_terms):
                        pass  # drop — this is a metric phrase, not a vessel name
                    else:
                        clean["vessel_name"] = name

        # imo
        if "imo" in slots:
            imo = str(slots["imo"]).strip()
            if imo.isdigit() and 7 <= len(imo) <= 10:
                clean["imo"] = imo

        # port_name (for ops.port_query) — reject values that are clearly not port names (e.g. "negative PnL")
        if "port_name" in slots:
            name = str(slots["port_name"]).strip()
            name_lower = name.lower()
            # Do not treat PnL/finance phrases as port names
            if name_lower in ("negative pnl", "negative pn", "pnl", "revenue", "expense", "tce"):
                pass  # drop port_name
            elif "pnl" in name_lower or "revenue" in name_lower or "expense" in name_lower:
                pass  # drop
            elif 1 <= len(name) <= 80:
                clean["port_name"] = name

        # cargo_grades
        if "cargo_grades" in slots:
            try:
                vals = slots["cargo_grades"]
                if not isinstance(vals, list):
                    vals = [vals]
                clean_grades = []
                seen = set()
                for v in vals:
                    s = str(v).strip().lower()
                    if s and s not in seen:
                        seen.add(s)
                        clean_grades.append(s)
                if clean_grades:
                    clean["cargo_grades"] = clean_grades
            except Exception:
                pass

        # cargo_grade (compat with intents requiring singular slot)
        if "cargo_grade" in slots:
            cg = str(slots.get("cargo_grade") or "").strip().lower()
            if cg:
                clean["cargo_grade"] = cg
                if "cargo_grades" not in clean:
                    clean["cargo_grades"] = [cg]

        # likely_path hint (used by planner/router for specialized branches)
        if "likely_path" in slots:
            lp = str(slots.get("likely_path") or "").strip()
            if lp:
                clean["likely_path"] = lp

        return clean

    # =========================================================
    # SQL generation (safe wrapper)
    # =========================================================

    def generate_sql(
        self,
        *,
        question: str,
        intent_key: str,
        slots: Dict[str, Any],
        schema_hint: Dict[str, Any],
        agent: str,
        system_prompt: Optional[str] = None,
        temperature: float = 0,
        error_hint: Optional[str] = None,
        **kwargs,
    ) -> Dict[str, Any]:

        system = system_prompt or get_default_sql_json_prompt()

        raw = self._call_with_retry(
            system=system,
            user=json.dumps({
                "question": question,
                "intent": intent_key,
                "slots": slots,
                "schema_hint": schema_hint,
                "agent": agent,
            }),
            operation=f"sql_generation_{agent}",
            temperature=temperature,
            max_tokens=kwargs.get("max_tokens", getattr(self, "sql_max_tokens", 1024)),
            return_string=True,
        )

        if not raw:
            return {
                "sql": "SELECT 1 WHERE 1=0 LIMIT 1",
                "params": {},
                "tables": [],
                "confidence": 0.0,
            }

        cleaned = str(raw).strip()
        cleaned = re.sub(r"^```.*?\n", "", cleaned)
        cleaned = cleaned.replace("```", "")

        parsed = self._safe_json_load(cleaned, fallback=None)
        if isinstance(parsed, dict) and "sql" in parsed:
            result = parsed
        else:
            result = {
                "sql": cleaned,
                "params": {},
                "tables": [],
                "confidence": 0.9,
            }

        result.setdefault("params", {})
        result.setdefault("tables", [])
        result.setdefault("confidence", 0.9)

        result["sql"] = result["sql"].strip().rstrip(";")

        return result

    # =========================================================
    # Answer generation
    # =========================================================

    def generate_final_answer(
        self,
        *,
        question: str,
        merged_data: Dict[str, Any],
        session_context: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Alias function designed specifically for the voyage.summary override logic."""
        return self.summarize_answer(
            question=question,
            plan={"plan_type": "single", "intent_key": "voyage.summary"},
            merged=merged_data,
            session_context=session_context,
        )

    def summarize_answer(
        self,
        *,
        question: str,
        plan: Dict[str, Any],
        merged: Dict[str, Any],
        session_context: Optional[Dict[str, Any]] = None,
    ) -> str:

        intent_key = ""
        if isinstance(plan, dict):
            intent_key = str(plan.get("intent_key") or "").strip()

        # Graceful handling for out-of-scope / chit-chat queries.
        if intent_key == "out_of_scope":
            q = (question or "").strip()
            q_lower = q.lower()

            if q_lower in get_llm_out_of_scope_greeting_exact() or q_lower.startswith(get_llm_out_of_scope_greeting_prefixes()):
                return get_out_of_scope_response_template("greeting")

            if any(p in q_lower for p in get_llm_out_of_scope_identity_phrases()):
                return get_out_of_scope_response_template("identity")

            if any(k in q_lower for k in get_llm_out_of_scope_weather_keywords()):
                return get_out_of_scope_response_template("weather")
            return get_out_of_scope_response_template("default")

        merged = self._truncate_merged_data(merged, max_rows=10)
        merged_safe = self._convert_to_json_safe(merged)
        merged_rows = None
        if isinstance(merged_safe, dict):
            artifacts = merged_safe.get("artifacts")
            if isinstance(artifacts, dict) and isinstance(artifacts.get("merged_rows"), list):
                merged_rows = artifacts.get("merged_rows") or []

        # Strong hint for ranking intents so the model includes PnL/Revenue in the table
        ranking_hint = None
        if intent_key and str(intent_key).startswith("ranking.") and merged_rows:
            ranking_hint = get_llm_ranking_answer_hint()

        ops_only_hint = None
        if intent_key == "voyage.summary" and isinstance(merged_safe, dict):
            art = merged_safe.get("artifacts")
            if isinstance(art, dict) and art.get("finance_kpi_unavailable") is True:
                t = get_llm_ops_only_voyage_answer_instruction().strip()
                if t:
                    ops_only_hint = t

        style = self._derive_answer_style(question=question, intent_key=intent_key)

        if intent_key == "voyage.summary" and isinstance(merged_safe, dict) and not ops_only_hint:
            deterministic = self._build_deterministic_voyage_summary(
                question=question,
                merged_safe=merged_safe,
                style=style,
            )
            if deterministic:
                return deterministic

        recent_context_turns = (
            []
            if intent_key == "voyage.summary"
            else self._recent_turns_for_context(session_context, limit=2)
        )
        recent_context_text = (
            ""
            if intent_key == "voyage.summary"
            else self._recent_turns_prompt(session_context, limit=2)
        )

        system = get_llm_answer_generation_system_prompt()

        instruction_parts: List[str] = []
        if ranking_hint:
            instruction_parts.append(ranking_hint)
        if ops_only_hint:
            instruction_parts.append(ops_only_hint)
        combined_instruction = "\n\n".join(instruction_parts) if instruction_parts else None

        result = self._call_with_retry(
            system=system,
            user=json.dumps(
                {
                    "question": question,
                    "plan": plan,
                    "intent_key": intent_key,
                    "recent_context": recent_context_turns,
                    "recent_context_text": recent_context_text,
                    "data": {**(merged_safe if isinstance(merged_safe, dict) else {}), "style": style},
                    "merged_rows": merged_rows,
                    "business_answer_contract": get_answer_contract_sections(),
                    **({"instruction": combined_instruction} if combined_instruction else {}),
                }
            ),
            operation="answer_generation",
            return_string=True,
        )

        polished = self._polish_answer_if_needed(
            question=question,
            intent_key=intent_key,
            plan=plan,
            merged_safe=(merged_safe if isinstance(merged_safe, dict) else {}),
            merged_rows=(merged_rows if isinstance(merged_rows, list) else None),
            style=style,
            draft=(result or ""),
        )

        cleaned = self._postprocess_answer_markdown(
            polished,
            intent_key=intent_key,
            style=style,
            merged_safe=(merged_safe if isinstance(merged_safe, dict) else {}),
        )
        cleaned = self._ensure_ranking_voyages_answer(
            text=cleaned,
            intent_key=intent_key,
            merged_safe=(merged_safe if isinstance(merged_safe, dict) else {}),
        )
        cleaned = self._ensure_ranking_vessels_answer(
            text=cleaned,
            intent_key=intent_key,
            merged_safe=(merged_safe if isinstance(merged_safe, dict) else {}),
        )
        cleaned = self._ensure_cargo_profitability_answer(
            text=cleaned,
            intent_key=intent_key,
            merged_safe=(merged_safe if isinstance(merged_safe, dict) else {}),
        )
        for old, new in get_answer_postprocess_replacements():
            cleaned = cleaned.replace(old, new)
        cleaned = _enforce_table_rules(cleaned)
        return cleaned if cleaned else get_answer_generation_fallback("empty_answer")

    def _polish_answer_if_needed(
        self,
        *,
        question: str,
        intent_key: str,
        plan: Dict[str, Any],
        merged_safe: Dict[str, Any],
        merged_rows: Optional[List[Any]],
        style: Dict[str, Any],
        draft: str,
    ) -> str:
        text = (draft or "").strip()
        if not text:
            return ""

        if intent_key == "voyage.summary":
            return text

        system = get_answer_polish_system_prompt()

        user = {
            "question": question,
            "intent_key": intent_key,
            "plan": plan,
            "style": style,
            "data": merged_safe,
            "merged_rows": merged_rows,
            "draft_answer": text,
        }

        rewritten = self._call_with_retry(
            system=system,
            user=json.dumps(user, ensure_ascii=False),
            operation="answer_polish",
            return_string=True,
        )

        return (rewritten or text).strip()

    def _derive_answer_style(self, *, question: str, intent_key: str) -> Dict[str, Any]:
        q = (question or "").strip()
        ql = q.lower()

        narrative_triggers = get_llm_answer_narrative_triggers()
        narrative_summary = any(t in ql for t in narrative_triggers)

        financial_first = "financial summary" in ql or (
            any(k in ql for k in get_llm_answer_financial_first_terms())
            and not narrative_summary
        )

        ask_ports = any(k in ql for k in get_llm_answer_port_terms())
        ask_grades = any(k in ql for k in get_llm_answer_grade_terms())
        ask_remarks = any(k in ql for k in get_llm_answer_remark_terms()) or narrative_summary

        return {
            "intent_key": intent_key,
            "narrative_summary": bool(narrative_summary),
            "financial_first": bool(financial_first),
            "ask_ports": bool(ask_ports),
            "ask_grades": bool(ask_grades),
            "ask_remarks": bool(ask_remarks),
        }

    def _postprocess_answer_markdown(
        self,
        text: str,
        *,
        intent_key: str,
        style: Dict[str, Any],
        merged_safe: Dict[str, Any],
    ) -> str:
        s = (text or "").strip()
        if not s:
            return ""

        # Normalize bullets: enforce '-' (not '*')
        s = re.sub(r"(?m)^\*\s+", "- ", s)

        # Drop consecutive duplicate lines (common LLM glitch)
        lines = s.splitlines()
        dedup: List[str] = []
        for line in lines:
            if dedup and line.strip() and line.strip() == dedup[-1].strip():
                continue
            dedup.append(line)

        # Drop repeated sections entirely (another common glitch)
        out: List[str] = []
        seen_headings: set = set()
        i = 0
        while i < len(dedup):
            line = dedup[i]
            if re.match(r"^###\s+\S", line.strip()):
                heading = line.strip()
                if heading in seen_headings:
                    i += 1
                    while i < len(dedup) and not re.match(r"^###\s+\S", dedup[i].strip()):
                        i += 1
                    continue
                seen_headings.add(heading)
            out.append(line)
            i += 1

        s = "\n".join(out).strip()

        if intent_key == "voyage.summary" and style.get("narrative_summary") is True:
            s = self._ensure_voyage_narrative_summary(s, merged_safe=merged_safe)
        if intent_key == "voyage.summary":
            s = self._ensure_voyage_identity_line(s, merged_safe=merged_safe)


        # Eject sentence-rows from markdown tables (LLM glitch: puts conclusions inside table)
        def _is_sentence_cell(cell: str) -> bool:
            c = cell.strip()
            return len(c) > 40 and (' ' in c) and not c.replace('.','').replace(',','').replace('$','').replace('-','').replace('%','').replace('(','').replace(')','').replace(' ','').isalnum() == False and any(w in c.lower() for w in ['this ', 'the ', 'had ', 'has ', 'was ', 'were ', 'with ', 'among ', 'highest', 'lowest', 'most ', 'voyage '])
        table_lines = s.splitlines()
        result_lines = []
        ejected = []
        in_table = False
        for tl in table_lines:
            stripped = tl.strip()
            if stripped.startswith('|') and stripped.endswith('|'):
                in_table = True
                parts = [p.strip() for p in stripped.strip('|').split('|')]
                if parts and _is_sentence_cell(parts[0]) and all(not p.strip() for p in parts[1:]):
                    ejected.append('> ' + parts[0].strip())
                    continue
            else:
                if in_table and ejected:
                    result_lines.extend(ejected)
                    result_lines.append('')
                    ejected = []
                in_table = False
            result_lines.append(tl)
        if ejected:
            result_lines.extend(ejected)
        s = '\n'.join(result_lines).strip()


        # ── Eject conclusion sentences out of table rows ──
        _result, _ejected = [], []
        for _tl in s.splitlines():
            _stripped = _tl.strip()
            if _stripped.startswith('|') and _stripped.endswith('|'):
                _cells = [_c.strip() for _c in _stripped.strip('|').split('|')]
                _first = _cells[0] if _cells else ''
                _rest_empty = all(not _c for _c in _cells[1:])
                _looks_sentence = len(_first) > 25 and ' ' in _first and not _first.startswith('$') and not re.match(r'^[\d,\.\-\$\%\|]+$', _first)
                if _looks_sentence and _rest_empty:
                    _ejected.append(_first)
                    continue
            else:
                if _ejected:
                    _result.append('')
                    for _e in _ejected:
                        _result.append('_' + _e + '_')
                    _result.append('')
                    _ejected = []
            _result.append(_tl)
        if _ejected:
            _result.append('')
            for _e in _ejected:
                _result.append('_' + _e + '_')
        s = '\n'.join(_result).strip()

        # Readability cleanup for common LLM formatting glitches in narrative text:
        # - broken numeric grouping: "1, 878, 032" -> "1,878,032"
        # - compacted ranges: "123to456" -> "123 to 456"
        # - compacted number+word boundaries: "2301and" -> "2301 and"
        s = self._normalize_readability_text(s)

        return s.strip()

    def _ensure_ranking_voyages_answer(self, *, text: str, intent_key: str, merged_safe: Dict[str, Any]) -> str:
        """
        Deterministic safety net for ranking.voyages answers.
        If merged_rows contain KPI data but the LLM response still claims metrics are unavailable,
        rebuild a concise table directly from merged_rows.
        """
        ik = str(intent_key or "")
        if not ik.startswith("ranking.") or ik == "ranking.vessels":
            return text

        artifacts = merged_safe.get("artifacts") if isinstance(merged_safe, dict) else {}
        rows = artifacts.get("merged_rows") if isinstance(artifacts, dict) else None
        if not isinstance(rows, list):
            rows = []

        def _has_metric(v: Any) -> bool:
            return v not in (None, "", "Not available", "not available", "N/A", "n/a")

        def _normalize_row(r: Dict[str, Any]) -> Dict[str, Any]:
            fin = r.get("finance") if isinstance(r.get("finance"), dict) else {}
            return {
                "voyage_id": r.get("voyage_id") or fin.get("voyage_id"),
                "voyage_number": r.get("voyage_number") or fin.get("voyage_number"),
                "vessel_name": r.get("vessel_name") or fin.get("vessel_name"),
                "port_calls": r.get("port_calls") if r.get("port_calls") not in (None, "") else (r.get("port_count") if r.get("port_count") not in (None, "") else fin.get("port_calls")),
                "pnl": r.get("pnl") if r.get("pnl") not in (None, "") else fin.get("pnl"),
                "revenue": r.get("revenue") if r.get("revenue") not in (None, "") else fin.get("revenue"),
                "total_expense": r.get("total_expense") if r.get("total_expense") not in (None, "") else fin.get("total_expense"),
                "tce": r.get("tce") if r.get("tce") not in (None, "") else fin.get("tce"),
                "total_commission": r.get("total_commission") if r.get("total_commission") not in (None, "") else fin.get("total_commission"),
                "key_ports": r.get("key_ports"),
                "cargo_grades": r.get("cargo_grades"),
                "remarks": r.get("remarks"),
            }

        normalized_rows = [_normalize_row(r) for r in rows if isinstance(r, dict)]
        has_kpi_data = any(
            isinstance(r, dict) and (
                _has_metric(r.get("pnl"))
                or _has_metric(r.get("revenue"))
                or _has_metric(r.get("total_expense"))
                or _has_metric(r.get("port_calls"))
            )
            for r in normalized_rows
        )
        if not has_kpi_data:
            finance_rows = (merged_safe.get("finance") or {}).get("rows") if isinstance(merged_safe.get("finance"), dict) else None
            if isinstance(finance_rows, list) and finance_rows:
                normalized_rows = [
                    {
                        "voyage_id": fr.get("voyage_id"),
                        "voyage_number": fr.get("voyage_number"),
                        "vessel_name": fr.get("vessel_name"),
                        "port_calls": fr.get("port_calls") if fr.get("port_calls") not in (None, "") else fr.get("port_count"),
                        "pnl": fr.get("pnl"),
                        "revenue": fr.get("revenue"),
                        "total_expense": fr.get("total_expense"),
                        "tce": fr.get("tce"),
                        "total_commission": fr.get("total_commission"),
                        "key_ports": [],
                        "cargo_grades": [],
                        "remarks": [],
                    }
                    for fr in finance_rows if isinstance(fr, dict)
                ]
                has_kpi_data = any(
                    _has_metric(r.get("pnl"))
                    or _has_metric(r.get("revenue"))
                    or _has_metric(r.get("total_expense"))
                    or _has_metric(r.get("port_calls"))
                    for r in normalized_rows
                )
        if not has_kpi_data:
            return text

        low = (text or "").lower()
        if "not available" not in low:
            return text
        # If the model returned a generic fallback despite data rows, rebuild.
        if low.strip() in ("not available in dataset.", "not available in dataset"):
            pass
        elif not any(k in low for k in ("pnl", "revenue", "total expense", "financial metrics", "port count", "port calls")):
            return text

        def _fmt_num(v: Any) -> str:
            s = self._fmt_usd(v)
            return s if s is not None else "Not available"

        def _fmt_ports(v: Any) -> str:
            if isinstance(v, list):
                out: List[str] = []
                for p in v:
                    if isinstance(p, dict):
                        pn = str(p.get("portName") or p.get("port_name") or "").strip()
                        at = str(p.get("activityType") or p.get("activity_type") or "").strip()
                        if pn:
                            out.append(f"{pn} ({at})" if at else pn)
                    elif p is not None:
                        s = str(p).strip()
                        if s:
                            out.append(s)
                out = out[:8]
                return ", ".join(out) if out else "Not available"
            return "Not available"

        def _fmt_grades(v: Any) -> str:
            if isinstance(v, list):
                out: List[str] = []
                for g in v:
                    if isinstance(g, dict):
                        gv = g.get("gradeName") or g.get("grade_name") or g.get("name")
                        if gv is not None:
                            s = str(gv).strip()
                            if s and s.lower() not in ("none", "null", "n/a"):
                                out.append(s)
                    elif g is not None:
                        s = str(g).strip()
                        if s and s.lower() not in ("none", "null", "n/a"):
                            out.append(s)
                out = list(dict.fromkeys(out))[:8]
                return ", ".join(out) if out else "Not available"
            return "Not available"

        def _fmt_remarks(v: Any) -> str:
            if isinstance(v, list):
                out: List[str] = []
                for r in v:
                    if isinstance(r, dict):
                        rv = r.get("remark")
                        if rv is not None:
                            s = str(rv).strip()
                            if s:
                                out.append(s)
                    elif r is not None:
                        s = str(r).strip()
                        if s:
                            out.append(s)
                return out[0] if out else "Not available"
            if isinstance(v, str) and v.strip():
                return v.strip()
            return "Not available"

        primary_metric = "pnl"
        descending = not any(k in low for k in ("lowest", "least", "worst"))
        if "revenue" in low and "pnl" not in low and "profit" not in low:
            primary_metric = "revenue"
        elif "commission" in low:
            primary_metric = "total_commission"
        elif "tce" in low:
            primary_metric = "tce"

        def _metric_sort_value(r: Dict[str, Any]) -> tuple:
            raw = r.get(primary_metric)
            try:
                val = float(raw)
            except Exception:
                val = None
            if descending:
                return (val is None, -(val or 0.0))
            return (val is None, (val if val is not None else float("inf")))

        normalized_rows.sort(key=_metric_sort_value)
        top = normalized_rows[:10]
        table_lines = [
            "### Summary",
            f"- Ranked the top {len(top)} voyages using available finance + ops data.",
            "- Included all available requested metrics such as PnL, revenue, total expense, port calls, ports, cargo grades, and remarks.",
            "",
            "### Results",
            "| Voyage # | Vessel Name | Port Calls | PnL | Revenue | Total expense | Key ports | Cargo grades | Remarks |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for r in top:
            if not isinstance(r, dict):
                continue
            vn = r.get("voyage_number") if r.get("voyage_number") is not None else "Not available"
            vessel_name = r.get("vessel_name") if r.get("vessel_name") not in (None, "") else "Not available"
            port_calls = r.get("port_calls") if r.get("port_calls") not in (None, "") else "Not available"
            table_lines.append(
                f"| {vn} | {vessel_name} | {port_calls} | {_fmt_num(r.get('pnl'))} | {_fmt_num(r.get('revenue'))} | {_fmt_num(r.get('total_expense'))} | {_fmt_ports(r.get('key_ports'))} | {_fmt_grades(r.get('cargo_grades'))} | {_fmt_remarks(r.get('remarks'))} |"
            )
        return "\n".join(table_lines).strip()

    def _ensure_ranking_vessels_answer(self, *, text: str, intent_key: str, merged_safe: Dict[str, Any]) -> str:
        if str(intent_key or "") != "ranking.vessels":
            return text

        artifacts = merged_safe.get("artifacts") if isinstance(merged_safe, dict) else {}
        rows = artifacts.get("merged_rows") if isinstance(artifacts, dict) else None
        if not isinstance(rows, list) or not rows:
            finance_rows = (merged_safe.get("finance") or {}).get("rows") if isinstance(merged_safe.get("finance"), dict) else None
            if isinstance(finance_rows, list) and finance_rows:
                rows = []
                for fr in finance_rows[:20]:
                    if not isinstance(fr, dict):
                        continue
                    rows.append({
                        "vessel_name": fr.get("vessel_name"),
                        "vessel_imo": fr.get("vessel_imo") or fr.get("imo"),
                        "voyage_count": fr.get("voyage_count"),
                        "tce": fr.get("avg_tce") if fr.get("avg_tce") not in (None, "") else fr.get("tce"),
                        "avg_pnl": fr.get("avg_pnl"),
                        "total_pnl": fr.get("total_pnl"),
                        "offhire_days": fr.get("total_offhire_days") if fr.get("total_offhire_days") not in (None, "") else fr.get("offhire_days"),
                    })
            else:
                return text
        else:
            normalized_rows = []
            for r in rows:
                if not isinstance(r, dict):
                    continue
                fin = r.get("finance") if isinstance(r.get("finance"), dict) else {}
                normalized_rows.append({
                    "vessel_name": r.get("vessel_name") or fin.get("vessel_name"),
                    "vessel_imo": r.get("vessel_imo") or r.get("imo") or fin.get("vessel_imo") or fin.get("imo"),
                    "voyage_count": r.get("voyage_count") if r.get("voyage_count") not in (None, "") else fin.get("voyage_count"),
                    "tce": r.get("tce") if r.get("tce") not in (None, "") else (r.get("avg_tce") if r.get("avg_tce") not in (None, "") else fin.get("avg_tce")),
                    "avg_pnl": r.get("avg_pnl") if r.get("avg_pnl") not in (None, "") else fin.get("avg_pnl"),
                    "total_pnl": r.get("total_pnl") if r.get("total_pnl") not in (None, "") else fin.get("total_pnl"),
                    "offhire_days": r.get("offhire_days") if r.get("offhire_days") not in (None, "") else (r.get("total_offhire_days") if r.get("total_offhire_days") not in (None, "") else fin.get("total_offhire_days")),
                    "unique_cargo_grades": r.get("unique_cargo_grades") if r.get("unique_cargo_grades") not in (None, "") else fin.get("unique_cargo_grades"),
                })
            rows = normalized_rows

        low = (text or "").lower()
        if not any(k in low for k in ("not available", "not directly calculable", "could not", "no financial data")):
            return text

        def _has_metric(v: Any) -> bool:
            return v not in (None, "", "Not available", "not available", "N/A", "n/a")

        if not any(
            isinstance(r, dict) and (
                _has_metric(r.get("tce"))
                or _has_metric(r.get("pnl"))
                or _has_metric(r.get("avg_pnl"))
                or _has_metric(r.get("total_pnl"))
                or _has_metric(r.get("voyage_count"))
            )
            for r in rows
        ):
            return text

        ql = low
        primary = "pnl"
        primary_label = "Average PnL"
        if "tce" in ql:
            primary = "tce"
            primary_label = "Average TCE"
        elif "total pnl" in ql or ("earned" in ql and "pnl" in ql):
            primary = "total_pnl"
            primary_label = "Total PnL"
        elif "voyage count" in ql or "number of voyages" in ql:
            primary = "voyage_count"
            primary_label = "Voyage Count"
        elif "offhire" in ql:
            primary = "offhire_days"
            primary_label = "Total Offhire Days"
        elif "diverse" in ql and "cargo" in ql:
            primary = "unique_cargo_grades"
            primary_label = "Unique Cargo Grades"

        def _fmt_metric(v: Any) -> str:
            if primary in ("pnl", "avg_pnl", "total_pnl", "tce"):
                val = self._fmt_usd(v) if primary != "voyage_count" else None
                if val is not None:
                    return val
            if v in (None, "", [], {}):
                return "Not available"
            try:
                n = float(v)
                if abs(n - int(n)) < 1e-9:
                    return f"{int(n):,}"
                return f"{n:,.2f}"
            except Exception:
                return str(v)

        def _fmt_count(v: Any) -> str:
            if v in (None, "", [], {}):
                return "Not available"
            try:
                s = str(v).replace("$", "").replace(",", "").strip()
                return f"{int(float(s)):,}"
            except Exception:
                return str(v)

        descending = not any(k in ql for k in ("lowest", "least", "worst"))

        def _metric_sort_value(r: Dict[str, Any]) -> tuple:
            raw = r.get(primary)
            if raw in (None, "") and primary == "pnl":
                raw = r.get("avg_pnl")
            try:
                val = float(raw)
            except Exception:
                val = None
            if descending:
                return (val is None, -(val or 0.0))
            return (val is None, (val if val is not None else float("inf")))

        ranked = [r for r in rows if isinstance(r, dict)]
        ranked.sort(key=_metric_sort_value)
        ranked = ranked[:10]
        table_lines = [
            "### Results",
            f"| Vessel Name | Vessel IMO | {primary_label} | Voyage Count |",
            "| --- | --- | --- | --- |",
        ]
        for r in ranked:
            vessel_name = r.get("vessel_name") or "Not available"
            imo = r.get("vessel_imo") or r.get("imo") or "Not available"
            metric_val = r.get(primary)
            if metric_val in (None, "") and primary == "pnl":
                metric_val = r.get("avg_pnl")
            table_lines.append(
                f"| {vessel_name} | {imo} | {_fmt_metric(metric_val)} | {_fmt_count(r.get('voyage_count'))} |"
            )
        return "\n".join(table_lines).strip()

    def _ensure_cargo_profitability_answer(self, *, text: str, intent_key: str, merged_safe: Dict[str, Any]) -> str:
        """
        Deterministic output guard for cargo profitability aggregates.
        Ensures one row per normalized cargo grade and stable KPI formatting.
        """
        ik = str(intent_key or "")
        if ik not in ("analysis.cargo_profitability", "analysis.cargoprofitability"):
            return text

        artifacts = merged_safe.get("artifacts") if isinstance(merged_safe, dict) else {}
        rows = artifacts.get("merged_rows") if isinstance(artifacts, dict) else None
        if not isinstance(rows, list) or not rows:
            return text

        def _grade_key(v: Any) -> str:
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

        grouped: Dict[str, Dict[str, Any]] = {}
        order: List[str] = []
        for r in rows:
            if not isinstance(r, dict):
                continue
            grades = r.get("cargo_grades")
            grade_raw = None
            if isinstance(grades, list) and grades:
                grade_raw = grades[0]
            else:
                grade_raw = (r.get("finance") or {}).get("cargo_grade") if isinstance(r.get("finance"), dict) else None
            if isinstance(grade_raw, dict):
                grade_raw = grade_raw.get("grade_name") or grade_raw.get("gradeName") or grade_raw.get("name") or grade_raw.get("grade")
            if isinstance(grade_raw, str) and grade_raw.strip().startswith("{") and grade_raw.strip().endswith("}"):
                try:
                    obj = ast.literal_eval(grade_raw.strip())
                    if isinstance(obj, dict):
                        g = obj.get("grade_name") or obj.get("gradeName") or obj.get("name") or obj.get("grade")
                        grade_raw = str(g).strip() if g not in (None, "", [], {}) else ""
                except Exception:
                    m = re.search(r"""['"](?:grade_name|gradeName|name|grade)['"]\s*:\s*['"]([^'"]+)['"]""", grade_raw)
                    if m:
                        grade_raw = m.group(1).strip()
            gk = _grade_key(grade_raw)
            if not gk:
                continue
            if gk not in grouped:
                order.append(gk)
                fin = r.get("finance") if isinstance(r.get("finance"), dict) else {}
                grouped[gk] = {
                    "display_grade": str(grade_raw).strip() if isinstance(grade_raw, str) and str(grade_raw).strip() else gk,
                    "pnl": r.get("pnl") if r.get("pnl") not in (None, "") else (fin.get("avg_pnl") or fin.get("pnl")),
                    "revenue": r.get("revenue") if r.get("revenue") not in (None, "") else (fin.get("avg_revenue") or fin.get("revenue")),
                    "actual_avg_pnl": r.get("actual_avg_pnl") if r.get("actual_avg_pnl") not in (None, "") else fin.get("actual_avg_pnl"),
                    "when_fixed_avg_pnl": r.get("when_fixed_avg_pnl") if r.get("when_fixed_avg_pnl") not in (None, "") else fin.get("when_fixed_avg_pnl"),
                    "variance_diff": r.get("variance_diff") if r.get("variance_diff") not in (None, "") else fin.get("variance_diff"),
                    "voyage_count": r.get("voyage_count") if r.get("voyage_count") not in (None, "") else fin.get("voyage_count"),
                    "ports": [],
                    "remarks": [],
                }
            g = grouped[gk]
            fin = r.get("finance") if isinstance(r.get("finance"), dict) else {}
            if g.get("pnl") in (None, ""):
                gp = r.get("pnl")
                if gp in (None, ""):
                    gp = fin.get("avg_pnl") or fin.get("pnl")
                if gp not in (None, ""):
                    g["pnl"] = gp
            if g.get("revenue") in (None, ""):
                gr = r.get("revenue")
                if gr in (None, ""):
                    gr = fin.get("avg_revenue") or fin.get("revenue")
                if gr not in (None, ""):
                    g["revenue"] = gr
            if g.get("actual_avg_pnl") in (None, ""):
                gap = r.get("actual_avg_pnl")
                if gap in (None, ""):
                    gap = fin.get("actual_avg_pnl")
                if gap not in (None, ""):
                    g["actual_avg_pnl"] = gap
            if g.get("when_fixed_avg_pnl") in (None, ""):
                gwp = r.get("when_fixed_avg_pnl")
                if gwp in (None, ""):
                    gwp = fin.get("when_fixed_avg_pnl")
                if gwp not in (None, ""):
                    g["when_fixed_avg_pnl"] = gwp
            if g.get("variance_diff") in (None, ""):
                gvd = r.get("variance_diff")
                if gvd in (None, ""):
                    gvd = fin.get("variance_diff")
                if gvd not in (None, ""):
                    g["variance_diff"] = gvd
            if g.get("voyage_count") in (None, ""):
                gv = r.get("voyage_count")
                if gv in (None, ""):
                    gv = fin.get("voyage_count")
                if gv not in (None, ""):
                    g["voyage_count"] = gv

            kp = r.get("key_ports")
            if isinstance(kp, list):
                for p in kp:
                    if isinstance(p, dict):
                        pn = str(p.get("portName") or p.get("port_name") or p.get("name") or "").strip()
                    else:
                        pn = ""
                        if p is not None:
                            ps = str(p).strip()
                            if ps.startswith("{") and ps.endswith("}"):
                                try:
                                    obj = ast.literal_eval(ps)
                                    if isinstance(obj, dict):
                                        pv = obj.get("port_name") or obj.get("portName") or obj.get("name")
                                        pn = str(pv).strip() if pv not in (None, "", [], {}) else ""
                                except Exception:
                                    m = re.search(r"""['"](?:port_name|portName|name)['"]\s*:\s*['"]([^'"]+)['"]""", ps)
                                    if m:
                                        pn = m.group(1).strip()
                            else:
                                pn = ps
                    if pn and pn not in g["ports"]:
                        g["ports"].append(pn)

            rem = r.get("remarks")
            if isinstance(rem, list):
                for rr in rem:
                    s = str(rr).strip() if rr is not None else ""
                    if s and s not in g["remarks"]:
                        g["remarks"].append(s)
            elif isinstance(rem, str) and rem.strip() and rem.strip() not in g["remarks"]:
                g["remarks"].append(rem.strip())

        if not grouped:
            return text

        has_variance_rows = any(
            isinstance(r, dict) and (
                r.get("variance_diff") not in (None, "")
                or r.get("actual_avg_pnl") not in (None, "")
                or r.get("when_fixed_avg_pnl") not in (None, "")
            )
            for r in grouped.values()
        )

        def _scenario_gap(item: Dict[str, Any]) -> Optional[float]:
            try:
                actual = float(item.get("actual_avg_pnl"))
                when_fixed = float(item.get("when_fixed_avg_pnl"))
                return abs(actual - when_fixed)
            except Exception:
                pass
            try:
                raw_gap = item.get("variance_diff")
                return abs(float(raw_gap)) if raw_gap not in (None, "") else None
            except Exception:
                return None

        def _sort_key(item: Dict[str, Any]) -> tuple:
            if has_variance_rows:
                v = _scenario_gap(item)
                return (v is None, -(v or 0.0))
            try:
                p = float(item.get("pnl"))
            except Exception:
                p = None
            return (p is None, -(p or 0.0))

        final_rows = [grouped[k] for k in order]
        final_rows.sort(key=_sort_key)
        final_rows = final_rows[:10]

        if has_variance_rows:
            table_lines = [
                "### Summary",
                f"- Cargo grades ranked by ACTUAL vs WHEN_FIXED PnL variance (top {len(final_rows)} shown).",
                "- Included common ports and recurring congestion/delay remarks where available.",
                "",
                "### Results",
                "| Cargo grade | ACTUAL Avg PnL | WHEN_FIXED Avg PnL | Variance | Voyage count | Common ports | Congestion/Delay remarks |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        else:
            table_lines = [
                "### Summary",
                f"- Most profitable cargo grades ranked by average PnL (top {len(final_rows)} shown).",
                "- Included common ports and recurring congestion/delay remarks where available.",
                "",
                "### Results",
                "| Cargo grade | Avg PnL | Avg Revenue | Voyage count | Common ports | Congestion/Delay remarks |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        for r in final_rows:
            ports = r.get("ports") or []
            if isinstance(ports, list):
                if len(ports) > 8:
                    ports_txt = ", ".join(ports[:8]) + f" (+{len(ports)-8} more)"
                else:
                    ports_txt = ", ".join(ports) if ports else "Not available"
            else:
                ports_txt = "Not available"
            remarks = r.get("remarks") or []
            if isinstance(remarks, list):
                remarks_txt = " | ".join(remarks[:3]) if remarks else "No recurring congestion/delay remarks"
            else:
                remarks_txt = str(remarks) if remarks else "No recurring congestion/delay remarks"
            vc = r.get("voyage_count")
            vc_txt = str(int(vc)) if isinstance(vc, (int, float)) else (str(vc) if vc not in (None, "") else "Not available")
            if has_variance_rows:
                variance_txt = self._fmt_usd(_scenario_gap(r)) or "Not available"
                table_lines.append(
                    f"| {r.get('display_grade') or 'Not available'} | "
                    f"{self._fmt_usd(r.get('actual_avg_pnl')) or 'Not available'} | "
                    f"{self._fmt_usd(r.get('when_fixed_avg_pnl')) or 'Not available'} | "
                    f"{variance_txt} | "
                    f"{vc_txt} | {ports_txt} | {remarks_txt} |"
                )
            else:
                table_lines.append(
                    f"| {r.get('display_grade') or 'Not available'} | {self._fmt_usd(r.get('pnl')) or 'Not available'} | {self._fmt_usd(r.get('revenue')) or 'Not available'} | {vc_txt} | {ports_txt} | {remarks_txt} |"
                )
        return "\n".join(table_lines).strip()

    @staticmethod
    def _normalize_readability_text(text: str) -> str:
        s = str(text or "")
        if not s:
            return s

        # Normalize accidental spacing after currency symbol.
        s = re.sub(r"\$\s+(?=\d)", "$", s)

        # Normalize common metric tokens if the model outputs spaced letters.
        s = re.sub(r"\bP\s*n\s*L\b", "PnL", s, flags=re.IGNORECASE)
        s = re.sub(r"\bT\s*C\s*E\b", "TCE", s, flags=re.IGNORECASE)

        # Remove spaces around commas for thousand groups in numbers.
        # Run a few passes to fix multi-group numbers (e.g., 1, 878, 032).
        for _ in range(3):
            s = re.sub(r"(?<=\d),\s+(?=\d{3}\b)", ",", s)
            s = re.sub(r"(?<=\d)\s+,\s*(?=\d{3}\b)", ",", s)

        # Fix compacted numeric ranges.
        s = re.sub(r"(?<=\d)\s*to\s*(?=\$?\d)", " to ", s, flags=re.IGNORECASE)

        def _fix_line_for_digit_letter_spacing(line: str) -> str:
            st = line.strip()
            if st.startswith("|") and st.endswith("|") and "|" in st[1:-1]:
                return line
            t = line
            t = re.sub(r"(?<=\d)(?=[A-Za-z])", " ", t)
            t = re.sub(r"(?<=[A-Za-z])(?=\d)", " ", t)
            t = re.sub(r"[ \t]+", " ", t)
            return t

        s = "\n".join(_fix_line_for_digit_letter_spacing(L) for L in s.splitlines())

        s = re.sub(r"\n{3,}", "\n\n", s)
        return s.strip()

    def _ensure_voyage_narrative_summary(self, text: str, *, merged_safe: Dict[str, Any]) -> str:
        lines = (text or "").splitlines()
        try:
            idx = next(i for i, l in enumerate(lines) if l.strip() == "### Summary")
        except StopIteration:
            return text

        j = idx + 1
        while j < len(lines) and not lines[j].strip():
            j += 1

        if j < len(lines) and lines[j].lstrip().startswith("- **"):
            hint = self._build_voyage_narrative_hint(merged_safe)
            if hint:
                lines.insert(j, hint)
        return "\n".join(lines).strip()

    @staticmethod
    def _fmt_usd(v: Any) -> Optional[str]:
        try:
            if v is None:
                return None
            fv = float(v)
            return f"${fv:,.2f}"
        except Exception:
            return None

    def _build_voyage_narrative_hint(self, merged_safe: Dict[str, Any]) -> str:
        fin = merged_safe.get("finance")
        row = None
        if isinstance(fin, dict) and isinstance(fin.get("rows"), list) and fin["rows"]:
            row = fin["rows"][0] if isinstance(fin["rows"][0], dict) else None

        if not isinstance(row, dict):
            return ""

        pnl = row.get("pnl")
        revenue = row.get("revenue")
        expense = row.get("total_expense")

        pnl_s = self._fmt_usd(pnl) or "Not available"
        rev_s = self._fmt_usd(revenue) or "Not available"
        exp_s = self._fmt_usd(expense) or "Not available"

        direction = ""
        try:
            if pnl is not None and float(pnl) >= 0:
                direction = "positive"
            elif pnl is not None:
                direction = "negative"
        except Exception:
            direction = ""

        if direction:
            return f"- Overall, this voyage finished {direction} (PnL {pnl_s}) on revenue {rev_s} and total expense {exp_s}."
        return f"- Overall, this voyage finished with PnL {pnl_s} on revenue {rev_s} and total expense {exp_s}."

    def _build_deterministic_voyage_summary(
        self,
        *,
        question: str,
        merged_safe: Dict[str, Any],
        style: Dict[str, Any],
    ) -> str:
        """
        Stable markdown for voyage.summary — same structure and wording every time
        when finance/ops KPI rows are present (demo-safe; no LLM paraphrase).
        """
        art = merged_safe.get("artifacts") if isinstance(merged_safe.get("artifacts"), dict) else {}
        if art.get("finance_kpi_unavailable") is True:
            return ""

        fin_rows = (merged_safe.get("finance") or {}).get("rows") if isinstance(merged_safe.get("finance"), dict) else []
        ops_rows = (merged_safe.get("ops") or {}).get("rows") if isinstance(merged_safe.get("ops"), dict) else []
        fin_row = fin_rows[0] if isinstance(fin_rows, list) and fin_rows and isinstance(fin_rows[0], dict) else {}
        ops_row = ops_rows[0] if isinstance(ops_rows, list) and ops_rows and isinstance(ops_rows[0], dict) else {}

        has_finance = any(
            fin_row.get(k) not in (None, "")
            for k in ("pnl", "revenue", "total_expense", "tce", "total_commission")
        )
        if not has_finance and not ops_row:
            return ""

        vn = fin_row.get("voyage_number") or ops_row.get("voyage_number") or ""
        vname, imo = self._extract_voyage_identity(merged_safe)
        lines: List[str] = ["### Summary"]
        id_parts = []
        if vn not in (None, ""):
            id_parts.append(f"**{vn}**")
        if vname:
            id_parts.append(f"**{vname}**")
        if imo:
            id_parts.append(f"(IMO {imo})")
        if id_parts:
            lines.append(f"- Voyage {' — '.join(id_parts)}.")
        else:
            lines.append("- Voyage summary from available finance and operations data.")

        if has_finance:
            lines.extend(["", "### Financial summary", "| Metric | Value |", "| --- | --- |"])
            for label, key in (
                ("Revenue", "revenue"),
                ("Total expense", "total_expense"),
                ("PnL", "pnl"),
                ("TCE", "tce"),
                ("Total commission", "total_commission"),
            ):
                val = fin_row.get(key)
                if val not in (None, ""):
                    fmt = self._fmt_usd(val) if key != "tce" else self._fmt_usd(val)
                    lines.append(f"| {label} | {fmt or val} |")

        if style.get("ask_ports", True):
            ports: List[str] = []
            if isinstance(ops_row.get("ports"), list):
                ports = [str(p).strip() for p in ops_row["ports"] if str(p).strip()]
            elif isinstance(ops_row.get("key_ports"), list):
                for p in ops_row["key_ports"]:
                    if isinstance(p, dict):
                        pn = str(p.get("portName") or p.get("port_name") or "").strip()
                        if pn:
                            ports.append(pn)
                    elif str(p).strip():
                        ports.append(str(p).strip())
            elif isinstance(ops_row.get("ports_json"), list):
                ports = [str(p).strip() for p in ops_row["ports_json"] if str(p).strip()]
            lines.extend(["", "### Main ports involved"])
            if ports:
                for p in ports[:12]:
                    lines.append(f"- {p}")
                if len(ports) > 12:
                    lines.append(f"- (+{len(ports) - 12} more)")
            else:
                lines.append("- Not available in dataset.")

        if style.get("ask_remarks", True):
            remarks_out: List[str] = []
            mongo = merged_safe.get("mongo")
            if isinstance(mongo, dict):
                raw_remarks = mongo.get("remarks")
                if isinstance(raw_remarks, list):
                    for item in raw_remarks[:8]:
                        if isinstance(item, dict):
                            dt = str(item.get("modifiedDate") or item.get("date") or "").strip()
                            who = str(item.get("modifiedByFull") or item.get("author") or "").strip()
                            body = str(item.get("remark") or item.get("text") or "").strip()
                            if body:
                                prefix = " | ".join(x for x in (dt, who) if x)
                                remarks_out.append(f"- {prefix} | {body[:400]}" if prefix else f"- {body[:400]}")
                        elif str(item).strip():
                            remarks_out.append(f"- {str(item).strip()[:400]}")
                rows = mongo.get("rows")
                if not remarks_out and isinstance(rows, list) and rows and isinstance(rows[0], dict):
                    for item in (rows[0].get("remarks") or [])[:8]:
                        if isinstance(item, dict):
                            body = str(item.get("remark") or item.get("text") or "").strip()
                            if body:
                                remarks_out.append(f"- {body[:400]}")
            if not remarks_out and isinstance(ops_row.get("remarks_preview"), list):
                for t in ops_row["remarks_preview"][:5]:
                    if str(t).strip():
                        remarks_out.append(f"- {str(t).strip()[:400]}")
            lines.extend(["", "### Voyage remarks"])
            if remarks_out:
                lines.extend(remarks_out)
            else:
                lines.append("- No remarks recorded for this voyage in the available data.")

        return "\n".join(lines).strip()

    @staticmethod
    def _norm_imo_text(v: Any) -> str:
        if v in (None, ""):
            return ""
        s = str(v).strip()
        if s.endswith(".0"):
            s = s[:-2]
        return s

    def _extract_voyage_identity(self, merged_safe: Dict[str, Any]) -> tuple:
        fin = merged_safe.get("finance")
        if isinstance(fin, dict) and isinstance(fin.get("rows"), list) and fin.get("rows"):
            r0 = fin["rows"][0]
            if isinstance(r0, dict):
                vname = str(r0.get("vessel_name") or "").strip()
                imo = self._norm_imo_text(r0.get("vessel_imo") or r0.get("imo"))
                if vname or imo:
                    return vname, imo

        ops = merged_safe.get("ops")
        if isinstance(ops, dict) and isinstance(ops.get("rows"), list) and ops.get("rows"):
            r0 = ops["rows"][0]
            if isinstance(r0, dict):
                vname = str(r0.get("vessel_name") or "").strip()
                imo = self._norm_imo_text(r0.get("vessel_imo") or r0.get("imo"))
                if vname or imo:
                    return vname, imo

        mongo = merged_safe.get("mongo")
        if isinstance(mongo, dict):
            rows = mongo.get("rows")
            if isinstance(rows, list) and rows and isinstance(rows[0], dict):
                r0 = rows[0]
                vname = str(r0.get("vesselName") or r0.get("vessel_name") or "").strip()
                imo = self._norm_imo_text(r0.get("vesselImo") or r0.get("vessel_imo") or r0.get("imo"))
                if vname or imo:
                    return vname, imo

        return "", ""

    def _ensure_voyage_identity_line(self, text: str, *, merged_safe: Dict[str, Any]) -> str:
        s = (text or "").strip()
        if not s:
            return s
        if re.search(r"(?im)^\s*-\s*\*\*Vessel\*\*:", s):
            return s

        vname, imo = self._extract_voyage_identity(merged_safe)
        if not vname and not imo:
            return s

        vessel_val = vname if vname else "Not available"
        if imo:
            vessel_val = f"{vessel_val} (IMO: {imo})"
        vessel_line = f"- **Vessel**: {vessel_val}"

        lines = s.splitlines()
        try:
            idx = next(i for i, l in enumerate(lines) if l.strip() == "### Summary")
        except StopIteration:
            return f"### Summary\n{vessel_line}\n\n{s}".strip()

        insert_at = idx + 1
        for i in range(idx + 1, len(lines)):
            line = lines[i].strip()
            if not line:
                continue
            if line.lower().startswith("- **voyage**"):
                insert_at = i + 1
                break
            if line.startswith("### "):
                insert_at = idx + 1
                break
            if line.startswith("- "):
                insert_at = i
                break
        lines.insert(insert_at, vessel_line)
        return "\n".join(lines).strip()

    # =========================================================
    # RETRY + JSON SAFE PARSER
    # =========================================================

    def _call_with_retry(
        self,
        *,
        system: str,
        user: str,
        operation: str,
        max_retries: int = 3,
        return_string: bool = False,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ):
        for _ in range(max_retries):
            try:
                raw = self._groq_chat(
                    system=system,
                    user=user,
                    temperature=temperature,
                    max_tokens=max_tokens,
                )

                if return_string:
                    return raw

                cleaned = raw.strip()
                cleaned = re.sub(r"^```.*?\n", "", cleaned)
                cleaned = cleaned.replace("```", "")

                return json.loads(cleaned)

            except Exception:
                time.sleep(0.5)

        return "" if return_string else None

    # =========================================================
    # UTILITIES
    # =========================================================

    def _safe_json_load(self, raw: str, fallback: Any):
        try:
            cleaned = (raw or "").strip()
            cleaned = re.sub(r"^```.*?\n", "", cleaned)
            cleaned = cleaned.replace("```", "")
            return json.loads(cleaned)
        except Exception:
            return fallback

    def _truncate_merged_data(self, merged: Dict[str, Any], max_rows: int):
        if not isinstance(merged, dict):
            return merged

        import copy
        out = copy.deepcopy(merged)

        def cap_rows(section_key: str):
            section = out.get(section_key)
            if isinstance(section, dict) and isinstance(section.get("rows"), list):
                section["rows"] = section["rows"][:max_rows]

        cap_rows("finance")
        cap_rows("ops")
        cap_rows("mongo")

        def _cap_list(v, n: int):
            return v[:n] if isinstance(v, list) else v

        def _cap_str(v, n: int):
            if isinstance(v, str) and len(v) > n:
                return v[:n] + "…"
            return v

        ops = out.get("ops")
        if isinstance(ops, dict) and isinstance(ops.get("rows"), list):
            _ik = str((((out.get("artifacts") or {}) if isinstance(out.get("artifacts"), dict) else {}) or {}).get("intent_key") or "").strip()
            _voy_sum = _ik == "voyage.summary"
            for r in ops["rows"]:
                if not isinstance(r, dict):
                    continue
                shrink_ops_row_json_fields(r, voyage_summary=_voy_sum)

        artifacts = out.get("artifacts")
        if isinstance(artifacts, dict) and isinstance(artifacts.get("merged_rows"), list):
            artifacts["merged_rows"] = artifacts["merged_rows"][:max_rows]
            for mr in artifacts["merged_rows"]:
                if not isinstance(mr, dict):
                    continue
                mr["key_ports"] = _cap_list(mr.get("key_ports"), 10)
                mr["cargo_grades"] = _cap_list(mr.get("cargo_grades"), 10)
                mr["commissions"] = _cap_list(mr.get("commissions"), 10)

                rem = mr.get("remarks")
                if isinstance(rem, list):
                    rem = rem[:3]
                    cleaned = []
                    for x in rem:
                        if isinstance(x, dict):
                            cleaned.append({
                                "remark": _cap_str(x.get("remark"), 80),
                                "modifiedDate": x.get("modifiedDate"),
                                "modifiedByFull": x.get("modifiedByFull"),
                            })
                        else:
                            cleaned.append(_cap_str(str(x), 80))
                    mr["remarks"] = cleaned
                elif isinstance(rem, str):
                    mr["remarks"] = _cap_str(rem, 300)

        return out

    def _convert_to_json_safe(self, obj: Any):
        if isinstance(obj, Decimal):
            return float(obj)
        elif isinstance(obj, (datetime, date, dt_time)):
            return obj.isoformat()
        elif isinstance(obj, dict):
            return {k: self._convert_to_json_safe(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_to_json_safe(i) for i in obj]
        return obj

    def _groq_chat(
        self,
        *,
        system: str,
        user: str,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> str:
        start = time.time()
        prompt_chars = len(system or "") + len(user or "")
        session_id = get_request_session_id()
        tracing_on = os.getenv("LANGCHAIN_TRACING_V2", "").strip().lower() in ("1", "true", "yes", "on")

        def _execute() -> str:
            completion = self.client.chat.completions.create(
                model=self.config.model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                temperature=(self.config.temperature if temperature is None else temperature),
                max_tokens=max_tokens,
            )
            return completion.choices[0].message.content or ""

        try:
            if tracing_on and session_id:
                try:
                    from langsmith import trace as ls_trace

                    with ls_trace(
                        name="groq_chat",
                        run_type="llm",
                        inputs={"prompt_chars": prompt_chars},
                        session_name=session_id,
                        metadata={"session_id": session_id},
                    ) as run:
                        response = _execute()
                        run.end(outputs={"response_chars": len(response)})
                        return response
                except Exception:
                    pass
            response = _execute()
            elapsed = round(time.time() - start, 3)
            logger.info(
                "LLM_CALL | model=%s | latency=%ss | prompt_chars=%s | response_chars=%s | session=%s",
                self.config.model,
                elapsed,
                prompt_chars,
                len(response),
                session_id or "-",
            )
            return response
        except Exception as exc:
            elapsed = round(time.time() - start, 3)
            logger.error(
                "LLM_ERROR | model=%s | latency=%ss | prompt_chars=%s | error=%s",
                self.config.model,
                elapsed,
                prompt_chars,
                str(exc)[:200],
            )
            raise


def _enforce_table_rules(text: str) -> str:
    lines = text.splitlines()
    output = []
    table_lines = []
    
    def _flush_table_block() -> None:
        nonlocal table_lines
        if table_lines:
            output.extend(_drop_empty_columns(table_lines))
            table_lines = []

    for line in lines:
        stripped = line.strip()
        if stripped.startswith("|"):
            table_lines.append(line)
            continue
        _flush_table_block()
        output.append(line)

    _flush_table_block()

    return "\n".join(output)


def _drop_empty_columns(table_lines: list) -> list:
    if len(table_lines) < 2:
        return table_lines

    def split_row(line):
        parts = line.strip().strip("|").split("|")
        return [p.strip() for p in parts]

    def is_separator_row(row: list[str]) -> bool:
        if not row:
            return False
        for cell in row:
            c = cell.strip()
            if not c:
                continue
            if not re.fullmatch(r":?-{3,}:?", c):
                return False
        return True

    def is_emptyish(val: str) -> bool:
        return str(val or "").strip().lower() in {"", "not available", "n/a", "none", "null", "-", "—", "not recorded"}

    def looks_sentence(text: str) -> bool:
        t = str(text or "").strip()
        if len(t) < 24 or " " not in t:
            return False
        if not any(ch.isalpha() for ch in t):
            return False
        if re.fullmatch(r"[\d\s,$%().:/\-]+", t):
            return False
        lowered = t.lower()
        sentence_markers = (
            " the ", " this ", " these ", " those ", " was ", " were ", " has ", " had ",
            " have ", " with ", " among ", " overall ", " appears ", " shows ", " indicate",
            " winner", " conclusion", " result", " voyages ", " vessel ", " cargo ", " port ",
        )
        return t.endswith((".", "!", "?")) or any(m in f" {lowered} " for m in sentence_markers)

    rows = [split_row(l) for l in table_lines]
    if not rows:
        return table_lines

    num_cols = max(len(r) for r in rows)
    rows = [r + [""] * (num_cols - len(r)) for r in rows]

    header_row = rows[0]
    separator_row = rows[1] if len(rows) > 1 and is_separator_row(rows[1]) else ["---"] * num_cols
    raw_data_rows = rows[2:] if len(rows) > 1 and is_separator_row(rows[1]) else rows[1:]

    cleaned_data_rows: list[list[str]] = []
    ejected_lines: list[str] = []
    for row in raw_data_rows:
        non_empty_cells = [c.strip() for c in row if not is_emptyish(c)]
        if len(non_empty_cells) == 1 and looks_sentence(non_empty_cells[0]):
            ejected_lines.append(non_empty_cells[0])
            continue
        if non_empty_cells and any(looks_sentence(c) for c in non_empty_cells) and len(non_empty_cells) <= 2:
            ejected_lines.append(" ".join(non_empty_cells).strip())
            continue
        cleaned_data_rows.append(row)

    if cleaned_data_rows:
        data_rows = cleaned_data_rows
    elif ejected_lines:
        data_rows = []
    else:
        data_rows = raw_data_rows

    keep_cols = []
    for col_idx in range(num_cols):
        vals = [str(r[col_idx] if col_idx < len(r) else "").strip() for r in data_rows]
        if any(not is_emptyish(v) for v in vals):
            keep_cols.append(col_idx)

    if not keep_cols:
        keep_cols = list(range(num_cols))

    result = []
    for row_idx, row in enumerate([header_row, separator_row, *data_rows]):
        kept = [row[i] if i < len(row) else "" for i in keep_cols]
        if row_idx != 1:
            kept = [
                "—" if is_emptyish(c) else c
                for c in kept
            ]
        else:
            kept = ["---"] * len(keep_cols)
        result.append("| " + " | ".join(kept) + " |")

    if ejected_lines:
        result.append("")
        result.extend(ejected_lines)

    return result