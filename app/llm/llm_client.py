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
import logging
import os
import re
import re as _re
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time
from decimal import Decimal
from typing import Any, Dict, List, Optional

from groq import Groq

logger = logging.getLogger(__name__)

MONGO_ONLY_VOYAGE_FIELDS = frozenset([
    # fixture / commercial
    "charterer", "broker", "commission", "commission rate", "commission rates",
    "cp date", "cp quantity", "demurrage rate", "laytime", "time bar",
    "fixture", "fixture terms", "bill of lading", "bl quantity", "shipper",
    # cargo
    "cargo grade", "grade", "cargo",
    # route / legs
    "route", "leg", "legs", "arrival", "departure",
    "load port", "discharge port", "port call",
    "distance", "passage days",
    # revenue / expense detail
    "freight rate", "revenue line", "expense line",
    "rebill", "worldscale", "ws ",
    # bunkers
    "bunker consumption", "bunker cost", "bunker grade",
    "hsbf", "lsgo", "rob", "stems", "bunker",
    # emissions
    "cii", "cii band", "co2", "sox", "nox",
    "emissions", "eeoi", "aer",
    # remarks
    "remark", "remarks", "who added", "added by",
    # projected results
    "projected", "projected pnl", "projected revenue",
    "projected tce", "tce projection",
    # metadata / source
    "metadata", "voyage metadata", "source link", "url",
])


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
    return any(field in text_lower for field in MONGO_ONLY_VOYAGE_FIELDS)


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

    def _deterministic_intent(self, text: str) -> Optional[str]:
        t = text.lower()
        has_specific_voyage_anchor = bool(
            re.search(r"\bvoyage(?:s)?\s+\d{3,5}\b", t)
            or (re.search(r"\bfor\s+voyages?\b", t) and re.search(r"\b\d{3,5}\b", t))
        )
        looks_specific_vessel_performance = bool(
            re.search(r"\bstena\s+[a-z0-9][a-z0-9\-]*\b", t) and any(k in t for k in ("overall", "performing", "voyage history", "trend"))
        )
        voyage_resolved_vessel_metadata_terms = (
            "operating status", "operational status", "is operating", "is operational",
            "hire rate", "hirerate", "scrubber", "passage type", "passage types",
            "account code", "market type",
        )

        if has_specific_voyage_anchor and any(k in t for k in voyage_resolved_vessel_metadata_terms):
            return "vessel.metadata"

        if any(k in t for k in ("average voyage duration", "average voyage days", "voyage duration in days")) and any(
            k in t for k in ("per vessel", "each vessel", "by vessel")
        ):
            return "aggregation.average"

        # FIRST: Delayed voyages with negative PnL / loss / root cause → finance.loss_due_to_delay (avoid mis-classification as port_query)
        if "delayed" in t and ("negative" in t and "pnl" in t or "negative pn" in t or "loss" in t or "root cause" in t):
            return "finance.loss_due_to_delay"

        # 1) Cargo-grade profitability (fleet aggregate) — must run before generic ranking
        # so grade-level aggregate asks don't get misrouted to ranking.pnl.
        cargo_grade_terms = ("cargo grade", "cargo grades", "grade", "grades", "cargo")
        aggregate_terms = ("highest", "top", "most", "average", "avg", "overall", "profit", "profitable", "pnl", "revenue")
        cargo_frequency_terms = ("frequent", "frequency", "most common", "most commonly", "appears most", "appears the most", "most carried")
        cargo_subject_is_primary = not any(
            k in t for k in ("voyages", "for voyage", "voyage-by-voyage", "vessel", "vessels", "module type", "module types")
        )
        if cargo_subject_is_primary and any(k in t for k in cargo_grade_terms) and ("when-fixed" in t or "when fixed" in t or ("actual" in t and "variance" in t)):
            return "analysis.cargo_profitability"
        if cargo_subject_is_primary and any(k in t for k in cargo_grade_terms) and any(
            k in t for k in ("average pnl", "avg pnl", "highest average pnl", "average revenue", "avg revenue")
        ):
            return "analysis.cargo_profitability"
        if (
            cargo_subject_is_primary
            and any(k in t for k in cargo_grade_terms)
            and any(k in t for k in cargo_frequency_terms)
            and not any(k in t for k in ("profitable", "profit", "pnl", "revenue", "overall"))
        ):
            return "ranking.cargo"
        if any(k in t for k in cargo_grade_terms) and any(k in t for k in ("negative pnl", "negative pn", "loss-making", "loss making", "loss-making voyages", "negative profit")):
            return "ranking.cargo"
        if cargo_subject_is_primary and any(k in t for k in cargo_grade_terms) and any(k in t for k in aggregate_terms):
            return "analysis.cargo_profitability"

        # Scenario comparison should win over plain voyage-vs-voyage comparison.
        if "when-fixed" in t or "when fixed" in t or ("actual" in t and "variance" in t):
            return "analysis.scenario_comparison"

        # Explicit voyage-to-voyage comparison with 2+ voyage numbers.
        if any(k in t for k in ("compare", "vs", "versus", "difference", "which is better")):
            nums = re.findall(r"\b\d{3,5}\b", t)
            if len(nums) >= 2:
                return "comparison.voyages"

        # Voyage-anchored port listing should stay voyage-scoped.
        if re.search(r"\bvoyage\s+\d{3,5}\b", t) and any(
            k in t for k in ("which ports", "ports were visited", "ports visited", "visited ports", "port list")
        ):
            return "voyage.metadata"

        # 0) "Tell me about voyage 1901" / "voyage 1901 summary" → voyage summary
        if "voyage" in t and any(k in t for k in ("tell me about", "details about", "information about", "summary", "summarize")):
            if re.search(r"\bvoyage\s+\d{3,5}\b", t):
                return "voyage.summary"

        # 0) "Tell me about vessel ..." → vessel summary
        if ("vessel" in t or "ship" in t) and (
            "tell me about" in t
            or "details about" in t
            or "information about" in t
            or "summary" in t
        ):
            return "vessel.summary"

        # 0a) Explicit entity-anchored vessel asks should stay vessel-scoped,
        # not fleet rankings (e.g., "For vessel X, show voyage-by-voyage trend...").
        # Keep this generic and avoid fleet phrases like "which vessel/per vessel/each vessel".
        if ("for vessel " in t or "for ship " in t) and not any(
            p in t for p in ("which vessel", "per vessel", "each vessel", "vessels have", "vessel have")
        ):
            return "vessel.summary"

        # 0) Commission ranking
        if ("commission" in t) and ("top" in t) and ("voyage" in t):
            return "ranking.voyages_by_commission"

        # 0b) Vessel screening: high voyage count + above-average profitability
        if ("high voyage count" in t or "many voyages" in t) and ("above-average" in t or "above average" in t) and ("profit" in t or "pnl" in t or "profitability" in t):
            return "ranking.vessels"

        # 1b) Vessel-level fleet aggregates should stay vessel-scoped, not voyage-scoped.
        vessel_ranking_signals = (
            "which vessel", "which vessels", "top vessel", "top vessels", "vessel has", "vessels have",
            "per vessel", "each vessel", "across all voyages", "fleet"
        )
        vessel_metric_signals = (
            "pnl", "profit", "profitability", "revenue", "tce", "expense", "cost",
            "voyage count", "number of voyage", "speed", "hire rate", "contract", "offhire"
        )
        if ("vessel" in t or "vessels" in t) and any(k in t for k in vessel_ranking_signals) and any(
            k in t for k in vessel_metric_signals
        ):
            return "ranking.vessels"

        # 2) Voyage-ranking phrasing should stay voyage-scoped even when extra output
        # fields like cargo grade / ports are requested.
        if any(k in t for k in ("most profitable voyages", "top profitable voyages", "top performing voyages")):
            return "ranking.voyages_by_pnl"

        # 2) Ranking by PnL
        if ("top" in t or "highest" in t or "rank" in t) and ("pnl" in t or "profit" in t):
            return "ranking.pnl"

        # 2b) Ranking by revenue
        if ("top" in t or "highest" in t or "rank" in t) and "revenue" in t:
            return "ranking.revenue"

        # Emissions / climate metric rankings.
        if any(k in t for k in ("co2", "emission", "emissions", "eeoi", "aer", "sox", "nox", "cii")) and any(
            k in t for k in ("top", "highest", "worst", "lowest", "most", "least", "high")
        ):
            return "ranking.voyages"

        # Natural language voyage-performance phrasing.
        if any(k in t for k in ("top performing voyages", "most profitable voyages", "top performing voyage", "most profitable voyage", "least profitable voyage")):
            if not looks_specific_vessel_performance:
                return "ranking.voyages_by_pnl"
        if any(k in t for k in ("best voyage", "worst voyage")) and not looks_specific_vessel_performance:
            return "ranking.voyages_by_pnl"

        # 4) Port-call ranking (voyages with most port calls / ports visited / port stops)
        if (
            any(k in t for k in ("port call", "port calls", "port count", "port counts", "port stops", "port visits"))
            and any(k in t for k in ("top", "most", "highest", "rank", "visited"))
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
        _port_fleet_signals = (
            "most visit", "most common", "most commonly", "most frequent",
            "commonly visit", "frequently visit",
            "busiest port", "popular port",
            "which port", "top port",
        )
        if "port" in t and any(k in t for k in _port_fleet_signals):
            return "ranking.ports"
        # Broader catch: any combo of (most/common/frequent/busiest) + (visit*) + port
        if "port" in t and "most" in t and any(k in t for k in ("visit", "common", "frequent", "busy")):
            return "ranking.ports"

        # 9) Voyage count per vessel (how many voyages does each/per vessel)
        if "voyage" in t and any(k in t for k in ("each vessel", "per vessel", "how many voyage", "number of voyage", "vessel have", "vessels have")):
            return "aggregation.count"

        # 10) Fleet-level vessel aggregate/screening queries.
        fleet_vessel_terms = (
            "which vessel", "which vessels", "top vessels", "vessels with", "show vessels",
            "operating vessels", "active vessels", "scrubber vessels", "non-scrubber vessels",
            "market type has", "fastest on ballast", "fastest on laden",
            "longest contract", "highest hire rate", "expensive to operate",
        )
        vessel_metadata_agg_terms = (
            "operating", "operational", "active vessels",
            "scrubber", "non-scrubber", "non scrubber",
            "hire rate", "hirerate",
            "ballast", "laden", "default ballast speed", "default laden speed",
            "contract", "duration", "current contract",
            "pool", "short pool", "long pool",
            "market type",
        )
        agg_terms = (
            "highest", "lowest", "top", "most", "least", "best", "worst",
            "count", "average", "avg", "total", "longest", "fastest",
        )
        explicit_single_vessel = bool(re.search(r"\b(?:vessel|ship)\s+[a-z0-9][a-z0-9\- ]{1,40}\b", t))
        if (
            ("vessel" in t or "vessels" in t)
            and any(k in t for k in vessel_metadata_agg_terms)
            and not explicit_single_vessel
        ):
            return "ranking.vessel_metadata"
        if any(k in t for k in fleet_vessel_terms):
            return "ranking.vessels"
        if ("vessel" in t or "vessels" in t) and any(k in t for k in agg_terms) and not explicit_single_vessel:
            return "ranking.vessels"

        # 11) Delay/waiting-centric ranking.
        if any(k in t for k in ("delay", "waiting cost", "waiting", "offhire")) and any(
            k in t for k in ("highest", "biggest", "most", "worst", "top")
        ):
            return "ops.offhire_ranking"

        return None

    # =========================================================
    # Extract intent and slots
    # =========================================================

    def extract_intent_slots(
        self,
        *,
        text: str,
        supported_intents: List[str],
        schema_hint: Optional[Dict[str, Any]] = None,
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
        deterministic = self._deterministic_intent(text_norm)

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
        metadata_keywords = (
            "passage type",
            "passage types",
            "consumption profile",
            "consumption profiles",
            "consumption",
            "default consumption",
            "speed",
            "ifo",
            "mgo",
            "ballast",
            "laden",
            "non passage",
            "non-passage",
            "idle",
            "load",
            "discharge",
            "heat",
            "clean",
            "inert",
            "hire rate",
            "hirerate",
            "hire_rate",
            "hire-rate",
            "scrubber",
            "market type",
            "contract history",
            "contract",
            "tags",
            "account code",
            "is vessel operating",
            "operating status",
            "operational",
            "is operating",
            "owner",
            "duration",
            "cp date",
            "delivery",
            "extracted at",
            "fixture",
            "charterer",
            "laycan",
            "demurrage",
            "freight",
            "cargo quantity",
            "cargoes",
            "leg",
            "legs",
            "route leg",
            "bunker",
            "bunkers",
            "emission",
            "emissions",
            "co2",
            "projected result",
            "projected pnl",
            "url",
            "source link",
            "commercial metadata",
            "voyage metadata",
            "vessel metadata",
            "metadata",
        )

        # Metadata-first routing for vessel/voyage-anchored questions.
        # Allow metadata override when early deterministic pick is summary intent.
        fleet_wide_markers = (
            "which vessels",
            "which vessel has the",
            "top vessels",
            "vessels with",
            "market type has",
            "across all",
            "across fleet",
            "fleet-wide",
            "all vessels",
            "most vessels",
            "highest voyage count",
            "lowest voyage count",
        )
        looks_fleet_wide = any(m in tl for m in fleet_wide_markers)

        if any(k in tl for k in metadata_keywords) and (
            not deterministic or deterministic in ("voyage.summary", "vessel.summary", "ranking.vessels")
        ):
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
            }

        # =========================================================
        # 4️⃣ LLM CALL — Build description-rich intent list from INTENT_REGISTRY
        # =========================================================
        # CHANGE 1: Import registry and inject descriptions into the prompt so
        # the LLM can distinguish entity-anchored vs fleet-wide aggregate intents.
        # No hardcoding — descriptions come purely from the registry.
        # =========================================================
        from app.registries.intent_registry import INTENT_REGISTRY

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

        system = f"""
You are a maritime finance intent classifier.
Return ONLY valid JSON with keys: intent_key, slots.

SUPPORTED INTENTS (read descriptions carefully before classifying):
{intents_formatted}

CLASSIFICATION RULES:
- First identify the PRIMARY subject the rows should represent: voyages, vessels, cargo grades, ports, module types, or a single anchored entity.
- Supporting columns do NOT change the primary subject. Example: if the user asks for voyages and also wants cargo grades/ports/remarks as extra columns, the intent is still a voyage intent.
- If the user names a SPECIFIC vessel by name or IMO → use vessel.summary or vessel.metadata
- If the user names a SPECIFIC voyage by number → use voyage.summary
- If the user asks about the ENTIRE fleet with no specific entity → use ranking.*, aggregation.*, or analysis.* intents
- Intents marked FLEET-WIDE must NEVER have vessel_name or voyage_number in slots — those fields should be absent
- NEVER extract vessel_name from query phrases like "highest PnL", "most voyages", "best performing", "earned the most" — those are metrics, not vessel names
- If the user asks for voyages ranked by PnL/revenue/commission/port calls and also requests vessel name, cargo grade, key ports, remarks, margin %, or expense ratio, keep it as a voyage ranking intent.
- If the user asks for module type breakdowns, use `analysis.by_module_type` even if cargo grades or ports are requested as output columns.
- If the user asks for vessels with voyage count / average PnL / most common cargo grade, use `ranking.vessels` because the grouped subject is vessels.
- If the user asks for voyages that visited a named port and then says rank by PnL/revenue, keep the intent voyage-ranking and extract the port as a filter slot.
- If the user asks about one specific vessel overall, its trend, voyage history, best/worst voyage, or whether it is performing well, use `vessel.summary`.
- NEVER output voyage_ids
- voyage_numbers must be int list
- limit must be int (default 10 if user says "top N" without specifying N)
- If no intent clearly fits → out_of_scope (use sparingly — always prefer the closest matching intent over out_of_scope)
- Ranking aliases (allowed):
  - ranking.pnl: Rank voyages/entities by PnL (e.g., "top 10 voyages by PnL", "highest profit voyages")
  - ranking.revenue: Rank voyages/entities by revenue
  - ranking.port_calls: Rank voyages by number of port calls/visits/stops (e.g., "show 10 voyages with most port calls")
- Use composite.query only when the question genuinely requires combining heterogeneous data in a way no single ranking/analysis intent can represent.

SLOT EXTRACTION:
- cargo_grades: list[str] | null
  Any petroleum/chemical cargo grade or product type the user mentions
  (e.g. "Naphtha", "Crude", "VLSFO", "Jet Fuel", "DPP", "CPP", "LNG", "Gasoil", etc.).
  Normalize to lowercase. Return null if no grade mentioned.
  Examples:
    "Show Naphtha voyages"     -> ["naphtha"]
    "crude and fuel oil ships" -> ["crude", "fuel oil"]
    "all voyages last month"   -> null
"""

        result = self._call_with_retry(
            system=system,
            user=json.dumps({"query": text_norm}),
            operation="intent_extraction",
        )

        if not result or not isinstance(result, dict):
            return {"intent_key": "out_of_scope", "slots": slots}

        intent = result.get("intent_key", "out_of_scope")
        llm_slots = result.get("slots", {}) or {}

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
                    from app.registries.intent_registry import INTENT_REGISTRY
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

        system = system_prompt or "Return SQL JSON only."

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
    ) -> str:
        """Alias function designed specifically for the voyage.summary override logic."""
        return self.summarize_answer(
            question=question,
            plan={"plan_type": "single", "intent_key": "voyage.summary"},
            merged=merged_data
        )

    def summarize_answer(
        self,
        *,
        question: str,
        plan: Dict[str, Any],
        merged: Dict[str, Any],
    ) -> str:

        intent_key = ""
        if isinstance(plan, dict):
            intent_key = str(plan.get("intent_key") or "").strip()

        # Graceful handling for out-of-scope / chit-chat queries.
        if intent_key == "out_of_scope":
            q = (question or "").strip()
            q_lower = q.lower()

            greeting_exact = {
                "hi", "hello", "hey", "hiya", "yo",
                "good morning", "good afternoon", "good evening",
                "help", "start",
            }
            if q_lower in greeting_exact or any(q_lower.startswith(p) for p in ("hi ", "hello ", "hey ")):
                return (
                    "### Hello\n"
                    "- I'm **Digital Sales Agent**, your maritime finance + operations analytics assistant.\n"
                    "- I can help you analyze **voyages, vessels, ports, cargo grades, delays/offhire, remarks**, and related **financial KPIs** (PnL, revenue, expense, TCE, commissions).\n\n"
                    "### Try asking\n"
                    "- \"For voyage 1901, summarize financials, key ports, cargo grades, and remarks\"\n"
                    "- \"Top 10 voyages by commission and include key ports and cargo grades\"\n"
                    "- \"For port Rotterdam, summarize the most common cargo grades across voyages\"\n"
                    "- \"Tell me about vessel Stena Superior: recent performance, frequent ports, and notable remarks\"\n"
                )

            identity_phrases = (
                "who are you", "who r you", "who are u", "who r u",
                "what are you", "what are u",
                "what can you do", "what can u do",
                "what do you do", "what do u do",
            )
            if any(p in q_lower for p in identity_phrases):
                return (
                    "### About Digital Sales Agent\n"
                    "- I'm **Digital Sales Agent**, a maritime analytics assistant focused on **voyage finance + operations**.\n"
                    "- I can answer questions about **PnL, revenue, expenses, TCE, commissions**, plus **ports/routes, cargo grades, delays/offhire, and voyage remarks**.\n\n"
                    "### Try asking\n"
                    "- \"For voyage 1901, summarize financials, key ports, cargo grades, and remarks\"\n"
                    "- \"Top 10 voyages by commission and include key ports and cargo grades\"\n"
                    "- \"For port Rotterdam, summarize the most common cargo grades across voyages\"\n"
                )

            if any(k in q_lower for k in ["weather", "temperature", "rain", "forecast", "climate"]):
                return (
                    "### Summary\n"
                    "- I can't provide live weather/forecast data from this system.\n"
                    "- If you want, tell me the **location and date/time**, and I can help you interpret weather impacts on voyages (delays, routing) using your operational/remark data.\n\n"
                    "### What I can help with here\n"
                    "- Voyage / vessel performance (P&L, costs, TCE, commission)\n"
                    "- Routes, ports, cargo grades, delays/offhire, and voyage remarks\n\n"
                    "### Try asking\n"
                    "- \"For voyage 1901, summarize financials, key ports, and remarks\"\n"
                    "- \"Top 5 most profitable voyages with key ports and remarks\"\n"
                )
            return (
                "### Summary\n"
                "- This question is outside the supported dataset/skills for this assistant.\n\n"
                "### What I can help with here\n"
                "- Voyage / vessel performance (P&L, costs, TCE, commission)\n"
                "- Routes, ports, cargo grades, delays/offhire, and voyage remarks\n\n"
                "### Try asking\n"
                "- \"Tell me about vessel Stena Superior: voyage profitability over time and frequent ports\"\n"
                "- \"For voyage 1901, financial summary + main ports + remarks\"\n"
            )

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
            ranking_hint = "Each object in merged_rows has numeric fields pnl, revenue, total_expense at the top level. You MUST include PnL and Revenue (and Total expense when present) as columns in the Results table. Do NOT say financial metrics are not available."

        style = self._derive_answer_style(question=question, intent_key=intent_key)

        system = """
You are a flagship-quality maritime analytics assistant (finance + operations).

HARD RULES:
- Use ONLY the provided JSON. Do NOT invent numbers, entities, or causes.
- If a value is missing/NULL, say "Not available" (do NOT convert to 0.0 unless the JSON explicitly says 0).
- Produce clean, readable Markdown with consistent headings and tables.
- TABLE RULE: Never put a conclusion, summary sentence, or explanatory text as a row inside a markdown table. All conclusions and summary text MUST appear BELOW the table as plain text paragraphs, completely outside the table. A table row must only contain data values, never sentences.
- EMPTY COLUMN RULE: Before rendering any table, scan every column across ALL rows. If a column contains only null, empty string, or 'Not available' for every single row — DROP that column entirely from the table. Do NOT render a column that has no real data in any row. A column must appear only if at least one row has an actual value.
- DELAY REASON RULE: If the question asks for delay reasons but the data has no populated delay_reason values, do NOT show a delay reason column. Instead add one line below the table: 'Delay reasons were not recorded for these voyages in the system.'
- Keep lists short and scannable. Never dump huge raw lists.
- Never repeat the same '###' heading more than once.
- You will receive style flags in data.style. Follow them strictly.
- Do NOT omit rows or metrics for brevity. Include all available data for every voyage/row in the result set.
- VERDICT FIRST RULE: Always open your response with exactly one sentence that states a direct judgment or conclusion answering the user's question. This sentence must contain an opinion or verdict (e.g. 'Voyage 1901 was profitable with no operational issues.' or 'NHC was the most profitable cargo grade.'). Never open with a table, a data list, or phrases like 'Based on the provided data...' or 'The dataset shows...'
- ARCHETYPE RULE: Identify the query type and apply the matching structure:
  DIAGNOSTIC (what went wrong / root cause / explain): prose narrative only, no tables. Format: Verdict -> Financial impact -> Remarks interpreted.
  RANKING (top N / highest / lowest / most): named winner in sentence 1, then ranked table (max 5 rows), then one insight line.
  SNAPSHOT (summary of one voyage/vessel): Verdict -> Financial table (Revenue, Expense, PnL, TCE only) -> Ports (max 5) -> Remarks classified.
  COMPARISON (actual vs fixed / compare voyages): Pattern statement first, then comparison table, then remarks that confirm the pattern.
  FLEET/VESSEL PROFILE (how is vessel X doing): Performance verdict -> Best voyage -> Worst voyage -> Trend if visible.
  AGGREGATE (by grade / by module type / by segment): Winner named first, then grouped table sorted by performance, then pattern note.
- REMARKS CLASSIFICATION RULE: Every remark shown to the user must be prefixed with its category in brackets. Categories: [Operational Issue] [Financial Adjustment] [Administrative] [Delay Related]. Never show a raw unclassified remark string to the user. If no remarks exist, write 'No remarks on record.' once only - never repeat it per row.
- PORT BREVITY RULE: Show a maximum of 5 ports per voyage. Format must be: 'Port A (L), Port B (D), Port C (D) (+N more)'. Never bullet-list more than 5 ports. Never show a 10+ port list.
- NUMBERS WITH CONTEXT RULE: When showing PnL and revenue is also available, include the margin percentage inline: '$982K PnL on $7.2M revenue (13.6% margin)'. When showing offhire days and expense is available, include: '67 offhire days - estimated cost impact $X'.
- INCIDENT FORMAT RULE: For diagnostic queries (what went wrong, root cause, explain delays, give incident summary), use flowing prose paragraphs - NOT tables. Tables are only for ranking, comparison, and snapshot queries.
- FOLLOW-UP RULE: If the voyage or vessel was already introduced in a prior response in this session, do NOT re-introduce it. Skip 'Voyage 1901 operated by Stena Conquest...' and go directly to the new information being asked.
- EMPTY COLUMN RULE: Before rendering any table, scan every column across ALL rows. If a column contains only null, empty string, or 'Not available' for every single row - DROP that column entirely. A column must only appear if at least one row has an actual value.
- TABLE RULE: Never put a conclusion, summary sentence, or explanatory text as a row inside a markdown table. All conclusions MUST appear BELOW the table as plain text. A table row contains only data values.
- VESSEL ID RULE: The vessel_name field is ALWAYS populated in the merged data. Use vessel_name as the row identifier. If you see a column called vessel_id or voyage_id containing a long hex string, do NOT show that column — drop it entirely. The column header must be 'Vessel' or 'Vessel Name', never 'Vessel ID'.
- AMBIGUOUS VOYAGE RULE: If the data contains multiple rows with the same voyage_number but different vessel_name values, do NOT silently pick one. Open the response with exactly this pattern: 'Voyage [X] exists across [N] vessels in the dataset. Showing all results below — specify a vessel name to narrow down.' Then show a table with voyage_number, vessel_name, PnL, revenue as the first columns so the user can identify which vessel they meant. Never merge rows from different vessels even if voyage_number matches.

DATA PRIORITY:
- If data.artifacts.merged_rows exists, it is the PRIMARY joined dataset (one item per voyage).
- Prefer merged_rows over raw mongo/finance/ops sections when available.
- In merged_rows, KPIs may appear at the TOP LEVEL (pnl, revenue, total_expense, tce, total_commission) even if finance.rows is empty.
- In merged_rows, ops enrichment may appear as cargo_grades, key_ports, and remarks (even if ops.rows is empty).
- When grades/ports/remarks exist in the JSON, include them. Do NOT claim they are unavailable.
- If data.artifacts.coverage is present, use it to avoid false "Not available" claims.
- For ranking.* intents: each item in merged_rows HAS pnl, revenue, total_expense at the top level. You MUST include PnL and Revenue (and Total expense when present) as columns in the Results table.

STYLE / STRUCTURE (always follow):
- Start with a 2–4 bullet **Summary** of the key result.
- Use '-' for bullet points (not '*').
- Use sections with '###' headings only.
- Prefer tables for numeric KPIs; include currency formatting for USD amounts.
- Cap long lists:
  - Ports: show at most 8; if more, add "(+N more)".
  - Grades: show at most 8; if more, add "(+N more)".
  - Remarks: show at most 3 short bullets; if more, add "(+N more)".

STYLE FLAGS (data.style):
- If narrative_summary=true: Summary MUST start with 1–2 narrative bullets (full sentences) BEFORE any KPI/template bullets.
- If narrative_summary=false and financial_first=true: lead with KPI bullets + the Financials table.
- If financial_first=false: keep the response more narrative/operational first, but still include the Financials table.

TEMPLATES BY INTENT:

1) voyage.summary (single voyage):
IMPORTANT: Tailor the emphasis to the user's wording.
- If the question contains phrases like "what happened" or "summarize", write a brief 2–4 sentence narrative in the Summary (still using bullets) describing what stands out operationally and financially, then include the tables/lists.
- If the question asks specifically for "financial summary" first, lead with the KPI line and table.
### Summary
- **Voyage**: <voyage_number>
- **Vessel**: <vessel_name> (IMO: <imo>) when available
- **PnL / Revenue / Expense / TCE**: include if present
- **Key ports**: 5–8 max with (L/D) if present
- **Remarks**: 0–3 bullets; if none, say "No remarks recorded"

### Financials (ACTUAL)
| Metric | Value |
| --- | --- |
| Revenue | ... |
| Total expense | ... |
| PnL | ... |
| TCE | ... |
| Total commission | ... |

### Operational snapshot
- **Key ports**: <comma-separated capped list>
- **Cargo grades** (if present): <capped list>

### Remarks
- <bullet 1>
- <bullet 2>

2) ranking.* (multiple voyages):
- CRITICAL: merged_rows for ranking ALWAYS contain pnl, revenue, total_expense at the top level. Include PnL and Revenue (and Total expense, TCE, Total commission when present) as columns in the Results table.
- Include ALL rows in the result set.
- When merged_rows contain offhire_days: include **Offhire days** as a column.
- When merged_rows contain is_delayed: include **Is delayed** as a column if the user asked for delay status.
- When the question asks for "most port calls" or merged_rows contain port_calls: include **Port calls** as a column.
- If the user asks for margin % or expense ratio and both revenue and total_expense are available, derive it in the answer:
  - margin % = pnl / revenue * 100
  - expense ratio = total_expense / revenue
- If the user asks about commission types and merged_rows contain commissions, include a compact **Commission types** column derived from the commissionType values.
### Summary
- **Ranking**: what is being ranked and limit
- **Top result**: voyage_number + key metric value (e.g. PnL)

### Results
| Voyage # | PnL | Revenue | Total expense | Total commission | Key ports | Cargo grades | Remarks |
| --- | --- | --- | --- | --- | --- | --- | --- |
(Only include columns that exist in the JSON and are relevant to the question.)

2b) ranking.vessels (vessel-level aggregates):
- When merged_rows contain vessel_imo, vessel_name, voyage_count (no voyage_id), show a **vessel-level** table.
- Choose columns based on the user's metric words:
  - "expensive/cost/expense/bunker" -> include Total expense / Bunker cost if present
  - "revenue/earning" -> include Revenue
  - "tce" -> include TCE
  - "pnl/profit/performance" -> include PnL
  - "operating/scrubber/market type" -> include those categorical columns when present
### Summary
- **Vessels**: what was ranked and by which metric
- **Count**: how many vessels

### Results
| Vessel (IMO) | Vessel name | Voyage count | Chosen metric(s) | Cargo grades |
| --- | --- | --- | --- | --- |
- List the most common cargo grades per vessel from the cargo_grades array in each row.

3) analysis.* and aggregation.* (fleet-wide aggregates):
### Summary
- **What was grouped by** and **what metric**

### Results
Use a compact table. If a metric is missing for a group, show "Not available".

FAILSAFE:
- ONLY if the provided JSON is completely empty (no rows anywhere) then output exactly: "Not available in dataset."

4) vessel.summary (single vessel / overview):
- Write a short narrative briefing (2–5 sentences) describing what we know about the vessel's voyage performance.
- If the question asks about "recently", include a **Recent voyages** table with the latest 3 voyages by end date.
- If the question asks "good or bad" or "best/worst", include **Best voyage** and **Worst voyage** (by PnL) as a compact 2–3 row table.
- If ports/grades/remarks are present in ops rows, include:
  - ### Frequent ports: up to 8 ports (add "(+N more)" if needed)
  - ### Common cargo grades: up to 8 grades (add "(+N more)" if needed)
  - ### Recent remarks: up to 3 short bullets; ignore empty/null remarks
- If ports/grades/remarks are missing, state that plainly in one line under a "### Data coverage" section.
"""

        result = self._call_with_retry(
            system=system,
            user=json.dumps(
                {
                    "question": question,
                    "plan": plan,
                    "intent_key": intent_key,
                    "data": {**(merged_safe if isinstance(merged_safe, dict) else {}), "style": style},
                    "merged_rows": merged_rows,
                    **({("instruction"): ranking_hint} if ranking_hint else {}),
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
        cleaned = _enforce_table_rules(cleaned)
        return cleaned if cleaned else "Not available in dataset."

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

        # Always polish — every response must be question-driven, not template-driven
        should_polish = True

        system = (
            "You are a senior maritime analyst writing professional answers for a shipping analytics platform.\n"
            "\n"
            "YOUR ONLY JOB: Read the user question carefully and answer EXACTLY what was asked — nothing more, nothing less.\n"
            "Do NOT produce a generic template. Do NOT add sections the user did not ask for.\n"
            "\n"
            "UNIVERSAL RULES:\n"
            "- Structure your answer around the user question, not around a fixed template.\n"
            "- If they asked for a summary → write 2-4 narrative sentences first, then support with data.\n"
            "- If they asked for a ranking → lead with the ranked table, add 1-2 sentence insight after.\n"
            "- If they asked for remarks or delays → quote the actual remarks and explain what they mean.\n"
            "- If they asked for ports → list them with context (load/discharge), not a raw comma dump.\n"
            "- If they asked a yes/no or count question → answer it directly in the first sentence.\n"
            "- Blend narrative and tables — do not dump raw data without context.\n"
            "- Use ONLY the provided JSON. Never invent numbers, ports, remarks, or vessel names.\n"
            "- If a value is missing in JSON, say Not available — never assume or default to 0.\n"
            "- Keep the response concise but complete. No filler. No repetition.\n"
            "- REMARKS RULE: Never quote full contract text or long raw remarks verbatim. Summarize each remark in 1 short sentence max. Cap at 3 remarks.\n"
            "- TABLE RULE: Never put a conclusion sentence as a row inside a markdown table. Conclusions go BELOW the table as plain text.\n"
            "- VESSEL RULE: Always show vessel name clearly, not a raw database ID or UUID.\n"
        )

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

        narrative_triggers = (
            "what happened",
            "summarize",
            "summary of what happened",
            "what went wrong",
            "root cause",
            "brief me",
            "give me",
            "executive summary",
            "explain",
            "explaining",
            "walk me through",
            "overview of",
            "what are the",
            "what were",
            "generate",
        )
        narrative_summary = any(t in ql for t in narrative_triggers)

        financial_first = "financial summary" in ql or (
            any(k in ql for k in ("revenue", "expense", "expenses", "pnl", "tce", "commission"))
            and not narrative_summary
        )

        ask_ports = any(k in ql for k in ("port", "ports", "route", "routing"))
        ask_grades = any(k in ql for k in ("grade", "grades", "cargo"))
        ask_remarks = any(k in ql for k in ("remark", "remarks", "issue", "issues", "delay", "delays")) or narrative_summary

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

        # Fix compacted number-word boundaries in narrative text.
        s = re.sub(r"(?<=\d)(?=[A-Za-z])", " ", s)
        s = re.sub(r"(?<=[A-Za-z])(?=\d)", " ", s)

        # Normalize whitespace but keep markdown line breaks.
        s = re.sub(r"[ \t]+", " ", s)
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
            for r in ops["rows"]:
                if not isinstance(r, dict):
                    continue
                r["ports_json"] = _cap_list(r.get("ports_json"), 20)
                r["grades_json"] = _cap_list(r.get("grades_json"), 20)
                r["remarks_json"] = _cap_list(r.get("remarks_json"), 10)

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