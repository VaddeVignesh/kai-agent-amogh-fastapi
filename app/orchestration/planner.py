# app/orchestration/planner.py

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from app.registries.intent_registry import (
    SUPPORTED_INTENTS,
    resolve_intent,
)


# =========================================================
# Plan Models
# =========================================================

@dataclass
class ExecutionStep:
    agent: str
    operation: str
    inputs: Dict[str, Any]


@dataclass
class ExecutionPlan:
    plan_type: str  # single | composite
    intent_key: str
    required_slots: List[str]
    confidence: float
    steps: List[ExecutionStep]


# =========================================================
# Planner (HYBRID RULE + INTENT)
# =========================================================

class Planner:

    # Core intents that are ALWAYS composite
    HARD_COMPOSITE_INTENTS = {
        "ranking.voyages",
        "ranking.voyages_by_pnl",
        "ranking.voyages_by_revenue",
        "ranking.voyages_by_commission",
        "ranking.vessels",
        "ranking.cargo",
        "ranking.ports",
        "analysis.segment_performance",
        "analysis.high_revenue_low_pnl",
        "analysis.revenue_vs_pnl",
        "analysis.profitability",
        "comparison.voyages",
        "comparison.vessels",
        "comparison.scenario",
    }

    def __init__(self, llm_client):
        self.llm = llm_client

    # =========================================================
    # Build Plan
    # =========================================================

    def build_plan(
        self,
        *,
        text: str,
        session_context: Optional[Dict[str, Any]] = None,
        intent_key: Optional[str] = None,
        slots: Optional[Dict[str, Any]] = None,
    ) -> ExecutionPlan:
        """
        Deterministic planner using already extracted intent + slots.
        No second LLM call. Prevents router/planner mismatch.
        """
        text_lower = text.lower()

        # Use router-provided intent + slots
        intent_key = resolve_intent(intent_key or "out_of_scope")
        slots = slots or {}

        # Never run composite plans for out-of-scope. Avoids accidental SQL/Mongo calls
        # when the extractor falls back to out_of_scope for a valid-looking question.
        if intent_key == "out_of_scope":
            return ExecutionPlan(
                plan_type="single",
                intent_key=intent_key,
                required_slots=[],
                confidence=0.70,
                steps=[],
            )

        # -----------------------------------------------------
        # 1️⃣ ENTITY ANCHOR (ALWAYS SINGLE)
        # -----------------------------------------------------
        
        # SINGLE voyage only (also recover from out_of_scope when query is clearly an entity summary)
        if slots.get("voyage_numbers") and len(slots["voyage_numbers"]) == 1:
            # Avoid forcing single for ranking/compare/include intents (e.g., "top 5 including voyage 1901")
            rankingish = any(k in text_lower for k in ("top", "rank", "compare", "vs", "versus", "variance", "including", "include"))
            if not rankingish and ("voyage" in text_lower or (intent_key or "").startswith("voyage.")):
                return ExecutionPlan(
                    plan_type="single",
                    intent_key="voyage.summary",
                    required_slots=[],
                    confidence=0.95,
                    steps=[],
                )

        # SINGLE vessel only (only when the intent is actually a vessel summary and not a trend/ranking ask)
        if (
            slots.get("vessel_name")
            and (intent_key or "").startswith("vessel.")
            and not any(k in text_lower for k in ("over time", "trend", "most frequent", "frequent ports", "ports visited"))
        ):
            vessel_intent = intent_key if intent_key in ("vessel.summary", "vessel.entity", "vessel.metadata") else "vessel.summary"
            return ExecutionPlan(
                plan_type="single",
                intent_key=vessel_intent,
                required_slots=[],
                confidence=0.95,
                steps=[],
            )

        # SINGLE port query (keep as single; router can enrich with Mongo when needed)
        if intent_key in ("ops.port_query", "ops.voyages_by_port") and slots.get("port_name"):
            rankingish = any(k in text_lower for k in ("top", "rank", "compare", "vs", "versus", "variance"))
            if not rankingish:
                return ExecutionPlan(
                    plan_type="single",
                    intent_key=intent_key,
                    required_slots=[],
                    confidence=0.92,
                    steps=[],
                )

        # -----------------------------------------------------
        # 2️⃣ INTENT-SPECIFIC ROUTING
        # -----------------------------------------------------
        
        composite_targets = {
            "ranking.voyages",
            "ranking.voyages_by_pnl",
            "ranking.voyages_by_revenue",
            "ranking.voyages_by_commission",
            "analysis.scenario_comparison",
            "analysis.cargo_profitability",
            "analysis.by_module_type",
            "ops.delayed_voyages",
            # NOTE: ops.port_query is handled as single when port_name is present
            # "ops.port_query",
            "analysis.segment_performance",
            "composite.query",
        }

        if intent_key in self.HARD_COMPOSITE_INTENTS or intent_key in composite_targets:
            return self._build_composite(intent_key, text, slots, confidence=0.90)

        # Rule-based overrides for composite intents based on query phrasing
        if "offhire" in text_lower and ("pnl" in text_lower or "tce" in text_lower):
            return self._build_composite(intent_key, text, slots, confidence=0.95)

        if "delayed" in text_lower and ("pnl" in text_lower or "expense" in text_lower):
            return self._build_composite(intent_key, text, slots, confidence=0.95)

        if "over time" in text_lower or "trend" in text_lower:
            return self._build_composite(intent_key, text, slots, confidence=0.92)

        if "cargo" in text_lower and "port" in text_lower:
            return self._build_composite(intent_key, text, slots, confidence=0.92)

        # -----------------------------------------------------
        # 3️⃣ Default: Single
        # -----------------------------------------------------
        return ExecutionPlan(
            plan_type="single",
            intent_key=intent_key,
            required_slots=[],
            confidence=0.80,
            steps=[],
        )

    # =========================================================
    # Composite Builder
    # =========================================================

    def _build_composite(
        self,
        intent_key: str,
        text: str,
        slots: Dict[str, Any],
        confidence: float = 0.9,
    ) -> ExecutionPlan:

        # Higher default for by_module_type so we get multiple module types and PnL spread
        default_limit = 50 if intent_key == "analysis.by_module_type" else 10
        limit = slots.get("limit", default_limit)

        steps: List[ExecutionStep] = []

        no_mongo_intents = {
            "analysis.scenario_comparison",
            "analysis.by_module_type",
            "analysis.cargo_profitability",
            "ranking.vessels",
        }
        use_mongo = intent_key not in no_mongo_intents

        # STEP 0 — Mongo (optional): resolve specific anchor(s) before ranking
        # This allows queries like "including voyage 1901" to deterministically
        # resolve voyageId/imo first and carry it through later steps.
        text_lower = (text or "").lower()
        has_entity_hint = bool(
            slots.get("voyage_id")
            or slots.get("voyage_number")
            or slots.get("voyage_numbers")
            or slots.get("imo")
            or slots.get("vessel_name")
            or ("voyage" in text_lower and any(ch.isdigit() for ch in text_lower))
        )
        if has_entity_hint and use_mongo:
            steps.append(
                ExecutionStep(
                    agent="mongo",
                    operation="resolveAnchor",
                    inputs={
                        "goal": text,
                    },
                )
            )

        # STEP 1 — Finance (composite always uses dynamic SQL for finance; registry only for single-query)
        steps.append(
            ExecutionStep(
                agent="finance",
                operation="dynamicSQL",
                inputs={
                    "question": text,
                    "limit": limit,
                    "intent_key": intent_key,
                },
            )
        )

        # STEP 2 — Ops (scenario comparison does not require ops)
        if intent_key != "analysis.scenario_comparison":
            steps.append(
                ExecutionStep(
                    agent="ops",
                    operation="dynamicSQL",
                    inputs={
                        "voyage_ids": "$finance.voyage_ids"
                    },
                )
            )

        # STEP 3 — Mongo (skip for aggregate intents)
        if use_mongo:
            steps.append(
                ExecutionStep(
                    agent="mongo",
                    operation="fetchRemarks",
                    inputs={
                        "voyage_ids": "$finance.voyage_ids"
                    },
                )
            )

        # STEP 4 — Merge
        steps.append(
            ExecutionStep(
                agent="llm",
                operation="merge",
                inputs={},
            )
        )

        return ExecutionPlan(
            plan_type="composite",
            intent_key=intent_key,
            required_slots=[],
            confidence=confidence,
            steps=steps,
        )