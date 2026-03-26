# app/orchestration/planner.py

from __future__ import annotations
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from app.registries.intent_registry import (
    INTENT_REGISTRY,
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
# Planner (REGISTRY-DRIVEN, NO HARDCODED INTENT LISTS)
# =========================================================

class Planner:

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
        force_composite: bool = False,
    ) -> ExecutionPlan:
        """
        Deterministic planner using already extracted intent + slots.
        No second LLM call. Prevents router/planner mismatch.

        force_composite=True: skip all single-path checks and go straight to
        composite + dynamic SQL. Used by graph_router when registry SQL returns
        zero rows with no entity anchor (fleet-wide query misclassified as single).
        """
        text_lower = text.lower()

        intent_key = resolve_intent(intent_key or "out_of_scope")
        slots = slots or {}

        # ── force_composite override ──────────────────────────────────────────
        # When zero-row escalation fires in n_run_single with no entity anchor,
        # remap entity-level intents to their fleet-wide equivalents so the
        # composite path generates the right SQL (e.g. vessel.summary → ranking.vessels).
        _ENTITY_TO_FLEET = {
            "vessel.summary": "ranking.vessels",
            "vessel.entity":  "ranking.vessels",
            "voyage.summary": "ranking.voyages",
            "voyage.entity":  "ranking.voyages",
        }
        _has_entity_anchor = bool(
            slots.get("voyage_number")
            or slots.get("voyage_numbers")
            or slots.get("voyage_id")
            or slots.get("vessel_name")
            or slots.get("imo")
        )
        if force_composite and not _has_entity_anchor:
            intent_key = _ENTITY_TO_FLEET.get(intent_key, intent_key)

        if force_composite:
            return self._build_composite(intent_key, text, slots, confidence=0.90)

        # Never run composite plans for out-of-scope.
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

        # SINGLE voyage only
        if slots.get("voyage_numbers") and len(slots["voyage_numbers"]) == 1:
            rankingish = any(k in text_lower for k in ("top", "rank", "compare", "vs", "versus", "variance", "including", "include"))
            if not rankingish and ("voyage" in text_lower or (intent_key or "").startswith("voyage.")):
                return ExecutionPlan(
                    plan_type="single",
                    intent_key="voyage.summary",
                    required_slots=[],
                    confidence=0.95,
                    steps=[],
                )

        # SINGLE vessel only
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

        # SINGLE port query
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
        # 2️⃣ REGISTRY-DRIVEN COMPOSITE ROUTING
        # -----------------------------------------------------
        intent_cfg = INTENT_REGISTRY.get(intent_key, {})
        if intent_cfg.get("route") == "composite":
            return self._build_composite(intent_key, text, slots, confidence=0.90)

        # Text-based composite overrides
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

        default_limit = 50 if intent_key == "analysis.by_module_type" else 10
        limit = slots.get("limit", default_limit)

        steps: List[ExecutionStep] = []

        intent_cfg = INTENT_REGISTRY.get(intent_key, {})
        use_mongo = intent_cfg.get("needs", {}).get("mongo", True)

        # STEP 0 — Mongo anchor resolution (only when mongo needed + entity hint present)
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
                    inputs={"goal": text},
                )
            )

        # STEP 1 — Finance dynamic SQL
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

        # STEP 2 — Ops dynamic SQL (skip for scenario comparison)
        if intent_key != "analysis.scenario_comparison":
            steps.append(
                ExecutionStep(
                    agent="ops",
                    operation="dynamicSQL",
                    inputs={"voyage_ids": "$finance.voyage_ids"},
                )
            )

        # STEP 3 — Mongo fetch remarks (skip for aggregate intents)
        if use_mongo:
            steps.append(
                ExecutionStep(
                    agent="mongo",
                    operation="fetchRemarks",
                    inputs={"voyage_ids": "$finance.voyage_ids"},
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