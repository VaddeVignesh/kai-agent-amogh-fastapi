# app/orchestration/planner.py

from __future__ import annotations
from dataclasses import dataclass
import logging
from typing import Any, Dict, List, Optional

from app.config.routing_rules_loader import (
    get_planner_entity_to_fleet_intent_map,
    get_planner_finance_keywords,
    get_planner_mongo_keywords,
    get_planner_port_rankingish_terms,
    get_planner_rankingish_terms,
    get_planner_single_vessel_composite_terms,
    get_planner_text_composite_overrides,
    get_planner_voyage_metadata_terms,
)
from app.registries.intent_loader import get_yaml_registry_facade

_INTENT_FACADE = get_yaml_registry_facade(validate_parity=True)
INTENT_REGISTRY = _INTENT_FACADE["INTENT_REGISTRY"]
resolve_intent = _INTENT_FACADE["resolve_intent"]

# DEPRECATED Phase 5C — active only as fallback. Values now live in routing_rules.yaml.
FINANCE_KEYWORDS = get_planner_finance_keywords()
MONGO_KEYWORDS = get_planner_mongo_keywords()

logger = logging.getLogger(__name__)


def _is_mixed_voyage_query(user_input: str, session_context: dict = None) -> bool:
    """Returns True if query needs both PostgreSQL and MongoDB sources.
    
    PRIMARY: Uses structured intent required_sources if available and confident.
    FALLBACK: Uses keyword matching when structured intent is absent.
    """
    # PRIMARY: structured intent required_sources
    _si = (session_context or {}).get("_structured_intent")
    if _si and _si.get("required_sources") and _si.get("confidence") in ("high", "medium"):
        sources = _si["required_sources"]
        return "postgres" in sources and "mongo" in sources
    # FALLBACK: keyword matching only when structured intent absent
    text = (user_input or "").lower()
    needs_finance = any(kw in text for kw in FINANCE_KEYWORDS)
    needs_mongo = any(kw in text for kw in MONGO_KEYWORDS)
    return needs_finance and needs_mongo


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
    """
    Execution planner for multi-agent query routing.

    Determines which agents to invoke (finance, ops, mongo) and in what
    configuration (single, composite, registry, dynamic) based on the
    classified intent and query context.

    Note: Contains keyword-based heuristics for source selection and
    composite query detection. These are scheduled for replacement with
    schema-driven config routing in a future refactor.
    """

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
        session_context = session_context if isinstance(session_context, dict) else {}

        # Phase 5C Slice 1: structured intent owns source decision
        _si = session_context.get("_structured_intent")
        _sources_resolved = False
        if _si and _si.get("required_sources") and _si.get("confidence") in ("high", "medium"):
            _resolved_sources = _si["required_sources"]
            _sources_resolved = True
            logger.info(
                f"[planner] sources from structured intent: {_resolved_sources} | "
                f"confidence={_si.get('confidence')}"
            )

        intent_key = resolve_intent(intent_key or "out_of_scope")
        slots = slots or {}

        def _with_plan_log(plan: ExecutionPlan, branch_name: str) -> ExecutionPlan:
            plan_type = plan.plan_type
            logger.info(
                f"[planner] intent_key={intent_key!r} | "
                f"plan_type={plan_type!r} | "
                f"note=ops_step_may_be_skipped_if_finance_is_aggregate"
            )
            return plan

        # ── force_composite override ──────────────────────────────────────────
        # When zero-row escalation fires in n_run_single with no entity anchor,
        # remap entity-level intents to their fleet-wide equivalents so the
        # composite path generates the right SQL (e.g. vessel.summary → ranking.vessels).
        entity_to_fleet = get_planner_entity_to_fleet_intent_map()
        _has_entity_anchor = bool(
            slots.get("voyage_number")
            or slots.get("voyage_numbers")
            or slots.get("voyage_id")
            or slots.get("vessel_name")
            or slots.get("imo")
        )
        if force_composite and not _has_entity_anchor:
            intent_key = entity_to_fleet.get(intent_key, intent_key)

        if force_composite:
            return _with_plan_log(
                self._build_composite(intent_key, text, slots, confidence=0.90),
                "force_composite",
            )

        # Never run composite plans for out-of-scope.
        if intent_key == "out_of_scope":
            return _with_plan_log(ExecutionPlan(
                plan_type="single",
                intent_key=intent_key,
                required_slots=[],
                confidence=0.70,
                steps=[],
            ), "out_of_scope_single")

        # -----------------------------------------------------
        # 1️⃣ ENTITY ANCHOR (ALWAYS SINGLE)
        # -----------------------------------------------------

        if (
            slots.get("cargo_grades")
            and not slots.get("voyage_number")
            and not slots.get("voyage_id")
            and not slots.get("voyage_numbers")
        ):
            return _with_plan_log(ExecutionPlan(
                plan_type="composite",
                intent_key=intent_key,
                required_slots=[],
                confidence=0.93,
                steps=[
                    ExecutionStep(
                        agent="mongo",
                        operation="cargo_grade_lookup",
                        inputs={
                            "cargo_grades": slots.get("cargo_grades"),
                            "slots": slots,
                        },
                    ),
                    ExecutionStep(
                        agent="ops",
                        operation="voyage_ids_from_step",
                        inputs={
                            "voyage_ids": "$mongo.voyage_ids",
                            "intent_key": intent_key,
                            "slots": slots,
                        },
                    ),
                    ExecutionStep(
                        agent="llm",
                        operation="merge",
                        inputs={},
                    ),
                ],
            ), "cargo_grade_composite")

        # MIXED voyage intent: financial + metadata in one anchored query.
        if (
            intent_key in ("voyage.metadata", "voyage.summary")
            and (
                (isinstance(slots.get("voyage_numbers"), list) and len(slots.get("voyage_numbers") or []) >= 1)
                or slots.get("voyage_number")
                or slots.get("voyage_id")
            )
            and _is_mixed_voyage_query(text, session_context)
        ):
            return _with_plan_log(ExecutionPlan(
                plan_type="multi",
                intent_key="voyage.summary",
                required_slots=[],
                confidence=0.96,
                steps=[
                    ExecutionStep(
                        agent="finance",
                        operation="single_intent",
                        inputs={
                            "step_index": 1,
                            "intent_key": "voyage.summary",
                            "description": "Fetch actual PnL, revenue, expense, TCE from PostgreSQL",
                            "voyage_number": slots.get("voyage_number"),
                            "voyage_id": slots.get("voyage_id"),
                            "vessel_imo": slots.get("vessel_imo"),
                            "vessel_name": slots.get("vessel_name"),
                            "scenario": slots.get("scenario"),
                            "slots": slots,
                        },
                    ),
                    ExecutionStep(
                        agent="mongo",
                        operation="single_intent",
                        inputs={
                            "step_index": 2,
                            "intent_key": "voyage.metadata",
                            "description": "Fetch remarks, ports, cargo, fixture from MongoDB",
                            "slots": slots,
                        },
                    ),
                ],
            ), "mixed_voyage_multi")

        # SINGLE voyage only
        if slots.get("voyage_numbers") and len(slots["voyage_numbers"]) == 1:
            rankingish = any(k in text_lower for k in get_planner_rankingish_terms())
            if not rankingish and ("voyage" in text_lower or (intent_key or "").startswith("voyage.")):
                voyage_intent = intent_key if (intent_key or "").startswith("voyage.") else "voyage.summary"
                if voyage_intent == "voyage.summary":
                    if any(k in text_lower for k in get_planner_voyage_metadata_terms()):
                        voyage_intent = "voyage.metadata"
                return _with_plan_log(ExecutionPlan(
                    plan_type="single",
                    intent_key=voyage_intent,
                    required_slots=[],
                    confidence=0.95,
                    steps=[],
                ), "single_voyage")

        # SINGLE vessel only
        if (
            slots.get("vessel_name")
            and (intent_key or "").startswith("vessel.")
            and not any(k in text_lower for k in get_planner_single_vessel_composite_terms())
        ):
            vessel_intent = intent_key if intent_key in ("vessel.summary", "vessel.entity", "vessel.metadata") else "vessel.summary"
            return _with_plan_log(ExecutionPlan(
                plan_type="single",
                intent_key=vessel_intent,
                required_slots=[],
                confidence=0.95,
                steps=[],
            ), "single_vessel")

        # SINGLE port query
        if intent_key in ("ops.port_query", "ops.voyages_by_port") and slots.get("port_name"):
            rankingish = any(k in text_lower for k in get_planner_port_rankingish_terms())
            if not rankingish:
                return _with_plan_log(ExecutionPlan(
                    plan_type="single",
                    intent_key=intent_key,
                    required_slots=[],
                    confidence=0.92,
                    steps=[],
                ), "single_port")

        # -----------------------------------------------------
        # 2️⃣ REGISTRY-DRIVEN COMPOSITE ROUTING
        # -----------------------------------------------------
        intent_cfg = INTENT_REGISTRY.get(intent_key, {})
        if intent_cfg.get("route") == "composite":
            return _with_plan_log(
                self._build_composite(intent_key, text, slots, confidence=0.90),
                "intent_registry_composite",
            )

        # Text-based composite overrides
        if not _sources_resolved:
            for override in get_planner_text_composite_overrides():
                all_terms = override.get("all") or []
                any_terms = override.get("any") or []
                if all(term in text_lower for term in all_terms) and (not any_terms or any(term in text_lower for term in any_terms)):
                    return _with_plan_log(
                        self._build_composite(intent_key, text, slots, confidence=float(override.get("confidence") or 0.90)),
                        override.get("name") or "text_composite",
                    )

        # -----------------------------------------------------
        # 3️⃣ Default: Single
        # -----------------------------------------------------
        return _with_plan_log(ExecutionPlan(
            plan_type="single",
            intent_key=intent_key,
            required_slots=[],
            confidence=0.80,
            steps=[],
        ), "default_single")

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