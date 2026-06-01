from __future__ import annotations

from typing import Any, Dict

from app.orchestration.handlers.router_context import (
    bind_graph_router_globals,
    build_router_context,
)


def handle_plan(router: Any, state: Dict[str, Any]) -> Dict[str, Any]:
    bind_graph_router_globals(globals())
    ctx = build_router_context(router)
    self = router

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
