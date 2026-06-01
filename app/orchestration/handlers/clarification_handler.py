from __future__ import annotations

from typing import Any, Dict

from app.orchestration.handlers.router_context import (
    bind_graph_router_globals,
    build_router_context,
)


def handle_make_clarification(router: Any, state: Dict[str, Any]) -> Dict[str, Any]:
    bind_graph_router_globals(globals())
    ctx = build_router_context(router)
    self = router

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
