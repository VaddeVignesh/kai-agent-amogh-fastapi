from __future__ import annotations

from typing import Any, Dict

from app.orchestration.handlers.router_context import (
    bind_graph_router_globals,
    build_router_context,
)


def handle_validate_slots(router: Any, state: Dict[str, Any]) -> Dict[str, Any]:
    bind_graph_router_globals(globals())
    ctx = build_router_context(router)
    self = router

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

    # Multi-voyage summary: voyage_numbers satisfies the voyage_number requirement.
    if intent_key in ("voyage.summary", "voyage.metadata") and "voyage_number" in missing:
        vnums = slots.get("voyage_numbers")
        if isinstance(vnums, list) and len(vnums) >= 1:
            missing = [k for k in missing if k != "voyage_number"]
        elif slots.get("voyage_id") not in (None, "", [], {}):
            missing = [k for k in missing if k != "voyage_number"]

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
