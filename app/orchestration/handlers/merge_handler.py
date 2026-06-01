from __future__ import annotations

from typing import Any, Dict

from app.orchestration.handlers.router_context import (
    bind_graph_router_globals,
    build_router_context,
)


def handle_merge(router: Any, state: Dict[str, Any]) -> Dict[str, Any]:
    bind_graph_router_globals(globals())
    ctx = build_router_context(router)
    self = router

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
