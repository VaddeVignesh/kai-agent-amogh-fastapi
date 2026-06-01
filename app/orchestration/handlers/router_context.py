from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RouterExecutionContext:
    """Minimal execution context shared by extracted graph-router handlers."""

    router: Any
    llm: Any
    redis: Any
    mongo_agent: Any
    finance_agent: Any
    ops_agent: Any
    planner: Any


def build_router_context(router: Any) -> RouterExecutionContext:
    return RouterExecutionContext(
        router=router,
        llm=router.llm,
        redis=router.redis,
        mongo_agent=router.mongo_agent,
        finance_agent=router.finance_agent,
        ops_agent=router.ops_agent,
        planner=router.planner,
    )


def bind_graph_router_globals(target_globals: dict[str, Any]) -> None:
    """Expose graph_router module globals to mechanically extracted handlers."""
    from app.orchestration import graph_router as gr

    for name in dir(gr):
        if not name.startswith("__"):
            target_globals.setdefault(name, getattr(gr, name))
