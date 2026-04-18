"""
AlphaAgent — multi-agent NLQ system.

Lazy import of `build_graph` / `run_agent` so that importing `agent.config`
or `agent.safe_exec` does not pull in langgraph (which is heavy and may not
be installed in every environment, e.g. a CI job that only runs API tests).
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

__all__ = ["build_graph", "run_agent"]


def __getattr__(name: str) -> Any:
    if name in {"build_graph", "run_agent"}:
        from agent import graph as _graph
        return getattr(_graph, name)
    raise AttributeError(f"module 'agent' has no attribute {name!r}")


if TYPE_CHECKING:
    from agent.graph import build_graph, run_agent  # noqa: F401
