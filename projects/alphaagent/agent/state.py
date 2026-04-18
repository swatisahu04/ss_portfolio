"""Shared state passed between LangGraph nodes."""
from __future__ import annotations

from typing import Any, Literal, TypedDict


class AgentState(TypedDict, total=False):
    # Inputs
    question: str

    # Planner output
    query_plan: dict[str, Any]        # intent, marts_required, time_context
    intent: Literal["analytical", "factual", "comparative", "risk", "unknown"]

    # SQL writer output
    generated_sql: str
    sql_attempt: int                  # for retry loop

    # Critic output
    sql_valid: bool
    sql_errors: list[str]
    sql_explain_cost: float | None

    # Execution
    result_rows: list[dict[str, Any]]
    result_columns: list[str]
    execution_ms: int

    # Explainer output
    answer: str
    citations: list[str]              # which columns/rows were cited
    chart_spec: dict[str, Any] | None

    # Observability
    llm_tokens_used: int
    llm_cost_usd: float
    final_status: Literal["success", "failed", "blocked"]
    error: str | None
