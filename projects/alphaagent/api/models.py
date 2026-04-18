"""
Pydantic request/response models for the AlphaAgent API.

Keep these stable — the Streamlit UI and any external callers will depend on
them. Versioned implicitly via the /v1 route prefix; breaking changes require
a new prefix.
"""
from __future__ import annotations

from datetime import date
from typing import Any, Literal

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# NLQ (Natural Language Query) endpoint
# ---------------------------------------------------------------------------

class AskRequest(BaseModel):
    question: str = Field(..., min_length=3, max_length=500,
                          description="Natural-language question about portfolios, prices, or risk.")
    user_id: str | None = Field(None, description="Optional caller identifier for audit trail.")


class AskResponse(BaseModel):
    question: str
    answer: str
    generated_sql: str | None
    sql_valid: bool
    sql_errors: list[str] = []
    result_columns: list[str] = []
    result_rows: list[dict[str, Any]] = []
    row_count: int = 0
    citations: list[str] = []
    chart_spec: dict[str, Any] | None = None
    final_status: Literal["success", "failed", "blocked"]
    error: str | None = None
    # Observability
    llm_cost_usd: float = 0.0
    llm_tokens_used: int = 0
    execution_ms: int = 0
    attempt_count: int = 1


# ---------------------------------------------------------------------------
# Portfolio analytics endpoints (direct SQL, no agent involved)
# ---------------------------------------------------------------------------

class PortfolioPerformancePoint(BaseModel):
    as_of_date: date
    nav: float
    daily_return: float | None
    mtd_return: float | None
    ytd_return: float | None


class PortfolioPerformanceResponse(BaseModel):
    portfolio_id: str
    from_date: date | None
    to_date: date | None
    points: list[PortfolioPerformancePoint]


class PortfolioRiskPoint(BaseModel):
    as_of_date: date
    volatility_30d_annualized: float | None
    sharpe_30d: float | None
    beta_30d: float | None
    max_drawdown_30d: float | None


class PortfolioRiskResponse(BaseModel):
    portfolio_id: str
    from_date: date | None
    to_date: date | None
    points: list[PortfolioRiskPoint]


# ---------------------------------------------------------------------------
# Meta / ops endpoints
# ---------------------------------------------------------------------------

class HealthResponse(BaseModel):
    status: Literal["ok", "degraded", "down"]
    version: str
    db_reachable: bool
    llm_configured: bool
    checks: dict[str, Any] = {}


class LineageNode(BaseModel):
    name: str
    namespace: str
    type: str  # "dataset" | "job"


class LineageEdge(BaseModel):
    source: str
    target: str
    relation: str  # "produces" | "consumes"


class LineageResponse(BaseModel):
    nodes: list[LineageNode]
    edges: list[LineageEdge]
    marquez_url: str | None = None
    note: str | None = None


class AgentQueryLogEntry(BaseModel):
    id: int
    asked_at: str
    question: str
    generated_sql: str | None
    sql_valid: bool
    executed: bool
    latency_ms: int | None
    cost_usd: float | None
    row_count: int | None
    final_status: str | None


class AgentQueryLogResponse(BaseModel):
    entries: list[AgentQueryLogEntry]
    total: int
