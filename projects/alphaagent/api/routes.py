"""
API routes for AlphaAgent.

Two flavors of endpoint:

  (1) /v1/ask  — agent-backed natural-language query (expensive, cached, rate-limited)
  (2) /v1/portfolio/* — deterministic SQL analytics against the marts
                        (cheap, no LLM, direct queries into fct_portfolio_*)

The NLQ endpoint is the "wow factor" for a demo; the analytics endpoints are
what a real BI/dashboard layer would call. Both share auth + structured logging.
"""
from __future__ import annotations

import time
from datetime import date
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from agent.config import get_agent_settings
from api.deps import get_pool, rate_limit, require_api_key


def run_agent(question: str):  # pragma: no cover - replaced lazily on first call
    """
    Lazy shim: we defer importing agent.graph (and therefore langgraph)
    until /v1/ask is actually invoked. This keeps the API importable in
    minimal environments (e.g. CI jobs that only test analytics endpoints,
    or the Streamlit UI hitting /v1/portfolio/*).
    """
    from agent.graph import run_agent as _real
    globals()["run_agent"] = _real
    return _real(question)
from api.logging_config import api_log
from api.models import (
    AgentQueryLogEntry,
    AgentQueryLogResponse,
    AskRequest,
    AskResponse,
    HealthResponse,
    LineageEdge,
    LineageNode,
    LineageResponse,
    PortfolioPerformancePoint,
    PortfolioPerformanceResponse,
    PortfolioRiskPoint,
    PortfolioRiskResponse,
)

router = APIRouter(prefix="/v1")

API_VERSION = "0.1.0"


# ---------------------------------------------------------------------------
# Natural-language query — the agent
# ---------------------------------------------------------------------------

@router.post("/ask",
             response_model=AskResponse,
             dependencies=[Depends(rate_limit), Depends(require_api_key)],
             summary="Ask a natural-language question against the portfolio marts")
def ask(body: AskRequest, request: Request) -> AskResponse:
    t0 = time.perf_counter()
    try:
        state = run_agent(body.question)
    except Exception as e:
        api_log.exception("agent_crash", extra={"question": body.question,
                                                "user_id": body.user_id})
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"agent crashed: {e}",
        )
    execution_ms = int((time.perf_counter() - t0) * 1000)

    api_log.info("ask_completed", extra={
        "question": body.question,
        "user_id": body.user_id,
        "sql_valid": state.get("sql_valid", False),
        "final_status": state.get("final_status"),
        "row_count": len(state.get("result_rows", [])),
        "llm_cost_usd": state.get("llm_cost_usd", 0.0),
        "execution_ms": execution_ms,
        "attempt_count": state.get("sql_attempt", 1),
    })

    return AskResponse(
        question=body.question,
        answer=state.get("answer", ""),
        generated_sql=state.get("generated_sql"),
        sql_valid=state.get("sql_valid", False),
        sql_errors=state.get("sql_errors", []) or [],
        result_columns=state.get("result_columns", []) or [],
        result_rows=state.get("result_rows", []) or [],
        row_count=len(state.get("result_rows", []) or []),
        citations=state.get("citations", []) or [],
        chart_spec=state.get("chart_spec"),
        final_status=state.get("final_status", "failed"),
        error=state.get("error"),
        llm_cost_usd=round(state.get("llm_cost_usd", 0.0), 6),
        llm_tokens_used=state.get("llm_tokens_used", 0),
        execution_ms=execution_ms,
        attempt_count=state.get("sql_attempt", 1),
    )


# ---------------------------------------------------------------------------
# Portfolio performance — direct SQL into marts.fct_portfolio_performance_daily
# ---------------------------------------------------------------------------

@router.get("/portfolio/{portfolio_id}/performance",
            response_model=PortfolioPerformanceResponse,
            dependencies=[Depends(require_api_key)],
            summary="Daily NAV + MTD/YTD returns for a portfolio")
def portfolio_performance(
    portfolio_id: str,
    from_date: date | None = Query(None, alias="from"),
    to_date: date | None = Query(None, alias="to"),
    limit: int = Query(500, le=2000),
) -> PortfolioPerformanceResponse:
    pool = get_pool()
    sql = """
        SELECT as_of_date, nav, daily_return, mtd_return, ytd_return
        FROM marts.fct_portfolio_performance_daily
        WHERE portfolio_id = %s
          AND (%s::date IS NULL OR as_of_date >= %s)
          AND (%s::date IS NULL OR as_of_date <= %s)
        ORDER BY as_of_date
        LIMIT %s
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (portfolio_id, from_date, from_date, to_date, to_date, limit))
            rows = cur.fetchall()

    points = [
        PortfolioPerformancePoint(
            as_of_date=r[0],
            nav=float(r[1]) if r[1] is not None else 0.0,
            daily_return=float(r[2]) if r[2] is not None else None,
            mtd_return=float(r[3]) if r[3] is not None else None,
            ytd_return=float(r[4]) if r[4] is not None else None,
        )
        for r in rows
    ]
    return PortfolioPerformanceResponse(
        portfolio_id=portfolio_id, from_date=from_date, to_date=to_date, points=points
    )


# ---------------------------------------------------------------------------
# Portfolio risk
# ---------------------------------------------------------------------------

@router.get("/portfolio/{portfolio_id}/risk",
            response_model=PortfolioRiskResponse,
            dependencies=[Depends(require_api_key)],
            summary="Rolling 30-day risk metrics for a portfolio")
def portfolio_risk(
    portfolio_id: str,
    from_date: date | None = Query(None, alias="from"),
    to_date: date | None = Query(None, alias="to"),
    limit: int = Query(500, le=2000),
) -> PortfolioRiskResponse:
    pool = get_pool()
    sql = """
        SELECT as_of_date, volatility_30d_annualized, sharpe_30d,
               beta_30d, max_drawdown_30d
        FROM marts.fct_portfolio_risk_daily
        WHERE portfolio_id = %s
          AND (%s::date IS NULL OR as_of_date >= %s)
          AND (%s::date IS NULL OR as_of_date <= %s)
        ORDER BY as_of_date
        LIMIT %s
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (portfolio_id, from_date, from_date, to_date, to_date, limit))
            rows = cur.fetchall()

    points = [
        PortfolioRiskPoint(
            as_of_date=r[0],
            volatility_30d_annualized=float(r[1]) if r[1] is not None else None,
            sharpe_30d=float(r[2]) if r[2] is not None else None,
            beta_30d=float(r[3]) if r[3] is not None else None,
            max_drawdown_30d=float(r[4]) if r[4] is not None else None,
        )
        for r in rows
    ]
    return PortfolioRiskResponse(
        portfolio_id=portfolio_id, from_date=from_date, to_date=to_date, points=points
    )


# ---------------------------------------------------------------------------
# Portfolio catalog — useful for dropdown in the UI
# ---------------------------------------------------------------------------

@router.get("/portfolios",
            dependencies=[Depends(require_api_key)],
            summary="List of all portfolios")
def list_portfolios() -> dict[str, Any]:
    pool = get_pool()
    sql = """
        SELECT DISTINCT portfolio_id
        FROM marts.fct_portfolio_performance_daily
        ORDER BY portfolio_id
    """
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return {"portfolios": [r[0] for r in rows]}


# ---------------------------------------------------------------------------
# Data-quality summary — consumed by the DQ tab in the UI
# ---------------------------------------------------------------------------

@router.get("/dq/summary",
            dependencies=[Depends(require_api_key)],
            summary="Latest DQ check results summary")
def dq_summary() -> dict[str, Any]:
    pool = get_pool()
    # metadata.dq_results is populated by the dq runner; fall back to a
    # stub result if the table doesn't exist yet (common in local dev).
    sql = """
        SELECT check_name, status, severity, run_at, details
        FROM metadata.dq_results
        WHERE run_at >= CURRENT_DATE - INTERVAL '7 days'
        ORDER BY run_at DESC
        LIMIT 200
    """
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
        return {
            "checks": [
                {"check_name": r[0], "status": r[1], "severity": r[2],
                 "run_at": r[3].isoformat() if r[3] else None, "details": r[4]}
                for r in rows
            ]
        }
    except Exception as e:
        api_log.warning("dq_summary_unavailable", extra={"error": str(e)})
        return {"checks": [], "note": "metadata.dq_results not yet populated"}


# ---------------------------------------------------------------------------
# Agent query log — for the "Agent Observability" tab
# ---------------------------------------------------------------------------

@router.get("/agent/queries",
            response_model=AgentQueryLogResponse,
            dependencies=[Depends(require_api_key)],
            summary="Recent agent query log entries")
def agent_queries(limit: int = Query(50, le=500)) -> AgentQueryLogResponse:
    pool = get_pool()
    sql = """
        SELECT id, asked_at, question, generated_sql, sql_valid, executed,
               latency_ms, cost_usd, row_count, final_status
        FROM metadata.agent_query_log
        ORDER BY asked_at DESC
        LIMIT %s
    """
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (limit,))
                rows = cur.fetchall()
        entries = [
            AgentQueryLogEntry(
                id=r[0],
                asked_at=r[1].isoformat() if r[1] else "",
                question=r[2],
                generated_sql=r[3],
                sql_valid=bool(r[4]),
                executed=bool(r[5]),
                latency_ms=r[6],
                cost_usd=float(r[7]) if r[7] is not None else None,
                row_count=r[8],
                final_status=r[9],
            )
            for r in rows
        ]
        return AgentQueryLogResponse(entries=entries, total=len(entries))
    except Exception as e:
        api_log.warning("agent_query_log_unavailable", extra={"error": str(e)})
        return AgentQueryLogResponse(entries=[], total=0)


# ---------------------------------------------------------------------------
# Lineage — Marquez-backed when available, static fallback otherwise
# ---------------------------------------------------------------------------

@router.get("/lineage",
            response_model=LineageResponse,
            dependencies=[Depends(require_api_key)],
            summary="Data lineage: pipeline jobs and their dataset dependencies")
def lineage() -> LineageResponse:
    # In production we'd call the Marquez API. For the demo we return a hand-
    # curated graph so the UI has something to render even without Marquez.
    nodes = [
        LineageNode(name="raw.prices", namespace="postgres", type="dataset"),
        LineageNode(name="raw.trades", namespace="postgres", type="dataset"),
        LineageNode(name="raw.securities_master", namespace="postgres", type="dataset"),
        LineageNode(name="raw.portfolios", namespace="postgres", type="dataset"),
        LineageNode(name="stg_prices_daily", namespace="dbt", type="dataset"),
        LineageNode(name="stg_trades", namespace="dbt", type="dataset"),
        LineageNode(name="int_positions_daily", namespace="dbt", type="dataset"),
        LineageNode(name="fct_portfolio_performance_daily", namespace="dbt", type="dataset"),
        LineageNode(name="fct_portfolio_risk_daily", namespace="dbt", type="dataset"),
        LineageNode(name="fct_position_attribution", namespace="dbt", type="dataset"),
        LineageNode(name="load_raw_batch", namespace="airflow", type="job"),
        LineageNode(name="stream_consumer", namespace="airflow", type="job"),
        LineageNode(name="dbt_run", namespace="airflow", type="job"),
    ]
    edges = [
        LineageEdge(source="load_raw_batch", target="raw.prices", relation="produces"),
        LineageEdge(source="load_raw_batch", target="raw.securities_master", relation="produces"),
        LineageEdge(source="load_raw_batch", target="raw.portfolios", relation="produces"),
        LineageEdge(source="stream_consumer", target="raw.trades", relation="produces"),
        LineageEdge(source="raw.prices", target="stg_prices_daily", relation="consumes"),
        LineageEdge(source="raw.trades", target="stg_trades", relation="consumes"),
        LineageEdge(source="stg_prices_daily", target="int_positions_daily", relation="consumes"),
        LineageEdge(source="stg_trades", target="int_positions_daily", relation="consumes"),
        LineageEdge(source="int_positions_daily", target="fct_portfolio_performance_daily", relation="consumes"),
        LineageEdge(source="int_positions_daily", target="fct_portfolio_risk_daily", relation="consumes"),
        LineageEdge(source="int_positions_daily", target="fct_position_attribution", relation="consumes"),
    ]
    return LineageResponse(
        nodes=nodes, edges=edges,
        marquez_url="http://localhost:3000",
        note="Static fallback shown. Marquez at localhost:3000 has live column-level lineage.",
    )


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@router.get("/health", response_model=HealthResponse, summary="Health check")
def health() -> HealthResponse:
    s = get_agent_settings()
    checks: dict[str, Any] = {}
    db_ok = False
    try:
        pool = get_pool()
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        db_ok = True
        checks["db"] = "ok"
    except Exception as e:
        checks["db"] = f"error: {e}"

    llm_ok = bool(s.anthropic_api_key or s.openai_api_key)
    checks["llm"] = "configured" if llm_ok else "missing_api_key"

    overall = "ok" if db_ok and llm_ok else ("degraded" if db_ok else "down")
    return HealthResponse(
        status=overall,
        version=API_VERSION,
        db_reachable=db_ok,
        llm_configured=llm_ok,
        checks=checks,
    )
