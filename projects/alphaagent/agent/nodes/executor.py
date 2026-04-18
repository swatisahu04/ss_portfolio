"""
Executor node — runs the validated SQL against the read-only DB role.

Also logs to metadata.agent_query_log for observability.
"""
from __future__ import annotations

import psycopg

from agent.config import get_agent_settings
from agent.safe_exec import run_safely
from agent.state import AgentState


def executor(state: AgentState) -> AgentState:
    sql = state["generated_sql"]
    try:
        rows, cols, ms = run_safely(sql)
        out: AgentState = {
            **state,
            "result_rows": rows,
            "result_columns": cols,
            "execution_ms": ms,
        }
        _log(state["question"], sql, len(rows), ms, state.get("llm_cost_usd", 0.0),
             True, None, model=get_agent_settings().llm_model)
        return out
    except Exception as e:
        _log(state["question"], sql, 0, 0, state.get("llm_cost_usd", 0.0),
             False, str(e), model=get_agent_settings().llm_model)
        return {
            **state,
            "result_rows": [],
            "result_columns": [],
            "execution_ms": 0,
            "error": f"execution failed: {e}",
            "final_status": "failed",
        }


def _log(question: str, sql: str, rc: int, ms: int, cost: float,
         success: bool, err: str | None, model: str) -> None:
    s = get_agent_settings()
    try:
        # Use the privileged user to write the log
        dsn = (
            f"postgresql://{s.pg_user.replace('agent_user', 'alphaagent')}"
            f":{s.pg_password.replace('agent_readonly', 'alphaagent')}"
            f"@{s.pg_host}:{s.pg_port}/{s.pg_db}"
        )
        with psycopg.connect(dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO metadata.agent_query_log "
                    "(question, generated_sql, row_count, latency_ms, cost_usd, success, error, llm_model) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (question, sql, rc, ms, cost, success, err, model),
                )
    except Exception:
        # Never let logging break the request
        pass
