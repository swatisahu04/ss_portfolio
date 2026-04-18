"""
Critic node — validates the generated SQL WITHOUT re-running an LLM.

Checks (in order):
  1. Parses as PostgreSQL
  2. Top-level is SELECT / WITH / UNION
  3. No banned ops (DDL/DML)
  4. Only `marts.*` or `metadata.*` tables referenced
  5. EXPLAIN dry-run produces an estimated cost

On failure: routes back to the writer with the error message.
On success: routes to the executor.
"""
from __future__ import annotations

from agent.safe_exec import explain_cost, validate_sql
from agent.state import AgentState


def critic(state: AgentState) -> AgentState:
    sql = state.get("generated_sql", "")
    ok, errors = validate_sql(sql)
    cost = explain_cost(sql) if ok else None

    return {
        **state,
        "sql_valid": ok,
        "sql_errors": errors,
        "sql_explain_cost": cost,
    }
