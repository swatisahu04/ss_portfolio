"""
SQL Writer node — takes the query plan + schemas + few-shot examples, emits Postgres SQL.

Uses few-shot examples rotated from the eval set so the model gets high-signal
patterns specific to our mart design.
"""
from __future__ import annotations

from agent.llm import complete
from agent.schema_loader import schema_prompt
from agent.state import AgentState

SYSTEM = (
    "You are the SQL Writer agent. Produce a single PostgreSQL SELECT statement "
    "to answer the user's question using ONLY the marts.* tables provided. "
    "Always schema-qualify table names. Never emit DDL or DML. "
    "Return ONLY the SQL, no explanation, no markdown fences."
)

FEW_SHOT = """
Q: What is the YTD return for portfolio P-001?
SQL: SELECT ytd_return
     FROM marts.fct_portfolio_performance_daily
     WHERE portfolio_id = 'P-001'
     ORDER BY as_of_date DESC
     LIMIT 1;

Q: Which Growth-strategy portfolios beat SPY YTD, and by how much?
SQL: WITH latest AS (
       SELECT portfolio_id, MAX(as_of_date) AS as_of_date
       FROM marts.fct_portfolio_performance_daily GROUP BY portfolio_id
     )
     SELECT p.portfolio_id, p.portfolio_name, f.ytd_return, f.spy_excess_return_ytd
     FROM marts.fct_portfolio_performance_daily f
     JOIN latest USING (portfolio_id, as_of_date)
     JOIN marts.dim_portfolios p USING (portfolio_id)
     WHERE p.strategy = 'Growth' AND f.spy_excess_return_ytd > 0
     ORDER BY f.spy_excess_return_ytd DESC;

Q: Top 5 portfolios by 30-day Sharpe ratio.
SQL: WITH latest AS (
       SELECT portfolio_id, MAX(as_of_date) AS as_of_date
       FROM marts.fct_portfolio_risk_daily GROUP BY portfolio_id
     )
     SELECT r.portfolio_id, r.sharpe_30d, r.vol_30d
     FROM marts.fct_portfolio_risk_daily r
     JOIN latest USING (portfolio_id, as_of_date)
     ORDER BY r.sharpe_30d DESC NULLS LAST
     LIMIT 5;
"""

USER_TEMPLATE = """Schema:
{schema}

Few-shot examples:
{examples}

Question: {question}

Query plan (for context):
{plan}

Write the SQL:"""


def sql_writer(state: AgentState) -> AgentState:
    prompt = USER_TEMPLATE.format(
        schema=schema_prompt(),
        examples=FEW_SHOT,
        question=state["question"],
        plan=state.get("query_plan", {}),
    )
    resp = complete(prompt, system=SYSTEM, max_tokens=800)
    sql = resp.text.strip()
    # Strip markdown fences if the model added them despite the instruction
    if sql.startswith("```"):
        sql = sql.strip("`").lstrip("sql").strip()

    return {
        **state,
        "generated_sql": sql,
        "sql_attempt": state.get("sql_attempt", 0) + 1,
        "llm_tokens_used": state.get("llm_tokens_used", 0) + resp.input_tokens + resp.output_tokens,
        "llm_cost_usd": state.get("llm_cost_usd", 0.0) + resp.cost_usd,
    }
