"""
Planner node — classify intent + pick mart subset.

Output contract (strict JSON):
  {
    "intent": "analytical|factual|comparative|risk|unknown",
    "marts_required": ["fct_portfolio_performance_daily", ...],
    "time_context": "ytd|mtd|all|explicit_range",
    "entities": ["P-001", "SPY", ...]
  }
"""
from __future__ import annotations

import json

from agent.llm import complete
from agent.schema_loader import load_mart_schemas
from agent.state import AgentState

SYSTEM = (
    "You are the Planner agent in a multi-agent text-to-SQL system "
    "for portfolio analytics. Be precise and concise. Output STRICT JSON only."
)

USER_TEMPLATE = """Available mart tables:
{tables}

Available intents:
  - analytical: aggregations, trends, comparisons
  - factual: single-entity lookups ("what is X")
  - comparative: between two or more entities
  - risk: volatility, Sharpe, beta, drawdown
  - unknown: cannot map to marts

Question: {question}

Return a JSON object with keys:
  intent, marts_required (list of table names), time_context, entities (list).
"""


def planner(state: AgentState) -> AgentState:
    schemas = load_mart_schemas()
    tables_block = "\n".join(f"- {s.table}: {s.description}" for s in schemas)
    prompt = USER_TEMPLATE.format(tables=tables_block, question=state["question"])

    resp = complete(prompt, system=SYSTEM, max_tokens=400)
    try:
        plan = json.loads(resp.text.strip())
    except json.JSONDecodeError:
        plan = {"intent": "unknown", "marts_required": [], "time_context": "all", "entities": []}

    return {
        **state,
        "query_plan": plan,
        "intent": plan.get("intent", "unknown"),
        "llm_tokens_used": state.get("llm_tokens_used", 0) + resp.input_tokens + resp.output_tokens,
        "llm_cost_usd": state.get("llm_cost_usd", 0.0) + resp.cost_usd,
    }
