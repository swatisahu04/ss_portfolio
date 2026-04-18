"""
LangGraph definition — the state machine that orchestrates planner → writer → critic → executor → explainer.

Edges:
  planner → sql_writer (always)
  sql_writer → critic (always)
  critic → [sql_writer if invalid AND attempt < max_retries] else executor
  executor → explainer
  explainer → END
"""
from __future__ import annotations

from langgraph.graph import END, StateGraph

from agent.config import get_agent_settings
from agent.nodes.critic import critic
from agent.nodes.executor import executor
from agent.nodes.explainer import explainer
from agent.nodes.planner import planner
from agent.nodes.sql_writer import sql_writer
from agent.state import AgentState


def _route_after_critic(state: AgentState) -> str:
    s = get_agent_settings()
    if state.get("sql_valid"):
        return "executor"
    if state.get("sql_attempt", 0) <= s.agent_max_retries:
        return "sql_writer"
    return "explainer"  # explainer will handle empty result + error message


def build_graph():
    g = StateGraph(AgentState)

    g.add_node("planner", planner)
    g.add_node("sql_writer", sql_writer)
    g.add_node("critic", critic)
    g.add_node("executor", executor)
    g.add_node("explainer", explainer)

    g.set_entry_point("planner")
    g.add_edge("planner", "sql_writer")
    g.add_edge("sql_writer", "critic")
    g.add_conditional_edges("critic", _route_after_critic, {
        "sql_writer": "sql_writer",
        "executor": "executor",
        "explainer": "explainer",
    })
    g.add_edge("executor", "explainer")
    g.add_edge("explainer", END)

    return g.compile()


def run_agent(question: str) -> AgentState:
    graph = build_graph()
    init: AgentState = {"question": question, "sql_attempt": 0,
                        "llm_tokens_used": 0, "llm_cost_usd": 0.0}
    return graph.invoke(init)


if __name__ == "__main__":
    import sys
    q = " ".join(sys.argv[1:]) or "What is the YTD return for portfolio P-001?"
    result = run_agent(q)
    print("=" * 60)
    print(f"Q: {q}")
    print(f"A: {result.get('answer')}")
    print(f"SQL: {result.get('generated_sql')}")
    print(f"Rows: {len(result.get('result_rows', []))}")
    print(f"Cost: ${result.get('llm_cost_usd', 0):.6f}")
