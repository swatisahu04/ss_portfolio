"""
Explainer node — turns result rows into a grounded natural-language answer.

Grounding strategy:
  - Only references numbers that appear in the result set
  - Uses [cell:column] annotations the UI can replace with actual values
  - Returns a chart_spec hint the UI can use for visualization
"""
from __future__ import annotations

import json

from agent.llm import complete
from agent.state import AgentState

SYSTEM = (
    "You are the Explainer agent. Given a user question and a result set, "
    "summarize the result in 2-4 sentences. Ground every number in the result "
    "using [cell:column_name] or [cell:column_name:row_index] notation. "
    "If the result is empty, say so clearly. Do not invent numbers."
)

USER_TEMPLATE = """Question: {question}

Result columns: {columns}

First {n_show} rows:
{rows}

Return a JSON object:
{{
  "answer": "...",
  "citations": ["column1", "column2"],
  "chart_spec": {{"type": "bar|line|table|none", "x": "...", "y": "..."}}
}}
"""


def explainer(state: AgentState) -> AgentState:
    rows = state.get("result_rows", [])
    if not rows:
        return {
            **state,
            "answer": "No data returned for that question.",
            "citations": [],
            "chart_spec": {"type": "none"},
            "final_status": "success",
        }

    show = rows[:20]
    prompt = USER_TEMPLATE.format(
        question=state["question"],
        columns=state.get("result_columns", []),
        n_show=len(show),
        rows=json.dumps(show, default=str, indent=2),
    )
    resp = complete(prompt, system=SYSTEM, max_tokens=500)
    try:
        parsed = json.loads(resp.text.strip())
        answer = parsed.get("answer", "")
        citations = parsed.get("citations", [])
        chart_spec = parsed.get("chart_spec", {"type": "table"})
    except json.JSONDecodeError:
        answer = resp.text.strip()
        citations = []
        chart_spec = {"type": "table"}

    return {
        **state,
        "answer": answer,
        "citations": citations,
        "chart_spec": chart_spec,
        "final_status": "success",
        "llm_tokens_used": state.get("llm_tokens_used", 0) + resp.input_tokens + resp.output_tokens,
        "llm_cost_usd": state.get("llm_cost_usd", 0.0) + resp.cost_usd,
    }
