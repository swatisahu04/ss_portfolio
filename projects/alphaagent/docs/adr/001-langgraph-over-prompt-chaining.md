# ADR-001: LangGraph for the agent orchestration, not prompt chaining

**Status:** Accepted · **Date:** 2026-04-18 · **Author:** Swati Sahu

## Context

The core value proposition of AlphaAgent is a reliable text-to-SQL pipeline.
A user asks a natural-language question; the system must plan, write SQL,
validate safety, execute, and explain the result — and the whole thing needs
to be reproducible, testable, and *debuggable when it breaks in production*.

Three realistic options existed:

1. **Single prompt.** Stuff question + schema + few-shot examples into one
   request to Claude. Let the LLM return SQL + answer in one JSON blob.
2. **Prompt chain.** A short Python script that calls the LLM repeatedly in
   a fixed sequence (`plan()`, then `write_sql()`, then `explain()`), wiring
   the output of each into the input of the next.
3. **State-machine agent framework.** LangGraph / LangChain agents /
   AutoGen / CrewAI — frameworks that let you declare nodes, edges, and
   conditional routing over a shared state object.

## Decision

**LangGraph 0.1** as the orchestration layer, with five named nodes
(`planner`, `sql_writer`, `critic`, `executor`, `explainer`) and a single
conditional edge after the critic that routes back to the writer for
self-correction.

## Consequences

### Why not option 1 (single prompt)

- **Nothing to evaluate.** You cannot measure planning quality, SQL
  correctness, and answer faithfulness independently if they are all produced
  in one shot. The eval harness wouldn't be able to tell us *why* an answer
  was wrong.
- **No safety gate.** The only place to enforce the SELECT-only rule is
  post-hoc regex on the output — brittle, and hostile to prompt injection.
- **No retry granularity.** If the SQL is subtly wrong, we can only retry the
  whole thing; we cannot ask the writer to regenerate while keeping the plan.

### Why not option 2 (prompt chain)

- It would work, but it hides the *graph structure* behind control flow.
  Running something in production means explaining its behavior to on-call
  engineers. "Here's a `.py` file with nested `if` statements" is a bad
  answer; "here's the state-machine diagram" is a good one.
- Conditional retry logic (critic → writer) becomes awkward to express as
  linear code, especially once we want to add timeouts, attempt caps, and
  observability hooks.

### Why LangGraph specifically

- **First-class state.** `AgentState` is a `TypedDict`, which means every
  node sees the same shape and IDE completion works. Nodes read what they
  need from state and return partial updates — functional-style and
  explicit, not reliant on mutable globals.
- **Conditional edges as data.** `_route_after_critic(state)` is a pure
  function; the routing decision is testable in isolation without spinning
  up the whole graph.
- **Interop with LangChain.** If we later swap Claude for a local model
  (e.g. Llama 3 on Databricks), the node signatures don't change.
- **LangGraph is already what the market is using.** JD-checking: Citi's
  "design and implement intelligent agents" and Carta's "NLQ infrastructure"
  both implicitly expect a named framework.

### Costs accepted

- A pinned dependency (`langgraph>=0.1.0`) that is <1 year old and therefore
  may have breaking API changes. Mitigation: the `graph.py` surface area is
  small (build_graph + run_agent), and each node is a plain function — we
  can hand-port to a Python state machine in an afternoon if LangGraph goes
  off the rails.
- Slightly heavier cold-start (`import langgraph` is ~300 ms). Mitigation:
  the API lazily imports `run_agent` on first use, so the liveness probe
  isn't paying for it.

### Observability wins

Because every node logs tokens + cost into `AgentState` and the final state
is persisted to `metadata.agent_query_log`, we can answer "why did this
request cost $0.08?" by replaying the graph with the same question and
seeing the per-node cost breakdown. This is the kind of per-invocation
accountability that separates a "toy demo" from "ready for production."

## References

- `agent/graph.py` — the state machine itself
- `agent/state.py` — the shared `AgentState` shape
- `tests/unit/test_api_routes.py::test_ask_success` — end-to-end smoke
