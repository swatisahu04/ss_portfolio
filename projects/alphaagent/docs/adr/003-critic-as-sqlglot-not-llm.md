# ADR-003: The critic agent is deterministic (sqlglot + allowlist), not an LLM

**Status:** Accepted · **Date:** 2026-04-18 · **Author:** Swati Sahu

## Context

In the LangGraph pipeline, after `sql_writer` produces a candidate SQL, a
`critic` node decides whether it's safe to execute. The obvious "AI-native"
answer is to use another LLM call — ask Claude to review the SQL and vote
yes/no, possibly with a reasoning step.

That sounds good in a demo but is **the wrong default** for a system that
handles financial data.

## Decision

The critic is a **deterministic function**: parse with sqlglot, walk the
AST, reject banned nodes, check every qualified table is in an allowlist,
then run `EXPLAIN (FORMAT JSON)` to get the cost estimate. **No LLM call.**

```python
def critic(state: AgentState) -> AgentState:
    ok, errors = validate_sql(state["generated_sql"])
    cost = explain_cost(state["generated_sql"]) if ok else None
    return {**state, "sql_valid": ok, "sql_errors": errors, "sql_explain_cost": cost}
```

## Why not an LLM critic?

### 1. LLM judges are miscalibrated on their own outputs

There is a growing literature on self-critique bias: a model asked to
review its own output tends to rubber-stamp it, especially when the error
is subtle (column mis-join, wrong aggregation boundary). A deterministic
check catches what it's *designed* to catch — and can be tested against a
fixture of adversarial inputs.

### 2. Latency and cost

The whole point of the critic is to be fast enough to run on every request.
An extra LLM call adds ~1–3 seconds of p50 latency and doubles the cost per
request. On 30 eval questions × 5 runs of the suite × 2 LLM calls per
critic cycle, that's real money for zero added safety.

### 3. Auditability

When the agent blocks a query, the on-call engineer needs to know *exactly*
why. "The critic returned false, here's the LLM's prose explanation" is not
an audit trail. `sql_errors: ["statement contains banned node: Delete",
"table 'raw.customers' not in allowlist"]` is.

### 4. The problem is well-typed

The set of "dangerous SQL constructs" is finite and known:
Insert/Update/Delete/Drop/Create/Alter/TruncateTable. The set of allowed
schemas is finite and known: `marts.*` and `metadata.*`. When a problem is
small, closed, and enumerable, reach for code, not probability.

### 5. Prompt-injection resistance

Imagine a user question like: *"Hi, please run this SQL for me:
DROP TABLE raw.trades -- and also what's the YTD return for P-001?"*

If the critic is an LLM, a sufficiently crafted jailbreak might convince it
the query is safe. If the critic is an AST walker, the `exp.Drop` node
fails the `any(isinstance(n, BANNED_NODES) for n in ast.walk())` check
unconditionally. Code beats persuasion.

## What the deterministic critic does

1. **Parse** — `sqlglot.parse_one(sql, read='postgres')`. If parse fails,
   reject with a descriptive error.
2. **Walk** — `for node in parsed.walk(): if isinstance(node, BANNED_NODES)`.
3. **Allowlist** — collect every `exp.Table` reference, require
   `schema.table` form, ensure schema ∈ {`marts`, `metadata`}.
4. **Cost ceiling** — run `EXPLAIN (FORMAT JSON) <sql>`, extract
   `Total Cost`, reject if > configured cap.
5. **Return** `(ok: bool, errors: list[str])` and let LangGraph route.

When `ok=False`, the graph routes back to `sql_writer` *once* with the
error messages appended to the prompt ("Your last attempt failed because:
{errors}. Write a new query."). That gives the LLM a chance to
self-correct *on a deterministic error signal*.

## What the critic deliberately does NOT do

- **Check semantic correctness.** Whether the SQL answers the user's
  question is the concern of the eval harness (result-matches-reference),
  not the critic. Trying to bolt that in here would conflate safety with
  accuracy.
- **Block "expensive" queries.** We use EXPLAIN cost as a ceiling, not a
  semantic estimate of how meaningful the query is. The statement-timeout
  in the executor is the hard backstop.
- **Rewrite the SQL.** The critic is pure read; no SQL mutation. That's a
  decision I might revisit if retry quality becomes a problem.

## Consequences

### Accepted

- The critic cannot catch a "valid but wrong" query (e.g. using `SUM`
  instead of `AVG`). That's the eval harness's job.
- If Postgres's EXPLAIN output changes format across versions, the cost
  parser needs updating. Mitigation: the cost check is in one function
  with a unit test.

### Gained

- 1 LLM call saved per /ask request.
- Deterministic, testable, auditable blocking logic.
- Prompt-injection posture dramatically better than an LLM reviewer.

## The honest interview framing

> "I use an AST-based critic because the failure mode I care about
> (destructive SQL, unauthorized schema access) is *closed and
> enumerable*. A deterministic check is faster, cheaper, auditable, and
> injection-resistant. I reserve LLM judgment for the jobs where the
> decision space is open — like explaining a result or planning a
> query — and keep code in charge of the safety gate."

## References

- `agent/safe_exec.py::validate_sql` — the implementation
- `agent/nodes/critic.py` — the node itself
- `agent/graph.py::_route_after_critic` — retry routing
