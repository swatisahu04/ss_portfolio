# Building AlphaAgent in One Weekend: A Staff-Level Portfolio Project for Data + AI Engineers

**Subhead:** How I turned five NYC fintech job descriptions into a single multi-agent text-to-SQL system — and what I learned about why the critic should be code, not an LLM.

*Target length: ~1500 words. Audience: Staff-level DE/AI engineers, hiring managers at asset managers, readers of the "build in public" genre on Substack.*

---

## The problem with "AI features" in data teams

Every JD I'm targeting this spring says some version of the same thing:

> *"Build the next-generation analytics platform. Put AI on top of the data warehouse. Prove accuracy."*

Carta wants someone to own their natural-language query infrastructure. Citi is hiring a Senior AI Data Engineer specifically to "design and implement intelligent agents" with "evaluation strategies." JPMorgan wants AI-powered risk anomaly detection over a Databricks + Snowflake stack. ClearBridge wants a data engineer who can also generate insights for non-technical stakeholders.

They are describing the same role from five angles. And they are all implicitly asking the same thing: *show me you can put an LLM in front of a warehouse without it melting down in production.*

So I spent this past weekend building a single project that answers all five JDs at once. It's called **AlphaAgent**. This is the write-up.

## What AlphaAgent does

One sentence: **a multi-agent text-to-SQL + portfolio analytics platform for asset managers, with a 30-question regression harness in CI.**

You ask it a question in English — *"Which 5 portfolios had the highest 30-day Sharpe ratio?"* — and five small agents take it from there:

1. **Planner** — parses intent, identifies which marts you need, extracts entities and a time context.
2. **SQL Writer** — given the plan + the schema, writes PostgreSQL.
3. **Critic** — parses the SQL with sqlglot, walks the AST, rejects banned nodes, checks every referenced table is in an allowlist, runs EXPLAIN, and returns a verdict.
4. **Executor** — runs the SQL as a read-only user with a statement timeout.
5. **Explainer** — turns the result rows into a grounded natural-language answer with column-level citations.

All of this is orchestrated by LangGraph, backed by a dbt medallion warehouse (raw → staging → intermediate → marts), fed by both batch and streaming ingestion (Redpanda-Kafka for trades, Python COPY loads for securities master and prices), lineage-tracked in Marquez via OpenLineage, quality-gated by Great Expectations, served through FastAPI, and demoed in a five-tab Streamlit UI.

## The architecture, in one picture

```
Sources → Ingest → dbt medallion → marts → API → {UI, Agent}
                                       ↓
                                  GE + Marquez
```

Synthetic data: 50 portfolios × 500 securities × 730 days, generated with geometric Brownian motion for prices, Dirichlet-weighted position allocations, and five strategy archetypes (Growth, Value, Balanced, ESG, Income). The generator intentionally embeds a few data-quality problems — missing prices on one day, a duplicate trade — so the DQ checks have something real to catch.

## Three architecture decisions that tell the real story

If I had to pick three choices that say more about how I think than a list of technologies ever could, they'd be these. Each is written up as a full ADR in the repo under `docs/adr/`.

### 1. LangGraph over prompt-chaining (ADR-001)

The cheap answer to "build an agent" is to write a Python script that calls Claude three times in sequence. That would work for the demo. It's also wrong.

A state-machine framework like LangGraph pays back in three places a hand-rolled chain doesn't: you can *evaluate each node independently* (planning quality vs. SQL correctness vs. answer faithfulness are three different metrics), you can *retry with structured feedback* (critic → writer loop with the error message piped back into the prompt), and you can *explain the thing to on-call* when it breaks at 3am. "Here's the state diagram" is an answer; "here's a Python file with nested `if` statements" is not.

### 2. Postgres standing in for Snowflake (ADR-002)

Every JD on my target list names Snowflake or Databricks. But I built on Postgres. Why?

Because a reviewer needs to be able to clone my repo and run the whole thing locally in ten minutes. Snowflake credentials, firewall carve-outs, and the $2/hour warehouse clock are not what the demo is about. The dbt project is warehouse-agnostic by construction — swapping to Snowflake is a profile change and a handful of dialect-specific SQL idioms (documented in the ADR). In an interview, "I optimized for local reproducibility and documented the port" is a stronger answer than "I burned my weekend on credentials."

### 3. The critic is code, not an LLM (ADR-003)

This is the choice I'm most opinionated about.

The obvious "AI-native" design is: after the sql_writer produces SQL, ask another LLM call to review it for safety. This is wrong. LLMs are miscalibrated on their own outputs; they rubber-stamp when asked to self-critique, especially on subtle errors. They double the per-request latency and cost. Their verdicts are not auditable — "the critic said yes, here's its reasoning" is not an audit trail.

Worse, the safety problem is *closed and enumerable*. The set of dangerous SQL constructs is finite: Insert, Update, Delete, Drop, Create, Alter, Truncate. The set of allowed schemas is finite: `marts` and `metadata`. When a problem is finite and enumerable, reach for code.

So AlphaAgent's critic is a pure function: parse with sqlglot, walk the AST, reject banned nodes, enforce a schema allowlist, check EXPLAIN cost. When it rejects, the error is specific and actionable: `"statement contains banned node: Delete"` or `"table 'raw.customers' not in allowlist"`. Code beats persuasion. An adversarial prompt injection that would fool an LLM reviewer cannot get past an AST check.

I reserve LLM judgment for the jobs where the decision space is genuinely open — planning, explaining — and keep deterministic code in charge of the safety gate.

## The eval harness (where the honesty lives)

A demo's job is to *look* good. A portfolio project's job is to prove the author can *measure* their system honestly. So AlphaAgent ships with 30 golden questions split across easy / medium / hard and five categories (factual, analytical, comparative, risk, attribution). Each question has a reference SQL; the harness compares the agent's results to the reference within 1e-4 numeric tolerance.

Five metrics:
- **parse_ok** — does the SQL parse?
- **critic_passed** — does the safety validator accept it?
- **contains_keywords** — do expected SQL elements appear?
- **executes** — does it run without error?
- **result_matches_ref** — this is the headline.

The suite runs in CI on every PR. A regression drops merge-ability. Prompt changes get measured before they ship.

This is the difference between "I built a demo" and "I built a system." The demo works until you change something. The system tells you when you've broken it.

## What I'm leaving for weekend two

The cut list, in the spirit of a staff-level scope decision rather than a humble-brag of shippable features:

- **Snowflake port** — validated locally with the swap ADR; actually porting is ~1 hour if I had credentials.
- **Databricks / Delta Lake path** — the JDs want it; the production answer is bronze/silver/gold tables managed by Spark Structured Streaming.
- **RBAC + row-level security** — single-tenant today; a multi-tenant wealth management platform would need per-portfolio RLS.
- **Real-time anomaly detection** — the streaming path is in; a proper anomaly detector on it is weekend 2.
- **Terraform IaC** — Docker Compose is enough for the demo.
- **Next.js UI with server-side rendering** — Streamlit is faster to ship; the real thing would be React + shadcn.

Naming the cut list matters as much as the feature list. Staff engineering is the discipline of saying "not yet" on purpose.

## Lessons, in bullets, earned the hard way

- **Write the eval harness before the second agent.** I wrote mine third. If I'd written it second, I would have caught two subtle prompt bugs in the writer a day earlier.
- **Read-only DB roles are the single highest-leverage safety control.** Five minutes to add, eliminates an entire class of failure.
- **Mock mode is a feature, not a debug tool.** `AGENT_LLM_MOCK=1` lets CI run the whole pipeline for zero cents, which means regression tests run on every PR without making a finance person twitch.
- **Structured JSON logs from day one.** The morning you need to debug "why was this request expensive?" is the wrong morning to instrument.
- **Sqlglot is underrated.** It should be the default validation layer in every text-to-SQL system. The entire industry is still using regex for this and I don't understand why.

## If you're hiring — or if you're building

If you're hiring for Staff/Senior-Staff/VP Data or AI Engineering at a NYC bank, fintech, or asset manager and this is the shape of work you want done, I'd like to talk. I'm in the loop at a few places but taking intros seriously right now.

If you're building something in this space and you disagree with any of my architecture choices — especially the LLM-critic-vs-AST-critic one — I'd genuinely love the pushback. Disagreement is where learning happens.

Repo, runbook, ADRs, and eval scorecard: [github.com/swatisahu/alphaagent](#).

Follow for the weekend-2 post: Snowflake port, Spark Structured Streaming anomaly detector, and a very frank writeup of what broke.

— Swati Sahu
