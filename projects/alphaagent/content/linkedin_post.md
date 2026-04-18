# LinkedIn announcement — AlphaAgent

**Target length:** ~200 words.
**Primary audience:** NYC fintech hiring managers, Staff+ data engineering ICs, AI-native product leaders.
**Goal:** one click to GitHub, one reply from someone I want to talk to.

---

I spent this weekend building **AlphaAgent** — an agentic text-to-SQL + portfolio analytics platform for asset managers.

Ask it *"Which 5 portfolios had the highest 30-day Sharpe ratio?"* and watch a 5-node LangGraph pipeline (planner → sql_writer → critic → executor → explainer) plan the query, generate read-only SQL, validate it against a schema allow-list with sqlglot, execute against a dbt medallion warehouse, and return a grounded natural-language answer — with every step tracked in Marquez and regression-tested against a 30-question golden eval set in CI.

Why it matters for the Staff/Senior Staff DE conversations I'm in:

→ **Carta:** "Own NLQ infrastructure" — that's the whole product.
→ **Citi:** "Design intelligent agents + evaluation strategies" — exactly the shape of the agent + eval harness.
→ **JPM:** "Data cataloging + lineage + AI on top of the warehouse" — OpenLineage-instrumented end to end.
→ **ClearBridge:** "Asset management data engineering with DQ + governance" — Great Expectations baked in.

Biggest lesson: **the critic should be code, not an LLM.** AST validation beats self-critique on safety, cost, and auditability every time.

Full write-up + repo in the comments.

#DataEngineering #LLMs #AgenticAI #Fintech #LangGraph #dbt #Snowflake #Staff

---

**Comment 1 (posts link to repo):**
→ GitHub: https://github.com/swatisahu/alphaagent
→ Architecture decisions I wrote up: three ADRs in `docs/adr/` — LangGraph vs. prompt-chaining, Postgres-as-Snowflake-stand-in, deterministic-critic-over-LLM-critic.

**Comment 2 (invites engagement):**
If you're hiring or building in this space — or if you disagree with any of my design choices — I'd genuinely love the pushback. Drop me a DM.
