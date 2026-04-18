# ADR-002: Postgres locally, Snowflake-swappable, for the weekend build

**Status:** Accepted · **Date:** 2026-04-18 · **Author:** Swati Sahu

## Context

Every JD I'm targeting names Snowflake or Databricks as the primary
warehouse. ClearBridge, JPMorgan, Carta, and Capital One all list Snowflake
or Databricks explicitly; Citi's stack hints at both. So the "right" answer
looks like it should be: build on Snowflake.

But we have a weekend, a laptop, and a cost budget that needs to stay under
$20 of LLM spend on the demo. Snowflake's smallest warehouse is ~$2/hour,
and the 30-question eval suite + dbt rebuilds + interactive exploration would
chew through that budget in an afternoon. Plus, an external warehouse adds
network latency, a credentials story, and a firewall carve-out — none of
which make the demo *better*, just harder to stand up on a reviewer's laptop.

## Decision

Use **PostgreSQL 16** as the warehouse for the weekend build. Design
everything (dbt project structure, SQL idioms, data types, connection
strings) so that a reviewer can swap it for Snowflake in under an hour.

## Swap plan

What would change to move to Snowflake:

1. `dbt_project/profiles.yml` — change `type: postgres` to `type: snowflake`,
   add `account`, `warehouse`, `role`, `database`, `schema`. That's it.
2. `agent/config.py` — the `pg_dsn_readonly` property becomes `warehouse_dsn`
   with the Snowflake connector string.
3. A handful of SQL idioms (see below).
4. `docker-compose.yml` drops the `postgres` service; Airflow's connection
   uses the Snowflake-python-connector.

What would **not** change:

- dbt model DAG and schema layout — the medallion is warehouse-agnostic.
- Agent nodes, safety validator, API, UI — none of them know the
  warehouse is Postgres.
- OpenLineage events — the namespace switches from `postgres://...` to
  `snowflake://...`, but the shapes are identical.

## SQL dialect differences

Documented so the reviewer knows what they'd touch:

| Intent | Postgres (today) | Snowflake (production) |
|---|---|---|
| Monthly compound return | `EXP(SUM(LN(1 + daily_return))) - 1` | Same — Snowflake supports EXP/LN |
| Date truncation | `date_trunc('year', as_of_date)` | Same |
| JSON output | `JSONB` | `VARIANT` |
| UPSERT in raw load | `INSERT ... ON CONFLICT DO NOTHING` | `MERGE INTO ... WHEN NOT MATCHED` |
| Rolling stats | `STDDEV(...) OVER (ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)` | Same |

The only SQL that would genuinely need rewriting is the batch-load `COPY
FROM STDIN` — Snowflake would use staged files on S3 + `COPY INTO`. That's
isolated to `ingestion/batch/load_raw.py` and clearly labeled.

## Consequences

### Gains

- **Fully local reproducibility.** `make demo` works without an AWS account,
  Snowflake credentials, or a credit card.
- **Faster iteration.** `make transform` runs in seconds, not minutes, so
  the multi-agent eval loop stays tight.
- **Cheaper evals.** Zero warehouse cost; the only LLM cost is the 30
  golden questions × 1 run = under $0.50 with Claude Sonnet.

### Losses

- **No Snowflake-specific features.** If I wanted to demo Snowpark, Dynamic
  Tables, or Cortex, I'd need to move. I don't — they aren't the story.
- **Perceived mismatch with JD stack.** Mitigated by explicit swap docs +
  this ADR + the "production path" section of `PLAN.md`.

## The honest version for the interview

> "Postgres is standing in for Snowflake because I wanted the reviewer to
> be able to clone and run the whole thing locally in 10 minutes. The dbt
> project is warehouse-agnostic; swapping is a profile change. If I had
> another afternoon I'd port it to my work Snowflake account — but I'd
> rather show the agent, the evals, and the lineage working than burn my
> weekend on credentials."

## References

- `docker/postgres/init.sql` — schemas + read-only role
- `dbt_project/profiles.yml` — the swap point
- `docs/adr/001-langgraph-over-prompt-chaining.md` — companion ADR
