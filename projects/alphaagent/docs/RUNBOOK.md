# AlphaAgent Runbook

Operational playbook for the AlphaAgent demo. Written as if this were a
production system — because the point is to show Staff-level thinking:
every component has failure modes, and every failure mode has a named
response.

**On-call contact:** you (this is a portfolio project, not a real service)
**Dashboards:** Streamlit :8501 · Marquez :3000 · Airflow :8080
**Logs:** `docker compose logs -f <service>`

---

## 0. Quick triage flowchart

```
Is the UI loading?
├── No  → Is API /v1/health green?     → go to §1
│         Is Postgres up?                → go to §2
└── Yes → Is an answer wrong/empty?    → go to §3
         Is an eval regression firing? → go to §4
         Is Kafka consumer lagging?    → go to §5
         Is a dbt build failing?       → go to §6
         Is DQ raising warnings?       → go to §7
```

---

## 1. API is down (`/v1/health` fails)

**Symptoms.** UI sidebar shows "API unreachable" · `curl localhost:8000/v1/health`
returns connection refused or 5xx.

**Diagnose.**
```bash
docker compose ps api            # is the container running?
docker compose logs api --tail=50
```

**Common causes.**
- **DB pool cannot connect.** `db_pool_init_failed` in startup logs. →
  Check §2 (Postgres).
- **Missing `ANTHROPIC_API_KEY`.** Health will still return `status: degraded`
  with `llm: missing_api_key`. The `/v1/ask` endpoint returns 500 on every
  call. → `export ANTHROPIC_API_KEY=...` and restart: `make api`.
- **Port 8000 taken.** `OSError: [Errno 48] Address already in use`. → Find
  and kill: `lsof -iTCP:8000 -sTCP:LISTEN`.

**Recovery.** `make api` (dev) or `docker compose restart api` (compose).

---

## 2. Postgres unreachable

**Symptoms.** `/v1/health.db_reachable: false` · any SQL call returns
connection errors.

**Diagnose.**
```bash
docker compose exec postgres pg_isready -U alphaagent
docker compose logs postgres --tail=100
```

**Common causes.**
- **Container still initializing.** On first startup, `docker/postgres/init.sql`
  runs — give it ~15 seconds. Health flips to green once init is done.
- **Volume corruption from a hard kill.** → `make nuke && make up` (this
  destroys local data — expected for a demo).
- **Wrong credentials in `.env`.** Confirm values match `docker-compose.yml`:
  user `alphaagent` / db `alphaagent` / pw `alphaagent`.

**Recovery.** `docker compose restart postgres && sleep 10 && make api`.

---

## 3. Agent returns wrong / empty answers

**Symptoms.** `/v1/ask` returns `final_status: success` but `answer` is
empty, or clearly wrong relative to what the user asked.

**Diagnose.**
```bash
# Look at the query log — the whole audit trail is here.
docker compose exec postgres psql -U alphaagent -c "
  SELECT id, asked_at, question, sql_valid, executed,
         row_count, cost_usd, latency_ms, final_status
  FROM metadata.agent_query_log
  ORDER BY asked_at DESC
  LIMIT 20;
"
```

**Triage tree.**
- `sql_valid = false` → Critic rejected. Check `sql_errors` in the response
  body — usually a schema allowlist miss, meaning the SQL referenced a
  non-mart table. → Add the table to `agent/safe_exec.py::ALLOWED_SCHEMAS`
  *only if the table genuinely should be queryable*. Otherwise fix the
  planner prompt.
- `executed = false` → SQL ran but returned no rows. Marts may be empty.
  → `make transform` and re-run. If rows are still zero, `make seed` first.
- `executed = true, row_count = 0` → SQL is correct but the data really is
  empty for that portfolio/window. Confirm with a direct query into
  `marts.*` — this is usually a user problem, not a system one.
- `row_count > 0 but answer is wrong` → Explainer drift. The SQL was right
  but the NL answer miscites. Check `answer` and `citations` — if citations
  don't cover the claim, the explainer prompt needs tightening.

---

## 4. Eval regression

**Symptoms.** CI job `agent-eval-regression` fails on a PR · local
`make eval` shows `result_matches_ref` below the threshold.

**Diagnose.**
```bash
# Compare to main's scorecard
git show main:evals/scorecard.md > /tmp/main-scorecard.md
diff /tmp/main-scorecard.md evals/scorecard.md | head -40

# Per-question CSV is more actionable than the MD summary
column -t -s, evals/results.csv | head -40
```

**Common causes.**
- **Prompt change regressed a question class.** → Look at which difficulty
  bucket dropped. If `hard` dropped but `easy`/`medium` held, the writer
  prompt's few-shots need a new example for that pattern.
- **Schema change broke reference SQL.** → If `reference_sql_parses`
  dropped, a column rename or table move happened. Update `evals/golden.yaml`
  first, then re-run.
- **Model downgrade.** If someone set `llm_model` to a cheaper model, the
  match rate may legitimately drop. Policy decision: revert, or accept and
  rewrite prompts.

**Recovery.** Fix the root cause, not the test. Adding `xfail` markers to
golden questions to make CI green is expressly forbidden in this repo.

---

## 5. Kafka consumer lag / trade stream issues

**Symptoms.** `raw.trades` row count stops growing while the producer is
running · consumer logs show `rebalancing` repeatedly.

**Diagnose.**
```bash
# Lag per partition
docker compose exec redpanda rpk group describe alphaagent-consumer

# Backlog of messages on the topic
docker compose exec redpanda rpk topic describe trades.v1

# Consumer logs
docker compose logs -f consumer --tail=100
```

**Common causes.**
- **Redpanda not up yet.** Producer can outrun consumer on cold start. →
  Give it 30 seconds; the consumer will catch up.
- **Postgres upsert slow.** Consumer commits per-batch; if Postgres is
  under pressure, batches queue up. → Check `docker compose logs postgres`
  for slow-query warnings. Dial back `--replay-speed` on the producer.
- **Deduplication storm.** If the consumer restarts mid-flight, it re-reads
  its last committed offset and re-inserts. The `ON CONFLICT (trade_id)
  DO NOTHING` clause absorbs this — the `dupes` counter in the consumer log
  is expected to increment briefly.

**Recovery.** `make consume` restarts from the committed offset. Truly
wedged? `docker compose restart redpanda consumer`.

---

## 6. dbt build failure

**Symptoms.** `make dbt-run` exits non-zero · red in `dbt_project/target/run_results.json`.

**Diagnose.**
```bash
cd dbt_project
dbt run --select +<failing_model>+   # build only the failing model and its refs
dbt test --select <failing_model>    # run tests against it
cat target/run_results.json | jq '.results[] | select(.status == "error")'
```

**Common causes.**
- **Upstream raw data missing.** Staging models will fail fast if
  `raw.prices` is empty. → `make seed`.
- **SQL typo in a new model.** → Read the dbt log, the error line number is
  usually correct.
- **Postgres running out of work_mem.** Window functions over
  `stg_prices_daily` are memory-heavy. → In `dbt_project/profiles.yml`,
  raise `statement_timeout` / tune `work_mem`.

---

## 7. Data Quality warnings

**Symptoms.** Streamlit **DQ** tab shows failures · `metadata.dq_results`
has rows with `status = 'fail'`.

**Diagnose.**
```bash
docker compose exec postgres psql -U alphaagent -c "
  SELECT check_name, status, severity, details, run_at
  FROM metadata.dq_results
  WHERE run_at >= CURRENT_DATE - INTERVAL '3 days'
    AND status != 'pass'
  ORDER BY run_at DESC;
"
```

**Common checks and their meaning.**

| Check | What it means when it fails |
|---|---|
| `fct_portfolio_performance_daily.nav_not_null` | A portfolio has no NAV for a trading date — missing prices upstream |
| `assert_nav_breach` | Portfolio NAV vs. reconstructed-from-positions differs > 5bps — unit error somewhere |
| `stg_trades.no_duplicates` | Deduplication of batch + stream failed — check `stg_trades.sql` union logic |
| `fct_position_attribution.contribution_sums_to_return` | Sum of contributions should equal portfolio return — usually means a security is missing from `securities_master` |

**Policy.** `severity: error` blocks the Airflow DAG's `promote_to_marts`
task. `severity: warn` posts to the DQ tab but does not block.

---

## 8. Smoke-test after any change

Run before merging to main:

```bash
make down && make up && sleep 15
make seed
make transform
make ge
AGENT_LLM_MOCK=1 make eval     # no API cost; catches regressions in non-LLM code
python3 -m pytest tests/unit -v
```

All five should pass. Then:

```bash
make api &
curl -s localhost:8000/v1/health | jq
curl -s localhost:8000/v1/portfolios | jq
kill %1
```

---

## 9. Rollback

All changes ship as PRs gated by CI. Rollback is `git revert <sha>` + merge.
The demo is stateless at the application layer — restarting `api` and `consumer`
picks up the previous code. Database schema changes are forward-only (no
migrations yet; this is a weekend build), so schema rollback requires
`make nuke && make up && make seed && make transform`.
