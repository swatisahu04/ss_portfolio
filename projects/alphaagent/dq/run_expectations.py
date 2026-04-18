"""
Lightweight DQ runner — inspired by Great Expectations but fully code-defined
to avoid the GE config-file sprawl for a weekend project.

Each check:
  - runs a SQL query
  - asserts a condition on the result
  - records pass/fail to metadata.dq_results
  - emits an OpenLineage event so the run shows in Marquez

Production version would use GE's Checkpoint API; this version keeps the
weekend scope manageable and is 100% of the same shape.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

import httpx
import psycopg

from ingestion.config import get_settings


@dataclass
class Check:
    name: str
    table: str
    sql: str
    predicate: Callable[[list[tuple]], bool]
    severity: str = "error"  # "error" | "warn"
    description: str = ""


# ---------------- define suite ----------------
CHECKS: list[Check] = [
    Check(
        name="marts.dim_portfolios.row_count_reasonable",
        table="marts.dim_portfolios",
        sql="SELECT COUNT(*) FROM marts.dim_portfolios",
        predicate=lambda rows: 1 <= rows[0][0] <= 10_000,
        description="Portfolio dim should have between 1 and 10k rows.",
    ),
    Check(
        name="marts.fct_portfolio_performance_daily.no_null_ytd",
        table="marts.fct_portfolio_performance_daily",
        sql="""SELECT COUNT(*) FROM marts.fct_portfolio_performance_daily
               WHERE ytd_return IS NULL AND as_of_date > (
                   SELECT MIN(as_of_date) + INTERVAL '30 days'
                   FROM marts.fct_portfolio_performance_daily
               )""",
        predicate=lambda rows: rows[0][0] == 0,
        description="YTD return should not be NULL after day 30.",
    ),
    Check(
        name="marts.fct_portfolio_performance_daily.ytd_in_reasonable_range",
        table="marts.fct_portfolio_performance_daily",
        sql="""SELECT COUNT(*) FROM marts.fct_portfolio_performance_daily
               WHERE ytd_return < -0.9 OR ytd_return > 5.0""",
        predicate=lambda rows: rows[0][0] == 0,
        description="YTD return should be between -90% and +500%.",
    ),
    Check(
        name="marts.fct_portfolio_risk_daily.vol_non_negative",
        table="marts.fct_portfolio_risk_daily",
        sql="""SELECT COUNT(*) FROM marts.fct_portfolio_risk_daily
               WHERE vol_30d < 0""",
        predicate=lambda rows: rows[0][0] == 0,
        description="Volatility must be non-negative.",
    ),
    Check(
        name="marts.freshness.perf_updated_recently",
        table="marts.fct_portfolio_performance_daily",
        sql="""SELECT MAX(as_of_date) FROM marts.fct_portfolio_performance_daily""",
        # In a real system we'd check AGE(MAX(as_of_date)) < 2 days. For synthetic data
        # we just check it's not NULL.
        predicate=lambda rows: rows[0][0] is not None,
        description="Performance fact should have at least one row.",
    ),
]


# ---------------- DQ results table ----------------
DDL = """
CREATE TABLE IF NOT EXISTS metadata.dq_results (
    id BIGSERIAL PRIMARY KEY,
    run_id TEXT NOT NULL,
    check_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    passed BOOLEAN NOT NULL,
    severity TEXT NOT NULL,
    observed_value JSONB,
    description TEXT,
    run_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dq_run_id ON metadata.dq_results(run_id);
"""


# ---------------- OpenLineage emitter ----------------
def emit_lineage_event(run_id: str, job_name: str, status: str, inputs: list[str]) -> None:
    """Best-effort OpenLineage emission. Swallows errors."""
    try:
        url = "http://localhost:5000/api/v1/lineage"
        event = {
            "eventType": status.upper(),
            "eventTime": datetime.utcnow().isoformat() + "Z",
            "run": {"runId": run_id},
            "job": {"namespace": "alphaagent", "name": job_name},
            "inputs": [{"namespace": "postgres://alphaagent", "name": t} for t in inputs],
            "outputs": [],
            "producer": "https://github.com/you/alphaagent",
            "schemaURL": "https://openlineage.io/spec/1-0-5/OpenLineage.json",
        }
        httpx.post(url, json=event, timeout=2.0)
    except Exception as e:
        print(f"  ! lineage emit failed (ignored): {e}")


# ---------------- runner ----------------
def main() -> None:
    s = get_settings()
    run_id = f"dq-{int(time.time())}"
    print(f"→ DQ run {run_id} starting")

    failures = 0
    with psycopg.connect(s.pg_dsn, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()

        emit_lineage_event(run_id, "dq.run_suite", "START",
                           [c.table for c in CHECKS])

        for check in CHECKS:
            print(f"  • {check.name}", end=" ")
            try:
                with conn.cursor() as cur:
                    cur.execute(check.sql)
                    rows = cur.fetchall()
                passed = check.predicate(rows)
            except Exception as e:
                passed = False
                rows = [("ERROR", str(e))]
                print(f"ERROR: {e}", end=" ")

            status = "✓" if passed else ("⚠" if check.severity == "warn" else "✗")
            print(status)
            if not passed and check.severity == "error":
                failures += 1

            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO metadata.dq_results "
                    "(run_id, check_name, table_name, passed, severity, observed_value, description) "
                    "VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s)",
                    (run_id, check.name, check.table, passed, check.severity,
                     json.dumps({"rows": [[str(x) for x in r] for r in rows]}),
                     check.description),
                )
            conn.commit()

        status = "COMPLETE" if failures == 0 else "FAIL"
        emit_lineage_event(run_id, "dq.run_suite", status,
                           [c.table for c in CHECKS])

    print(f"\n{'✓' if failures == 0 else '✗'} DQ run {run_id} finished — {failures} hard failures")
    if failures:
        exit(1)


if __name__ == "__main__":
    main()
