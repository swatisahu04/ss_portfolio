"""
SQL safety validation + bounded execution.

The critic agent calls `validate_sql`; the executor calls `run_safely`.
"""
from __future__ import annotations

import time

import psycopg
import sqlglot
from sqlglot import exp

from agent.config import get_agent_settings

# Note: sqlglot >= 23.x uses `Alter` (was `AlterTable`) and `TruncateTable`
# (was `Truncate`). Any write/DDL node in this tuple causes validate_sql to reject.
BANNED_NODES = (
    exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create,
    exp.Alter, exp.TruncateTable,
)


class UnsafeSQLError(Exception):
    pass


def validate_sql(sql: str) -> tuple[bool, list[str]]:
    """
    Returns (ok, errors).
    Rejects:
      - empty / unparseable SQL
      - any non-SELECT / non-CTE statement
      - any DDL or DML
      - queries referencing schemas other than `marts` or `metadata`
    """
    errors: list[str] = []

    if not sql or not sql.strip():
        return False, ["empty SQL"]

    try:
        parsed = sqlglot.parse(sql, read="postgres")
    except Exception as e:
        return False, [f"parse error: {e}"]

    if len(parsed) != 1:
        errors.append("only one statement allowed per request")

    tree = parsed[0]

    # Banned statement types
    for banned in BANNED_NODES:
        if list(tree.find_all(banned)):
            errors.append(f"statement type {banned.__name__} is not allowed")

    # Must be a SELECT at top level (possibly with CTEs)
    if not isinstance(tree, (exp.Select, exp.With, exp.Union)):
        errors.append(f"top-level statement must be SELECT, got {type(tree).__name__}")

    # Schema allowlist: only `marts` or `metadata`.
    # In sqlglot >= 20, Table.db is the (string) schema name; Table.args["db"] is
    # an Identifier object. Always prefer the string accessor.
    for t in tree.find_all(exp.Table):
        db = (t.db or "").lower()
        if db and db not in ("marts", "metadata"):
            errors.append(f"schema '{db}' is not allowed; use `marts.*`")
        # Also check bare table names that don't have a schema qualifier — we require qualification
        if not db and t.name not in _known_mart_tables():
            errors.append(f"table '{t.name}' must be schema-qualified, e.g. marts.{t.name}")

    return len(errors) == 0, errors


def _known_mart_tables() -> set[str]:
    # Small allowlist; could be derived from schema_loader
    return {
        "dim_portfolios",
        "fct_portfolio_performance_daily",
        "fct_portfolio_risk_daily",
        "fct_position_attribution",
        "fct_trade_activity_daily",
    }


def run_safely(sql: str) -> tuple[list[dict], list[str], int]:
    """
    Execute SQL with the read-only role and enforced row + time limits.
    Returns (rows_as_dicts, column_names, execution_ms).
    """
    s = get_agent_settings()
    # Enforce a LIMIT if the user didn't include one
    lower = sql.lower()
    if "limit" not in lower:
        sql = f"SELECT * FROM ({sql.rstrip(';')}) _bounded LIMIT {s.agent_max_rows}"

    start = time.perf_counter()
    with psycopg.connect(
        s.pg_dsn_readonly,
        options=f"-c statement_timeout={s.agent_query_timeout_s * 1000}",
        autocommit=True,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d.name for d in cur.description] if cur.description else []
            rows = cur.fetchall()
    ms = int((time.perf_counter() - start) * 1000)
    return [dict(zip(cols, r)) for r in rows], cols, ms


def explain_cost(sql: str) -> float | None:
    """Run EXPLAIN on the query and return estimated total cost. Swallows errors."""
    try:
        s = get_agent_settings()
        with psycopg.connect(s.pg_dsn_readonly, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f"EXPLAIN (FORMAT JSON) {sql}")
                plan = cur.fetchone()[0][0]["Plan"]
                return float(plan.get("Total Cost", 0))
    except Exception:
        return None
