"""
Introspect marts + read dbt model docs → build a compact prompt context.

We only expose the `marts` schema to the agent; staging/intermediate are internal.
"""
from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache

import psycopg

from agent.config import get_agent_settings


@dataclass
class MartSchema:
    table: str
    columns: list[tuple[str, str]]    # (name, dtype)
    description: str = ""


INTROSPECT_SQL = """
SELECT
    table_name,
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = 'marts'
ORDER BY table_name, ordinal_position
"""

DESCRIPTIONS = {
    "dim_portfolios": (
        "One row per portfolio. Dimension table. "
        "Columns of interest: portfolio_id (PK), strategy "
        "(Growth|Value|Balanced|ESG|Income), portfolio_manager, current_nav_usd."
    ),
    "fct_portfolio_performance_daily": (
        "Daily portfolio performance. Grain: (portfolio_id, as_of_date). "
        "Columns: daily_return, mtd_return, ytd_return, spy_excess_return_ytd, "
        "agg_excess_return_ytd, portfolio_nav_usd."
    ),
    "fct_portfolio_risk_daily": (
        "Rolling 30-day risk metrics per portfolio-day. "
        "Columns: vol_30d (annualized), sharpe_30d, beta_vs_spy_30d."
    ),
    "fct_position_attribution": (
        "Position-level attribution. Grain: (portfolio_id, security_id, as_of_date). "
        "Columns: ticker, sector, asset_type, weight, contribution_to_return, "
        "security_daily_return, market_value_usd."
    ),
    "fct_trade_activity_daily": (
        "Daily trade activity per portfolio. Grain: (portfolio_id, trade_date). "
        "Columns: num_trades, num_buys, num_sells, total_gross_usd, total_fees_usd, "
        "turnover_ratio (one-sided)."
    ),
}


@lru_cache(maxsize=1)
def load_mart_schemas() -> list[MartSchema]:
    s = get_agent_settings()
    by_table: dict[str, list[tuple[str, str]]] = {}
    with psycopg.connect(s.pg_dsn_readonly, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(INTROSPECT_SQL)
            for table, col, dtype in cur.fetchall():
                by_table.setdefault(table, []).append((col, dtype))

    return [
        MartSchema(table=t, columns=cols, description=DESCRIPTIONS.get(t, ""))
        for t, cols in sorted(by_table.items())
    ]


def schema_prompt() -> str:
    """Produce a compact schema block for LLM prompts."""
    schemas = load_mart_schemas()
    lines = []
    for s in schemas:
        lines.append(f"-- marts.{s.table}")
        if s.description:
            lines.append(f"-- {s.description}")
        cols_line = ", ".join(f"{n} {t}" for n, t in s.columns)
        lines.append(f"CREATE TABLE marts.{s.table} ({cols_line});")
        lines.append("")
    return "\n".join(lines)
