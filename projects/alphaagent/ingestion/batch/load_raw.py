"""
Load generated Parquet files into Postgres `raw` schema.

Tables created here mirror the generator outputs. dbt reads from these.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import psycopg
from psycopg import sql

from ingestion.config import get_settings

RAW_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"

TABLES = [
    ("securities", "securities.parquet", [
        ("security_id", "TEXT PRIMARY KEY"),
        ("ticker", "TEXT NOT NULL"),
        ("name", "TEXT"),
        ("asset_type", "TEXT"),
        ("sector", "TEXT"),
        ("region", "TEXT"),
        ("currency", "TEXT"),
        ("listed_exchange", "TEXT"),
        ("active", "BOOLEAN"),
    ]),
    ("prices_daily", "prices_daily.parquet", [
        ("security_id", "TEXT NOT NULL"),
        ("price_date", "DATE NOT NULL"),
        ("close", "NUMERIC(18,6)"),
        ("volume", "BIGINT"),
        ("PRIMARY KEY", "(security_id, price_date)"),
    ]),
    ("portfolios", "portfolios.parquet", [
        ("portfolio_id", "TEXT PRIMARY KEY"),
        ("portfolio_name", "TEXT"),
        ("strategy", "TEXT"),
        ("base_currency", "TEXT"),
        ("inception_date", "DATE"),
        ("portfolio_manager", "TEXT"),
        ("aum_usd", "NUMERIC(20,2)"),
    ]),
    ("positions_daily", "positions_daily.parquet", [
        ("portfolio_id", "TEXT NOT NULL"),
        ("security_id", "TEXT NOT NULL"),
        ("position_date", "DATE NOT NULL"),
        ("quantity", "NUMERIC(20,4)"),
        ("PRIMARY KEY", "(portfolio_id, security_id, position_date)"),
    ]),
    ("trades", "trades.parquet", [
        ("trade_id", "TEXT"),   # not PK — we expect dupes from generator
        ("portfolio_id", "TEXT NOT NULL"),
        ("security_id", "TEXT NOT NULL"),
        ("trade_date", "DATE NOT NULL"),
        ("side", "TEXT"),
        ("quantity", "NUMERIC(20,4)"),
        ("price", "NUMERIC(18,6)"),
        ("gross_amount", "NUMERIC(20,2)"),
        ("fees", "NUMERIC(20,2)"),
        ("trader", "TEXT"),
    ]),
    ("benchmarks_daily", "benchmarks_daily.parquet", [
        ("benchmark_id", "TEXT NOT NULL"),
        ("price_date", "DATE NOT NULL"),
        ("close", "NUMERIC(18,6)"),
        ("PRIMARY KEY", "(benchmark_id, price_date)"),
    ]),
]


def ensure_table(cur: psycopg.Cursor, table: str, schema_cols: list[tuple[str, str]]) -> None:
    col_defs = [f"{name} {dtype}" if name != "PRIMARY KEY" else f"PRIMARY KEY {dtype}"
                for name, dtype in schema_cols]
    stmt = sql.SQL("CREATE TABLE IF NOT EXISTS raw.{} ({})").format(
        sql.Identifier(table),
        sql.SQL(", ").join(sql.SQL(c) for c in col_defs),
    )
    cur.execute(stmt)


def copy_df(cur: psycopg.Cursor, table: str, df: pd.DataFrame) -> None:
    cur.execute(sql.SQL("TRUNCATE TABLE raw.{} CASCADE").format(sql.Identifier(table)))
    cols = sql.SQL(", ").join(sql.Identifier(c) for c in df.columns)
    copy_sql = sql.SQL("COPY raw.{} ({}) FROM STDIN WITH (FORMAT CSV, HEADER FALSE, NULL '')").format(
        sql.Identifier(table), cols
    )
    # Stream via COPY
    with cur.copy(copy_sql) as copy:
        for row in df.itertuples(index=False, name=None):
            copy.write_row(row)


def main() -> None:
    s = get_settings()
    print(f"→ connecting to {s.pg_host}:{s.pg_port}/{s.pg_db}")
    with psycopg.connect(s.pg_dsn, autocommit=False) as conn:
        with conn.cursor() as cur:
            for table, fname, schema_cols in TABLES:
                path = RAW_DIR / fname
                if not path.exists():
                    print(f"  ! missing {path}; run `make seed` first. Skipping.")
                    continue
                ensure_table(cur, table, schema_cols)
                df = pd.read_parquet(path)
                print(f"→ loading raw.{table} ({len(df):,} rows)")
                copy_df(cur, table, df)
        conn.commit()
    print("✓ raw load complete")


if __name__ == "__main__":
    main()
