"""
Synthetic asset-management dataset generator.

Design goals:
  1. Realistic-enough to tell a good story (correlated returns, sector weights).
  2. Small enough that a weekend Docker setup can handle it.
  3. Intentionally *slightly* dirty — so DQ checks have something to catch.

Outputs Parquet files under ./data/raw/ for the batch loader to pick up.
"""

from __future__ import annotations

import argparse
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

rng = np.random.default_rng(seed=42)
fake = Faker()
Faker.seed(42)

OUT_DIR = Path(__file__).resolve().parents[1] / "data" / "raw"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ------------------------------ reference data ------------------------------

SECTORS = [
    "Technology", "Healthcare", "Financials", "Consumer Discretionary",
    "Communication Services", "Industrials", "Consumer Staples",
    "Energy", "Utilities", "Materials", "Real Estate",
]
REGIONS = ["US", "EU", "APAC", "EM"]
STRATEGIES = ["Growth", "Value", "Balanced", "ESG", "Income"]
ASSET_TYPES_WEIGHTS = {"equity": 0.70, "etf": 0.20, "bond": 0.10}

BENCHMARKS = [
    {"ticker": "SPY", "name": "S&P 500 ETF", "asset_type": "etf"},
    {"ticker": "AGG", "name": "US Aggregate Bond ETF", "asset_type": "etf"},
    {"ticker": "VT",  "name": "World Equity ETF", "asset_type": "etf"},
]


# ------------------------------ generators ------------------------------

def gen_securities(n: int = 500) -> pd.DataFrame:
    rows = []
    for i in range(n):
        at = rng.choice(list(ASSET_TYPES_WEIGHTS), p=list(ASSET_TYPES_WEIGHTS.values()))
        ticker = f"{fake.lexify('???').upper()}{i:03d}"
        rows.append({
            "security_id": f"SEC-{i:05d}",
            "ticker": ticker,
            "name": fake.company(),
            "asset_type": at,
            "sector": rng.choice(SECTORS) if at == "equity" else "Diversified",
            "region": rng.choice(REGIONS, p=[0.55, 0.20, 0.15, 0.10]),
            "currency": "USD",
            "listed_exchange": rng.choice(["NYSE", "NASDAQ", "LSE", "TSE"]),
            "active": True,
        })
    for b in BENCHMARKS:
        rows.append({
            "security_id": f"BMK-{b['ticker']}",
            "ticker": b["ticker"],
            "name": b["name"],
            "asset_type": b["asset_type"],
            "sector": "Benchmark",
            "region": "US",
            "currency": "USD",
            "listed_exchange": "NYSE",
            "active": True,
        })
    return pd.DataFrame(rows)


def gen_prices_daily(securities: pd.DataFrame, days: int) -> pd.DataFrame:
    """
    Geometric brownian motion with sector-correlated drift + one common market factor.
    Realistic enough that P&L numbers are differentiated across strategies.
    """
    end = date.today()
    start = end - timedelta(days=days)
    dates = pd.bdate_range(start=start, end=end, freq="B")  # business days only

    # One common "market" factor each day
    market = rng.normal(loc=0.0003, scale=0.010, size=len(dates))

    # Sector betas to market
    sector_betas = {s: rng.uniform(0.6, 1.4) for s in SECTORS}
    sector_betas["Benchmark"] = 1.0
    sector_betas["Diversified"] = 0.8

    rows = []
    for _, sec in securities.iterrows():
        beta = sector_betas.get(sec["sector"], 1.0)
        idio = rng.normal(loc=0.0, scale=0.015, size=len(dates))
        # bonds have lower vol + negative correlation to equity market
        if sec["asset_type"] == "bond":
            beta = -0.15
            idio *= 0.2
        daily_returns = beta * market + idio
        start_price = float(rng.uniform(20, 300))
        prices = start_price * np.cumprod(1 + daily_returns)
        volumes = rng.integers(low=10_000, high=5_000_000, size=len(dates))

        for d, p, v in zip(dates, prices, volumes):
            rows.append({
                "security_id": sec["security_id"],
                "price_date": d.date(),
                "close": round(float(p), 4),
                "volume": int(v),
            })
    df = pd.DataFrame(rows)

    # Intentional dirt #1: drop a handful of prices on a fake "missed holiday"
    # (lets Great Expectations flag freshness/completeness)
    missing_date = df["price_date"].sample(1, random_state=7).iloc[0]
    mask = (df["price_date"] == missing_date) & (df["security_id"].str.startswith("SEC-001"))
    df = df[~mask].reset_index(drop=True)

    return df


def gen_portfolios(n: int = 50) -> pd.DataFrame:
    rows = []
    for i in range(n):
        rows.append({
            "portfolio_id": f"P-{i:03d}",
            "portfolio_name": f"{fake.company()} {rng.choice(['Fund', 'Trust', 'LP'])}",
            "strategy": rng.choice(STRATEGIES),
            "base_currency": "USD",
            "inception_date": fake.date_between(start_date="-10y", end_date="-2y"),
            "portfolio_manager": fake.name(),
            "aum_usd": round(float(rng.uniform(5e7, 5e9)), 2),
        })
    return pd.DataFrame(rows)


def gen_positions_daily(
    portfolios: pd.DataFrame,
    securities: pd.DataFrame,
    prices: pd.DataFrame,
) -> pd.DataFrame:
    """
    Each portfolio holds ~30 securities (weighted by strategy).
    Positions roll forward daily; no trade activity modeled here (that's the stream).
    """
    # skip benchmarks for holdings
    sec_universe = securities[~securities["security_id"].str.startswith("BMK-")]

    rows = []
    unique_dates = sorted(prices["price_date"].unique())

    for _, p in portfolios.iterrows():
        strategy = p["strategy"]
        # strategy-specific universe filter
        if strategy == "Growth":
            pool = sec_universe[sec_universe["sector"].isin(
                ["Technology", "Healthcare", "Consumer Discretionary", "Communication Services"]
            )]
        elif strategy == "Value":
            pool = sec_universe[sec_universe["sector"].isin(
                ["Financials", "Energy", "Industrials", "Materials", "Utilities"]
            )]
        elif strategy == "Income":
            pool = sec_universe[sec_universe["asset_type"] == "bond"]
        elif strategy == "ESG":
            pool = sec_universe[sec_universe["sector"].isin(
                ["Healthcare", "Technology", "Utilities", "Real Estate"]
            )]
        else:  # Balanced
            pool = sec_universe

        if len(pool) < 30:
            pool = sec_universe  # fallback

        picks = pool.sample(n=min(30, len(pool)), random_state=int(p["portfolio_id"].split("-")[1]))
        # Dirichlet weights
        weights = rng.dirichlet(alpha=np.ones(len(picks)) * 2.0)
        total_val = p["aum_usd"]

        for (_, sec), w in zip(picks.iterrows(), weights):
            # derive a starting quantity from Day 1 price
            day1 = prices[
                (prices["security_id"] == sec["security_id"])
                & (prices["price_date"] == unique_dates[0])
            ]
            if day1.empty:
                continue
            qty = round((total_val * w) / day1.iloc[0]["close"], 2)
            for d in unique_dates:
                rows.append({
                    "portfolio_id": p["portfolio_id"],
                    "security_id": sec["security_id"],
                    "position_date": d,
                    "quantity": qty,
                })
    df = pd.DataFrame(rows)
    return df


def gen_trades(portfolios: pd.DataFrame, securities: pd.DataFrame, days: int) -> pd.DataFrame:
    """
    Trade stream — a fraction of portfolios trade each day.
    One row per trade. Gets published to Kafka by the streaming producer.
    """
    end = date.today()
    start = end - timedelta(days=days)
    dates = pd.bdate_range(start=start, end=end, freq="B")
    sec_universe = securities[~securities["security_id"].str.startswith("BMK-")]

    rows = []
    trade_id = 0
    for d in dates:
        n_trades_today = rng.integers(5, 40)
        for _ in range(n_trades_today):
            p = portfolios.sample(1, random_state=trade_id % 10_000).iloc[0]
            s = sec_universe.sample(1, random_state=trade_id % 9_999).iloc[0]
            side = rng.choice(["BUY", "SELL"], p=[0.55, 0.45])
            qty = round(float(rng.uniform(100, 10_000)), 2)
            price = round(float(rng.uniform(15, 300)), 4)
            rows.append({
                "trade_id": f"T-{trade_id:09d}",
                "portfolio_id": p["portfolio_id"],
                "security_id": s["security_id"],
                "trade_date": d.date(),
                "side": side,
                "quantity": qty,
                "price": price,
                "gross_amount": round(qty * price, 2),
                "fees": round(qty * price * 0.0005, 2),  # 5 bps
                "trader": fake.name(),
            })
            trade_id += 1

    df = pd.DataFrame(rows)

    # Intentional dirt #2: duplicate a handful of trades (tests Kafka consumer idempotency)
    dupes = df.sample(n=5, random_state=11)
    df = pd.concat([df, dupes], ignore_index=True)

    return df


def gen_benchmark_perf(prices: pd.DataFrame) -> pd.DataFrame:
    """Daily benchmark close-price view — marts join to it for excess return."""
    bm = prices[prices["security_id"].str.startswith("BMK-")].copy()
    bm = bm.rename(columns={"security_id": "benchmark_id"})
    return bm[["benchmark_id", "price_date", "close"]]


# ------------------------------ main ------------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--portfolios", type=int, default=50)
    ap.add_argument("--securities", type=int, default=500)
    ap.add_argument("--days", type=int, default=730)
    args = ap.parse_args()

    print(f"→ generating {args.securities} securities...")
    securities = gen_securities(args.securities)
    securities.to_parquet(OUT_DIR / "securities.parquet", index=False)
    print(f"  {len(securities):,} rows")

    print(f"→ generating {args.days} days of prices...")
    prices = gen_prices_daily(securities, args.days)
    prices.to_parquet(OUT_DIR / "prices_daily.parquet", index=False)
    print(f"  {len(prices):,} rows")

    print(f"→ generating {args.portfolios} portfolios...")
    portfolios = gen_portfolios(args.portfolios)
    portfolios.to_parquet(OUT_DIR / "portfolios.parquet", index=False)
    print(f"  {len(portfolios):,} rows")

    print("→ generating positions...")
    positions = gen_positions_daily(portfolios, securities, prices)
    positions.to_parquet(OUT_DIR / "positions_daily.parquet", index=False)
    print(f"  {len(positions):,} rows")

    print("→ generating trades...")
    trades = gen_trades(portfolios, securities, args.days)
    trades.to_parquet(OUT_DIR / "trades.parquet", index=False)
    print(f"  {len(trades):,} rows (includes intentional dupes)")

    print("→ generating benchmark perf view...")
    bm = gen_benchmark_perf(prices)
    bm.to_parquet(OUT_DIR / "benchmarks_daily.parquet", index=False)
    print(f"  {len(bm):,} rows")

    print(f"\n✓ data written to {OUT_DIR}/")


if __name__ == "__main__":
    main()
