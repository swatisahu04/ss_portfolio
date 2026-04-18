"""
Kafka producer that simulates a live trade feed.

Reads the generated trades.parquet and publishes to `trades.v1` topic,
preserving trade_date ordering with a configurable replay speed.

Intentional dupes from the generator are preserved — tests consumer idempotency.
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer

from ingestion.config import get_settings

DATA_PATH = Path(__file__).resolve().parents[2] / "data" / "raw" / "trades.parquet"


def delivery_report(err, msg) -> None:
    if err is not None:
        print(f"delivery failed: {err}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--replay-speed", type=float, default=60.0,
                    help="Multiplier on real-time. 60 = 1 day/sec.")
    ap.add_argument("--max-messages", type=int, default=None,
                    help="Cap on messages (useful for demos)")
    args = ap.parse_args()

    s = get_settings()
    p = Producer({
        "bootstrap.servers": s.kafka_bootstrap,
        "linger.ms": 100,
        "enable.idempotence": True,
    })

    df = pd.read_parquet(DATA_PATH).sort_values("trade_date").reset_index(drop=True)
    if args.max_messages:
        df = df.head(args.max_messages)

    print(f"→ publishing {len(df):,} trades to {s.kafka_trades_topic}")
    prev_day = None
    sent = 0
    for _, row in df.iterrows():
        if prev_day is not None and row["trade_date"] != prev_day:
            time.sleep(1.0 / args.replay_speed)
        prev_day = row["trade_date"]

        payload = {
            "trade_id": row["trade_id"],
            "portfolio_id": row["portfolio_id"],
            "security_id": row["security_id"],
            "trade_date": str(row["trade_date"]),
            "side": row["side"],
            "quantity": float(row["quantity"]),
            "price": float(row["price"]),
            "gross_amount": float(row["gross_amount"]),
            "fees": float(row["fees"]),
            "trader": row["trader"],
        }
        p.produce(
            topic=s.kafka_trades_topic,
            key=row["trade_id"].encode(),
            value=json.dumps(payload).encode(),
            on_delivery=delivery_report,
        )
        sent += 1
        if sent % 1000 == 0:
            p.poll(0)
            print(f"  sent {sent:,}")

    p.flush(30)
    print(f"✓ done. {sent:,} messages sent.")


if __name__ == "__main__":
    main()
