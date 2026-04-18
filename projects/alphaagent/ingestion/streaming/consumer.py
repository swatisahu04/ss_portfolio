"""
Kafka consumer — reads `trades.v1` and upserts into raw.trades_stream.

Idempotent by `trade_id`. Duplicate events are swallowed silently;
the dupe counter is exposed for observability.
"""
from __future__ import annotations

import json
import signal
import sys
from datetime import datetime

import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException
from psycopg import sql

from ingestion.config import get_settings

DDL = """
CREATE TABLE IF NOT EXISTS raw.trades_stream (
    trade_id TEXT PRIMARY KEY,
    portfolio_id TEXT NOT NULL,
    security_id TEXT NOT NULL,
    trade_date DATE NOT NULL,
    side TEXT,
    quantity NUMERIC(20,4),
    price NUMERIC(18,6),
    gross_amount NUMERIC(20,2),
    fees NUMERIC(20,2),
    trader TEXT,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

UPSERT_SQL = """
INSERT INTO raw.trades_stream
  (trade_id, portfolio_id, security_id, trade_date, side, quantity, price, gross_amount, fees, trader)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (trade_id) DO NOTHING
"""

running = True


def shutdown(*_: object) -> None:
    global running
    running = False
    print("\n→ graceful shutdown requested")


def main() -> None:
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    s = get_settings()
    c = Consumer({
        "bootstrap.servers": s.kafka_bootstrap,
        "group.id": "alphaagent-trades-consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    c.subscribe([s.kafka_trades_topic])

    conn = psycopg.connect(s.pg_dsn, autocommit=False)
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()

    ingested = 0
    dupes = 0
    print(f"→ consuming {s.kafka_trades_topic} into raw.trades_stream")
    try:
        while running:
            msg = c.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            event = json.loads(msg.value())
            with conn.cursor() as cur:
                cur.execute(
                    UPSERT_SQL,
                    (
                        event["trade_id"],
                        event["portfolio_id"],
                        event["security_id"],
                        event["trade_date"],
                        event["side"],
                        event["quantity"],
                        event["price"],
                        event["gross_amount"],
                        event["fees"],
                        event["trader"],
                    ),
                )
                if cur.rowcount == 0:
                    dupes += 1
                else:
                    ingested += 1
            conn.commit()
            c.commit(msg, asynchronous=False)

            if (ingested + dupes) % 500 == 0:
                ts = datetime.utcnow().strftime("%H:%M:%S")
                print(f"  [{ts}] ingested={ingested:,} dupes={dupes:,}")
    finally:
        c.close()
        conn.close()
        print(f"\n✓ total ingested={ingested:,} dupes_swallowed={dupes:,}")


if __name__ == "__main__":
    main()
