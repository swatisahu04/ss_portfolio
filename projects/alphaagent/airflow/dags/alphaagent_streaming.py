"""
Long-running streaming consumer as an Airflow-managed daemon.
Simple but production-shaped: ExternalTaskSensor could gate downstream analytics.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="alphaagent_streaming_consumer",
    description="Kafka trades consumer (starts on schedule, runs until next trigger).",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["alphaagent", "streaming"],
    doc_md=__doc__,
) as dag:

    consume = BashOperator(
        task_id="consume_trades",
        bash_command="timeout 23h python -m ingestion.streaming.consumer || true",
        doc_md="Runs up to 23h, exits cleanly so next day's schedule can pick it up.",
    )
