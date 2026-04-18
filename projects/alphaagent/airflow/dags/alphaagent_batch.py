"""
AlphaAgent batch pipeline DAG.

Every hour during market days:
  seed (dev-only) → load_raw → dbt run → dbt test → DQ checks → publish lineage

Production notes:
  - `seed` is a dev convenience only; in prod, ingestion would be driven by
    Fivetran / dbt-sources or a streaming pipeline.
  - DQ failures at `error` severity fail the DAG; `warn` failures surface in
    Marquez and Slack but don't block.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "sla": timedelta(minutes=30),
    "email_on_failure": False,
}

DBT_ENV = {
    "PG_HOST": "postgres",
    "PG_PORT": "5432",
    "PG_DB": "alphaagent",
    "PG_USER": "alphaagent",
    "PG_PASSWORD": "alphaagent",
    "DBT_PROFILES_DIR": "/opt/airflow/dbt_project",
}


with DAG(
    dag_id="alphaagent_batch",
    default_args=DEFAULT_ARGS,
    description="AlphaAgent end-to-end batch: ingest → transform → DQ → lineage",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["alphaagent", "fintech", "daily"],
    doc_md=__doc__,
) as dag:

    start = EmptyOperator(task_id="start")

    seed = BashOperator(
        task_id="seed_data",
        bash_command="python -m data_generator.generate --portfolios 50 --securities 500 --days 730",
        doc_md="Dev-only: regenerate synthetic Parquet files.",
    )

    load_raw = BashOperator(
        task_id="load_raw_to_postgres",
        bash_command="python -m ingestion.batch.load_raw",
        doc_md="COPY raw Parquet files into `raw` schema in Postgres.",
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt_project && dbt deps --profiles-dir .",
        env=DBT_ENV,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_project && dbt run --profiles-dir .",
        env=DBT_ENV,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_project && dbt test --profiles-dir .",
        env=DBT_ENV,
    )

    dq_checks = BashOperator(
        task_id="dq_checks",
        bash_command="python -m dq.run_expectations",
        doc_md="Run Great-Expectations-style checks; fails the DAG on error-severity failures.",
    )

    publish_lineage = BashOperator(
        task_id="publish_lineage",
        bash_command="python -m lineage.emit_dbt_lineage",
        doc_md="Parse dbt manifest → emit OpenLineage events to Marquez.",
    )

    end = EmptyOperator(task_id="end", trigger_rule="all_done")

    (
        start
        >> seed
        >> load_raw
        >> dbt_deps
        >> dbt_run
        >> dbt_test
        >> dq_checks
        >> publish_lineage
        >> end
    )
