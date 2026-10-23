"""MBTA lakehouse DAG.

Every 15 minutes:
  gtfs_static (daily, short-circuits otherwise)
      └─▶ consume_batch ─▶ quality_gates ─▶ dbt_deps ─▶ dbt_run ─▶ dbt_snapshot ─▶ dbt_test

Backfill a range (re-runs are idempotent):
  airflow dags backfill mbta_lakehouse -s 2026-10-01 -e 2026-10-07
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator

DBT = "cd /opt/airflow/dbt && dbt {cmd} --profiles-dir . --target dev"

default_args = {
    "owner": "manan",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "sla": timedelta(minutes=20),
}

with DAG(
    dag_id="mbta_lakehouse",
    description="Kafka → S3 Parquet → quality gates → dbt",
    start_date=datetime(2026, 10, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["mbta", "lakehouse"],
) as dag:

    # GTFS static only needs refreshing once a day
    is_first_run_of_day = ShortCircuitOperator(
        task_id="is_first_run_of_day",
        python_callable=lambda logical_date, **_: logical_date.hour == 0 and logical_date.minute < 15,
        ignore_downstream_trigger_rules=False,
    )

    gtfs_static = BashOperator(
        task_id="gtfs_static",
        bash_command="python /opt/airflow/ingestion/gtfs_static.py",
    )

    consume_batch = BashOperator(
        task_id="consume_batch",
        bash_command="python /opt/airflow/consumer/consumer.py --max-messages 20000 --batch-id {{ ts_nodash }}",
        trigger_rule="none_failed",  # run whether or not gtfs ran today
    )

    quality_gates = BashOperator(
        task_id="quality_gates",
        bash_command="python /opt/airflow/quality/gates.py",
    )

    dbt_deps = BashOperator(task_id="dbt_deps", bash_command=DBT.format(cmd="deps"))
    dbt_run = BashOperator(task_id="dbt_run", bash_command=DBT.format(cmd="run"))
    dbt_snapshot = BashOperator(task_id="dbt_snapshot", bash_command=DBT.format(cmd="snapshot"))
    dbt_test = BashOperator(task_id="dbt_test", bash_command=DBT.format(cmd="test"))

    is_first_run_of_day >> gtfs_static >> consume_batch
    consume_batch >> quality_gates >> dbt_deps >> dbt_run >> dbt_snapshot >> dbt_test
