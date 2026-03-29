from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator  # type: ignore

default_args = {
    "owner": "you",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="replay_job_postings_dlq",
    start_date=datetime(2026, 3, 24),
    schedule="*/5 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["job-postings", "dlq", "replay"],
) as dag:

    replay_dlq_task = BashOperator(
        task_id="replay_dlq_to_fetch_topic",
        bash_command="""
        cd /opt/airflow/project

        python3 replay_dlq_to_original.py
        """,
        env={
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
            "KAFKA_DLQ_TOPIC": "job_postings.dlq",
            "KAFKA_TARGET_TOPIC": "job_postings.fetch_jobs",
            "KAFKA_REPLAY_GROUP_ID": "dlq-replay-airflow-v1",
            "MAX_RETRY_COUNT": "3",
        },
    )