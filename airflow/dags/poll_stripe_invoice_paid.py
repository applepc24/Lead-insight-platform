from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator # type: ignore

default_args = {
    "owner": "you",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="poll_stripe_invoice_paid",
    start_date=datetime(2026, 3, 6),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["lead-platform", "stripe", "producer", "kafka"],
) as dag:

    poll_stripe = BashOperator(
        task_id="poll_stripe_invoice_paid",
        bash_command="""
        cd /opt/airflow/project && \
        export KAFKA_BOOTSTRAP_SERVERS="host.docker.internal:9092" && \
        export KAFKA_TOPIC_STRIPE_INVOICE="stripe.invoice_events" && \
        export POLL_WINDOW_SECONDS="15552000" && \
        export CURSOR_FILE="/opt/airflow/project/.cursor_invoice_paid.json" && \
        echo "STRIPE_SECRET_KEY is set: ${STRIPE_SECRET_KEY:+yes}" && \
        echo "KAFKA_BOOTSTRAP_SERVERS=$KAFKA_BOOTSTRAP_SERVERS" && \
        python3 -u producer_stripe_invoice_paid.py
        """,
        env={
            "STRIPE_SECRET_KEY": os.environ["STRIPE_SECRET_KEY"],
        },
        append_env=True,
    )

    poll_stripe