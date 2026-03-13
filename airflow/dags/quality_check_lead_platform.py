from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from google.cloud import bigquery


PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET = os.environ.get("BQ_DATASET", "lead_platform")


def run_query_and_get_single_int(query: str) -> int:
    client = bigquery.Client(project=PROJECT_ID)
    rows = list(client.query(query).result())
    if not rows:
        raise ValueError("Query returned no rows")
    value = list(rows[0].values())[0]
    return int(value)


def check_dedup_uniqueness() -> None:
    stripe_query = f"""
    SELECT COUNT(*)
    FROM (
      SELECT event_id
      FROM `{PROJECT_ID}.{DATASET}.v_stripe_invoice_events_dedup`
      GROUP BY event_id
      HAVING COUNT(*) > 1
    )
    """

    web_query = f"""
    SELECT COUNT(*)
    FROM (
      SELECT event_id
      FROM `{PROJECT_ID}.{DATASET}.v_web_events_dedup`
      GROUP BY event_id
      HAVING COUNT(*) > 1
    )
    """

    stripe_duplicates = run_query_and_get_single_int(stripe_query)
    web_duplicates = run_query_and_get_single_int(web_query)

    if stripe_duplicates > 0 or web_duplicates > 0:
        raise ValueError(
            f"Dedup uniqueness check failed: "
            f"stripe_duplicates={stripe_duplicates}, web_duplicates={web_duplicates}"
        )


def check_source_row_counts() -> None:
    stripe_query = f"""
    SELECT COUNT(*)
    FROM `{PROJECT_ID}.{DATASET}.stg_stripe_invoice_events`
    """

    web_query = f"""
    SELECT COUNT(*)
    FROM `{PROJECT_ID}.{DATASET}.stg_web_user_events`
    """

    stripe_count = run_query_and_get_single_int(stripe_query)
    web_count = run_query_and_get_single_int(web_query)

    if stripe_count <= 0 or web_count <= 0:
        raise ValueError(
            f"Source row count check failed: "
            f"stripe_count={stripe_count}, web_count={web_count}"
        )


def check_stripe_not_null() -> None:
    query = f"""
    SELECT
      COUNTIF(event_id IS NULL)
      + COUNTIF(event_name IS NULL)
      + COUNTIF(event_source IS NULL)
      + COUNTIF(occurred_at IS NULL)
    FROM `{PROJECT_ID}.{DATASET}.stg_stripe_invoice_events`
    """

    null_count = run_query_and_get_single_int(query)

    if null_count > 0:
        raise ValueError(
            f"stg_stripe_invoice_events core not null check failed: null_count={null_count}"
        )


def check_web_not_null() -> None:
    query = f"""
    SELECT
      COUNTIF(event_id IS NULL)
      + COUNTIF(event_name IS NULL)
      + COUNTIF(event_source IS NULL)
      + COUNTIF(occurred_at IS NULL)
    FROM `{PROJECT_ID}.{DATASET}.stg_web_user_events`
    """

    null_count = run_query_and_get_single_int(query)

    if null_count > 0:
        raise ValueError(
            f"stg_web_user_events core not null check failed: null_count={null_count}"
        )


default_args = {
    "owner": "you",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="quality_check_lead_platform",
    start_date=datetime(2026, 3, 10),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["lead-platform", "quality-check"],
) as dag:

    check_dedup_uniqueness_task = PythonOperator(
        task_id="check_dedup_uniqueness",
        python_callable=check_dedup_uniqueness,
    )

    check_source_row_counts_task = PythonOperator(
        task_id="check_source_row_counts",
        python_callable=check_source_row_counts,
    )

    check_stripe_not_null_task = PythonOperator(
        task_id="check_stripe_not_null",
        python_callable=check_stripe_not_null,
    )

    check_web_not_null_task = PythonOperator(
        task_id="check_web_not_null",
        python_callable=check_web_not_null,
    )

    check_dedup_uniqueness_task >> check_source_row_counts_task
    check_source_row_counts_task >> [check_stripe_not_null_task, check_web_not_null_task]