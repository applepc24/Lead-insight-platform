from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from google.cloud import bigquery


def run_scalar_query(query: str) -> int:
    project_id = os.environ["GCP_PROJECT_ID"]
    client = bigquery.Client(project=project_id)
    rows = list(client.query(query).result())
    if not rows:
        return 0
    return list(rows[0].values())[0]


def check_stg_not_empty():
    query = """
    SELECT COUNT(*)
    FROM `lead-insight-platform.lead_platform.stg_job_postings`
    """
    count = run_scalar_query(query)
    print(f"[dq] stg_job_postings count={count}")
    if count == 0:
        raise ValueError("stg_job_postings is empty")


def check_int_not_empty():
    query = """
    SELECT COUNT(*)
    FROM `lead-insight-platform.lead_platform.int_job_postings_clean`
    """
    count = run_scalar_query(query)
    print(f"[dq] int_job_postings_clean count={count}")
    if count == 0:
        raise ValueError("int_job_postings_clean is empty")


def check_description_quality():
    project_id = os.environ["GCP_PROJECT_ID"]
    client = bigquery.Client(project=project_id)

    query = """
    SELECT
      source,
      total_postings,
      description_filled_count
    FROM `lead-insight-platform.lead_platform.mart_job_postings_source_quality`
    """

    rows = list(client.query(query).result())
    print(f"[dq] source quality rows={len(rows)}")

    for row in rows:
        source = row["source"]
        total_postings = row["total_postings"]
        description_filled_count = row["description_filled_count"]

        print(
            f"[dq] source={source}, total_postings={total_postings}, "
            f"description_filled_count={description_filled_count}"
        )

        if total_postings > 0 and description_filled_count == 0:
            raise ValueError(f"description quality check failed for source={source}")


default_args = {
    "owner": "you",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="quality_check_job_postings",
    start_date=datetime(2026, 3, 20),
    schedule="20 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["job-postings", "bigquery", "quality"],
) as dag:

    check_stg_task = PythonOperator(
        task_id="check_stg_not_empty",
        python_callable=check_stg_not_empty,
    )

    check_int_task = PythonOperator(
        task_id="check_int_not_empty",
        python_callable=check_int_not_empty,
    )

    check_quality_task = PythonOperator(
        task_id="check_description_quality",
        python_callable=check_description_quality,
    )

    [check_stg_task, check_int_task] >> check_quality_task