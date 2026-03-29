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

def check_int_no_duplicate_content_hash():
    query = """
    SELECT COUNT(*)
    FROM (
        SELECT content_hash
        FROM `lead-insight-platform.lead_platform.int_job_postings_clean`
        GROUP BY content_hash
        HAVING COUNT(*) > 1
    )
    """
    duplicate_count = run_scalar_query(query)
    print(f"[dq] int_job_postings_clean duplicate content_hash count={duplicate_count}")

    if duplicate_count > 0:
        raise ValueError(
            f"int_job_postings_clean has duplicated content_hash rows: {duplicate_count}"
        )

def check_int_required_fields():
    query = """
    SELECT COUNT(*)
    FROM `lead-insight-platform.lead_platform.int_job_postings_clean`
    WHERE title IS NULL
        OR original_url IS NULL
        OR source IS NULL
        OR collected_at IS NULL
    """
    invalid_count = run_scalar_query(query)
    print(f"[dq] int_job_postings_clean required field null count={invalid_count}")

    if invalid_count > 0:
        raise ValueError(
           f"int_job_postings_clean has rows with required fields missing: {invalid_count}" 
        )
    
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
        
        fill_rate = 0 if total_postings == 0 else description_filled_count / total_postings

        print(
            f"[dq] source={source}, total_postings={total_postings}, "
            f"description_filled_count={description_filled_count}, "
            f"fill_rate={fill_rate:.2f}"
        )

        if total_postings >= 5 and fill_rate < 0.3:
            raise ValueError(
                f"description quality check failed for source={source}, fill_rate={fill_rate:.2f}"
            )


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

    check_required_fields_task = PythonOperator(
        task_id = "check_int_required_fields",
        python_callable=check_int_required_fields
    )

    check_duplicate_task = PythonOperator(
        task_id="check_int_no_duplicate_content_hash",
        python_callable=check_int_no_duplicate_content_hash,
    )

    check_quality_task = PythonOperator(
        task_id="check_description_quality",
        python_callable=check_description_quality,
    )

    [check_stg_task, check_int_task] >> check_required_fields_task >> check_duplicate_task >> check_quality_task