from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from google.cloud import bigquery


def run_query(query: str) -> None:
    project_id = os.environ["GCP_PROJECT_ID"]
    client = bigquery.Client(project=project_id)
    job = client.query(query)
    job.result()


def build_int_job_postings_clean():
    query = """
    CREATE OR REPLACE VIEW `lead-insight-platform.lead_platform.int_job_postings_clean` AS
    SELECT
      posting_id,
      source,
      original_url,
      company_name,
      title,
      location,
      employment_type,
      experience_level,
      description_text,
      skills,
      collected_at,
      content_hash,
      raw_s3_key,
      processed_s3_key,
      curated_s3_key,
      loaded_at
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY source, original_url
          ORDER BY collected_at DESC, loaded_at DESC
        ) AS rn
      FROM `lead-insight-platform.lead_platform.stg_job_postings`
    )
    WHERE rn = 1
    """
    run_query(query)


def build_mart_job_postings_daily():
    query = """
    CREATE OR REPLACE VIEW `lead-insight-platform.lead_platform.mart_job_postings_daily` AS
    SELECT
      DATE(collected_at) AS collected_date,
      source,
      COUNT(*) AS posting_count
    FROM `lead-insight-platform.lead_platform.int_job_postings_clean`
    GROUP BY 1, 2
    ORDER BY collected_date DESC, source
    """
    run_query(query)


def build_mart_job_postings_source_quality():
    query = """
    CREATE OR REPLACE VIEW `lead-insight-platform.lead_platform.mart_job_postings_source_quality` AS
    SELECT
      source,
      COUNT(*) AS total_postings,
      COUNTIF(description_text IS NOT NULL AND description_text != '') AS description_filled_count,
      COUNTIF(content_hash IS NOT NULL AND content_hash != '') AS content_hash_filled_count,
      COUNTIF(title IS NULL OR title = '') AS missing_title_count,
      COUNTIF(company_name IS NULL OR company_name = '') AS missing_company_count
    FROM `lead-insight-platform.lead_platform.int_job_postings_clean`
    GROUP BY source
    ORDER BY total_postings DESC
    """
    run_query(query)


default_args = {
    "owner": "you",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="build_job_postings_marts",
    start_date=datetime(2026, 3, 20),
    schedule="10 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["job-postings", "bigquery", "mart"],
) as dag:

    build_int_task = PythonOperator(
        task_id="build_int_job_postings_clean",
        python_callable=build_int_job_postings_clean,
    )

    build_daily_mart_task = PythonOperator(
        task_id="build_mart_job_postings_daily",
        python_callable=build_mart_job_postings_daily,
    )

    build_quality_mart_task = PythonOperator(
        task_id="build_mart_job_postings_source_quality",
        python_callable=build_mart_job_postings_source_quality,
    )

    build_int_task >> [build_daily_mart_task, build_quality_mart_task]