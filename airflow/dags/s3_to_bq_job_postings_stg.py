from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import List

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator  # type: ignore
from google.cloud import bigquery


def list_s3_keys(bucket: str, prefix: str) -> List[str]:
    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "ap-northeast-2"))
    keys: List[str] = []
    token = None

    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token

        resp = s3.list_objects_v2(**kwargs)

        for obj in resp.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                keys.append(key)

        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break

    return keys


def download_json(bucket: str, key: str) -> dict:
    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "ap-northeast-2"))
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")
    return json.loads(body)


def ensure_bq_table(project_id: str, dataset: str, table: str):
    client = bigquery.Client(project=project_id)
    ds_ref = bigquery.DatasetReference(project_id, dataset)
    table_ref = ds_ref.table(table)

    schema = [
        bigquery.SchemaField("posting_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("original_url", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("company_name", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("location", "STRING"),
        bigquery.SchemaField("employment_type", "STRING"),
        bigquery.SchemaField("experience_level", "STRING"),
        bigquery.SchemaField("description_text", "STRING"),
        bigquery.SchemaField("skills", "STRING"),
        bigquery.SchemaField("collected_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("content_hash", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("raw_s3_key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("processed_s3_key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("curated_s3_key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
    ]

    try:
        client.get_table(table_ref)
        return
    except Exception:
        pass

    table_obj = bigquery.Table(table_ref, schema=schema)
    table_obj.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="loaded_at",
    )
    client.create_table(table_obj, exists_ok=True)


def ensure_checkpoint_table(project_id: str, dataset: str, table: str):
    client = bigquery.Client(project=project_id)
    ds_ref = bigquery.DatasetReference(project_id, dataset)
    table_ref = ds_ref.table(table)

    schema = [
        bigquery.SchemaField("s3_key", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP", mode="REQUIRED"),
    ]

    try:
        client.get_table(table_ref)
        return
    except Exception:
        pass

    table_obj = bigquery.Table(table_ref, schema=schema)
    client.create_table(table_obj, exists_ok=True)


def already_loaded_keys(project_id: str, dataset: str, checkpoint_table: str) -> set[str]:
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT s3_key
    FROM `{project_id}.{dataset}.{checkpoint_table}`
    """
    try:
        rows = client.query(query).result()
        return {r["s3_key"] for r in rows}
    except Exception:
        return set()


def append_rows_to_bq(project_id: str, dataset: str, table: str, rows: List[dict]):
    if not rows:
        return

    client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{table}"
    errors = client.insert_rows_json(table_id, rows)

    if errors:
        raise RuntimeError(f"BigQuery insert errors: {errors}")


def load_s3_to_bq(**context):
    project_id = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ.get("BQ_DATASET", "lead_platform")

    bucket = os.environ["S3_BUCKET"]
    prefix = os.environ.get("S3_PREFIX_JOB_POSTINGS", "curated/job_postings")

    target_table = os.environ.get("BQ_STG_JOB_POSTINGS_TABLE", "stg_job_postings")
    checkpoint_table = "etl_loaded_s3_keys_job_postings"

    ensure_bq_table(project_id, dataset, target_table)
    ensure_checkpoint_table(project_id, dataset, checkpoint_table)

    loaded = already_loaded_keys(project_id, dataset, checkpoint_table)
    keys = list_s3_keys(bucket, prefix)

    new_keys = [k for k in keys if k not in loaded]
    if not new_keys:
        print("[dag] no new curated job posting files to load")
        return

    now = datetime.now(timezone.utc).isoformat()

    bq_rows = []
    for k in sorted(new_keys):
        doc = download_json(bucket, k)

        bq_rows.append(
            {
                "posting_id": doc.get("posting_id"),
                "source": doc.get("source"),
                "original_url": doc.get("original_url"),
                "company_name": doc.get("company_name"),
                "title": doc.get("title"),
                "location": doc.get("location"),
                "employment_type": doc.get("employment_type"),
                "experience_level": doc.get("experience_level"),
                "description_text": doc.get("description_text"),
                "skills": doc.get("skills"),
                "collected_at": doc.get("collected_at"),
                "content_hash": doc.get("content_hash"),
                "raw_s3_key": doc.get("raw_s3_key"),
                "processed_s3_key": doc.get("processed_s3_key"),
                "curated_s3_key": doc.get("curated_s3_key"),
                "loaded_at": now,
            }
        )

    append_rows_to_bq(project_id, dataset, target_table, bq_rows)

    checkpoint_rows = [{"s3_key": k, "loaded_at": now} for k in sorted(new_keys)]
    append_rows_to_bq(project_id, dataset, checkpoint_table, checkpoint_rows)

    print(f"[dag] loaded files={len(new_keys)}, rows={len(bq_rows)}")


default_args = {
    "owner": "you",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="s3_to_bq_job_postings_stg",
    start_date=datetime(2026, 3, 20),
    schedule="0 * * * *",  # 매시간
    catchup=False,
    default_args=default_args,
    tags=["job-postings", "s3", "bigquery", "staging"],
) as dag:
    load_task = PythonOperator(
        task_id="load_s3_curated_job_postings_to_bq_staging",
        python_callable=load_s3_to_bq,
    )

    load_task