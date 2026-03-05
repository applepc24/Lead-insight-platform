from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import List

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
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
            if key.endswith(".jsonl"):
                keys.append(key)
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys


def download_jsonl(bucket: str, key: str) -> List[dict]:
    s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "ap-northeast-2"))
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8").splitlines()
    rows = []
    for line in body:
        line = line.strip()
        if not line:
            continue
        rows.append(json.loads(line))
    return rows


def ensure_bq_table_web(project_id: str, dataset: str, table: str):
    client = bigquery.Client(project=project_id)
    ds_ref = bigquery.DatasetReference(project_id, dataset)
    table_ref = ds_ref.table(table)

    schema = [
        bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_source", "STRING"),
        bigquery.SchemaField("event_name", "STRING"),
        bigquery.SchemaField("customer_key", "STRING"),
        bigquery.SchemaField("occurred_at", "TIMESTAMP"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        bigquery.SchemaField("properties_json", "STRING"),
        bigquery.SchemaField("s3_key", "STRING"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP"),  # BigQuery에 insert된 시각(관측용)
    ]

    try:
        client.get_table(table_ref)
        return
    except Exception:
        pass

    table_obj = bigquery.Table(table_ref, schema=schema)
    table_obj.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="ingested_at",  # ✅ web는 ingested_at로 파티셔닝 (늦게 도착해도 오늘 적재로 묶임)
    )
    client.create_table(table_obj)


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
    client.create_table(table_obj)


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


def load_s3_web_to_bq(**context):
    project_id = os.environ["GCP_PROJECT_ID"]
    dataset = os.environ.get("BQ_DATASET", "lead_platform")

    bucket = os.environ["S3_BUCKET"]
    prefix = os.environ.get("S3_PREFIX_WEB", "raw/web/events")

    target_table = os.environ.get("BQ_WEB_STG_TABLE", "stg_web_user_events")
    checkpoint_table = os.environ.get("BQ_WEB_CHECKPOINT_TABLE", "etl_loaded_s3_keys_web")

    ensure_bq_table_web(project_id, dataset, target_table)
    ensure_checkpoint_table(project_id, dataset, checkpoint_table)

    loaded = already_loaded_keys(project_id, dataset, checkpoint_table)
    keys = list_s3_keys(bucket, prefix)
    new_keys = [k for k in keys if k not in loaded]

    if not new_keys:
        print("[dag-web] no new s3 keys to load")
        return

    loaded_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    bq_rows: List[dict] = []
    for k in sorted(new_keys):
        events = download_jsonl(bucket, k)

        for e in events:
            # ✅ web 이벤트만
            if e.get("event_source") != "web":
                continue

            event_id = e.get("event_id") or k  # 개발용 fallback
            occurred_at = e.get("occurred_at")  # 있으면 그대로
            ingested_at = e.get("ingested_at") or loaded_at  # ✅ consumer가 넣어준 값 (없으면 null로 들어감)

            bq_rows.append({
                "event_id": event_id,
                "event_source": e.get("event_source"),
                "event_name": e.get("event_name"),
                "customer_key": e.get("customer_key"),
                "occurred_at": occurred_at,
                "ingested_at": ingested_at,
                "properties_json": json.dumps(e.get("properties", {}), ensure_ascii=False),
                "s3_key": k,
                "loaded_at": loaded_at,
            })

        # 파일 단위 checkpoint
        append_rows_to_bq(project_id, dataset, checkpoint_table, [{
            "s3_key": k,
            "loaded_at": loaded_at,
        }])

    append_rows_to_bq(project_id, dataset, target_table, bq_rows)
    print(f"[dag-web] loaded files={len(new_keys)}, rows={len(bq_rows)}")


default_args = {
    "owner": "you",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="s3_to_bq_web_user_events",
    start_date=datetime(2026, 3, 5),
    schedule="0 */6 * * *",  # 하루 4회
    catchup=False,
    default_args=default_args,
    tags=["lead-platform", "web", "s3", "bigquery"],
) as dag:
    load_task = PythonOperator(
        task_id="load_s3_web_raw_to_bq_staging",
        python_callable=load_s3_web_to_bq,
    )

    load_task