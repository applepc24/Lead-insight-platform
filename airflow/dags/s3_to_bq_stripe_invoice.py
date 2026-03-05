from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from typing import List

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
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


def ensure_bq_table(project_id: str, dataset: str, table: str):
    client = bigquery.Client(project=project_id)
    ds_ref = bigquery.DatasetReference(project_id, dataset)
    table_ref = ds_ref.table(table)

    schema = [
        bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("event_source", "STRING"),
        bigquery.SchemaField("event_name", "STRING"),
        bigquery.SchemaField("customer_key", "STRING"),
        bigquery.SchemaField("occurred_at", "TIMESTAMP"),
        bigquery.SchemaField("amount", "FLOAT"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("properties_json", "STRING"),
        bigquery.SchemaField("s3_key", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]

    try:
        client.get_table(table_ref)
        return
    except Exception:
        pass

    table_obj = bigquery.Table(table_ref, schema=schema)
    table_obj.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="ingested_at",
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
        # checkpoint 테이블이 아직 없으면 빈 세트
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
    prefix = os.environ.get("S3_PREFIX", "raw/stripe/invoices")

    target_table = os.environ.get("BQ_STG_TABLE", "stg_stripe_invoice_events")
    checkpoint_table = os.environ.get("BQ_CHECKPOINT_TABLE", "etl_loaded_s3_keys")

    # 테이블 보장
    ensure_bq_table(project_id, dataset, target_table)
    ensure_checkpoint_table(project_id, dataset, checkpoint_table)

    loaded = already_loaded_keys(project_id, dataset, checkpoint_table)

    # 최근 것만 보고 싶으면 prefix를 dt=YYYY-MM-DD로 제한할 수도 있음.
    # 지금은 prefix 전체 스캔(개발용)으로 간다.
    keys = list_s3_keys(bucket, prefix)

    # 신규 key만
    new_keys = [k for k in keys if k not in loaded]
    if not new_keys:
        print("[dag] no new s3 keys to load")
        return

    now = datetime.now(timezone.utc).isoformat()

    # 적재 rows 만들기
    bq_rows = []
    for k in sorted(new_keys):
        events = download_jsonl(bucket, k)

        for e in events:
            # Stripe invoice_paid만
            if e.get("event_source") != "stripe" or e.get("event_name") != "invoice_paid":
                continue

            # occurred_at: "2026-03-04T09:32:14Z" -> TIMESTAMP
            occurred_at = e.get("occurred_at")
            # BigQuery insert_rows_json은 ISO8601 string도 TIMESTAMP로 잘 받아줌(UTC 기준)

            bq_rows.append({
                "event_id": e.get("event_id") or e.get("event_id") or e.get("properties", {}).get("stripe_invoice_id") or k,
                "event_source": e.get("event_source"),
                "event_name": e.get("event_name"),
                "customer_key": e.get("customer_key"),
                "occurred_at": occurred_at,
                "amount": e.get("amount"),
                "currency": e.get("currency"),
                "properties_json": json.dumps(e.get("properties", {}), ensure_ascii=False),
                "s3_key": k,
                "ingested_at": now,
            })

        # checkpoint는 “파일 단위”로 찍는다 (한 파일에 이벤트 여러 개여도 1번만)
        append_rows_to_bq(project_id, dataset, checkpoint_table, [{
            "s3_key": k,
            "loaded_at": now,
        }])

    append_rows_to_bq(project_id, dataset, target_table, bq_rows)
    print(f"[dag] loaded files={len(new_keys)}, rows={len(bq_rows)}")

MERGE_SQL = """
MERGE `{{ params.project_id }}.{{ params.dataset }}.mart_lead_summary` T
USING (
  WITH base AS (
    SELECT
      customer_key,
      occurred_at,
      amount
    FROM `{{ params.project_id }}.{{ params.dataset }}.v_stripe_invoice_events_dedup`
    WHERE customer_key IS NOT NULL
  ),
  agg AS (
    SELECT
      customer_key,
      SUM(CASE WHEN DATE(occurred_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY) THEN amount ELSE 0 END) AS payment_total_7d,
      COUNTIF(DATE(occurred_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY)) AS payment_count_7d
    FROM base
    GROUP BY customer_key
  )
  SELECT
    customer_key,
    CAST(payment_total_7d AS NUMERIC) AS payment_total_7d,
    0 AS pricing_views_7d,
    0 AS docs_views_7d,
    CAST(payment_count_7d AS INT64) AS payment_count_7d,
    CAST(payment_count_7d * 10 AS INT64) AS lead_score,
    CASE
      WHEN payment_count_7d * 10 >= 70 THEN "HOT"
      WHEN payment_count_7d * 10 >= 30 THEN "WARM"
      ELSE "COLD"
    END AS lead_grade,
    CURRENT_DATE() AS as_of_dt,
    CURRENT_TIMESTAMP() AS updated_at
  FROM agg
) S
ON T.customer_key = S.customer_key AND T.as_of_dt = S.as_of_dt
WHEN MATCHED THEN UPDATE SET
  payment_total_7d = S.payment_total_7d,
  pricing_views_7d = S.pricing_views_7d,
  docs_views_7d = S.docs_views_7d,
  payment_count_7d = S.payment_count_7d,
  lead_score = S.lead_score,
  lead_grade = S.lead_grade,
  updated_at = S.updated_at
WHEN NOT MATCHED THEN
  INSERT (customer_key, payment_total_7d, pricing_views_7d, docs_views_7d, payment_count_7d, lead_score, lead_grade, as_of_dt, updated_at)
  VALUES (S.customer_key, S.payment_total_7d, S.pricing_views_7d, S.docs_views_7d, S.payment_count_7d, S.lead_score, S.lead_grade, S.as_of_dt, S.updated_at);
"""

default_args = {
    "owner": "you",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="s3_to_bq_stripe_invoice_paid",
    start_date=datetime(2026, 3, 5),
    schedule="0 */6 * * *",  # 하루 4회 (0시,6시,12시,18시)
    catchup=False,
    default_args=default_args,
    tags=["lead-platform", "stripe", "s3", "bigquery"],
) as dag:

    load_task = PythonOperator(
        task_id="load_s3_raw_to_bq_staging",
        python_callable=load_s3_to_bq,
    )

    merge_mart = BigQueryInsertJobOperator(
        task_id='merge_mart_lead_summary',
        configuration={
            "query": {
                "query": MERGE_SQL,
                "useLegacySql": False,
            }
        },
        params={
            "project_id": os.environ["GCP_PROJECT_ID"],
            "dataset": os.environ.get("BQ_DATASET", "lead_platform"),
        },
    )

    load_task >> merge_mart