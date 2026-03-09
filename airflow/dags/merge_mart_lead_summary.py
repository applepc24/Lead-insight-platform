from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator  # type: ignore

MERGE_SQL = """
MERGE `{{ params.project_id }}.{{ params.dataset }}.mart_lead_summary` T
USING (
  WITH stripe_agg AS (
    SELECT
      customer_key,
      SUM(
        CASE
          WHEN DATE(occurred_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY) AND CURRENT_DATE()
          THEN amount ELSE 0
        END
      ) AS payment_total_7d,
      COUNTIF(
        DATE(occurred_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY) AND CURRENT_DATE()
      ) AS payment_count_7d
    FROM `{{ params.project_id }}.{{ params.dataset }}.v_stripe_invoice_events_dedup`
    WHERE customer_key IS NOT NULL
    GROUP BY customer_key
  ),
  web_agg AS (
    SELECT
      customer_key,
      COUNTIF(
        event_name = 'pricing_view'
        AND DATE(occurred_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY) AND CURRENT_DATE()
      ) AS pricing_views_7d,
      COUNTIF(
        event_name = 'docs_view'
        AND DATE(occurred_at) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 6 DAY) AND CURRENT_DATE()
      ) AS docs_views_7d
    FROM `{{ params.project_id }}.{{ params.dataset }}.v_web_events_dedup`
    WHERE customer_key IS NOT NULL
    GROUP BY customer_key
  ),
  merged AS (
    SELECT
      COALESCE(s.customer_key, w.customer_key) AS customer_key,
      COALESCE(s.payment_total_7d, 0) AS payment_total_7d,
      COALESCE(s.payment_count_7d, 0) AS payment_count_7d,
      COALESCE(w.pricing_views_7d, 0) AS pricing_views_7d,
      COALESCE(w.docs_views_7d, 0) AS docs_views_7d
    FROM stripe_agg s
    FULL OUTER JOIN web_agg w
      ON s.customer_key = w.customer_key
  )
  SELECT
    customer_key,
    CAST(payment_total_7d AS NUMERIC) AS payment_total_7d,
    CAST(pricing_views_7d AS INT64) AS pricing_views_7d,
    CAST(docs_views_7d AS INT64) AS docs_views_7d,
    CAST(payment_count_7d AS INT64) AS payment_count_7d,
    CAST(
      pricing_views_7d * 5
      + docs_views_7d * 2
      + payment_count_7d * 10
      AS INT64
    ) AS lead_score,
    CASE
      WHEN (pricing_views_7d * 5 + docs_views_7d * 2 + payment_count_7d * 10) >= 70 THEN 'HOT'
      WHEN (pricing_views_7d * 5 + docs_views_7d * 2 + payment_count_7d * 10) >= 30 THEN 'WARM'
      ELSE 'COLD'
    END AS lead_grade,
    CURRENT_DATE() AS as_of_dt,
    CURRENT_TIMESTAMP() AS updated_at
  FROM merged
) S
ON T.customer_key = S.customer_key
AND T.as_of_dt = S.as_of_dt
WHEN MATCHED THEN
  UPDATE SET
    payment_total_7d = S.payment_total_7d,
    pricing_views_7d = S.pricing_views_7d,
    docs_views_7d = S.docs_views_7d,
    payment_count_7d = S.payment_count_7d,
    lead_score = S.lead_score,
    lead_grade = S.lead_grade,
    updated_at = S.updated_at
WHEN NOT MATCHED THEN
  INSERT (
    customer_key,
    payment_total_7d,
    pricing_views_7d,
    docs_views_7d,
    payment_count_7d,
    lead_score,
    lead_grade,
    as_of_dt,
    updated_at
  )
  VALUES (
    S.customer_key,
    S.payment_total_7d,
    S.pricing_views_7d,
    S.docs_views_7d,
    S.payment_count_7d,
    S.lead_score,
    S.lead_grade,
    S.as_of_dt,
    S.updated_at
  );
"""

default_args = {
    "owner": "you",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="merge_mart_lead_summary",
    start_date=datetime(2026, 3, 6),
    schedule=None,   # 수동 테스트용
    catchup=False,
    default_args=default_args,
    tags=["lead-platform", "mart", "bigquery"],
) as dag:

    merge_task = BigQueryInsertJobOperator(
        task_id="merge_mart_lead_summary",
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

    merge_task