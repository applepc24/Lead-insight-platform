-- =========================================================
-- Manual Data Quality Validation Queries
-- =========================================================
-- Purpose:
-- These queries are manually executable validation checks
-- for the current BigQuery pipeline state.
--
-- They are not yet automated monitoring jobs.
-- They are used to validate whether the loaded data
-- satisfies expected quality rules.
-- =========================================================


-- =========================================================
-- 1) UNIQUENESS
-- 목적:
-- dedup 레이어에서 event_id 중복이 없어야 한다.
-- 기대값:
-- duplicate_event_id_count = 0
-- =========================================================

-- Stripe dedup uniqueness
SELECT
  'stripe_dedup_event_id_uniqueness' AS check_name,
  COUNT(*) AS duplicate_event_id_count
FROM (
  SELECT event_id
  FROM `lead-insight-platform.lead_platform.v_stripe_invoice_events_dedup`
  GROUP BY event_id
  HAVING COUNT(*) > 1
);

-- Web dedup uniqueness
SELECT
  'web_dedup_event_id_uniqueness' AS check_name,
  COUNT(*) AS duplicate_event_id_count
FROM (
  SELECT event_id
  FROM `lead-insight-platform.lead_platform.v_web_events_dedup`
  GROUP BY event_id
  HAVING COUNT(*) > 1
);


-- =========================================================
-- 2) ROW COUNT
-- 목적:
-- standardized/source staging 테이블이 비어 있지 않은지 확인한다.
-- 기대값:
-- row_count > 0
-- =========================================================

SELECT
  'stg_stripe_invoice_events_row_count' AS check_name,
  COUNT(*) AS row_count
FROM `lead-insight-platform.lead_platform.stg_stripe_invoice_events`;

SELECT
  'stg_web_user_events_row_count' AS check_name,
  COUNT(*) AS row_count
FROM `lead-insight-platform.lead_platform.stg_web_user_events`;


-- =========================================================
-- 3) NOT NULL
-- 목적:
-- source staging 레이어에서 핵심 필드가 비어 있으면 안 된다.
-- 기대값:
-- 각 null count = 0
-- =========================================================

SELECT
  'stg_stripe_invoice_events_not_null_core_fields' AS check_name,
  COUNTIF(event_id IS NULL) AS null_event_id_count,
  COUNTIF(event_name IS NULL) AS null_event_name_count,
  COUNTIF(event_source IS NULL) AS null_event_source_count,
  COUNTIF(occurred_at IS NULL) AS null_occurred_at_count
FROM `lead-insight-platform.lead_platform.stg_stripe_invoice_events`;

SELECT
  'stg_web_user_events_not_null_core_fields' AS check_name,
  COUNTIF(event_id IS NULL) AS null_event_id_count,
  COUNTIF(event_name IS NULL) AS null_event_name_count,
  COUNTIF(event_source IS NULL) AS null_event_source_count,
  COUNTIF(occurred_at IS NULL) AS null_occurred_at_count
FROM `lead-insight-platform.lead_platform.stg_web_user_events`;


-- =========================================================
-- 4) FRESHNESS
-- 목적:
-- 마지막 적재/업데이트 시각이 언제인지 수동 점검한다.
-- 현재는 항상 켜진 운영 모니터링이 아니라,
-- 수동 실행 기준의 freshness 확인이다.
-- =========================================================

SELECT
  'stg_stripe_invoice_events_freshness' AS check_name,
  MAX(ingested_at) AS last_ingested_at,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(ingested_at), HOUR) AS freshness_lag_hours
FROM `lead-insight-platform.lead_platform.stg_stripe_invoice_events`;

SELECT
  'stg_web_user_events_freshness' AS check_name,
  MAX(ingested_at) AS last_ingested_at,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(ingested_at), HOUR) AS freshness_lag_hours
FROM `lead-insight-platform.lead_platform.stg_web_user_events`;

SELECT
  'mart_lead_summary_freshness' AS check_name,
  MAX(updated_at) AS last_updated_at,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(updated_at), HOUR) AS freshness_lag_hours
FROM `lead-insight-platform.lead_platform.mart_lead_summary`;


-- =========================================================
-- 5) SCHEMA DRIFT (minimal version)
-- 목적:
-- properties 안의 핵심 키가 비어 있는지 최소 수준으로 점검한다.
-- 완전한 drift detection은 아니고, 핵심 필드 존재 여부 확인이다.
-- =========================================================

SELECT
  'stripe_properties_required_keys' AS check_name,
  COUNT(*) AS missing_required_key_rows
FROM `lead-insight-platform.lead_platform.stg_stripe_invoice_events`
WHERE event_source = 'stripe'
  AND event_name = 'invoice_paid'
  AND (
    JSON_VALUE(SAFE.PARSE_JSON(properties_json), '$.stripe_invoice_id') IS NULL
    OR JSON_VALUE(SAFE.PARSE_JSON(properties_json), '$.stripe_customer_id') IS NULL
  );

SELECT
  'web_properties_basic_shape' AS check_name,
  COUNT(*) AS missing_properties_rows
FROM `lead-insight-platform.lead_platform.stg_web_user_events`
WHERE properties_json IS NULL;