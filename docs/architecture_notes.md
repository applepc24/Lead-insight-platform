# Architecture Notes

## Goal
Stripe / Web 이벤트를 Kafka로 수집하고, S3 raw → BigQuery standardized → curated → mart 레이어로 처리하는 데이터 플랫폼 MVP를 구성한다.

---

## Data Layers

### 1. Raw
- 외부/서비스 이벤트 원본을 손대지 않고 저장하는 레이어
- 장애 복구, 재처리, 원본 추적의 기준점 역할

### 2. Standardized
- 소스별로 다른 이벤트 구조를 공통 스키마로 맞추는 레이어
- downstream 변환, 품질 체크, dedup의 입력 역할

### 3. Curated
- 중복 제거, 품질 보정 등을 거쳐 신뢰 가능한 이벤트만 남긴 레이어
- mart 계산에 직접 사용되는 정제 레이어

### 4. Mart
- 고객/운영/비즈니스가 바로 사용할 수 있는 요약 레이어
- 리드 스코어, 일별 집계, 고객 차원 데이터를 제공

---

## Current Focus
- 현재는 Stripe / Web 두 소스를 Kafka로 수집하고 있다.
- S3 raw 저장 후 BigQuery에서 standardized / curated / mart 레이어를 구성하고 있다.
- DLQ / replay / retry 정책을 통해 실패 이벤트 복구 구조를 운영 중이다.


---

## Current Assets by Layer

### Raw
- S3 raw files
  - `raw/stripe/invoices/...`
  - `raw/web/events/...`

### Standardized
- `stg_stripe_invoice_events`
- `stg_web_user_events`
- `stg_events`

### Curated
- `v_stripe_invoice_events_dedup`
- `v_web_events_dedup`

### Mart
- `dim_customer`
- `fct_customer_daily`
- `mart_lead_summary`

### Operational / ETL Control
- `etl_loaded_s3_keys`
- `etl_loaded_s3_keys_web`

---

## Layer Responsibility Summary

### Raw
- 외부 소스에서 받은 이벤트 원본을 변경 없이 보관하는 레이어

### Standardized
- 소스별 이벤트를 공통 필드 체계로 정규화하는 레이어

### Curated
- 중복 제거와 품질 보정을 거쳐 신뢰 가능한 이벤트만 남기는 레이어

### Mart
- 고객/비즈니스가 바로 사용할 수 있도록 요약·집계한 레이어

### Operational / ETL Control
- 적재 이력, 체크포인트, 재처리 범위를 관리하는 운영 보조 레이어


---


## Event Standardization Strategy

현재 파이프라인은 소스별 staging 테이블을 유지하는 구조를 사용한다.

- `stg_stripe_invoice_events`
- `stg_web_user_events`

원래는 공통 이벤트 테이블(`stg_events`)로 표준화하는 레이어를 고려했지만,
현재 단계에서는 소스 수가 적고 이벤트 의미가 상이하므로 통합 레이어는 보류했다.

대신 source-specific staging을 유지하고,
필요한 분석 및 집계는 curated/fact 레이어에서 직접 조합한다.

---

### Unified standardized layer

소스별 이벤트를 공통 이벤트 구조로 정규화한다.

공통 테이블:
- `stg_events`

공통 필드 예시:

- `event_source`
- `event_name`
- `event_id`
- `customer_key`
- `occurred_at`
- `amount`
- `properties`
- `ingested_at`

이 레이어의 목적은:

- 서로 다른 SaaS 이벤트 구조를 **하나의 이벤트 모델로 통합**
- downstream 처리 (dedup / aggregation)의 공통 입력 제공
- 새로운 SaaS 소스를 쉽게 추가 가능하게 만드는 것

---

### Curated layer (deduplicated events)

중복 이벤트 제거 및 신뢰 가능한 이벤트만 남긴다.

- `v_stripe_invoice_events_dedup`
- `v_web_events_dedup`

이 레이어는 mart 계산의 직접 입력으로 사용된다.

---

## Data Quality Check Classification

### Hard Validation Checks
다음 항목은 규칙 위반 시 실패로 간주할 수 있는 검증이다.

- Stripe dedup event_id uniqueness
- Web dedup event_id uniqueness
- stg_events core field not null
- Stripe required properties key existence

### Operational Review Checks
다음 항목은 현재 상시 운영 모니터링보다는 수동 점검용 지표에 가깝다.

- stg_events freshness
- mart_lead_summary freshness
- web properties basic shape

---

## Airflow Quality Check Candidates (Phase 1)

초기 Airflow quality_check 단계에서는 실패 기준이 명확한 검증만 태스크로 올린다.

### 1. Dedup uniqueness check
- 대상:
  - `v_stripe_invoice_events_dedup`
  - `v_web_events_dedup`
- 실패 조건:
  - duplicate_event_id_count > 0

### 2. Core not null check
- 대상:
  - `stg_events`
- 실패 조건:
  - event_id / event_name / event_source / occurred_at 중 null count > 0