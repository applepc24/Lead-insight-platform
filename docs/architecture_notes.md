# Architecture Notes

## Goal
Stripe / Web / Job Postings 이벤트를 Kafka로 수집하고,  
S3 raw 저장 → BigQuery 적재 → 정제/집계 → 운영 모니터링까지 이어지는 데이터 플랫폼 MVP를 구성한다.

이 플랫폼의 목표는 단순 적재가 아니라 다음까지 포함하는 것이다.

- 비동기 이벤트 수집
- 원본 데이터 보존
- 정제 및 중복 제거
- mart 레이어 제공
- 실패 이벤트 격리 및 재처리
- 운영 관측 및 품질 모니터링

---

## Data Layers

### 1. Raw
외부/서비스 이벤트 원본을 가공 없이 저장하는 레이어다.

역할:
- 원본 보존
- 장애 복구 기준점
- 재처리 입력 소스
- 적재 추적 및 디버깅 근거

예시:
- `raw/stripe/invoices/...`
- `raw/web/events/...`
- `raw/job_postings/...`

---

### 2. Standardized / Staging
소스별로 다른 구조를 가진 데이터를 분석 가능한 테이블 형태로 적재하는 레이어다.

역할:
- 소스별 이벤트/문서를 BigQuery에서 조회 가능한 구조로 적재
- downstream 정제/품질 체크의 입력
- source-specific schema 유지

현재는 소스별 staging을 유지한다.

예시:
- `stg_stripe_invoice_events`
- `stg_web_user_events`
- `stg_job_postings`

---

### 3. Curated / Intermediate
중복 제거, 품질 보정, 최신 기준 선택 등을 통해 신뢰 가능한 데이터만 남기는 레이어다.

역할:
- 중복 제거
- 최신성 기준 반영
- downstream mart의 직접 입력
- 최종 분석용 정제 데이터 제공

예시:
- `v_stripe_invoice_events_dedup`
- `v_web_events_dedup`
- `int_job_postings_clean`

---

### 4. Mart
비즈니스/운영/분석에서 바로 사용할 수 있도록 요약·집계한 레이어다.

역할:
- 고객/운영 지표 제공
- 리드 스코어 계산
- 소스별 일별 집계
- 품질 요약 지표 제공

예시:
- `dim_customer`
- `fct_customer_daily`
- `mart_lead_summary`
- `mart_job_postings_daily`
- `mart_job_postings_source_quality`

---

### 5. Operational / ETL Control
적재 이력, 체크포인트, 재처리 범위, 실패 복구를 지원하는 운영 보조 레이어다.

역할:
- 적재된 S3 key 관리
- 중복 적재 방지 보조
- DLQ / replay 운영
- 장애 복구 지원

예시:
- `etl_loaded_s3_keys`
- `etl_loaded_s3_keys_web`
- `dlq_events`

---

## Current Focus

현재 플랫폼은 세 가지 흐름을 중심으로 구성되어 있다.

### 1. Stripe 이벤트 파이프라인
- Kafka 수집
- S3 raw 저장
- BigQuery staging / curated / mart 구성

### 2. Web 이벤트 파이프라인
- Kafka 수집
- S3 raw 저장
- BigQuery staging / curated / mart 구성

### 3. Job Postings 파이프라인
- Kafka fetch job 수집
- Worker 기반 HTML fetch
- S3 raw / processed / curated 저장
- BigQuery staging / intermediate / mart 구성
- DLQ / replay / retry 정책 운영

---

## Current Assets by Layer

### Raw
- `raw/stripe/invoices/...`
- `raw/web/events/...`
- `raw/job_postings/...`

### Standardized / Staging
- `stg_stripe_invoice_events`
- `stg_web_user_events`
- `stg_events`
- `stg_job_postings`

### Curated / Intermediate
- `v_stripe_invoice_events_dedup`
- `v_web_events_dedup`
- `int_job_postings_clean`

### Mart
- `dim_customer`
- `fct_customer_daily`
- `mart_lead_summary`
- `mart_job_postings_daily`
- `mart_job_postings_source_quality`

### Operational / ETL Control
- `etl_loaded_s3_keys`
- `etl_loaded_s3_keys_web`
- `dlq_events`

---

## Layer Responsibility Summary

### Raw
외부 소스에서 받은 원본 데이터를 변경 없이 저장한다.

### Standardized / Staging
소스별 이벤트/문서를 쿼리 가능한 구조로 적재한다.

### Curated / Intermediate
중복 제거 및 품질 보정을 거쳐 신뢰 가능한 데이터만 남긴다.

### Mart
운영/비즈니스/분석에서 바로 사용할 수 있도록 요약·집계한다.

### Operational / ETL Control
적재 이력, 체크포인트, DLQ, replay 등 운영 제어를 담당한다.

---

## Event Standardization Strategy

현재 Stripe / Web 이벤트 파이프라인은 source-specific staging을 유지하는 구조를 사용한다.

- `stg_stripe_invoice_events`
- `stg_web_user_events`

원래는 공통 이벤트 테이블(`stg_events`) 중심의 완전한 통합 레이어를 고려했지만,  
현재 단계에서는 소스 수가 적고 이벤트 의미가 상이하므로 source-specific staging을 우선 유지한다.

즉, 현재 전략은 다음과 같다.

- 적재는 source-specific staging으로 수행
- dedup 및 집계는 curated / fact / mart 레이어에서 수행
- 필요 시 `stg_events` 를 공통 표준 이벤트 레이어로 확장 가능하도록 여지를 남김

---

### Unified standardized layer (planned / optional)
공통 이벤트 구조로 정규화할 경우 사용할 수 있는 통합 레이어다.

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

이 레이어의 목적:
- 서로 다른 SaaS 이벤트 구조를 하나의 이벤트 모델로 통합
- downstream dedup / aggregation의 공통 입력 제공
- 새로운 소스 확장 비용 절감

현재 MVP에서는 부분적으로만 고려하고 있으며, 완전한 중심 레이어로 사용하지는 않는다.

---

## Job Postings Dedup / Idempotency Strategy

Job Postings 파이프라인은 단일 지점이 아니라 여러 단계에서 중복 방지와 정제를 수행한다.

### 1. Worker-level idempotency
Worker는 `curated_s3_key` 존재 여부를 확인하여 동일 `job_id` 재처리를 방지한다.

기준:
- `curated/job_postings/dt={dt}/{job_id}.json`

의미:
- 동일 메시지 replay
- 동일 `job_id` 중복 consume

에 대해서는 1차 방어 역할을 한다.

다만 이 단계는 본질적으로 `job_id` 기준 idempotency이며,  
동일 공고가 다른 `job_id` 로 들어오는 경우까지 막지는 않는다.

---

### 2. Content hash generation
Worker는 `description_text` 기준으로 `content_hash` 를 생성한다.

목적:
- 컨텐츠 기반 중복 후보 식별
- 품질 지표 활용
- 향후 dedup 고도화의 기반 제공

현재는 curated 문서에 저장되지만,  
최종 dedup 키로 직접 사용되지는 않는다.

---

### 3. BigQuery intermediate dedup
현재 실질적인 최종 dedup은 `int_job_postings_clean` 에서 수행한다.

기준:
- `source`
- `original_url`

정책:
- 동일 source 내 동일 original_url 에 대해
- `collected_at DESC`, `loaded_at DESC`
- 최신 1건만 유지

즉 현재 Job Postings dedup의 핵심 정책은:

**same source + same original_url → latest row only**

이다.

---

### 4. Mart-level usage
`mart_job_postings_daily`, `mart_job_postings_source_quality` 는  
이미 dedup된 `int_job_postings_clean` 기준으로 계산된다.

따라서 최종 리포트/대시보드에서는 dedup된 결과를 기준으로 지표를 보게 된다.

---

## Dedup Design Interpretation

현재 dedup 전략은 다음처럼 해석할 수 있다.

- Worker: `job_id` 기반 재처리 방지
- Intermediate: `source + original_url` 기준 최신본 유지
- `content_hash`: 현재는 후보 관측 및 품질 점검용
- Mart: dedup 결과 기반 집계

이 구조는 원본 보존과 분석 정제를 분리하는 1차 현실형 설계로 본다.

---

## Failure Handling / Replay Strategy

실패 이벤트는 DLQ로 격리하고, replay를 통해 재처리한다.

### Worker failure handling
실패 시:
- `failed_stage`
- `error_type`
- `error_message`
- `retry_count`
- `failed_at`

를 포함한 DLQ 메시지를 Kafka `job_postings.dlq` 토픽으로 보낸다.

### Replay strategy
Airflow replay DAG가 주기적으로 `job_postings.dlq` 를 읽고,  
조건에 맞는 fetch 실패 건만 원래 fetch topic으로 재주입한다.

재처리 조건:
- `failed_stage == "fetch"`
- `retry_count < MAX_RETRY_COUNT`

skip 조건:
- job payload 없음
- retry limit 초과
- replay unsupported stage

---

## Orchestration vs Observability Separation

Replay 실행 자체는 Airflow에서 담당하고,  
재처리 결과 및 실패 패턴 검증은 `dlq_events` 를 BigQuery로 적재한 뒤 Grafana 대시보드에서 모니터링하도록 분리했다.

이를 통해 다음과 같이 책임을 나눴다.

### Airflow
- orchestration
- replay execution
- batch scheduling
- quality check scheduling

### BigQuery / Grafana
- observability
- failure pattern analysis
- retry_count / failed_stage / error_type monitoring
- 운영 지표 가시화

즉, 실행과 관측의 책임을 분리한 구조다.

---

## Data Quality Check Classification

### Hard Validation Checks
규칙 위반 시 실패로 간주할 수 있는 검증이다.

- Stripe dedup event_id uniqueness
- Web dedup event_id uniqueness
- `stg_events` core field not null
- Stripe required properties key existence
- `int_job_postings_clean` required fields check
- `int_job_postings_clean` duplicate content hash candidate monitoring
- Job postings description fill-rate threshold

### Operational Review Checks
상시 장애 처리보다는 운영 점검 및 관찰 목적에 가까운 검증이다.

- `stg_events` freshness
- `mart_lead_summary` freshness
- web properties basic shape
- job postings source quality trend
- DLQ retry distribution
- fetch failure pattern by error_type

---

## Airflow Quality Check Candidates (Phase 1)

초기 Airflow quality_check 단계에서는 실패 기준이 명확한 검증만 태스크로 올린다.

### 1. Dedup uniqueness check
대상:
- `v_stripe_invoice_events_dedup`
- `v_web_events_dedup`

실패 조건:
- duplicate_event_id_count > 0

### 2. Core not null check
대상:
- `stg_events`

실패 조건:
- `event_id`
- `event_name`
- `event_source`
- `occurred_at`

중 하나라도 null count > 0

### 3. Job postings basic integrity check
대상:
- `int_job_postings_clean`

실패 조건:
- title / original_url / source / collected_at null row 존재

### 4. Job postings quality threshold
대상:
- `mart_job_postings_source_quality`

실패 조건:
- 일정 건수 이상 수집된 source에서 description fill rate가 임계치 미만

---

## Current Architecture Summary

현재 데이터 플랫폼은 다음 원칙 위에 구성되어 있다.

1. 원본은 raw에 보존한다.
2. 정제와 중복 제거는 downstream 레이어에서 수행한다.
3. 실패 이벤트는 DLQ로 격리한다.
4. 재처리는 Airflow replay DAG로 수행한다.
5. 실패 패턴 관측은 BigQuery + Grafana에서 수행한다.
6. orchestration과 observability를 분리한다.

이 구조를 통해 MVP 단계에서도  
수집 → 저장 → 정제 → 집계 → 실패 복구 → 운영 관측까지 이어지는 end-to-end 데이터 플랫폼을 구성한다.