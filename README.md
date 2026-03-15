# Lead Insight Platform

## 1. 프로젝트 개요

Lead Insight Platform은 웹 서비스의 사용자 행동 이벤트와 Stripe 결제 이벤트를 수집하여 데이터 레이크(S3)에 저장하고, 데이터 웨어하우스(BigQuery)에서 분석할 수 있도록 구축한 이벤트 기반 데이터 파이프라인 프로젝트입니다.

이 프로젝트의 목표는 단순히 데이터를 적재하는 것을 넘어 **데이터 수집 → 저장 → 검증 → 모니터링 → 장애 복구**까지 포함한 실제 데이터 플랫폼의 전체 흐름을 직접 설계하고 구현하는 것이었습니다.

현재 파이프라인은 다음 두 가지 이벤트 소스를 처리합니다.

- Web 사용자 행동 이벤트
- Stripe 결제 이벤트

Kafka 기반 이벤트 큐를 통해 이벤트 수집과 데이터 처리 단계를 분리하고, DLQ와 Replay 메커니즘을 통해 실패 이벤트를 복구할 수 있도록 설계했습니다. 또한 Airflow를 이용해 데이터 품질 검증을 수행하고 Grafana를 통해 파이프라인 상태를 모니터링합니다.

---

# 2. 프로젝트 목표

이 프로젝트는 데이터 엔지니어가 실제 운영 환경에서 고려해야 하는 다음 문제들을 직접 경험해보기 위해 시작되었습니다.

- 이벤트 수집 파이프라인은 안정적으로 동작하는가?
- 데이터 적재 지연이 발생하고 있지는 않은가?
- 이벤트 중복이나 데이터 품질 문제는 없는가?
- 실패 이벤트는 어떻게 복구할 수 있는가?
- 데이터 파이프라인 상태를 어떻게 모니터링할 것인가?

이를 해결하기 위해 다음과 같은 기술을 사용했습니다.

- Kafka 기반 이벤트 수집
- S3 Data Lake
- BigQuery Data Warehouse
- Airflow Data Quality Checks
- Grafana Monitoring
- DLQ + Replay 시스템

---

# 3. 시스템 아키텍처

전체 데이터 파이프라인 구조는 다음과 같습니다.

---

# 4. 사용 기술 (Tech Stack)

### Data Ingestion
- Kafka

### Data Storage
- Amazon S3 (Data Lake)
- BigQuery (Data Warehouse)

### Processing
- Python Consumers
- Airflow

### Monitoring
- Grafana

### Reliability
- DLQ (Dead Letter Queue)
- Replay Mechanism
- At-least-once processing

---

# 5. 데이터 흐름 (Data Flow)

## 1. 이벤트 발생

웹 애플리케이션과 Stripe 결제 시스템에서 이벤트가 발생합니다.

예시 이벤트

- page_view
- signup
- pricing_view
- invoice_paid

이 이벤트들은 JSON 형태로 생성됩니다.

---

## 2. Kafka 이벤트 큐 적재

생성된 이벤트는 Kafka 토픽으로 전송됩니다.

Kafka는 이벤트 수집 단계와 데이터 처리 단계를 분리하는 역할을 합니다.

이를 통해 다음과 같은 장점을 얻을 수 있습니다.

- 이벤트 처리 속도와 데이터 적재 속도 분리
- consumer 장애 시 이벤트 유실 방지
- 비동기 이벤트 처리 구조

---

## 3. Raw Data Lake 저장

Consumer는 Kafka 이벤트를 읽어 S3 Data Lake에 저장합니다.

저장 구조

raw/web/events/dt=YYYY-MM-DD/hour=HH
raw/stripe/invoices/dt=YYYY-MM-DD/hour=HH

이 구조는 다음과 같은 목적을 가집니다.

- 원본 데이터 보존
- 파티션 기반 분석 최적화
- 데이터 재처리(backfill) 가능

---

## 4. 실패 이벤트 처리 (DLQ)

S3 저장 과정에서 오류가 발생하면 이벤트는 DLQ 토픽으로 전송됩니다.

예시 DLQ 토픽

web.user_events_dlq
stripe.invoice_events_dlq

DLQ 이벤트에는 다음 정보가 기록됩니다.

- error_type
- retry_count
- original payload
- failed_at

---

## 5. Replay 시스템

DLQ에 저장된 이벤트는 replay script를 통해 다시 원본 토픽으로 전송할 수 있습니다.

DLQ Topic
↓
Replay Script
↓
Original Topic

retry_count를 Kafka header에 기록하여 재시도 횟수를 추적합니다.

---

## 6. BigQuery 데이터 적재

S3에 저장된 raw 데이터는 BigQuery에 적재됩니다.

현재 사용 중인 staging 테이블

- `stg_web_user_events`
- `stg_stripe_invoice_events`

이 단계에서는 이벤트 원본을 최대한 유지합니다.

---

# 6. 데이터 모델 (Data Model)

데이터 웨어하우스는 다음 계층 구조로 구성했습니다.

Source Events
│
▼
Staging Tables
│
▼
Fact Tables
│
▼
Mart Tables

---

## Staging Layer

이벤트 원본 데이터를 최대한 그대로 저장합니다.

### stg_web_user_events

| column | description |
|------|-------------|
| event_id | 이벤트 ID |
| event_name | 이벤트 이름 |
| user_id | 사용자 ID |
| occurred_at | 이벤트 발생 시각 |
| properties | JSON properties |
| ingested_at | 데이터 수집 시각 |

---

### stg_stripe_invoice_events

| column | description |
|------|-------------|
| event_id | 이벤트 ID |
| customer_id | 고객 ID |
| invoice_id | 인보이스 ID |
| amount | 결제 금액 |
| occurred_at | 이벤트 발생 시각 |
| properties | JSON properties |
| ingested_at | 데이터 수집 시각 |

---

# 7. 데이터 품질 검증 (Data Quality)

데이터 파이프라인의 신뢰성을 유지하기 위해 다음 검증을 수행합니다.

### 1. Deduplication

Kafka는 at-least-once delivery 특성 때문에 이벤트 중복이 발생할 수 있습니다.

event_id 기반 deduplication을 수행합니다.

```sql
SELECT event_id
FROM `lead-insight-platform.lead_platform.v_web_events_dedup`
GROUP BY event_id
HAVING COUNT(*) > 1

2. Not Null Validation

핵심 컬럼 null 여부를 검증합니다.

SELECT
COUNTIF(event_id IS NULL),
COUNTIF(event_name IS NULL)
FROM `lead-insight-platform.lead_platform.stg_web_user_events`

3. Freshness Check

데이터 적재 지연을 확인합니다.

SELECT
MAX(ingested_at),
TIMESTAMP_DIFF(
CURRENT_TIMESTAMP(),
MAX(ingested_at),
MINUTE
)
FROM `lead-insight-platform.lead_platform.stg_web_user_events`

4. Schema Validation

JSON properties 필드의 핵심 키 존재 여부를 검증합니다.

SELECT *
FROM `lead-insight-platform.lead_platform.stg_stripe_invoice_events`
WHERE JSON_VALUE(properties, '$.stripe_invoice_id') IS NULL


8. Monitoring

Grafana 대시보드를 통해 데이터 파이프라인 상태를 모니터링합니다.

주요 지표
	•	Event Volume
	•	Ingestion Lag
	•	Duplicate Events
	•	Kafka Lag
	•	DLQ Events

⸻

9. Lessons Learned

1. 이벤트 기반 파이프라인에서 중복 데이터 문제

Kafka는 at-least-once delivery 방식이기 때문에 동일 이벤트가 여러 번 처리될 수 있습니다.

이를 해결하기 위해
	•	event_id 기반 deduplication
	•	BigQuery view 기반 중복 제거
	•	Airflow DAG 기반 dedup 검증

을 구현했습니다.

⸻

2. 데이터 파이프라인에서 중요한 것은 “적재”가 아니라 “신뢰성”

초기에는 데이터 적재 자체에 집중했지만 실제 운영에서는 다음 문제가 더 중요했습니다.
	•	데이터 중복
	•	스키마 변경
	•	적재 지연
	•	이벤트 유실

이를 해결하기 위해
	•	Data Quality Checks
	•	DLQ
	•	Replay System

을 설계했습니다.

⸻

3. 데이터 파이프라인 운영에는 모니터링이 필수

Kafka 기반 파이프라인은 다음과 같은 장애가 발생할 수 있습니다.
	•	consumer 장애
	•	Kafka backlog 증가
	•	데이터 적재 지연

이를 위해 Grafana 대시보드를 구축하여 다음 지표를 모니터링했습니다.
	•	이벤트 발생량
	•	ingestion lag
	•	duplicate events
	•	Kafka lag
	•	DLQ events

⸻

4. 데이터 모델 계층 설계의 중요성

데이터 분석을 위해 다음 계층 구조를 사용했습니다.
	•	staging layer
	•	fact layer
	•	mart layer

이를 통해 데이터 처리 단계와 분석 단계를 명확히 분리했습니다.