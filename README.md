# Lead Insight Platform

## 1. 프로젝트 개요

Lead Insight Platform은 웹 서비스의 사용자 행동 이벤트와 결제 이벤트를 수집하고, 이를 데이터 웨어하우스(BigQuery)에 적재한 뒤 데이터 품질 검증과 파이프라인 모니터링까지 수행하는 소규모 데이터 수집 플랫폼 프로젝트입니다.

현재 파이프라인은 다음 두 가지 이벤트 소스를 처리합니다.

- Web 사용자 이벤트
- Stripe 결제 이벤트

수집된 이벤트는 Kafka를 통해 비동기적으로 전달되며, Consumer가 이를 BigQuery에 적재합니다. 이후 Airflow를 통해 데이터 품질 검증을 수행하고, Grafana 대시보드를 통해 파이프라인 상태와 주요 지표를 모니터링할 수 있도록 구성했습니다.

이 프로젝트의 목표는 단순한 데이터 적재가 아니라 **데이터 수집 → 저장 → 검증 → 모니터링**까지 포함한 데이터 파이프라인의 전체 흐름을 직접 설계하고 구현해보는 것이었습니다.

---

## 2. 프로젝트를 만든 이유

이 프로젝트는 단순히 ETL 스크립트를 작성하는 수준을 넘어서, 데이터 엔지니어가 실제로 운영 환경에서 고려해야 하는 문제들을 직접 경험해보기 위해 시작했습니다.

특히 다음과 같은 질문에 답할 수 있는 데이터 파이프라인을 만드는 것을 목표로 했습니다.

- 현재 파이프라인이 정상적으로 이벤트를 수집하고 있는가?
- 데이터 적재가 지연되고 있지는 않은가?
- 이벤트 중복이나 누락 같은 데이터 품질 문제는 발생하지 않는가?
- 수집된 이벤트를 기반으로 비즈니스 지표를 도출할 수 있는가?

이를 위해 Kafka 기반 이벤트 수집 구조를 구성하고, BigQuery를 데이터 웨어하우스로 사용했으며, Airflow를 이용해 데이터 품질 검증 작업을 수행하고 Grafana를 통해 파이프라인 상태를 시각화했습니다.

이 과정을 통해 데이터 수집 파이프라인의 설계, 데이터 모델링, 데이터 품질 관리, 그리고 운영 모니터링까지 데이터 엔지니어의 핵심 역할을 경험하는 것을 목표로 했습니다.

## 3. 시스템 아키텍처

이 프로젝트는 이벤트 기반 데이터 수집 파이프라인을 구성하여, 서비스 이벤트를 안정적으로 수집하고 데이터 웨어하우스에 적재한 뒤 데이터 품질 검증과 모니터링까지 수행하는 구조로 설계했습니다.

전체 시스템 구성은 다음과 같습니다.

Web / Stripe
     │
     ▼
  Kafka (Event Queue)
     │
     ▼
  Consumer
     │
     ▼
  BigQuery (Data Warehouse)
     │
     ▼
  Airflow (Data Quality Checks)
     │
     ▼
  Grafana (Monitoring Dashboard)

### 구성 요소 설명

**Web / Stripe 이벤트 소스**

- 웹 애플리케이션에서 발생하는 사용자 행동 이벤트와 Stripe 결제 이벤트를 수집합니다.
- 각 이벤트는 Kafka로 전달되어 비동기적으로 처리됩니다.

**Kafka (이벤트 큐)**

- 이벤트 수집 단계와 데이터 적재 단계를 분리하기 위해 메시지 큐로 Kafka를 사용했습니다.
- 이를 통해 이벤트 수집 속도와 데이터 처리 속도를 분리할 수 있으며, consumer 장애 시에도 이벤트를 안전하게 버퍼링할 수 있습니다.

**Consumer**

- Kafka에서 이벤트를 읽어 BigQuery에 적재하는 역할을 합니다.
- 이벤트는 idempotent 처리를 고려하여 중복 가능성을 허용하는 방식(at-least-once 처리)으로 설계했습니다.

**BigQuery (데이터 웨어하우스)**

- 이벤트 데이터를 저장하고 분석할 수 있도록 BigQuery를 데이터 웨어하우스로 사용했습니다.
- Web 이벤트와 Stripe 이벤트는 각각 staging 테이블에 저장됩니다.

**Airflow (데이터 품질 검증)**

- 적재된 데이터의 기본적인 품질을 검증하기 위해 Airflow DAG를 사용합니다.
- 주요 검증 항목:
  - event_id 중복 여부
  - 핵심 필드 null 여부
  - 데이터 적재 지연 여부

**Grafana (모니터링 대시보드)**

- 데이터 파이프라인 상태를 실시간으로 확인하기 위해 Grafana 대시보드를 구성했습니다.
- 주요 모니터링 지표:
  - 최근 이벤트 수
  - 데이터 ingestion 지연 시간
  - 이벤트 볼륨 추이
  - 데이터 품질 지표


## 4. 데이터 흐름 (Data Flow)

이 파이프라인은 이벤트 수집부터 데이터 웨어하우스 적재, 품질 검증, 모니터링까지 다음과 같은 흐름으로 동작합니다.

### 1. 이벤트 발생

웹 애플리케이션과 결제 시스템(Stripe)에서 사용자 행동 이벤트와 결제 이벤트가 발생합니다.

예를 들어 다음과 같은 이벤트가 생성될 수 있습니다.

- page_view
- signup
- pricing_view
- invoice_paid

이 이벤트들은 JSON 형태로 생성됩니다.

---

### 2. Kafka 이벤트 큐 적재

생성된 이벤트는 Kafka 토픽으로 전송됩니다.

Kafka는 이벤트 수집 단계와 데이터 처리 단계를 분리하는 역할을 합니다.

이를 통해 다음과 같은 장점을 얻을 수 있습니다.

- 이벤트 처리 속도와 데이터 적재 속도를 분리
- consumer 장애 시 이벤트 유실 방지
- 비동기 이벤트 처리 구조 구성

---

### 3. Consumer 데이터 처리

Consumer는 Kafka 토픽에서 이벤트를 읽어 BigQuery로 적재합니다.

이때 파이프라인은 **at-least-once 처리 방식**을 사용합니다.

즉 다음과 같은 전략을 사용합니다.

- 데이터 유실을 방지하기 위해 메시지 재처리를 허용
- 중복 이벤트는 downstream에서 dedup 처리

---

### 4. BigQuery 데이터 저장

Consumer가 처리한 이벤트는 BigQuery에 저장됩니다.

현재 데이터는 다음과 같은 staging 테이블에 적재됩니다.

- `stg_web_user_events`
- `stg_stripe_invoice_events`

이 단계에서는 이벤트 원본을 최대한 유지하는 것을 목표로 합니다.

---

### 5. 데이터 품질 검증

Airflow DAG를 통해 데이터 품질 검증 작업을 수행합니다.

검증 항목 예:

- event_id 중복 여부
- 핵심 필드 null 여부
- 데이터 적재 지연 여부

이를 통해 데이터 웨어하우스에 저장된 이벤트의 기본적인 신뢰성을 확인합니다.

---

### 6. 모니터링

Grafana 대시보드를 통해 파이프라인 상태를 모니터링합니다.

대표적인 모니터링 지표는 다음과 같습니다.

- 최근 이벤트 수
- ingestion 지연 시간
- 이벤트 볼륨 추이
- 중복 이벤트 발생 여부

이를 통해 데이터 파이프라인의 상태를 빠르게 확인할 수 있습니다.

## 5. 데이터 모델 (Data Model)

이 프로젝트에서는 데이터 웨어하우스 구조를 다음과 같은 계층으로 구성했습니다.

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

각 계층은 서로 다른 역할을 가지며 데이터 처리 단계를 분리합니다.

---

### 1. Staging Layer

Staging 테이블은 이벤트 원본 데이터를 최대한 그대로 저장하는 계층입니다.

현재 사용 중인 staging 테이블은 다음과 같습니다.

**stg_web_user_events**

웹 애플리케이션에서 발생하는 사용자 행동 이벤트를 저장합니다.

주요 컬럼:

- event_id
- event_name
- user_id
- occurred_at
- properties
- ingested_at

---

**stg_stripe_invoice_events**

Stripe 결제 시스템에서 발생하는 invoice 이벤트를 저장합니다.

주요 컬럼:

- event_id
- customer_id
- invoice_id
- amount
- occurred_at
- properties
- ingested_at

---

### 2. Fact Layer

Fact 테이블은 분석 가능한 단위로 정제된 이벤트 데이터를 저장하는 계층입니다.

예시:

**fct_customer_daily**

고객의 일별 활동을 집계한 테이블입니다.

예를 들어 다음과 같은 정보가 포함될 수 있습니다.

- customer_id
- activity_date
- web_events
- invoice_events
- total_activity

이 테이블은 사용자 행동 이벤트와 결제 이벤트를 통합하여 고객 단위 활동을 분석할 수 있도록 합니다.

---

### 3. Mart Layer

Mart 테이블은 비즈니스 분석을 위해 최종적으로 가공된 데이터입니다.

예시:

**mart_lead_summary**

리드 점수 기반 고객 분석을 위한 테이블입니다.

예시 컬럼:

- customer_id
- total_activity
- invoice_paid_count
- engagement_score
- lead_grade

이 테이블은 마케팅 또는 세일즈 팀이 고객의 관심도와 구매 가능성을 분석하는 데 활용할 수 있습니다.

## 6. 데이터 품질 검증 (Data Quality)

데이터 파이프라인에서 가장 중요한 문제 중 하나는 **데이터 신뢰성**입니다.

단순히 데이터를 적재하는 것만으로는 충분하지 않으며, 데이터가 다음 조건을 만족하는지 지속적으로 검증해야 합니다.

이 프로젝트에서는 다음과 같은 데이터 품질 검증 전략을 사용합니다.

---

### 1. Deduplication (중복 이벤트 검증)

Kafka 기반 이벤트 파이프라인에서는 동일한 이벤트가 여러 번 처리될 수 있습니다.

이는 다음과 같은 이유로 발생할 수 있습니다.

- consumer 재시작
- 메시지 재처리
- 네트워크 장애

따라서 downstream에서 **event_id 기반 deduplication 검증**을 수행합니다.

검증 예시:

```sql
SELECT event_id
FROM `lead-insight-platform.lead_platform.v_web_events_dedup`
GROUP BY event_id
HAVING COUNT(*) > 1
이 쿼리의 결과는 항상 0이어야 합니다.

### 2. Not Null Validation (핵심 필드 검증)

이벤트 데이터의 핵심 필드가 null이면 분석 데이터의 신뢰성이 크게 떨어집니다.

따라서 다음과 같은 필드를 검증합니다.
	•	event_id
	•	event_name
	•	occurred_at
	•	event_source

SELECT
COUNTIF(event_id IS NULL) AS null_event_id,
COUNTIF(event_name IS NULL) AS null_event_name
FROM `lead-insight-platform.lead_platform.stg_web_user_events`


3. Freshness Check (데이터 적재 지연 확인)

데이터 파이프라인이 정상 동작하고 있는지 확인하기 위해 **데이터 최신성(freshness)**을 모니터링합니다.

예를 들어 다음 쿼리를 통해 최근 적재 시간을 확인할 수 있습니다.

SELECT
MAX(ingested_at) AS last_ingested_at,
TIMESTAMP_DIFF(
  CURRENT_TIMESTAMP(),
  MAX(ingested_at),
  MINUTE
) AS ingestion_lag_minutes
FROM `lead-insight-platform.lead_platform.stg_web_user_events`

이 지표는 Grafana 대시보드에서 지속적으로 모니터링됩니다.

4. Schema Validation

이벤트의 properties 필드는 JSON 형태로 저장됩니다.

따라서 핵심 키가 존재하는지 검증합니다.

SELECT *
FROM `lead-insight-platform.lead_platform.stg_stripe_invoice_events`
WHERE JSON_VALUE(properties, '$.stripe_invoice_id') IS NULL

이를 통해 이벤트 스키마 변경으로 인한 데이터 품질 문제를 조기에 발견할 수 있습니다.


Airflow Quality Check DAG

데이터 품질 검증은 Airflow DAG를 통해 자동 실행됩니다.

검증 항목:
	•	dedup uniqueness
	•	null validation
	•	ingestion freshness

이 DAG는 데이터 파이프라인의 기본적인 데이터 신뢰성을 보장하는 역할을 합니다.


## 7. Monitoring (Grafana)

데이터 파이프라인의 안정성을 유지하기 위해 Grafana 기반 모니터링 대시보드를 구축했습니다.

데이터 파이프라인은 다음과 같은 문제가 발생할 수 있습니다.

- consumer 장애
- 데이터 적재 지연
- 이벤트 폭증
- 중복 데이터 발생

Grafana를 통해 이러한 문제를 실시간으로 확인할 수 있도록 주요 지표를 모니터링합니다.

---

### 1. Event Volume Monitoring

최근 이벤트 발생량을 모니터링합니다.

이 지표는 다음과 같은 문제를 감지하는 데 사용됩니다.

- 이벤트 수집 중단
- 이벤트 폭증
- 특정 서비스 장애

예시 쿼리:

```sql
SELECT
  COUNT(*) AS web_events_last_10min
FROM `lead-insight-platform.lead_platform.stg_web_user_events`
WHERE occurred_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)


2. Ingestion Freshness

데이터 적재 지연을 확인합니다.

consumer가 멈추면 ingestion lag가 빠르게 증가합니다.

SELECT
  TIMESTAMP_DIFF(
    CURRENT_TIMESTAMP(),
    MAX(ingested_at),
    MINUTE
  ) AS web_ingestion_lag_minutes
FROM `lead-insight-platform.lead_platform.stg_web_user_events`


3. Duplicate Event Detection

이벤트 중복 발생 여부를 확인합니다.

Kafka 기반 파이프라인에서는 at-least-once 처리로 인해 중복 이벤트가 발생할 수 있습니다.

SELECT
COUNT(*) AS duplicate_event_ids
FROM (
  SELECT event_id
  FROM `lead-insight-platform.lead_platform.v_web_events_dedup`
  GROUP BY event_id
  HAVING COUNT(*) > 1
)

4. Kafka Lag Monitoring

Kafka consumer가 정상적으로 이벤트를 처리하고 있는지 확인합니다.

Kafka lag는 다음을 의미합니다.

Kafka Lag = Latest Offset - Consumer Offset

Lag가 지속적으로 증가하면 consumer 처리 속도가 이벤트 유입 속도를 따라가지 못하는 상황입니다.


5. Pipeline Health Overview

Grafana 대시보드를 통해 다음 지표를 한눈에 확인할 수 있습니다.
	•	최근 이벤트 수
	•	ingestion lag
	•	duplicate events
	•	Kafka lag
	•	데이터 적재 상태

이를 통해 데이터 파이프라인의 상태를 빠르게 파악할 수 있습니다.

8. Lessons Learned

이 프로젝트를 진행하면서 단순히 데이터 파이프라인을 구축하는 것뿐만 아니라,
실제 데이터 플랫폼 운영에서 발생할 수 있는 여러 문제를 경험하고 해결하는 과정을 겪었습니다.

1. 이벤트 기반 파이프라인에서 중복 데이터 문제

Kafka 기반 이벤트 시스템에서는 동일 이벤트가 여러 번 처리될 수 있습니다.
이는 consumer 재시작, 네트워크 장애, 메시지 재전송 등의 상황에서 발생합니다.

처음에는 이벤트가 한 번만 처리된다고 가정했지만, 실제로는 at-least-once delivery 특성 때문에 중복 이벤트가 발생할 수 있음을 확인했습니다.

이를 해결하기 위해 다음 전략을 적용했습니다.
	•	event_id 기반 deduplication
	•	BigQuery view 기반 중복 제거
	•	Airflow DAG를 통한 dedup 검증

이를 통해 이벤트 재처리가 발생하더라도 데이터 분석 결과가 왜곡되지 않도록 설계했습니다.

⸻

2. 데이터 파이프라인에서 가장 중요한 것은 “적재”가 아니라 “신뢰성”

초기에는 Kafka → BigQuery 적재 자체에 집중했지만,
실제 데이터 플랫폼에서는 **데이터 품질 관리(Data Quality)**가 더 중요하다는 것을 깨달았습니다.

예를 들어 다음과 같은 문제가 발생할 수 있습니다.
	•	핵심 컬럼 NULL
	•	이벤트 스키마 변경
	•	데이터 적재 지연
	•	중복 이벤트 발생

이 문제를 해결하기 위해 다음 품질 검증을 추가했습니다.
	•	dedup uniqueness check
	•	핵심 필드 not null 검증
	•	ingestion freshness 모니터링
	•	schema validation

이를 통해 데이터 웨어하우스에 저장되는 데이터의 기본적인 신뢰성을 보장했습니다.

⸻

3. 데이터 파이프라인 운영에는 모니터링이 필수

데이터 파이프라인은 정상적으로 동작하다가도 다음과 같은 이유로 쉽게 장애가 발생할 수 있습니다.
	•	consumer 장애
	•	Kafka backlog 증가
	•	BigQuery 적재 지연
	•	이벤트 수집 중단

따라서 Grafana 대시보드를 구축하여 다음 지표를 지속적으로 모니터링했습니다.
	•	이벤트 발생량
	•	ingestion lag
	•	duplicate events
	•	Kafka lag

이러한 모니터링을 통해 데이터 파이프라인의 이상 상태를 빠르게 감지할 수 있도록 했습니다.

⸻

4. 데이터 모델 설계의 중요성

초기에는 이벤트 데이터를 단순히 저장하는 구조를 생각했지만,
데이터 분석을 고려하면 데이터 모델 계층 분리가 필요하다는 것을 깨달았습니다.

이 프로젝트에서는 다음과 같은 계층 구조를 사용했습니다.
	•	staging layer
	•	fact layer
	•	mart layer

이 구조를 통해 데이터 처리 단계를 명확히 분리하고 분석에 적합한 데이터 구조를 설계할 수 있었습니다.