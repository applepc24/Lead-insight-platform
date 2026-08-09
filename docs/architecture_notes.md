# Architecture Notes

## Goal
여러 채용 사이트의 공고를 Kafka 기반 fetch job으로 수집하고,  
S3 raw 저장 → BigQuery 적재 → 정제/집계 → 운영 모니터링까지 이어지는 데이터 플랫폼 MVP를 구성한다.

이 플랫폼의 목표는 단순 적재가 아니라 다음까지 포함하는 것이다.

- 비동기 수집 요청 처리
- 원본 데이터 보존
- 정제 및 중복 제거
- mart 레이어 제공
- 실패 이벤트 격리 및 재처리
- 운영 관측 및 품질 모니터링

---

## Data Layers

### 1. Raw
수집한 공고 페이지 원본을 가공 없이 저장하는 레이어다.

역할:
- 원본 보존
- 장애 복구 기준점
- 재처리 입력 소스
- 적재 추적 및 디버깅 근거

예시:
- `raw/job_postings/source={source}/dt={dt}/{job_id}.html`

---

### 2. Standardized / Staging
사이트마다 다른 구조의 공고 문서를 분석 가능한 테이블 형태로 적재하는 레이어다.

역할:
- 정제된 공고 문서를 BigQuery에서 조회 가능한 구조로 적재
- downstream 정제/품질 체크의 입력
- 사이트별 차이는 Worker 파싱 단계에서 흡수하고, staging은 단일 스키마를 유지

예시:
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
- `int_job_postings_clean`

---

### 4. Mart
비즈니스/운영/분석에서 바로 사용할 수 있도록 요약·집계한 레이어다.

역할:
- 운영/분석 지표 제공
- 소스별 일별 집계
- 품질 요약 지표 제공

예시:
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
- `etl_loaded_s3_keys_job_postings`
- `dlq_events`

---

## Current Focus

현재 플랫폼은 채용공고 파이프라인 하나를 중심으로 구성되어 있다.

### Job Postings 파이프라인
- Kafka fetch job 수집
- Worker 기반 HTML fetch
- S3 raw / processed / curated 저장
- BigQuery staging / intermediate / mart 구성
- DLQ / replay / retry 정책 운영

---

## Current Assets by Layer

### Raw
- `raw/job_postings/...`

### Standardized / Staging
- `stg_job_postings`

### Curated / Intermediate
- `int_job_postings_clean`

### Mart
- `mart_job_postings_daily`
- `mart_job_postings_source_quality`

### Operational / ETL Control
- `etl_loaded_s3_keys_job_postings`
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

## Source Standardization Strategy

채용 사이트는 렌더링 방식도, 공고 페이지의 메타데이터 구조도 제각각이다.  
이 차이를 **Collector와 Worker 두 단계에서 흡수**하고, 그 아래로는 단일 스키마만 흐르게 한다.

### 1. Collector — 렌더링 방식에 따른 수집 전략

목록 페이지에서 상세 URL을 추출하는 단계로, 사이트 렌더링 방식에 따라 도구가 갈린다.

| 대상 | 방식 | 이유 |
|---|---|---|
| Wanted | Playwright (headless Chromium) | JS 렌더링 SPA. 초기 HTML에 공고 링크가 없어 스크롤로 lazy-load를 유발해야 한다 |
| JobKorea | requests | SSR. 응답 HTML에 이미 링크가 포함되어 있어 브라우저가 불필요하다 |

Collector는 수집 방식과 무관하게 동일한 fetch job 메시지를 Kafka에 발행한다.

```
{"job_id", "source", "url", "collected_at", "retry_count"}
```

따라서 Worker는 URL이 어떤 방식으로 수집됐는지 알 필요가 없다.

### 2. Worker — 도메인별 파싱 전략

`extract_fields_by_domain()` 이 hostname으로 사이트별 파서를 선택한다.  
현재 Wanted / GroupBy / Catch / Saramin / JobKorea 5개 도메인을 처리하며, 매칭되지 않으면 og:title·meta description 기반 fallback을 쓴다.

**모든 파서는 동일한 6개 필드를 반환한다.**

- `company_name`, `title`, `location`, `employment_type`, `experience_level`, `description_text`

파서 내부는 JSON-LD(`@type == "JobPosting"`)를 1순위로 쓰고, 없거나 필드가 비면 og:title → title → meta description 순으로 내려가는 fallback 체인을 갖는다. 사이트별로 실제 다른 것은 title/description 포맷을 정리하는 **정제 규칙**이다. (예: Saramin은 JSON-LD가 없어 meta description 파싱에 전적으로 의존한다.)

일부 사이트는 파싱 외의 예외도 필요하다.

- Saramin: `<link rel="canonical">` 을 읽어 실제 공고 URL로 한 번 더 fetch, SSL 검증 비활성화

### 3. 결과

이 계약 덕분에 downstream(`stg_job_postings` 이하)은 출처 사이트를 몰라도 된다.  
새 사이트 추가 비용은 **파서 함수 1개 + 분기 1줄**이다.

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

- `stg_job_postings` not empty
- `int_job_postings_clean` required fields check
- `int_job_postings_clean` duplicate content hash candidate monitoring
- Job postings description fill-rate threshold

### Operational Review Checks
상시 장애 처리보다는 운영 점검 및 관찰 목적에 가까운 검증이다.

- `stg_job_postings` freshness
- job postings source quality trend
- DLQ retry distribution
- fetch failure pattern by error_type

---

## Airflow Quality Checks

실패 기준이 명확한 검증만 태스크로 올린다.  
`quality_check_job_postings` DAG가 아래 5개를 순서대로 수행한다.

```
[check_stg_not_empty, check_int_not_empty]
  → check_int_required_fields
  → check_int_no_duplicate_content_hash
  → check_description_quality
```

### 1. Staging not empty
대상: `stg_job_postings` / 실패 조건: row_count = 0

### 2. Intermediate not empty
대상: `int_job_postings_clean` / 실패 조건: row_count = 0

### 3. Required fields check
대상: `int_job_postings_clean`

실패 조건:
- title / original_url / source / collected_at null row 존재

### 4. Duplicate content hash check
대상: `int_job_postings_clean`

dedup 이후에도 동일 `content_hash` 가 남아 있는지 관찰한다.  
현재 dedup 키는 `source + original_url` 이므로, 같은 공고가 다른 URL로 들어온 경우를 잡는 보조 지표다.

### 5. Description quality threshold
대상: `mart_job_postings_source_quality`

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