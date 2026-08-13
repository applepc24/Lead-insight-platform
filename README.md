# Job Postings Data Platform

## 1. 프로젝트 개요

Job Postings Data Platform은 여러 채용 사이트의 공고 데이터를 수집하여 S3 데이터 레이크에 저장하고, BigQuery에서 정제·분석할 수 있도록 구축한 채용공고 수집 기반 데이터 파이프라인 프로젝트입니다.

이 프로젝트의 목표는 단순히 HTML을 수집하는 수준을 넘어서, **수집 → 저장 → 정제 → 품질 검증 → 모니터링 → 장애 복구**까지 포함한 실제 데이터 플랫폼의 전체 흐름을 직접 설계하고 구현하는 것이었습니다.

이 파이프라인은 **URL을 수집하는 Collector**와 **공고를 파싱하는 Worker**를 분리해서, 수집 경로와 파싱 규칙이 서로 독립적으로 확장되도록 설계했습니다.

| 계층 | 대상 사이트 | 역할 |
|---|---|---|
| **Collector** | Wanted, JobKorea | 목록 페이지에서 공고 URL을 수집해 Kafka에 발행 |
| **Worker 파서** | Wanted, GroupBy, Catch, Saramin, JobKorea | 공고 상세 HTML에서 6개 필드 추출 |

Collector는 사이트별 목록 페이지 구조와 렌더링 방식에 종속되지만, Worker는 hostname만 보고 파서를 선택하므로 **URL이 어떤 경로로 들어왔는지 알 필요가 없습니다.** 덕분에 두 계층을 각자의 속도로 늘릴 수 있고, 파서가 있는 사이트라면 어떤 수집 경로로 들어온 URL이든 동일하게 처리됩니다.

Collector 단계에서 사이트마다 렌더링 방식이 달라 수집 전략을 분리했습니다. **원티드는 JS 렌더링 SPA라 Playwright로 브라우저를 띄워 스크롤 lazy-load까지 유발한 뒤 렌더된 DOM에서 URL을 추출**하고, **잡코리아는 SSR이라 requests 한 번으로 충분**합니다. Collector는 수집 방식과 무관하게 동일한 형식의 fetch job 메시지를 발행하므로, Worker는 URL이 어떻게 수집됐는지 알 필요가 없습니다. 참고로 **공고 상세 페이지는 어느 사이트든 requests 한 번**으로 받습니다 — SEO 때문에 상세 페이지는 SSR로 내려오기 때문입니다.

Worker는 hostname으로 사이트별 파서를 선택하되 **모든 파서가 동일한 6개 필드를 반환**하도록 계약을 고정했습니다. 덕분에 하류 계층(BigQuery 적재·마트)은 출처 사이트를 몰라도 됩니다. 파서 선택과 사이트별 fetch 예외(SSL 정책, canonical 재요청)는 `DOMAIN_CONFIG` 한 곳에 모아서, **새 사이트 추가는 파서 함수 1개 + 설정 한 줄**로 끝납니다.

Kafka 기반 큐를 통해 수집 요청과 처리 단계를 분리했고, Worker가 공고 페이지를 fetch하여 S3에 raw / processed / curated 형태로 저장하도록 구성했습니다. 또한 DLQ와 Replay 메커니즘을 통해 실패한 fetch 작업을 복구할 수 있도록 설계했으며, Airflow를 사용해 BigQuery 적재 및 품질 검증을 수행하고 Grafana를 통해 운영 상태를 모니터링합니다.

---

## 2. 프로젝트 목표

이 프로젝트는 채용공고 수집 파이프라인을 직접 운영한다고 가정했을 때, 데이터 엔지니어가 실제로 고려해야 하는 문제들을 경험해보기 위해 시작되었습니다.

핵심 질문은 다음과 같았습니다.

- 채용공고 수집 요청은 안정적으로 처리되는가?
- fetch 실패는 어떻게 분리하고 복구할 수 있는가?
- 같은 공고가 중복 적재되지는 않는가?
- 원본과 정제 데이터를 어떻게 분리해 관리할 것인가?
- 파이프라인 상태를 어떻게 모니터링할 것인가?

이를 해결하기 위해 다음과 같은 기술과 구조를 사용했습니다.

- Kafka 기반 fetch job 큐
- Python Worker 기반 HTML 수집
- Amazon S3 Data Lake
- BigQuery Data Warehouse
- Airflow 기반 적재 및 Data Quality Check
- Grafana Monitoring
- DLQ + Replay 시스템
- At-least-once 처리 환경에서의 중복 방지 / 정제 전략

---

## 3. 시스템 아키텍처

전체 데이터 파이프라인 구조는 다음과 같습니다.

![Job Postings Data Platform Architecture](./architecture/job_postings_platform.drawio.png)

---

## 4. 사용 기술 (Tech Stack)

### Data Ingestion
- Kafka

### Data Collection / Processing
- Python Worker
- Playwright (JS 렌더링 사이트 수집)
- Requests (SSR 사이트 수집 / 상세 페이지 fetch)
- BeautifulSoup (공고 HTML 구조 추출)
- boto3

### Data Storage
- Amazon S3 (Data Lake)
- BigQuery (Data Warehouse)

### Orchestration
- Airflow

### Monitoring
- Grafana

### Reliability
- DLQ (Dead Letter Queue)
- Replay Mechanism
- At-least-once processing
- Worker-level idempotency
- BigQuery-level deduplication

---

## 5. 데이터 흐름 (Data Flow)

## 1. 채용공고 수집 요청 생성

수집 대상 채용공고 URL을 Kafka fetch topic에 넣습니다.

예시 입력 데이터:

- 공고 URL
- source
- collected_at
- job_id
- retry_count

이 데이터는 Worker가 실제 공고 페이지를 수집하기 위한 fetch job 역할을 합니다.

---

## 2. Kafka fetch topic 적재

생성된 수집 요청은 Kafka topic으로 전송됩니다.

예시 topic:

- `job_postings.fetch_jobs`

Kafka는 수집 요청 생성 단계와 실제 HTML 수집 단계를 분리하는 역할을 합니다.

이를 통해 다음과 같은 장점을 얻을 수 있습니다.

- 수집 요청과 처리 속도 분리
- Worker 장애 시 메시지 유실 완화
- 비동기 처리 구조
- Replay 및 DLQ 기반 복구 가능

---

## 3. Worker 기반 HTML 수집

Worker는 Kafka에서 fetch job을 읽고, 해당 URL의 HTML을 수집합니다.

수집 과정에서 다음을 수행합니다.

- HTTP 요청 수행
- source별 HTML 메타데이터 추출 (JSON-LD 우선, 없으면 og:title → title → meta description 폴백)
- canonical URL 재요청 처리 (`DOMAIN_CONFIG`에서 켠 사이트만 — 현재 Saramin)
- fetch 실패 시 DLQ 전송
- raw / processed / curated 문서 생성

사이트별 예외는 코드 곳곳에 흩어지지 않고 `DOMAIN_CONFIG` 한 곳에 모여 있습니다.

```python
DOMAIN_CONFIG = {
    "wanted.co.kr": {"parser": extract_wanted_fields},
    "saramin.co.kr": {
        "parser": extract_saramin_fields,
        "disable_ssl_verify": True,   # 인증서 체인이 불완전
        "refetch_canonical": True,    # 목록 URL이 리다이렉트용
    },
    ...
}
```

파서 라우팅, SSL 정책, canonical 재요청이 모두 이 표를 읽으므로 사이트를 추가하거나 뺄 때 고칠 곳이 한 군데입니다.

---

## 4. S3 Data Lake 저장

Worker는 수집 결과를 S3에 다음 3개 레이어로 저장합니다.

### Raw
수집한 HTML 원문 저장

예시:
- `raw/job_postings/source={source}/dt={dt}/{job_id}.html`

### Processed
원본 HTML의 기본 메타데이터 저장

예시:
- `processed/job_postings/source={source}/dt={dt}/{job_id}.json`

### Curated
정제된 공고 문서 저장

예시:
- `curated/job_postings/dt={dt}/{job_id}.json`

이 구조는 다음과 같은 목적을 가집니다.

- 원본 데이터 보존
- 단계별 산출물 분리
- 장애 복구 및 재처리 기준점 제공
- 정제 로직 변경 시 raw 기반 재처리 가능

---

## 5. 실패 이벤트 처리 (DLQ)

fetch / raw upload / processed 생성 / curated 생성 단계에서 오류가 발생하면, 해당 job은 DLQ topic으로 전송됩니다.

예시 DLQ topic:

- `job_postings.dlq`

DLQ 메시지에는 다음 정보가 포함됩니다.

- job payload
- error_type
- error_message
- failed_stage
- failed_at
- retry_count

이 구조를 통해 어떤 단계에서 왜 실패했는지 추적할 수 있습니다.

---

## 6. Replay 시스템

DLQ에 저장된 fetch 실패 job은 replay script를 통해 다시 원래 fetch topic으로 전송할 수 있습니다.

흐름:

DLQ Topic  
↓  
Replay Script / Airflow Replay DAG  
↓  
Original Fetch Topic

재처리 조건:
- `failed_stage == "fetch"`
- `retry_count < MAX_RETRY_COUNT`

이를 통해 일시적인 네트워크 장애나 외부 사이트 응답 문제를 복구할 수 있습니다.

---

## 7. BigQuery 데이터 적재

S3에 저장된 curated 데이터는 BigQuery staging 테이블로 적재됩니다.

현재 사용 중인 주요 테이블:

- `stg_job_postings`

이 단계에서는 Worker가 생성한 정제 문서를 테이블 형태로 적재하여 downstream 정제와 분석의 입력으로 사용합니다.

---

## 6. 데이터 모델 (Data Model)

데이터 웨어하우스는 다음 계층 구조로 구성했습니다.

Raw Files (S3)  
│  
▼  
Staging Table  
│  
▼  
Intermediate / Clean View  
│  
▼  
Mart Views

---

## Staging Layer

정제된 공고 문서를 BigQuery에 적재하는 레이어입니다.

### stg_job_postings

| column | description |
|------|-------------|
| posting_id | 공고 수집 작업 ID |
| source | 수집 소스 |
| original_url | 원본 공고 URL |
| company_name | 회사명 |
| title | 공고 제목 |
| location | 근무 지역 |
| employment_type | 고용 형태 |
| experience_level | 경력 수준 |
| description_text | 공고 설명 텍스트 |
| skills | 스킬 정보 |
| collected_at | 수집 시각 |
| content_hash | 공고 설명 기반 해시 |
| raw_s3_key | raw HTML 경로 |
| processed_s3_key | processed JSON 경로 |
| curated_s3_key | curated JSON 경로 |
| loaded_at | BigQuery 적재 시각 |

---

## Intermediate Layer

최종 중복 제거 및 최신 공고 기준 정제를 수행하는 레이어입니다.

### int_job_postings_clean

현재 중복 제거 기준은 다음과 같습니다.

- `source + original_url` 기준으로 그룹화
- `collected_at DESC`, `loaded_at DESC` 기준 최신 1건만 유지

즉 같은 source 내 동일 URL에 대해 최신 공고만 남깁니다.

---

## Mart Layer

운영 및 분석에 바로 사용할 수 있도록 요약한 레이어입니다.

### mart_job_postings_daily
일별 / 소스별 공고 수집량 집계

### mart_job_postings_source_quality
소스별 품질 지표 집계

예시 지표:
- total_postings
- description_filled_count
- content_hash_filled_count
- missing_title_count
- missing_company_count

---

## 7. 데이터 품질 검증 (Data Quality)

데이터 파이프라인의 신뢰성을 유지하기 위해 다음 검증을 수행합니다.

### 1. Deduplication

같은 공고가 반복 수집될 수 있으므로, BigQuery intermediate 레이어에서 최신 1건만 남깁니다.

```sql
SELECT
  posting_id,
  source,
  original_url
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY source, original_url
      ORDER BY collected_at DESC, loaded_at DESC
    ) AS rn
  FROM `lead-insight-platform.lead_platform.stg_job_postings`
)
WHERE rn = 1
```

### 2. Required Field Validation

핵심 필드 누락 여부를 검증합니다

```sql
SELECT COUNT(*)
FROM `lead-insight-platform.lead_platform.int_job_postings_clean`
WHERE title IS NULL
   OR original_url IS NULL
   OR source IS NULL
   OR collected_at IS NULL
```

### 3. Description Quality Check

설명 텍스트가 비어 있는 공고 비율을 점검합니다.

```sql
SELECT
  source,
  total_postings,
  description_filled_count
FROM `lead-insight-platform.lead_platform.mart_job_postings_source_quality`
```

### 4. Content Hash Monitoring

content_hash 는 현재 직접 dedup 키로 사용하지는 않지만,
동일/유사 공고 후보를 관찰하기 위한 품질 지표로 활용합니다.

## 8. Monitoring

Grafana 대시보드를 통해 채용공고 수집 파이프라인 상태를 모니터링합니다.

주요 지표:
	•	Job Postings Volume
	•	Source별 공고 수집량
	•	DLQ Events
	•	retry_count 분포
	•	error_type 분포
	•	failed_stage 분포
	•	description quality
	•	적재량 추이

특히 replay 결과 자체는 Airflow가 실행하고,
실패 패턴과 운영 상태는 BigQuery + Grafana에서 모니터링하도록 분리했습니다.

즉,
	•	Airflow = orchestration
	•	BigQuery / Grafana = observability

역할로 나누어 구성했습니다.

## 9. 신뢰성 점검 및 결함 수정

파이프라인이 동작하는 상태에서, 운영 관점으로 코드를 다시 점검해 결함 8건을 찾아 고쳤습니다.
각 항목은 먼저 재현해 실패를 확인하고, 그 동작을 테스트로 고정한 뒤 수정했습니다.

| # | 결함 | 증상 | 수정 |
|---|---|---|---|
| 1 | Kafka 메시지 검증 없음 (Worker) | 파이프라인 영구 정지 | 해석 게이트 + DLQ 경로 |
| 2 | Kafka 메시지 검증 없음 (DLQ 소비자 2곳) | 실패 이력 적재 중단 | 동일 패턴 적용 |
| 3 | `enable.auto.commit` 미설정 | 메시지 유실 | 명시적 비활성화 |
| 4 | 체크포인트 조회 fail-open | 전량 중복 적재 | fail-closed 전환 |
| 5 | `<meta>` 속성 순서 가정 | 필드가 조용히 `None` | 정규식 → HTML 파서 |
| 6 | `<link rel=canonical>` 속성 순서 가정 | canonical 재요청 누락 | 정규식 → HTML 파서 |
| 7 | `job` 이 `null` 일 때 재투입 스크립트 중단 | replay 중단 | 가드 순서 정정 |
| 8 | 도달 불가능한 에러 로깅 코드 | 로그 누락 | 삭제 |

### 1. 오염된 메시지 한 건이 파이프라인 전체를 멈춘다

Worker 가 Kafka 메시지를 검증 없이 파싱하고 있었습니다. JSON 이 깨졌거나 필수 필드가 없으면
예외가 메인 루프를 빠져나가 프로세스가 죽는데, 이때 오프셋을 커밋하지 못한 상태라
재시작하면 같은 메시지를 다시 읽고 또 죽습니다. 무한 크래시 루프가 되고 뒤에 쌓인
정상 공고는 처리되지 않습니다.

fetch·업로드 실패에는 이미 "DLQ 로 보내고 커밋하고 넘어가는" 경로가 있었고, 해석 단계에만
없었습니다. `parse_job_message()` 로 UTF-8 → JSON → 객체 여부 → 필수 필드를 검사하고,
해석 실패는 원본 바이트와 함께 DLQ 에 남깁니다.

같은 결함이 `replay_dlq_to_original.py` 와 `consumer_dlq_to_bigquery.py` 에도 있었습니다.
DLQ 가 막히면 실패 이력 자체가 쌓이지 않아 파이프라인이 왜 실패하는지 알 수 없게 됩니다.
다만 DLQ 소비자는 실패를 더 보낼 곳이 없으므로(DLQ 의 DLQ 는 없음) 재전송이 아니라
로그를 남기고 건너뛰는 것을 종착 처리로 두었습니다.

### 2. 자동 커밋이 단계별 수동 커밋을 무력화하고 있었다

Worker 는 단계마다 처리를 끝낸 뒤에만 커밋하도록 짜여 있는데, `create_consumer()` 가
`enable.auto.commit` 을 설정하지 않아 librdkafka 기본값인 `true` 로 동작하고 있었습니다.
백그라운드가 5초마다 별도로 커밋하므로, fetch 가 느린 사이(연결 3초 + 읽기 10초)
워커가 죽으면 처리되지 않은 메시지의 오프셋이 이미 커밋돼 있습니다.

같은 저장소의 다른 컨슈머 2개는 명시적으로 끄고 있어, 판단이 아니라 누락이었습니다.
시나리오 테스트가 `create_consumer` 를 통째로 대체하고 있어 설정이 한 번도 검증된 적이
없었던 것이 원인입니다.

### 3. 체크포인트를 못 읽으면 전부 다시 적재한다

`already_loaded_keys()` 가 모든 예외를 잡아 빈 집합을 돌려주고 있었습니다.
빈 집합은 호출부에서 "여태 적재한 파일이 하나도 없다"로 읽히므로, BigQuery 가 일시적으로
흔들리면 S3 의 전체 파일이 새 파일로 판정돼 히스토리 전량이 다시 들어갑니다.

실제로 일어난 일은 "체크포인트 표를 못 읽었다"이고 이것은 **모르는 상태**인데,
코드가 그것을 **확실히 없는 상태**로 바꿔치기하고 있었습니다. 적재가 한 주기 늦는 것보다
전량 중복이 훨씬 비싸고 DAG 에 `retries=2` 가 있으므로, 예외를 그대로 올려 태스크를
실패시키도록 바꿨습니다.

### 4. HTML 속성 순서를 가정한 정규식

구조를 읽는 4개 함수가 정규식으로 태그를 매칭하고 있었습니다. HTML 속성은 순서가 무의미한데
정규식은 왼쪽에서 오른쪽으로 읽으므로 순서를 강제할 수밖에 없고, 이 미스매치가 실제 버그
2건을 만들고 있었습니다.

```html
<meta content="..." name="description">     <!-- 유효한 HTML 인데 값을 못 읽음 -->
<link href="..." rel="canonical">           <!-- 마찬가지 -->
```

예외도 로그도 없이 필드가 비는 종류라, Saramin 의 경우 canonical 재요청이 조용히 스킵될 수
있었습니다. BeautifulSoup 으로 교체하면서 `re.IGNORECASE` / `re.DOTALL` / `re.escape` 가
함께 사라졌습니다 — 파서가 원래 하는 일을 정규식으로 흉내 내던 것들이었습니다.
문자열 정제 정규식과 `strip_html_tags()` 는 그대로 두었습니다(`content_hash` 정합성).

### 검증 방식

각 항목을 **재현 → 테스트로 고정 → 수정 → 뮤테이션 테스트** 순으로 처리했습니다.
뮤테이션 테스트는 의도적인 회귀를 소스에 심고 테스트가 잡아내는지 확인하는 방법으로,
"테스트가 통과한다"와 "테스트가 회귀를 잡는다"는 다르기 때문에 사용했습니다.

| | 이전 | 이후 |
|---|---|---|
| 테스트 | 207 | 296 |
| 뮤테이션 검출 | — | 35/35 |
| DAG 테스트 | 0 | 4 |

실제로 이 과정에서 테스트 구멍 6건이 드러났습니다. 예를 들어 필수 필드 검사를
`not job.get(field)` 에서 `field not in job` 으로 바꿔도 테스트가 전부 통과했는데,
"키는 있지만 값이 빈 문자열"인 경우를 아무도 검증하지 않고 있었습니다.

DAG 파일은 저장소 루트의 `airflow/` 디렉토리가 실제 Airflow 패키지 이름을 가려서 테스트가
하나도 없었고, DLQ 소비자 2개는 import 만으로 환경변수를 요구하고 네트워크 클라이언트를
생성해 불러올 수조차 없었습니다. **테스트할 수 없는 코드였다는 점이 결함이 오래 남은
이유이기도 합니다.** 설정 읽기와 클라이언트 생성을 실행 시점으로 옮겨 테스트를 붙였습니다.

## 10. Lessons Learned

### 1. 채용공고 수집에서 중요한 것은 단순 fetch 성공이 아니라 복구 가능성

초기에는 공고 HTML을 잘 가져오는 것 자체에 집중했지만, 실제 운영 관점에서는 다음 문제가 더 중요했습니다.
	•	외부 사이트 응답 실패
	•	DNS / timeout / SSL 오류
	•	동일 공고 중복 수집
	•	실패한 작업의 재처리
	•	어떤 단계에서 실패했는지 추적 가능성

이를 해결하기 위해 다음을 설계했습니다.
	•	DLQ
	•	Replay System
	•	failed_stage / error_type 기록
	•	retry_count 기반 재시도 제한

### 2. 중복 제거는 한 지점이 아니라 여러 레이어에서 나뉘어야 한다

현재 파이프라인은 다음과 같은 다층 구조를 사용합니다.
	•	Worker: job_id 기반 재처리 방지
	•	BigQuery intermediate: source + original_url 기준 dedup
	•	Mart: dedup 결과 기반 집계

즉 raw는 보존하고, 정제 레이어에서 신뢰 가능한 공고만 남기는 구조를 사용했습니다.

### 3. 원본 보존과 분석용 정제는 분리해야 한다

raw / processed / curated 를 분리해 저장함으로써 다음 장점을 얻었습니다.
	•	원본 HTML 보존
	•	정제 로직 변경 시 재처리 가능
	•	단계별 디버깅 가능
	•	데이터 품질 문제 발생 시 역추적 가능

⸻

### 4. 운영에서는 실행과 관측을 분리하는 것이 중요하다

Replay 실행 자체는 Airflow에서 담당하고,
재처리 결과 및 실패 패턴 검증은 dlq_events 를 BigQuery에 적재한 뒤 Grafana에서 모니터링하도록 구성했습니다.

이를 통해 다음 역할을 분리했습니다.
	•	Airflow: 스케줄링 및 재처리 실행
	•	BigQuery / Grafana: 운영 상태 관측 및 실패 패턴 분석

⸻

### 5. 데이터 플랫폼에서 중요한 것은 적재 자체보다 신뢰성이다

이 프로젝트를 통해 단순 적재보다 더 중요한 것이 무엇인지 체감할 수 있었습니다.
	•	실패 이벤트 격리
	•	중복 방지
	•	품질 검증
	•	재처리 가능성
	•	운영 모니터링

결국 데이터 플랫폼의 핵심은 “데이터가 들어오느냐”가 아니라
“문제가 생겨도 신뢰할 수 있게 운영되느냐” 라는 점을 배웠습니다.
