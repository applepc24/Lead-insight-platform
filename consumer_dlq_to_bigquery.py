import json
import os
from datetime import datetime
from typing import Optional

from confluent_kafka import Consumer
from google.cloud import bigquery

TABLE = "dlq_events"


def load_config() -> dict:
    """설정을 import 시점이 아니라 실행 시점에 읽는다.

    모듈 최상단에서 os.environ[...] 을 읽고 Consumer/BigQuery 클라이언트를 만들면
    import 만 해도 환경변수를 요구하고 네트워크 클라이언트가 생긴다. 그러면
    테스트에서 이 파일을 불러올 수가 없다.
    """
    return {
        "bootstrap": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "dlq_topic": os.environ["KAFKA_DLQ_TOPIC"],
        "group_id": os.environ.get("KAFKA_DLQ_CONSUMER_GROUP", "dlq-bq-writer"),
        "project_id": os.environ["GCP_PROJECT_ID"],
        "dataset": os.environ["BQ_DATASET"],
    }


def parse_dlq_payload(raw: bytes) -> Optional[dict]:
    """DLQ 메시지를 payload dict 로 해석한다. 해석할 수 없으면 None.

    DLQ 소비자는 실패를 더 보낼 곳이 없다 — DLQ 의 DLQ 는 없다. 그래서 워커와 달리
    재전송이 아니라 "로그를 남기고 건너뛰는 것"이 종착 처리다. 다만 조용히 버리면
    안 되므로 무엇이 왜 버려졌는지는 원본과 함께 남긴다.

    이 방어가 없으면 오염된 DLQ 메시지 한 건에 적재 소비자가 죽고, 오프셋을
    커밋하지 못해 재시작해도 같은 자리에서 또 죽는다 — 워커에 있던 것과 같은
    poison pill 이다. DLQ 가 막히면 실패 이력 자체가 쌓이지 않는다.
    """
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        print(f"[dlq-bq] skip: cannot parse dlq message ({e}) raw={raw[:200]!r}")
        return None

    if not isinstance(payload, dict):
        print(f"[dlq-bq] skip: dlq payload must be an object, got {type(payload).__name__}")
        return None

    return payload


def extract_job(payload: dict) -> dict:
    """payload 에서 job 을 꺼낸다. dict 가 아니면 빈 dict 로 흡수한다.

    payload.get("job", {}) 만으로는 부족하다. dict.get 의 기본값은 키가 없을 때만
    쓰이므로 {"job": null} 이면 None 이 그대로 나오고, 뒤이은 job.get(...) 이
    AttributeError 로 터진다.
    """
    job = payload.get("job")
    return job if isinstance(job, dict) else {}


def build_row(payload: dict) -> dict:
    job = extract_job(payload)

    return {
        "failed_at": payload.get("failed_at", datetime.utcnow().isoformat()),
        "failed_stage": payload.get("failed_stage"),
        "error_type": payload.get("error_type"),
        "error_message": payload.get("error_message"),
        "retry_count": job.get("retry_count", 0),
        "source": job.get("source"),
        "url": job.get("url"),
        "payload": json.dumps(payload, ensure_ascii=False),
    }


def write_to_bigquery(bq, project_id: str, dataset: str, row: dict):
    table_id = f"{project_id}.{dataset}.{TABLE}"
    return bq.insert_rows_json(table_id, [row])


def main():
    config = load_config()

    consumer = Consumer({
        "bootstrap.servers": config["bootstrap"],
        "group.id": config["group_id"],
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    bq = bigquery.Client(project=config["project_id"])

    consumer.subscribe([config["dlq_topic"]])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("[dlq-bq] consume error:", msg.error())
            continue

        payload = parse_dlq_payload(msg.value())
        if payload is None:
            consumer.commit(message=msg, asynchronous=False)
            continue

        row = build_row(payload)

        errors = write_to_bigquery(bq, config["project_id"], config["dataset"], row)
        if errors:
            print("[dlq-bq] insert error:", errors)
            continue

        consumer.commit(message=msg, asynchronous=False)
        print("[dlq-bq] stored DLQ event")


if __name__ == "__main__":
    main()
