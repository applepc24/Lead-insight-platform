import json
import os
from typing import Optional

from confluent_kafka import Consumer, Producer


def load_config() -> dict:
    """설정을 import 시점이 아니라 실행 시점에 읽는다.

    모듈 최상단에서 os.environ[...] 을 읽고 Consumer 를 만들면 import 만 해도
    환경변수를 요구하고 네트워크 클라이언트가 생긴다. 그러면 테스트에서 이 파일을
    불러올 수가 없다.
    """
    return {
        "bootstrap": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "dlq_topic": os.environ["KAFKA_DLQ_TOPIC"],
        "target_topic": os.environ["KAFKA_TARGET_TOPIC"],
        "group_id": os.environ.get("KAFKA_REPLAY_GROUP_ID", "dlq-replay-v1"),
        "max_retry_count": int(os.environ.get("MAX_RETRY_COUNT", "3")),
    }


def parse_dlq_payload(raw: bytes) -> Optional[dict]:
    """DLQ 메시지를 payload dict 로 해석한다. 해석할 수 없으면 None.

    DLQ 소비자는 실패를 더 보낼 곳이 없다 — DLQ 의 DLQ 는 없다. 그래서 워커와 달리
    재전송이 아니라 "로그를 남기고 건너뛰는 것"이 종착 처리다. 다만 조용히 버리면
    안 되므로 무엇이 왜 버려졌는지는 원본과 함께 남긴다.

    이 방어가 없으면 오염된 DLQ 메시지 한 건에 재투입 스크립트가 죽고, 오프셋을
    커밋하지 못해 다시 돌려도 같은 자리에서 또 죽는다 — 워커에 있던 것과 같은
    poison pill 이다.
    """
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        print(f"[replay] skip: cannot parse dlq message ({e}) raw={raw[:200]!r}")
        return None

    if not isinstance(payload, dict):
        print(f"[replay] skip: dlq payload must be an object, got {type(payload).__name__}")
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


def main():
    config = load_config()

    print(f"[replay] bootstrap={config['bootstrap']}")
    print(f"[replay] dlq_topic={config['dlq_topic']}")
    print(f"[replay] target_topic={config['target_topic']}")
    print(f"[replay] group_id={config['group_id']}")

    consumer = Consumer({
        "bootstrap.servers": config["bootstrap"],
        "group.id": config["group_id"],
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({"bootstrap.servers": config["bootstrap"]})

    consumer.subscribe([config["dlq_topic"]])
    replayed = 0
    skipped = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                break
            if msg.error():
                print("[replay] consume error:", msg.error())
                continue

            payload = parse_dlq_payload(msg.value())
            if payload is None:
                consumer.commit(message=msg, asynchronous=False)
                skipped += 1
                continue

            job = extract_job(payload)
            failed_stage = payload.get("failed_stage")
            error_type = payload.get("error_type")

            if not job:
                print("[replay] skip: no job in payload")
                consumer.commit(message=msg, asynchronous=False)
                skipped += 1
                continue

            # job 이 dict 임을 확인한 뒤에야 안전하게 읽을 수 있다.
            retry_count = int(job.get("retry_count", 0))

            if retry_count >= config["max_retry_count"]:
                print(f"[replay] skip: retry limit exceed job_id = {job.get('job_id')} retry_count={retry_count}")
                consumer.commit(message=msg, asynchronous=False)
                skipped += 1
                continue

            if failed_stage != "fetch":
                print(f"[replay] skip: unsupported failed stage= {failed_stage} job_id={job.get('job_id')} retry_count={retry_count}")
                consumer.commit(message=msg, asynchronous=False)
                skipped += 1
                continue

            if not job.get("job_id"):
                print("[replay] skip: job has no job_id")
                consumer.commit(message=msg, asynchronous=False)
                skipped += 1
                continue

            producer.produce(
                config["target_topic"],
                key=job["job_id"].encode("utf-8"),
                value=json.dumps(job, ensure_ascii=False).encode("utf-8"),
            )
            producer.flush()

            consumer.commit(message=msg, asynchronous=False)
            replayed += 1
            print(
                f"[replay] resent job_id={job.get('job_id')} "
                f"stage={failed_stage} error={error_type} retry_count={retry_count}"
            )

    finally:
        consumer.close()
        print(f"[replay] done. replayed={replayed}, skipped={skipped}")


if __name__ == "__main__":
    main()
