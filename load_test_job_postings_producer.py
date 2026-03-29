import json
import os
import uuid
from datetime import datetime, UTC
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
FETCH_TOPIC = os.environ.get("KAFKA_TOPIC_JOB_FETCH", "job_postings.fetch_jobs")

producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


VALID_URLS = [
    "https://www.wanted.co.kr/wd/242151",
    "https://groupby.kr/positions/9644",
    "https://www.catch.co.kr/NCS/RecruitInfoDetails/543587",
]

FAIL_URLS = [
    "https://invalid-url-test-1234.com",  # dns error
    "https://10.255.255.1",               # timeout 유도
]


def build_job(url: str, source: str = "load_test") -> dict:
    return {
        "job_id": str(uuid.uuid4()),
        "source": source,
        "url": url,
        "collected_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "retry_count": 0,
    }


def send_jobs(jobs: list[dict]) -> None:
    for job in jobs:
        producer.produce(
            FETCH_TOPIC,
            key=job["job_id"].encode("utf-8"),
            value=json.dumps(job, ensure_ascii=False).encode("utf-8"),
        )
    producer.flush()


def build_test_batch(
    valid_count: int,
    fail_count: int,
) -> list[dict]:
    jobs = []

    for i in range(valid_count):
        url = VALID_URLS[i % len(VALID_URLS)]
        jobs.append(build_job(url))

    for i in range(fail_count):
        url = FAIL_URLS[i % len(FAIL_URLS)]
        jobs.append(build_job(url))

    return jobs


if __name__ == "__main__":
    valid_count = int(os.environ.get("LOAD_TEST_VALID_COUNT", "20"))
    fail_count = int(os.environ.get("LOAD_TEST_FAIL_COUNT", "5"))

    jobs = build_test_batch(valid_count=valid_count, fail_count=fail_count)
    send_jobs(jobs)

    print(
        f"[load-test] sent total={len(jobs)} "
        f"valid={valid_count} fail={fail_count} "
        f"topic={FETCH_TOPIC} bootstrap={KAFKA_BOOTSTRAP}"
    )