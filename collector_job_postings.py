import json
import os
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
FETCH_TOPIC = os.environ.get("KAFKA_TOPIC_JOB_FETCH", "job_postings.fetch_jobs")
DLQ_TOPIC = os.environ.get("KAFKA_TOPIC_JOB_DLQ", "job_postings.dlq")


def build_job_message(url: str, source: str = "seed") -> dict:
    return {
        "job_id": str(uuid.uuid4()),
        "source": source,
        "url": url,
        "collected_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "retry_count": 0,
    }


def main():
    print(f"[collector] kafka_bootstrap={KAFKA_BOOTSTRAP}, fetch_topic={FETCH_TOPIC}")
    print(f"[collector] dlq_topic={DLQ_TOPIC}")

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    seed_urls = [
        "https://invalid-url-test-1234.com",
        "https://example.com",
        "https://10.255.255.1",
        "http://httpstat.us/500",
        "https://www.wanted.co.kr/wd/242151",
    ]

    published = 0

    for url in seed_urls:
        job_message = build_job_message(url, source="seed")
        key = job_message["job_id"].encode("utf-8")
        value = json.dumps(job_message, ensure_ascii=False).encode("utf-8"
                                                                   )
        producer.produce(FETCH_TOPIC, key=key, value=value)
        published += 1
        print("[collector] queued job:")
        print(json.dumps(job_message, indent=2, ensure_ascii=False))
    
    producer.flush()
    print(f"[collector] published_count={published}")

if __name__ == "__main__":
    main()