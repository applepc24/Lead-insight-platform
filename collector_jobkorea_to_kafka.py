import json
import os
import re
import uuid
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
import urllib3
from confluent_kafka import Producer
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

LIST_URL = "https://www.jobkorea.co.kr/Search/?stext=%EA%B0%9C%EB%B0%9C%EC%9E%90"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
FETCH_TOPIC = os.environ.get("KAFKA_TOPIC_JOB_FETCH", "job_postings.fetch_jobs")

def fetch_html(url: str) -> str:
    response = requests.get(
        url,
        timeout=15,
        headers={
            "User-Agent": "Mozilla/5.0 (compatible; LeadInsightBot/0.1)"
        },
        verify=False,
    )
    response.raise_for_status()
    return response.text


def extract_jobkorea_detail_urls(html: str) -> list[str]:
    matches = re.findall(r"/Recruit/GI_Read/\d+", html)
    unique_matches = sorted(set(matches))

    urls = []
    for path in unique_matches:
        full_url = urljoin("https://www.jobkorea.co.kr", path)
        urls.append(full_url)

    return urls

def build_job_message(url: str, source: str = "jobkorea") -> dict:
    return {
        "job_id": str(uuid.uuid4()),
        "source": source,
        "url": url,
        "collected_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "retry_count": 0,
    }

def main():
    print(f"[collector] kafka_bootstrap={KAFKA_BOOTSTRAP}, fetch_topic={FETCH_TOPIC}")
    print(f"[collector] list_url={LIST_URL}")

    html = fetch_html(LIST_URL)
    print(f"[collector] fetched html length={len(html)}")

    detail_urls = extract_jobkorea_detail_urls(html)
    print(f"[collector] extracted_urls_count={len(detail_urls)}")

    if not detail_urls:
        print("[collector] No detail URLs found.")
        return
    
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    published = 0
    for url in detail_urls[:20]:
        job_message = build_job_message(url, source="jobkorea")
        producer.produce(
            FETCH_TOPIC,
            key=job_message["job_id"].encode("utf-8"),
            value=json.dumps(job_message, ensure_ascii=False).encode("utf-8"),
        )
        published += 1
        print(f"[collector] queued: {url}")
    
    producer.flush()
    print(f"[collector] published_count={published}")

if __name__ == "__main__":
    main()