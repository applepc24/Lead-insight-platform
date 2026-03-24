import json
import os
import re
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()

LIST_URL = "https://www.wanted.co.kr/wdlist/518?country=kr&job_sort=job.latest_order&years=0&locations=all"

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
FETCH_TOPIC = os.environ.get("KAFKA_TOPIC_JOB_FETCH", "job_postings.fetch_jobs")


def extract_wanted_links_from_html(html: str) -> list[str]:
    matches = re.findall(r'href="(/wd/\d+)"', html)
    urls = sorted(set(f"https://www.wanted.co.kr{path}" for path in matches))
    return urls


def build_job_message(url: str, source: str = "wanted") -> dict:
    return {
        "job_id": str(uuid.uuid4()),
        "source": source,
        "url": url,
        "collected_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "retry_count": 0,
    }


def fetch_wanted_detail_urls() -> list[str]:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            locale="ko-KR",
            viewport={"width": 1440, "height": 1200},
        )
        page = context.new_page()

        page.goto(LIST_URL, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(5000)

        page.mouse.wheel(0, 3000)
        page.wait_for_timeout(2000)
        page.mouse.wheel(0, 3000)
        page.wait_for_timeout(2000)

        html = page.content()
        print(f"[collector] final_url={page.url}")
        print(f"[collector] title={page.title()}")
        print(f"[collector] rendered_html_length={len(html)}")

        urls = extract_wanted_links_from_html(html)

        browser.close()
        return urls


def main():
    print(f"[collector] kafka_bootstrap={KAFKA_BOOTSTRAP}, fetch_topic={FETCH_TOPIC}")
    print(f"[collector] list_url={LIST_URL}")

    detail_urls = fetch_wanted_detail_urls()
    print(f"[collector] extracted_url_count={len(detail_urls)}")

    if not detail_urls:
        print("[collector] no detail urls found")
        return

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    published = 0
    for url in detail_urls[:20]:
        job_message = build_job_message(url, source="wanted")
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