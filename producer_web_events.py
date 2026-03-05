import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC_WEB_EVENTS", "web.user_events")

CUSTOMERS = [
    "user@company.com",
    "alpha@startup.com",
    "beta@corp.com",
]

EVENT_TYPES = ["pricing_view", "docs_view", "signup_attempt", "login"]

p = Producer({"bootstrap.servers": BOOTSTRAP})


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def main():
    print(f"[web-producer] bootstrap={BOOTSTRAP}, topic={TOPIC}")

    while True:
        customer = random.choice(CUSTOMERS)
        et = random.choice(EVENT_TYPES)

        event = {
            "event_source": "web",
            "event_name": et,
            "event_id": f"evt_{uuid.uuid4().hex}",
            "customer_key": customer,  # 너가 확정한 customer_key=email
            "occurred_at": now_iso(),
            "properties": {
                "path": "/pricing" if et == "pricing_view" else "/docs" if et == "docs_view" else "/",
                "user_agent": "simulator",
            },
        }

        p.produce(TOPIC, json.dumps(event).encode("utf-8"))
        p.flush(1.0)
        print("[sent]", event["event_name"], event["customer_key"], event["occurred_at"])
        time.sleep(2)


if __name__ == "__main__":
    main()