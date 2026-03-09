import json
import os
import time
from datetime import datetime, timezone
from typing import Set

import stripe
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

STRIPE_SECRET_KEY = os.environ["STRIPE_SECRET_KEY"]
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC_STRIPE_INVOICE", "stripe.invoice_events")
CURSOR_FILE = os.environ.get("CURSOR_FILE", ".cursor_invoice_paid.json")

# 테스트용: 특정 test clock만 읽기
TEST_CLOCK_ID = os.environ.get("STRIPE_TEST_CLOCK_ID", "")

stripe.api_key = STRIPE_SECRET_KEY


def iso_utc(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def load_seen_ids():
    if not os.path.exists(CURSOR_FILE):
        return set()
    with open(CURSOR_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return set(data.get("seen_invoice_ids", []))


def save_seen_ids(seen_ids) -> None:
    with open(CURSOR_FILE, "w", encoding="utf-8") as f:
        json.dump({"seen_invoice_ids": sorted(seen_ids)}, f)


def build_event(invoice: dict) -> dict:
    st = invoice.get("status_transitions") or {}
    paid_at = int(st.get("paid_at") or invoice["created"])

    amount_paid_cents = invoice.get("amount_paid")
    amount_dollars = None
    if amount_paid_cents is not None:
        amount_dollars = round(amount_paid_cents / 100.0, 2)

    return {
        "event_source": "stripe",
        "event_name": "invoice_paid",
        "event_id": invoice["id"],
        "customer_key": invoice.get("customer_email"),
        "customer_key_fallback": invoice.get("customer"),
        "occurred_at": iso_utc(paid_at),
        "amount": amount_dollars,
        "currency": invoice.get("currency"),
        "properties": {
            "stripe_invoice_id": invoice["id"],
            "stripe_customer_id": invoice.get("customer"),
            "subscription_id": invoice.get("subscription"),
            "status": invoice.get("status"),
            "created": invoice.get("created"),
            "paid_at": st.get("paid_at"),
            "number": invoice.get("number"),
            "hosted_invoice_url": invoice.get("hosted_invoice_url"),
            "test_clock": invoice.get("test_clock"),
        },
    }


def main():
    print(f"[producer] kafka_bootstrap={KAFKA_BOOTSTRAP}, topic={TOPIC}")
    print(f"[producer] test_clock_id={TEST_CLOCK_ID}")

    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    seen_ids = load_seen_ids()

    sent = 0
    scanned_customers = 0
    scanned_invoices = 0

    if not TEST_CLOCK_ID:
        raise ValueError("STRIPE_TEST_CLOCK_ID is not set")

    customers = stripe.Customer.list(test_clock=TEST_CLOCK_ID, limit=100)

    for cust in customers.auto_paging_iter():
        scanned_customers += 1
        customer_id = cust["id"]
        customer_email = cust.get("email")
        print(f"[producer] checking customer={customer_id}, email={customer_email}")

        invoices = stripe.Invoice.list(customer=customer_id, limit=100)

        for inv in invoices.auto_paging_iter():
            scanned_invoices += 1

            if inv["id"] in seen_ids:
                continue

            if inv.get("status") != "paid":
                continue

            event = build_event(inv)
            key = (event["customer_key"] or event["customer_key_fallback"] or event["event_id"]).encode("utf-8")
            value = json.dumps(event, ensure_ascii=False).encode("utf-8")

            producer.produce(TOPIC, key=key, value=value)
            seen_ids.add(inv["id"])
            sent += 1

    producer.flush()
    save_seen_ids(seen_ids)

    print(
        f"[producer] scanned_customers={scanned_customers}, "
        f"scanned_invoices={scanned_invoices}, sent_paid={sent}, "
        f"seen_ids={len(seen_ids)}"
    )


if __name__ == "__main__":
    main()