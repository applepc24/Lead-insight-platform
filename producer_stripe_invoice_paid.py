import json
import os
import time
from datetime import datetime, timezone

import stripe
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

STRIPE_SECRET_KEY = os.environ["STRIPE_SECRET_KEY"]
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC_STRIPE_INVOICE", "stripe.invoice_events")
POLL_WINDOW_SECONDS = int(os.environ.get("POLL_WINDOW_SECONDS", "86400"))
CURSOR_FILE = os.environ.get("CURSOR_FILE", ".cursor_invoice_paid.json")

stripe.api_key = STRIPE_SECRET_KEY


def iso_utc(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def load_cursor_ts() -> int:
    # unix seconds
    if not os.path.exists(CURSOR_FILE):
        return int(time.time()) - POLL_WINDOW_SECONDS
    with open(CURSOR_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return int(data.get("last_seen_created", int(time.time()) - POLL_WINDOW_SECONDS))


def save_cursor_ts(ts: int) -> None:
    with open(CURSOR_FILE, "w", encoding="utf-8") as f:
        json.dump({"last_seen_created": ts}, f)


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
        "event_id": invoice["id"],  # in_...
        "customer_key": invoice.get("customer_email"),  # email
        "customer_key_fallback": invoice.get("customer"),  # cus_...
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
        },
    }


def main():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

    cursor = load_cursor_ts()
    now = int(time.time())
    created_gte = cursor
    created_lte = now

    print(f"[producer] polling invoices created in [{iso_utc(created_gte)} .. {iso_utc(created_lte)}]")

    params = {"limit": 100, "created": {"gte": created_gte, "lte": created_lte}}
    invoices = stripe.Invoice.list(**params)

    max_created_seen = cursor
    sent = 0
    scanned = 0

    while True:
        data = invoices.get("data", [])
        for inv in data:
            scanned += 1
            inv_created = int(inv["created"])
            if inv_created > max_created_seen:
                max_created_seen = inv_created

            if inv.get("status") != "paid":
                continue

            event = build_event(inv)
            key = (event["customer_key"] or event["customer_key_fallback"] or event["event_id"]).encode("utf-8")
            value = json.dumps(event, ensure_ascii=False).encode("utf-8")

            producer.produce(TOPIC, key=key, value=value)
            sent += 1

        producer.flush()

        if not invoices.get("has_more"):
            break
        last_id = data[-1]["id"]
        invoices = stripe.Invoice.list(**params, starting_after=last_id)

    save_cursor_ts(max_created_seen)
    print(f"[producer] scanned={scanned}, sent_paid={sent}, cursor_saved={iso_utc(max_created_seen)}")


if __name__ == "__main__":
    main()