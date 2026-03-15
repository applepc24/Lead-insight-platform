import json
import os
from datetime import datetime

from confluent_kafka import Consumer
from google.cloud import bigquery

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
DLQ_TOPIC = os.environ["KAFKA_DLQ_TOPIC"]
GROUP_ID = os.environ.get("KAFKA_DLQ_CONSUMER_GROUP", "dlq-bq-writer")

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
DATASET = os.environ["BQ_DATASET"]
TABLE = "dlq_events"

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

bq = bigquery.Client(project=PROJECT_ID)


def write_to_bigquery(row):
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"
    errors = bq.insert_rows_json(table_id, [row])
    return errors

    if errors:
        print("[dlq-bq] insert error:", errors)


def main():
    consumer.subscribe([DLQ_TOPIC])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("[dlq-bq] consume error:", msg.error())
            continue

        payload = json.loads(msg.value().decode())

        row = {
            "failed_at": datetime.utcnow().isoformat(),
            "source_topic": payload.get("source_topic"),
            "error_type": payload.get("error_type"),
            "retry_count": payload.get("retry_count", 0),
            "payload": json.dumps(payload),
        }

        errors = write_to_bigquery(row)
        if errors:
            print("[dlq-bq] insert error:", errors)
            continue

        consumer.commit(message=msg, asynchronous=False)
        print("[dlq-bq] stored DLQ event")


if __name__ == "__main__":
    main()