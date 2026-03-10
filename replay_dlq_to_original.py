import json
import os

from confluent_kafka import Consumer, Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DLQ_TOPIC = os.environ["KAFKA_DLQ_TOPIC"]
TARGET_TOPIC = os.environ["KAFKA_TARGET_TOPIC"]
GROUP_ID = os.environ.get("KAFKA_REPLAY_GROUP_ID", "dlq-replay-v1")
MAX_RETRY_COUNT = int(os.environ.get("MAX_RETRY_COUNT", "3"))

consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": GROUP_ID,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

producer = Producer({"bootstrap.servers": BOOTSTRAP})


def main():
    consumer.subscribe([DLQ_TOPIC])
    replayed = 0

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                break
            if msg.error():
                print("[replay] consume error:", msg.error())
                continue

            payload = json.loads(msg.value().decode("utf-8"))
            retry_count = int(payload.get("retry_count", 0))
            error_type = payload.get("error_type")

            if error_type == "parse_error":
                print(f"[replay] skip malformed message from {DLQ_TOPIC}")
                consumer.commit(message=msg, asynchronous=False)
                continue

            if retry_count >= MAX_RETRY_COUNT:
                print(f"[replay] skip message over max retry from {DLQ_TOPIC} retry_count={retry_count}")
                consumer.commit(message=msg, asynchronous=False)
                continue

            if error_type == "raw_write_error":
                original_lines = payload["original_lines"]

                for line in original_lines:
                    producer.produce(TARGET_TOPIC, value=line.encode("utf-8"))
                producer.flush()

                consumer.commit(message=msg, asynchronous=False)
                replayed += len(original_lines)
                print(f"[replay] resent {len(original_lines)} messages from {DLQ_TOPIC} -> {TARGET_TOPIC} retry_count={retry_count + 1}")
                continue

    finally:
        consumer.close()
        print(f"[replay] done. replayed={replayed}")


if __name__ == "__main__":
    main()