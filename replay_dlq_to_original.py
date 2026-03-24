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
    print(f"[replay] bootstrap={BOOTSTRAP}")
    print(f"[replay] dlq_topic={DLQ_TOPIC}")
    print(f"[replay] target_topic={TARGET_TOPIC}")
    print(f"[replay] group_id={GROUP_ID}")


    consumer.subscribe([DLQ_TOPIC])
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

            payload = json.loads(msg.value().decode("utf-8"))
            job = payload.get("job", {})
            failed_stage = payload.get("failed_stage")
            error_type = payload.get("error_type")
            retry_count = int(job.get("retry_count", 0))
            
            if not job:
                print("[replay] skip: no job in payload")
                consumer.commit(message=msg, asynchronous=False)
                skipped += 1
                continue

            if retry_count >= MAX_RETRY_COUNT:
                print(f"[replay] skip: retry limit exceed job_id = {job.get('job_id')} retry_count={retry_count}")
                consumer.commit(message=msg, asynchronous=False)
                skipped += 1
                continue
            
            if failed_stage != "fetch":
                print(f"[replay] skip: unsupported failed stage= {failed_stage} job_id={job.get('job_id')} retry_count={retry_count}")
                consumer.commit(message=msg, asynchronous=False)
                skipped += 1
                continue

            producer.produce(
                TARGET_TOPIC,
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