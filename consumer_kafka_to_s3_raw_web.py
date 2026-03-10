import json
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime, timezone

import boto3
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

load_dotenv()

# Kafka
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC_WEB_EVENTS", "web.user_events")
DLQ_TOPIC = os.environ.get("KAFKA_DLQ_TOPIC_WEB", "web.user_events_dlq")

# S3
AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_PREFIX_WEB", "raw/web/events")

# Buffer flush policy
MAX_RECORDS = int(os.environ.get("S3_FLUSH_MAX_RECORDS", "200"))
MAX_SECONDS = int(os.environ.get("S3_FLUSH_MAX_SECONDS", "5"))


def dt_hour_from_occurred_at(occurred_at: str) -> tuple[str, str]:
    if not occurred_at or len(occurred_at) < 13:
        return ("1970-01-01", "00")
    return (occurred_at[:10], occurred_at[11:13])


def build_s3_key(dt: str, hour: str) -> str:
    ts_ms = int(time.time() * 1000)
    return f"{S3_PREFIX}/dt={dt}/hour={hour}/part-{ts_ms}-{uuid.uuid4().hex}.jsonl"


@dataclass
class Buffer:
    dt: Optional[str] = None
    hour: Optional[str] = None
    lines: List[str] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)

    def reset(self):
        self.dt = None
        self.hour = None
        self.lines.clear()
        self.created_at = time.time()


def upload_lines(s3, dt: str, hour: str, lines: List[str]) -> str:
    key = build_s3_key(dt, hour)
    body = ("\n".join(lines) + "\n").encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/x-ndjson")
    return key


def main():
    s3 = boto3.client("s3", region_name=AWS_REGION)

    dlq_producer = Producer({"bootstrap.servers": BOOTSTRAP})

    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "raw-writer-web-s3-v1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 10 * 60 * 1000,
    })
    c.subscribe([TOPIC])

    print(f"[consumer-s3-web] topic={TOPIC}, bootstrap={BOOTSTRAP}")
    print(f"[consumer-s3-web] s3://{S3_BUCKET}/{S3_PREFIX}/dt=YYYY-MM-DD/hour=HH/part-*.jsonl")
    print(f"[consumer-s3-web] flush: max_records={MAX_RECORDS}, max_seconds={MAX_SECONDS}")
    print(f"[consumer-web] dlq_topic={DLQ_TOPIC}")

    buf = Buffer()
    last_msg = None

    def should_flush() -> bool:
        if not buf.lines:
            return False
        if len(buf.lines) >= MAX_RECORDS:
            return True
        if (time.time() - buf.created_at) >= MAX_SECONDS:
            return True
        return False

    def flush_and_commit():
        nonlocal last_msg
        if not buf.lines or buf.dt is None or buf.hour is None:
            return
        
        try:
            key = upload_lines(s3, buf.dt, buf.hour, buf.lines)
            print(f"[flush-web] uploaded {len(buf.lines)} records -> s3://{S3_BUCKET}/{key}")

            # 업로드 성공 후에만 commit
            if last_msg is not None:
                c.commit(message=last_msg, asynchronous=False)
                last_msg = None

            buf.reset()
        except Exception as e:
            print("[DLQ-web] failed to write raw output:", e)

            dlq_payload = {
                "error_type": "raw_write_error",
                "retry_count": 0,
                "original_lines": buf.lines,
                "error": str(e),
                "failed_at": time.time(),
                "source_topic": TOPIC,
                "stage": "raw_write",
                "dt": buf.dt,
                "hour": buf.hour,
            }
            dlq_producer.produce(
                DLQ_TOPIC,
                value=json.dumps(dlq_payload, ensure_ascii=False).encode("utf-8")   
            )
            dlq_producer.flush()

            if last_msg is not None:
                c.commit(message=last_msg, asynchronous=False)
                last_msg = None

            buf.reset()

    try:
        while True:
            msg = c.poll(1.0)

            if should_flush():
                flush_and_commit()

            if msg is None:
                continue
            if msg.error():
                print("[consumer-s3-web] error:", msg.error())
                continue

            try:
                event = json.loads(msg.value().decode("utf-8"))
            
            except Exception as e:
                print("[DLQ-web] failed to parse:", e)

                dlq_payload = {
                    "error_type": "parse_error",
                    "retry_count": 0,
                    "original_value": msg.value().decode("utf-8", errors="replace"),
                    "error": str(e),
                    "failed_at": time.time(),
                    "source_topic": TOPIC,
                }

                dlq_producer.produce(
                    DLQ_TOPIC,
                    value=json.dumps(dlq_payload, ensure_ascii=False).encode("utf-8")
                )
                dlq_producer.flush()

                c.commit(message=msg, asynchronous=False)
                continue

            ingested_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            event["ingested_at"] = ingested_at

            dt, hour = dt_hour_from_occurred_at(ingested_at)

            if buf.dt is None:
                buf.dt, buf.hour = dt, hour

            if (dt != buf.dt) or (hour != buf.hour):
                flush_and_commit()
                buf.dt, buf.hour = dt, hour

            buf.lines.append(json.dumps(event, ensure_ascii=False))
            last_msg = msg

            if should_flush():
                flush_and_commit()

    except KeyboardInterrupt:
        print("\n[consumer-s3-web] stopping... final flush")
        if buf.lines:
            flush_and_commit()
    finally:
        c.close()


if __name__ == "__main__":
    main()