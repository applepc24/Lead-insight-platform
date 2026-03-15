import json
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import List, Optional

import boto3
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv
from pathlib import Path


load_dotenv()

# Kafka
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC_STRIPE_INVOICE", "stripe.invoice_events")
DLQ_TOPIC = os.environ.get("KAFKA_DLQ_TOPIC", "stripe.invoice_events_dlq")

# S3
AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ.get("S3_PREFIX", "raw/stripe/invoices")

# Buffer flush policy
MAX_RECORDS = int(os.environ.get("S3_FLUSH_MAX_RECORDS", "200"))   # N개 모이면 업로드
MAX_SECONDS = int(os.environ.get("S3_FLUSH_MAX_SECONDS", "5"))     # 또는 T초마다 업로드

OUTPUT_MODE = os.getenv("OUTPUT_MODE", "s3")  # s3 | local
LOCAL_OUTPUT_DIR = os.getenv("LOCAL_OUTPUT_DIR", "tmp_e2e_raw")

def dt_hour_from_occurred_at(occurred_at: str) -> tuple[str, str]:
    # "2026-03-04T09:32:14Z" -> ("2026-03-04", "09")
    if not occurred_at or len(occurred_at) < 13:
        return ("1970-01-01", "00")
    return (occurred_at[:10], occurred_at[11:13])

def build_s3_key(dt: str, hour: str) -> str:
    # 충돌 방지: unique file name
    # raw/stripe/invoices/dt=YYYY-MM-DD/hour=HH/part-<unixms>-<uuid>.jsonl
    ts_ms = int(time.time() * 1000)
    return f"{S3_PREFIX}/dt={dt}/hour={hour}/part-{ts_ms}-{uuid.uuid4().hex}.jsonl"

@dataclass
class Buffer:
    dt: Optional[str] = None
    hour: Optional[str] = None
    lines: List[str] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    retry_count: int = 0

    def reset(self):
        self.dt = None
        self.hour = None
        self.lines.clear()
        self.created_at = time.time()
        self.retry_count = 0

def upload_lines(s3, dt: str, hour: str, lines: List[str]) -> str:
    key = build_s3_key(dt, hour)
    body = ("\n".join(lines) + "\n").encode("utf-8")
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body, ContentType="application/x-ndjson")
    return key


def write_lines_to_local(base_dir: str, dt: str, hour: str, lines: List[str]) -> str:
    ts_ms = int(time.time() * 1000)
    file_dir = Path(base_dir) / f"dt={dt}" / f"hour={hour}"
    file_dir.mkdir(parents=True, exist_ok=True)

    file_path = file_dir / f"part-{ts_ms}-{uuid.uuid4().hex}.jsonl"
    body = "\n".join(lines) + "\n"
    file_path.write_text(body, encoding="utf-8")

    return str(file_path)

def main():
    s3 = boto3.client("s3", region_name=AWS_REGION)

    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "raw-writer-s3-v2",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        # 안정성: 처리 시간이 좀 길어져도 리밸런스 안 나게 넉넉히
        "max.poll.interval.ms": 10 * 60 * 1000,
    })
    c.subscribe([TOPIC])

    dlq_producer = Producer({"bootstrap.servers": BOOTSTRAP})

    print(f"[consumer-s3] topic={TOPIC}, bootstrap={BOOTSTRAP}")
    print(f"[consumer-s3] dlq_topic={DLQ_TOPIC}")
    print(f"[consumer-s3] s3://{S3_BUCKET}/{S3_PREFIX}/dt=YYYY-MM-DD/hour=HH/part-*.jsonl")
    print(f"[consumer-s3] flush: max_records={MAX_RECORDS}, max_seconds={MAX_SECONDS}")

    buf = Buffer()
    last_msg = None
    last_flush = time.time()

    def should_flush() -> bool:
        if not buf.lines:
            return False
        if len(buf.lines) >= MAX_RECORDS:
            return True
        if (time.time() - buf.created_at) >= MAX_SECONDS:
            return True
        return False

    def flush_and_commit():
        nonlocal last_flush, last_msg
        if not buf.lines or buf.dt is None or buf.hour is None:
            return
        try:
            if OUTPUT_MODE == "local":
                key = write_lines_to_local(LOCAL_OUTPUT_DIR, buf.dt, buf.hour, buf.lines)
                print(f"[flush] wrote {len(buf.lines)} records -> {key}")
            else:
                key = upload_lines(s3, buf.dt, buf.hour, buf.lines)
                print(f"[flush] uploaded {len(buf.lines)} records -> s3://{S3_BUCKET}/{key}")
            
            if last_msg is not None:
                c.commit(message=last_msg, asynchronous=False)
                last_msg = None
            buf.reset()
            last_flush = time.time()
        except Exception as e:
            print("[DLQ] failed to write raw output:", e)
            dlq_payload = {
                "error_type": "raw_write_error",
                "retry_count": buf.retry_count,
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
            last_flush = time.time()

    try:
        while True:
            msg = c.poll(1.0)

            # 주기적으로 flush (메시지가 안 와도)
            if should_flush():
                flush_and_commit()

            if msg is None:
                continue
            if msg.error():
                print("[consumer-s3] error:", msg.error())
                continue
            headers = dict(msg.headers() or [])
            retry_count = int(headers.get("retry_count", b"0").decode("utf-8"))

            try:
                event = json.loads(msg.value().decode("utf-8"))
                if event.get("event_source") != "stripe" or event.get("event_name") != "invoice_paid":
                    c.commit(message=msg, asynchronous=False)
                    continue
            except Exception as e:
                print("[DLQ] failed to parse:", e)

                dlq_payload = {
                    "error_type": "parse_error",
                    "retry_count": retry_count,
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

            occurred_at = event.get("occurred_at") or "1970-01-01T00:00:00Z"
            dt, hour = dt_hour_from_occurred_at(occurred_at)

            # 버퍼가 비어있으면 파티션 키 세팅
            if buf.dt is None:
                buf.dt, buf.hour = dt, hour
                buf.retry_count = retry_count

            # dt/hour가 바뀌면 먼저 flush하고 새 버퍼로
            if (dt != buf.dt) or (hour != buf.hour):
                flush_and_commit()
                buf.dt, buf.hour = dt, hour
                buf.retry_count = retry_count

            buf.lines.append(json.dumps(event, ensure_ascii=False))
            last_msg = msg

            # 조건 충족 시 flush
            if should_flush():
                flush_and_commit()

    except KeyboardInterrupt:
        print("\n[consumer-s3] stopping... final flush")
        # 남은 거 flush
        if buf.lines:
            flush_and_commit()
    finally:
        c.close()

if __name__ == "__main__":
    main()