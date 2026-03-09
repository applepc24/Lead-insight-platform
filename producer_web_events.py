import json
import os
import random
import time
import uuid
from datetime import datetime, timezone, timedelta

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("KAFKA_TOPIC_WEB_EVENTS", "web.user_events")

# UTC 기준으로 생성하지만, 트래픽 패턴은 KST(한국 시간) 기준으로 흉내냄
KST_OFFSET_HOURS = 9

CUSTOMER_PROFILES = {
    "user@company.com": {
        "weights": {
            "pricing_view": 0.35,
            "docs_view": 0.35,
            "signup_attempt": 0.10,
            "login": 0.20,
        }
    },
    "alpha@startup.com": {
        "weights": {
            "pricing_view": 0.20,
            "docs_view": 0.50,
            "signup_attempt": 0.10,
            "login": 0.20,
        }
    },
    "beta@corp.com": {
        "weights": {
            "pricing_view": 0.15,
            "docs_view": 0.25,
            "signup_attempt": 0.10,
            "login": 0.50,
        }
    },
}

p = Producer({"bootstrap.servers": BOOTSTRAP})


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_kst() -> datetime:
    return now_utc() + timedelta(hours=KST_OFFSET_HOURS)


def to_iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def choose_customer() -> str:
    # 고객별 전체 유입 비중
    customers = list(CUSTOMER_PROFILES.keys())
    weights = [0.5, 0.25, 0.25]  # user@company.com이 좀 더 자주 들어오게
    return random.choices(customers, weights=weights, k=1)[0]


def choose_event_for_customer(customer: str) -> str:
    profile = CUSTOMER_PROFILES[customer]["weights"]
    event_names = list(profile.keys())
    weights = list(profile.values())
    return random.choices(event_names, weights=weights, k=1)[0]


def path_for_event(event_name: str) -> str:
    if event_name == "pricing_view":
        return "/pricing"
    if event_name == "docs_view":
        return "/docs"
    if event_name == "signup_attempt":
        return "/signup"
    return "/login"


def traffic_multiplier_by_kst_hour(hour: int) -> float:
    """
    시간대별 트래픽 강도.
    숫자가 클수록 sleep이 짧아져서 이벤트가 많이 발생.
    """
    if 0 <= hour < 6:
        return 0.3   # 새벽: 매우 적음
    if 6 <= hour < 9:
        return 0.7   # 출근 전후: 조금 증가
    if 9 <= hour < 12:
        return 1.4   # 오전 피크
    if 12 <= hour < 14:
        return 1.1   # 점심 시간
    if 14 <= hour < 18:
        return 1.5   # 오후 피크
    if 18 <= hour < 22:
        return 0.9   # 저녁
    return 0.5       # 늦은 밤


def next_sleep_seconds() -> float:
    """
    시간대별로 이벤트 도착 간격 조절.
    multiplier가 클수록 더 자주 발생.
    """
    hour = now_kst().hour
    mult = traffic_multiplier_by_kst_hour(hour)

    # 기본 평균 간격을 3.5초 정도로 두고, 시간대 multiplier 반영
    base = random.uniform(2.0, 5.0)
    sleep_sec = base / mult

    # 너무 짧거나 길지 않게 제한
    return max(0.8, min(sleep_sec, 10.0))


def maybe_burst() -> bool:
    """
    가끔 짧은 burst(몰림) 발생.
    """
    hour = now_kst().hour
    mult = traffic_multiplier_by_kst_hour(hour)

    # 피크 시간대일수록 burst 확률 증가
    burst_prob = min(0.05 * mult, 0.18)
    return random.random() < burst_prob


def send_one_event() -> None:
    customer = choose_customer()
    event_name = choose_event_for_customer(customer)
    occurred_at = now_utc()

    event = {
        "event_source": "web",
        "event_name": event_name,
        "event_id": f"evt_{uuid.uuid4().hex}",
        "customer_key": customer,
        "occurred_at": to_iso_z(occurred_at),
        "properties": {
            "path": path_for_event(event_name),
            "user_agent": "simulator",
            "traffic_hour_kst": now_kst().hour,
        },
    }

    p.produce(
        TOPIC,
        key=customer.encode("utf-8"),
        value=json.dumps(event, ensure_ascii=False).encode("utf-8"),
    )
    p.flush(1.0)

    print(
        f"[sent] {event['event_name']:<14} "
        f"{event['customer_key']:<20} "
        f"{event['occurred_at']} "
        f"(kst_hour={event['properties']['traffic_hour_kst']})"
    )


def main():
    print(f"[web-producer] bootstrap={BOOTSTRAP}, topic={TOPIC}")

    while True:
        # 가끔 burst: 짧은 시간에 3~8개 몰아서 생성
        if maybe_burst():
            burst_count = random.randint(3, 8)
            print(f"[burst] sending {burst_count} events")
            for _ in range(burst_count):
                send_one_event()
                time.sleep(random.uniform(0.1, 0.4))
        else:
            send_one_event()

        time.sleep(next_sleep_seconds())


if __name__ == "__main__":
    main()