from datetime import datetime, date, timedelta

def test_duplicate_stripe_input():
    duplicate_event = {
        "event_source": "stripe",
        "event_name": "invoice_paid",
        "event_id": "in_001",
        "customer_key": "user@company.com",
        "customer_key_fallback": "cus_001",
        "occurred_at": "2026-03-04T10:00:00",
        "amount": 49.0,
        "currency": "usd",
        "properties": {},
        "ingested_at": "2026-03-04T10:01:00",
        "dt": "2026-03-04",
    }

    events = [duplicate_event.copy() for _ in range(5)]

    assert len(events) == 5
    assert events[0]["event_id"] == "in_001"
    assert events[4]["event_id"] == "in_001"

def dedup_events(events):
    seen = set()
    result = []

    for event in events:
        key = (event["event_source"], event["event_id"])
        if key not in seen:
            seen.add(key)
            result.append(event)

    return result


def test_duplicate_stripe_event_should_be_deduped():
    duplicate_event = {
        "event_source": "stripe",
        "event_name": "invoice_paid",
        "event_id": "in_001",
        "customer_key": "user@company.com",
        "customer_key_fallback": "cus_001",
        "occurred_at": "2026-03-04T10:00:00",
        "amount": 49.0,
        "currency": "usd",
        "properties": {},
        "ingested_at": "2026-03-04T10:01:00",
        "dt": "2026-03-04",
    }

    events = [duplicate_event.copy() for _ in range(5)]

    deduped = dedup_events(events)

    assert len(deduped) == 1
    assert deduped[0]["event_id"] == "in_001"
    assert deduped[0]["customer_key"] == "user@company.com"
    assert deduped[0]["amount"] == 49.0


def build_mart_lead_summary(events, as_of_dt):
    summary = {}

    window_start = as_of_dt - timedelta(days=6)

    for event in events:
        occurred_date = datetime.fromisoformat(event["occurred_at"]).date()

        if not (window_start <= occurred_date <= as_of_dt):
            continue

        customer_key = event["customer_key"]

        if customer_key not in summary:
            summary[customer_key] = {
                "payment_total_7d": 0.0,
                "pricing_views_7d": 0,
                "docs_views_7d": 0,
                "payment_count_7d": 0,
                "lead_score": 0,
            }

        row = summary[customer_key]

        if event["event_name"] == "invoice_paid":
            row["payment_total_7d"] += event["amount"]
            row["payment_count_7d"] += 1
        elif event["event_name"] == "pricing_view":
            row["pricing_views_7d"] += 1
        elif event["event_name"] == "docs_view":
            row["docs_views_7d"] += 1

    for row in summary.values():
        row["lead_score"] = (
            row["pricing_views_7d"] * 5
            + row["docs_views_7d"] * 2
            + row["payment_count_7d"] * 10
        )

    return summary


def test_duplicate_stripe_event_should_not_inflate_mart():
    as_of_dt = date(2026, 3, 4)

    duplicate_event = {
        "event_source": "stripe",
        "event_name": "invoice_paid",
        "event_id": "in_001",
        "customer_key": "user@company.com",
        "customer_key_fallback": "cus_001",
        "occurred_at": "2026-03-04T10:00:00",
        "amount": 49.0,
        "currency": "usd",
        "properties": {},
        "ingested_at": "2026-03-04T10:01:00",
        "dt": "2026-03-04",
    }

    events = [duplicate_event.copy() for _ in range(5)]
    deduped = dedup_events(events)
    mart = build_mart_lead_summary(deduped, as_of_dt)

    row = mart["user@company.com"]

    assert row["payment_count_7d"] == 1
    assert row["payment_total_7d"] == 49.0
    assert row["lead_score"] == 10

def test_future_event_should_not_be_included_in_7d_window():
    as_of_dt = date(2026, 3, 4)

    today_event = {
        "event_source": "stripe",
        "event_name": "invoice_paid",
        "event_id": "in_today",
        "customer_key": "user@company.com",
        "customer_key_fallback": "cus_001",
        "occurred_at": "2026-03-04T10:00:00",
        "amount": 49.0,
        "currency": "usd",
        "properties": {},
        "ingested_at": "2026-03-04T10:01:00",
        "dt": "2026-03-04",
    }

    future_event = {
        "event_source": "stripe",
        "event_name": "invoice_paid",
        "event_id": "in_future",
        "customer_key": "user@company.com",
        "customer_key_fallback": "cus_001",
        "occurred_at": "2026-04-03T10:00:00",
        "amount": 99.0,
        "currency": "usd",
        "properties": {},
        "ingested_at": "2026-04-03T10:01:00",
        "dt": "2026-04-03",
    }

    events = [today_event, future_event]
    mart = build_mart_lead_summary(events, as_of_dt)

    row = mart["user@company.com"]

    assert row["payment_count_7d"] == 1
    assert row["payment_total_7d"] == 49.0
    assert row["lead_score"] == 10

def test_reprocessing_same_input_should_be_idempotent():
    as_of_dt = date(2026, 3, 4)

    events = [
        {
            "event_source": "stripe",
            "event_name": "invoice_paid",
            "event_id": "in_001",
            "customer_key": "user@company.com",
            "customer_key_fallback": "cus_001",
            "occurred_at": "2026-03-04T10:00:00",
            "amount": 49.0,
            "currency": "usd",
            "properties": {},
            "ingested_at": "2026-03-04T10:01:00",
            "dt": "2026-03-04",
        },
        {
            "event_source": "web",
            "event_name": "pricing_view",
            "event_id": "web_001",
            "customer_key": "user@company.com",
            "customer_key_fallback": None,
            "occurred_at": "2026-03-04T11:00:00",
            "amount": 0.0,
            "currency": None,
            "properties": {},
            "ingested_at": "2026-03-04T11:01:00",
            "dt": "2026-03-04",
        },
    ]

    first_run = build_mart_lead_summary(dedup_events(events), as_of_dt)
    second_run = build_mart_lead_summary(dedup_events(events), as_of_dt)

    assert first_run == second_run


def test_only_same_customer_key_should_merge_across_sources():
    as_of_dt = date(2026, 3, 4)

    events = [
        {
            "event_source": "web",
            "event_name": "pricing_view",
            "event_id": "web_001",
            "customer_key": "user@company.com",
            "customer_key_fallback": None,
            "occurred_at": "2026-03-04T09:00:00",
            "amount": 0.0,
            "currency": None,
            "properties": {},
            "ingested_at": "2026-03-04T09:01:00",
            "dt": "2026-03-04",
        },
        {
            "event_source": "stripe",
            "event_name": "invoice_paid",
            "event_id": "in_001",
            "customer_key": "user@company.com",
            "customer_key_fallback": "cus_001",
            "occurred_at": "2026-03-04T10:00:00",
            "amount": 49.0,
            "currency": "usd",
            "properties": {},
            "ingested_at": "2026-03-04T10:01:00",
            "dt": "2026-03-04",
        },
        {
            "event_source": "stripe",
            "event_name": "invoice_paid",
            "event_id": "in_002",
            "customer_key": "clock-user@company.com",
            "customer_key_fallback": "cus_002",
            "occurred_at": "2026-03-04T11:00:00",
            "amount": 99.0,
            "currency": "usd",
            "properties": {},
            "ingested_at": "2026-03-04T11:01:00",
            "dt": "2026-03-04",
        },
    ]

    mart = build_mart_lead_summary(dedup_events(events), as_of_dt)

    user_row = mart["user@company.com"]
    clock_user_row = mart["clock-user@company.com"]

    assert user_row["payment_count_7d"] == 1
    assert user_row["pricing_views_7d"] == 1
    assert user_row["lead_score"] == 15

    assert clock_user_row["payment_count_7d"] == 1
    assert clock_user_row["pricing_views_7d"] == 0
    assert clock_user_row["lead_score"] == 10

def test_duplicate_web_event_should_not_inflate_behavior_score():
    as_of_dt = date(2026, 3, 4)

    duplicate_event = {
        "event_source": "web",
        "event_name": "pricing_view",
        "event_id": "web_001",
        "customer_key": "user@company.com",
        "customer_key_fallback": None,
        "occurred_at": "2026-03-04T12:00:00",
        "amount": 0.0,
        "currency": None,
        "properties": {},
        "ingested_at": "2026-03-04T12:01:00",
        "dt": "2026-03-04",
    }

    events = [duplicate_event.copy() for _ in range(10)]

    deduped = dedup_events(events)
    mart = build_mart_lead_summary(deduped, as_of_dt)

    row = mart["user@company.com"]

    assert row["pricing_views_7d"] == 1
    assert row["docs_views_7d"] == 0
    assert row["lead_score"] == 5

def test_event_order_should_not_change_final_mart_result():
    as_of_dt = date(2026, 3, 4)

    pricing_event = {
        "event_source": "web",
        "event_name": "pricing_view",
        "event_id": "web_001",
        "customer_key": "user@company.com",
        "customer_key_fallback": None,
        "occurred_at": "2026-03-04T09:00:00",
        "amount": 0.0,
        "currency": None,
        "properties": {},
        "ingested_at": "2026-03-04T09:01:00",
        "dt": "2026-03-04",
    }

    payment_event = {
        "event_source": "stripe",
        "event_name": "invoice_paid",
        "event_id": "in_001",
        "customer_key": "user@company.com",
        "customer_key_fallback": "cus_001",
        "occurred_at": "2026-03-04T10:00:00",
        "amount": 49.0,
        "currency": "usd",
        "properties": {},
        "ingested_at": "2026-03-04T10:01:00",
        "dt": "2026-03-04",
    }

    docs_event = {
        "event_source": "web",
        "event_name": "docs_view",
        "event_id": "web_002",
        "customer_key": "user@company.com",
        "customer_key_fallback": None,
        "occurred_at": "2026-03-04T11:00:00",
        "amount": 0.0,
        "currency": None,
        "properties": {},
        "ingested_at": "2026-03-04T11:01:00",
        "dt": "2026-03-04",
    }

    events_a = [pricing_event, payment_event, docs_event]
    events_b = [payment_event, docs_event, pricing_event]

    mart_a = build_mart_lead_summary(dedup_events(events_a), as_of_dt)
    mart_b = build_mart_lead_summary(dedup_events(events_b), as_of_dt)

    row_a = mart_a["user@company.com"]
    row_b = mart_b["user@company.com"]

    assert mart_a == mart_b
    assert row_a["payment_count_7d"] == 1
    assert row_a["payment_total_7d"] == 49.0
    assert row_a["pricing_views_7d"] == 1
    assert row_a["docs_views_7d"] == 1
    assert row_a["lead_score"] == 17

def test_7day_window_boundary_should_be_exact():
    as_of_dt = date(2026, 3, 4)

    events = [
        {
            "event_source": "stripe",
            "event_name": "invoice_paid",
            "event_id": "in_today",
            "customer_key": "user@company.com",
            "customer_key_fallback": "cus_001",
            "occurred_at": "2026-03-04T10:00:00",  # as_of_dt
            "amount": 10.0,
            "currency": "usd",
            "properties": {},
            "ingested_at": "2026-03-04T10:01:00",
            "dt": "2026-03-04",
        },
        {
            "event_source": "stripe",
            "event_name": "invoice_paid",
            "event_id": "in_minus_6",
            "customer_key": "user@company.com",
            "customer_key_fallback": "cus_001",
            "occurred_at": "2026-02-26T10:00:00",  # as_of_dt - 6일
            "amount": 20.0,
            "currency": "usd",
            "properties": {},
            "ingested_at": "2026-02-26T10:01:00",
            "dt": "2026-02-26",
        },
        {
            "event_source": "stripe",
            "event_name": "invoice_paid",
            "event_id": "in_minus_7",
            "customer_key": "user@company.com",
            "customer_key_fallback": "cus_001",
            "occurred_at": "2026-02-25T10:00:00",  # as_of_dt - 7일
            "amount": 30.0,
            "currency": "usd",
            "properties": {},
            "ingested_at": "2026-02-25T10:01:00",
            "dt": "2026-02-25",
        },
        {
            "event_source": "stripe",
            "event_name": "invoice_paid",
            "event_id": "in_minus_8",
            "customer_key": "user@company.com",
            "customer_key_fallback": "cus_001",
            "occurred_at": "2026-02-24T10:00:00",  # as_of_dt - 8일
            "amount": 40.0,
            "currency": "usd",
            "properties": {},
            "ingested_at": "2026-02-24T10:01:00",
            "dt": "2026-02-24",
        },
    ]

    mart = build_mart_lead_summary(dedup_events(events), as_of_dt)
    row = mart["user@company.com"]

    # 포함: 10 + 20
    assert row["payment_count_7d"] == 2
    assert row["payment_total_7d"] == 30.0
    assert row["lead_score"] == 20

def test_partial_failure_then_retry_should_converge_to_same_result():
    as_of_dt = date(2026, 3, 4)

    first_batch = [
        {
            "event_source": "stripe",
            "event_name": "invoice_paid",
            "event_id": "in_001",
            "customer_key": "user@company.com",
            "customer_key_fallback": "cus_001",
            "occurred_at": "2026-03-04T10:00:00",
            "amount": 49.0,
            "currency": "usd",
            "properties": {},
            "ingested_at": "2026-03-04T10:01:00",
            "dt": "2026-03-04",
        },
        {
            "event_source": "web",
            "event_name": "pricing_view",
            "event_id": "web_001",
            "customer_key": "user@company.com",
            "customer_key_fallback": None,
            "occurred_at": "2026-03-04T11:00:00",
            "amount": 0.0,
            "currency": None,
            "properties": {},
            "ingested_at": "2026-03-04T11:01:00",
            "dt": "2026-03-04",
        },
    ]

    second_batch = [
        {
            "event_source": "web",
            "event_name": "docs_view",
            "event_id": "web_002",
            "customer_key": "user@company.com",
            "customer_key_fallback": None,
            "occurred_at": "2026-03-04T12:00:00",
            "amount": 0.0,
            "currency": None,
            "properties": {},
            "ingested_at": "2026-03-04T12:01:00",
            "dt": "2026-03-04",
        },
        {
            "event_source": "stripe",
            "event_name": "invoice_paid",
            "event_id": "in_002",
            "customer_key": "user@company.com",
            "customer_key_fallback": "cus_001",
            "occurred_at": "2026-03-04T13:00:00",
            "amount": 99.0,
            "currency": "usd",
            "properties": {},
            "ingested_at": "2026-03-04T13:01:00",
            "dt": "2026-03-04",
        },
    ]

    # 정상 전체 처리
    full_run_events = first_batch + second_batch
    full_run_result = build_mart_lead_summary(dedup_events(full_run_events), as_of_dt)

    # 부분 처리 후 실패 -> 재실행
    partial_run_events = first_batch
    _partial_result = build_mart_lead_summary(dedup_events(partial_run_events), as_of_dt)

    retried_events = first_batch + second_batch
    retried_result = build_mart_lead_summary(dedup_events(retried_events), as_of_dt)

    assert full_run_result == retried_result

    row = retried_result["user@company.com"]
    assert row["payment_count_7d"] == 2
    assert row["payment_total_7d"] == 148.0
    assert row["pricing_views_7d"] == 1
    assert row["docs_views_7d"] == 1
    assert row["lead_score"] == 27