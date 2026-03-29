import importlib
import json
import sys
from pathlib import Path

import pytest
import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

MODULE_UNDER_TEST = "worker_job_postings_to_s3"
worker = importlib.import_module(MODULE_UNDER_TEST)


def make_job(
    *,
    job_id: str = "job-123",
    source: str = "seed",
    url: str = "https://www.wanted.co.kr/wd/242151",
    collected_at: str = "2026-03-24T10:53:33.864638Z",
    retry_count: int = 0,
) -> dict:
    return {
        "job_id": job_id,
        "source": source,
        "url": url,
        "collected_at": collected_at,
        "retry_count": retry_count,
    }


class FakeMsg:
    def __init__(self, value: dict):
        self._value = json.dumps(value, ensure_ascii=False).encode("utf-8")

    def value(self):
        return self._value

    def error(self):
        return None


class FakeConsumer:
    def __init__(self, jobs):
        self.jobs = list(jobs)
        self.subscribed = None
        self.committed = []
        self.closed = False

    def subscribe(self, topics):
        self.subscribed = topics

    def poll(self, timeout):
        if self.jobs:
            return FakeMsg(self.jobs.pop(0))
        raise KeyboardInterrupt()

    def commit(self, message=None, asynchronous=False):
        self.committed.append(
            {
                "message": message,
                "asynchronous": asynchronous,
            }
        )

    def close(self):
        self.closed = True


class FakeProducer:
    def __init__(self):
        self.produced = []
        self.flush_count = 0

    def produce(self, topic, key=None, value=None):
        self.produced.append(
            {
                "topic": topic,
                "key": key,
                "value": value,
            }
        )

    def flush(self):
        self.flush_count += 1


class FakeS3Client:
    pass


class FakeHttpSession:
    pass


@pytest.fixture
def scenario_env(monkeypatch):
    created = {}

    def _setup(*, jobs):
        consumer = FakeConsumer(jobs)
        producer = FakeProducer()
        s3_client = FakeS3Client()
        http_session = FakeHttpSession()

        monkeypatch.setattr(worker, "FETCH_TOPIC", "job_postings.fetch_jobs")
        monkeypatch.setattr(worker, "DLQ_TOPIC", "job_postings.dlq")
        monkeypatch.setattr(worker, "S3_BUCKET", "test-bucket")
        monkeypatch.setattr(worker, "MAX_RETRY_COUNT", 3)

        monkeypatch.setattr(worker, "create_consumer", lambda: consumer)
        monkeypatch.setattr(worker, "create_producer", lambda: producer)
        monkeypatch.setattr(worker, "create_s3_client", lambda: s3_client)
        monkeypatch.setattr(worker, "create_http_session", lambda: http_session)

        created["consumer"] = consumer
        created["producer"] = producer
        created["s3_client"] = s3_client
        created["http_session"] = http_session
        return created

    return _setup


class TestWorkerScenarios:
    def test_normal_job_should_upload_all_outputs_and_commit_once(self, monkeypatch, scenario_env):
        env = scenario_env(jobs=[make_job()])

        uploaded = []

        monkeypatch.setattr(worker, "s3_object_exists", lambda s3, bucket, key: False)
        monkeypatch.setattr(worker, "fetch_html", lambda session, url: "<html>ok</html>")
        monkeypatch.setattr(
            worker,
            "refetch_saramin_with_canonical",
            lambda session, url, html: html,
        )

        def fake_upload_json_to_s3(s3_client, bucket, target_key, data):
            uploaded.append(("json", bucket, target_key, data))

        def fake_upload_raw_html_to_s3(s3_client, bucket, key, html):
            uploaded.append(("raw", bucket, key, html))

        monkeypatch.setattr(worker, "upload_json_to_s3", fake_upload_json_to_s3)
        monkeypatch.setattr(worker, "upload_raw_html_to_s3", fake_upload_raw_html_to_s3)

        monkeypatch.setattr(
            worker,
            "build_processed_document",
            lambda job, s3_paths, html: {
                "kind": "processed",
                "job_id": job["job_id"],
            },
        )
        monkeypatch.setattr(
            worker,
            "build_curated_document",
            lambda job, s3_paths, html: {
                "kind": "curated",
                "posting_id": job["job_id"],
            },
        )

        with pytest.raises(KeyboardInterrupt):
            worker.main()

        consumer = env["consumer"]
        producer = env["producer"]

        assert consumer.subscribed == ["job_postings.fetch_jobs"]
        assert len(consumer.committed) == 1
        assert producer.produced == []

        assert len(uploaded) == 3
        assert uploaded[0][0] == "raw"
        assert uploaded[1][0] == "json"
        assert uploaded[2][0] == "json"

        raw_upload = uploaded[0]
        processed_upload = uploaded[1]
        curated_upload = uploaded[2]

        assert raw_upload[1] == "test-bucket"
        assert processed_upload[1] == "test-bucket"
        assert curated_upload[1] == "test-bucket"

        assert raw_upload[2].endswith(".html")
        assert processed_upload[2].endswith(".json")
        assert curated_upload[2].endswith(".json")

        assert processed_upload[3]["kind"] == "processed"
        assert curated_upload[3]["kind"] == "curated"

    def test_duplicate_job_should_skip_fetch_and_upload_but_commit(self, monkeypatch, scenario_env):
        env = scenario_env(jobs=[make_job()])

        fetch_called = {"value": False}
        upload_called = {"value": False}

        monkeypatch.setattr(worker, "s3_object_exists", lambda s3, bucket, key: True)

        def fake_fetch_html(session, url):
            fetch_called["value"] = True
            return "<html>should not happen</html>"

        def fake_upload_raw_html_to_s3(*args, **kwargs):
            upload_called["value"] = True

        def fake_upload_json_to_s3(*args, **kwargs):
            upload_called["value"] = True

        monkeypatch.setattr(worker, "fetch_html", fake_fetch_html)
        monkeypatch.setattr(worker, "upload_raw_html_to_s3", fake_upload_raw_html_to_s3)
        monkeypatch.setattr(worker, "upload_json_to_s3", fake_upload_json_to_s3)

        with pytest.raises(KeyboardInterrupt):
            worker.main()

        consumer = env["consumer"]
        producer = env["producer"]

        assert len(consumer.committed) == 1
        assert producer.produced == []
        assert fetch_called["value"] is False
        assert upload_called["value"] is False

    def test_fetch_failure_should_send_to_dlq_and_commit(self, monkeypatch, scenario_env):
        env = scenario_env(jobs=[make_job(url="https://invalid-url-test-1234.com")])

        monkeypatch.setattr(worker, "s3_object_exists", lambda s3, bucket, key: False)

        def fake_fetch_html(session, url):
            raise requests.exceptions.ConnectionError(
                "Failed to resolve 'invalid-url-test-1234.com'"
            )

        monkeypatch.setattr(worker, "fetch_html", fake_fetch_html)

        uploaded = {"value": False}

        def fake_upload_raw_html_to_s3(*args, **kwargs):
            uploaded["value"] = True

        def fake_upload_json_to_s3(*args, **kwargs):
            uploaded["value"] = True

        monkeypatch.setattr(worker, "upload_raw_html_to_s3", fake_upload_raw_html_to_s3)
        monkeypatch.setattr(worker, "upload_json_to_s3", fake_upload_json_to_s3)

        with pytest.raises(KeyboardInterrupt):
            worker.main()

        consumer = env["consumer"]
        producer = env["producer"]

        assert len(consumer.committed) == 1
        assert uploaded["value"] is False
        assert len(producer.produced) == 1
        assert producer.flush_count == 1

        dlq_record = producer.produced[0]
        assert dlq_record["topic"] == "job_postings.dlq"

        payload = json.loads(dlq_record["value"].decode("utf-8"))
        assert payload["failed_stage"] == "fetch"
        assert payload["error_type"] == "dns_error"
        assert payload["job"]["job_id"] == "job-123"
        assert payload["job"]["retry_count"] == 1

    def test_retry_limit_exceeded_should_skip_processing_and_commit(self, monkeypatch, scenario_env):
        env = scenario_env(jobs=[make_job(retry_count=3)])

        called = {
            "fetch": False,
            "upload": False,
        }

        def fake_fetch_html(session, url):
            called["fetch"] = True
            return "<html>unexpected</html>"

        def fake_upload_raw_html_to_s3(*args, **kwargs):
            called["upload"] = True

        def fake_upload_json_to_s3(*args, **kwargs):
            called["upload"] = True

        monkeypatch.setattr(worker, "fetch_html", fake_fetch_html)
        monkeypatch.setattr(worker, "upload_raw_html_to_s3", fake_upload_raw_html_to_s3)
        monkeypatch.setattr(worker, "upload_json_to_s3", fake_upload_json_to_s3)

        with pytest.raises(KeyboardInterrupt):
            worker.main()

        consumer = env["consumer"]
        producer = env["producer"]

        assert len(consumer.committed) == 1
        assert producer.produced == []
        assert called["fetch"] is False
        assert called["upload"] is False

    def test_raw_upload_failure_should_send_raw_upload_dlq_and_commit(self, monkeypatch, scenario_env):
        env = scenario_env(jobs=[make_job()])

        monkeypatch.setattr(worker, "s3_object_exists", lambda s3, bucket, key: False)
        monkeypatch.setattr(worker, "fetch_html", lambda session, url: "<html>ok</html>")
        monkeypatch.setattr(
            worker,
            "refetch_saramin_with_canonical",
            lambda session, url, html: html,
        )

        def fake_upload_raw_html_to_s3(*args, **kwargs):
            raise RuntimeError("raw upload failed intentionally")

        processed_called = {"value": False}
        curated_called = {"value": False}

        def fake_upload_json_to_s3(s3_client, bucket, target_key, data):
            if data.get("kind") == "processed":
                processed_called["value"] = True
            if data.get("kind") == "curated":
                curated_called["value"] = True

        monkeypatch.setattr(worker, "upload_raw_html_to_s3", fake_upload_raw_html_to_s3)
        monkeypatch.setattr(worker, "upload_json_to_s3", fake_upload_json_to_s3)

        monkeypatch.setattr(
            worker,
            "build_processed_document",
            lambda job, s3_paths, html: {"kind": "processed"},
        )
        monkeypatch.setattr(
            worker,
            "build_curated_document",
            lambda job, s3_paths, html: {"kind": "curated"},
        )

        with pytest.raises(KeyboardInterrupt):
            worker.main()

        consumer = env["consumer"]
        producer = env["producer"]

        assert len(consumer.committed) == 1
        assert processed_called["value"] is False
        assert curated_called["value"] is False

        assert len(producer.produced) == 1
        payload = json.loads(producer.produced[0]["value"].decode("utf-8"))
        assert payload["failed_stage"] == "raw_upload"
        assert payload["error_type"] == "RuntimeError"
        assert payload["job"]["retry_count"] == 1