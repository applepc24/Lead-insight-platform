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
    def __init__(self, value):
        # bytes 를 그대로 실을 수 있어야 오염 메시지를 재현할 수 있다.
        if isinstance(value, (bytes, bytearray)):
            self._value = bytes(value)
        else:
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
            "refetch_canonical_url",
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
            "refetch_canonical_url",
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

def stub_happy_path(monkeypatch, uploaded):
    """정상 처리 경로를 전부 스텁으로 대체한다 (오염 메시지 테스트의 대조군용)."""
    monkeypatch.setattr(worker, "s3_object_exists", lambda s3, bucket, key: False)
    monkeypatch.setattr(worker, "fetch_html", lambda session, url: "<html>ok</html>")
    monkeypatch.setattr(worker, "refetch_canonical_url", lambda session, url, html: html)
    monkeypatch.setattr(
        worker, "upload_raw_html_to_s3",
        lambda s3, bucket, key, html: uploaded.append(key),
    )
    monkeypatch.setattr(
        worker, "upload_json_to_s3",
        lambda s3, bucket, key, data: uploaded.append(key),
    )
    monkeypatch.setattr(
        worker, "build_processed_document", lambda job, s3_paths, html: {"kind": "processed"}
    )
    monkeypatch.setattr(
        worker, "build_curated_document", lambda job, s3_paths, html: {"kind": "curated"}
    )


POISON_MESSAGES = [
    pytest.param(b"{not valid json", id="깨진_JSON"),
    pytest.param(b"\xff\xfe invalid utf-8", id="UTF8_아님"),
    pytest.param(json.dumps([1, 2, 3]).encode("utf-8"), id="dict_아님"),
    pytest.param(json.dumps("문자열").encode("utf-8"), id="문자열_JSON"),
    pytest.param(json.dumps({"job_id": "only-id"}).encode("utf-8"), id="필수필드_누락"),
    pytest.param(b"", id="빈_메시지"),
    # 키는 있지만 값이 비어 있는 경우. `field not in job` 으로 검사하면 통과해버려서
    # fetch_html("") 까지 흘러간 뒤에야 실패한다 — 실패 지점만 뒤로 밀릴 뿐이다.
    pytest.param(
        json.dumps({**make_job(), "url": ""}).encode("utf-8"), id="url_이_빈문자열"
    ),
    pytest.param(
        json.dumps({**make_job(), "url": None}).encode("utf-8"), id="url_이_null"
    ),
    pytest.param(
        json.dumps({**make_job(), "collected_at": ""}).encode("utf-8"),
        id="collected_at_이_빈문자열",
    ),
]


class TestPoisonMessageHandling:
    """오염된 메시지 한 건이 파이프라인 전체를 멈추면 안 된다.

    해석 단계(decode/json/필수 필드/날짜 형식)가 try 밖에 있으면 예외가 main 루프를
    빠져나가 워커가 죽는다. 이때 오프셋은 커밋되지 않았으므로 재시작하면 같은 메시지를
    다시 읽고 또 죽는다 — 무한 크래시 루프이고, 뒤에 쌓인 정상 공고는 영원히 처리되지
    않는다. fetch·업로드 실패에는 이미 DLQ 경로가 있는데 해석 단계에만 없었다.
    """

    @pytest.mark.parametrize("raw", POISON_MESSAGES)
    def test_poison_message_should_not_kill_worker(self, monkeypatch, scenario_env, raw):
        env = scenario_env(jobs=[raw])
        stub_happy_path(monkeypatch, [])

        # KeyboardInterrupt 는 FakeConsumer 가 메시지를 다 소진했다는 신호다.
        # 다른 예외로 빠져나오면 워커가 죽은 것이다.
        with pytest.raises(KeyboardInterrupt):
            worker.main()

    @pytest.mark.parametrize("raw", POISON_MESSAGES)
    def test_poison_message_should_be_committed(self, monkeypatch, scenario_env, raw):
        """커밋하지 않으면 재시작 때 같은 메시지를 다시 읽어 영원히 막힌다."""
        env = scenario_env(jobs=[raw])
        stub_happy_path(monkeypatch, [])

        with pytest.raises(KeyboardInterrupt):
            worker.main()

        assert len(env["consumer"].committed) == 1

    @pytest.mark.parametrize("raw", POISON_MESSAGES)
    def test_poison_message_should_go_to_dlq(self, monkeypatch, scenario_env, raw):
        """조용히 버리면 안 된다 — 무엇이 왜 들어왔는지 남겨야 원인을 찾는다."""
        env = scenario_env(jobs=[raw])
        stub_happy_path(monkeypatch, [])

        with pytest.raises(KeyboardInterrupt):
            worker.main()

        produced = env["producer"].produced
        assert len(produced) == 1
        assert produced[0]["topic"] == "job_postings.dlq"

        payload = json.loads(produced[0]["value"].decode("utf-8"))
        assert payload["failed_stage"] == "parse"
        assert payload["error_message"]

    @pytest.mark.parametrize("raw", POISON_MESSAGES)
    def test_poison_message_should_not_block_following_jobs(
        self, monkeypatch, scenario_env, raw
    ):
        """이 테스트가 이 결함의 실제 피해를 재현한다.

        오염 메시지 뒤에 줄 서 있던 정상 공고가 처리되는지를 본다.
        """
        env = scenario_env(jobs=[raw, make_job(job_id="job-after-poison")])
        uploaded = []
        stub_happy_path(monkeypatch, uploaded)

        with pytest.raises(KeyboardInterrupt):
            worker.main()

        assert any("job-after-poison" in key for key in uploaded), (
            "오염 메시지 뒤의 정상 공고가 처리되지 않았다 — 파이프라인이 막힌 것이다"
        )
        assert len(env["consumer"].committed) == 2

    def test_invalid_collected_at_should_go_to_dlq_instead_of_crashing(
        self, monkeypatch, scenario_env
    ):
        """build_s3_paths 의 fromisoformat 이 ValueError 를 던지는 경로.

        JSON 으로는 멀쩡하고 필수 필드도 다 있어서 해석 단계는 통과한다.
        S3 경로를 만드는 시점에야 깨지므로 별도 방어가 필요하다.
        """
        env = scenario_env(jobs=[make_job(collected_at="어제")])
        stub_happy_path(monkeypatch, [])

        with pytest.raises(KeyboardInterrupt):
            worker.main()

        assert len(env["consumer"].committed) == 1
        assert len(env["producer"].produced) == 1

        payload = json.loads(env["producer"].produced[0]["value"].decode("utf-8"))
        assert payload["failed_stage"] == "parse"

    @pytest.mark.parametrize("raw", POISON_MESSAGES)
    def test_dlq_record_must_stay_readable_by_dlq_consumers(
        self, monkeypatch, scenario_env, raw
    ):
        """DLQ 레코드의 job 필드는 반드시 dict 여야 한다.

        replay_dlq_to_original.py 와 consumer_dlq_to_bigquery.py 는 둘 다
        `payload.get("job", {})` 로 꺼낸 뒤 곧바로 `job.get(...)` 을 부른다.
        job 을 null 로 실어 보내면 기본값이 적용되지 않아 (키가 있으므로) None 이
        반환되고, DLQ 소비자 쪽이 AttributeError 로 죽는다 — 결함을 옮기는 셈이다.
        """
        env = scenario_env(jobs=[raw])
        stub_happy_path(monkeypatch, [])

        with pytest.raises(KeyboardInterrupt):
            worker.main()

        payload = json.loads(env["producer"].produced[0]["value"].decode("utf-8"))

        job = payload.get("job", {})
        assert isinstance(job, dict), "job 이 dict 가 아니면 DLQ 소비자가 죽는다"
        assert job.get("retry_count", 0) == 0
