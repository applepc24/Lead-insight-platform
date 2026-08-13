"""DLQ 소비자 두 개의 오염 메시지 방어.

워커에 있던 poison pill 이 DLQ 소비자에도 그대로 있었다. 둘 다 json.loads 를
try 없이 부르고 있어서, 오염된 DLQ 메시지 한 건에 프로세스가 죽고 오프셋을
커밋하지 못해 재시작해도 같은 자리에서 또 죽었다.

DLQ 가 막히면 실패 이력 자체가 쌓이지 않으므로, 파이프라인이 왜 실패하는지
알 수 없게 된다 — 관측 수단을 잃는 방향이라 워커 정지 못지않게 나쁘다.

워커와 다른 점: DLQ 소비자는 실패를 더 보낼 곳이 없다. DLQ 의 DLQ 는 없다.
그래서 재전송이 아니라 "로그를 남기고 건너뛰는 것"이 종착 처리다.
"""

import json

import pytest

import consumer_dlq_to_bigquery as bq_consumer
import replay_dlq_to_original as replay


POISON_PAYLOADS = [
    pytest.param(b"{not valid json", id="깨진_JSON"),
    pytest.param(b"\xff\xfe invalid utf-8", id="UTF8_아님"),
    pytest.param(json.dumps([1, 2, 3]).encode("utf-8"), id="dict_아님"),
    pytest.param(json.dumps("문자열").encode("utf-8"), id="문자열_JSON"),
    pytest.param(b"", id="빈_메시지"),
]

MODULES = [
    pytest.param(replay, id="replay_dlq_to_original"),
    pytest.param(bq_consumer, id="consumer_dlq_to_bigquery"),
]


class TestDlqPayloadParsing:
    @pytest.mark.parametrize("module", MODULES)
    @pytest.mark.parametrize("raw", POISON_PAYLOADS)
    def test_poison_payload_should_return_none_instead_of_raising(self, module, raw):
        """예외가 밖으로 나가면 그 순간 소비자가 죽는다."""
        assert module.parse_dlq_payload(raw) is None

    @pytest.mark.parametrize("module", MODULES)
    def test_valid_payload_should_be_returned(self, module):
        payload = {"job": {"job_id": "j1"}, "failed_stage": "fetch"}

        result = module.parse_dlq_payload(json.dumps(payload).encode("utf-8"))

        assert result == payload


class TestExtractJob:
    """dict.get 의 기본값은 키가 없을 때만 적용된다.

    {"job": null} 이면 payload.get("job", {}) 가 None 을 돌려주고, 뒤이은
    job.get(...) 이 AttributeError 로 터진다. 워커가 DLQ 로 보내는 레코드에
    job 을 null 로 실었다면 여기서 죽었을 것이다.
    """

    @pytest.mark.parametrize("module", MODULES)
    @pytest.mark.parametrize(
        "job_value",
        [None, "문자열", [1, 2, 3], 42],
        ids=["null", "문자열", "배열", "숫자"],
    )
    def test_non_dict_job_should_become_empty_dict(self, module, job_value):
        result = module.extract_job({"job": job_value})

        assert result == {}
        assert result.get("retry_count", 0) == 0  # 이어지는 호출이 안전해야 한다

    @pytest.mark.parametrize("module", MODULES)
    def test_missing_job_key_should_become_empty_dict(self, module):
        assert module.extract_job({"failed_stage": "fetch"}) == {}

    @pytest.mark.parametrize("module", MODULES)
    def test_dict_job_should_pass_through(self, module):
        job = {"job_id": "j1", "retry_count": 2}

        assert module.extract_job({"job": job}) == job


class TestDlqEventRow:
    """consumer_dlq_to_bigquery 가 만드는 BigQuery 행."""

    def test_row_should_survive_null_job(self):
        """job 이 null 이어도 행 생성이 죽지 않아야 한다."""
        row = bq_consumer.build_row(
            {"job": None, "failed_stage": "parse", "error_type": "malformed_message"}
        )

        assert row["retry_count"] == 0
        assert row["source"] is None
        assert row["url"] is None
        assert row["failed_stage"] == "parse"

    def test_row_should_keep_original_payload_for_investigation(self):
        """원본 payload 를 통째로 남겨야 나중에 원인을 되짚을 수 있다."""
        payload = {"job": {"job_id": "j1", "source": "wanted", "url": "https://a.com"}}

        row = bq_consumer.build_row(payload)

        assert json.loads(row["payload"]) == payload
        assert row["source"] == "wanted"
        assert row["url"] == "https://a.com"


class TestImportHasNoSideEffects:
    """import 만으로 환경변수를 요구하거나 네트워크 클라이언트를 만들면 안 된다.

    이 테스트 파일이 두 모듈을 그냥 import 할 수 있다는 사실 자체가 검증이다.
    설정 읽기는 load_config() 안으로 옮겼다.
    """

    @pytest.mark.parametrize("module", MODULES)
    def test_config_is_read_lazily(self, module, monkeypatch):
        monkeypatch.delenv("KAFKA_DLQ_TOPIC", raising=False)

        with pytest.raises(KeyError):
            module.load_config()
