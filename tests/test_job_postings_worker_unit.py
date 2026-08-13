import importlib
import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import pytest
import requests


# 네 worker 파일명에 맞게 수정
# 예: "worker_job_postings_to_s3"
MODULE_UNDER_TEST = "worker_job_postings_to_s3"

worker = importlib.import_module(MODULE_UNDER_TEST)


class DummyResponse:
    def __init__(self, status_code: int | None = None):
        self.status_code = status_code


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


class TestBuildS3Paths:
    def test_build_s3_paths_should_generate_expected_keys(self):
        job = make_job(
            job_id="abc-123",
            source="seed",
            collected_at="2026-03-24T10:53:33.864638Z",
        )

        result = worker.build_s3_paths(job)

        assert result == {
            "raw_s3_key": "raw/job_postings/source=seed/dt=2026-03-24/abc-123.html",
            "processed_s3_key": "processed/job_postings/source=seed/dt=2026-03-24/abc-123.json",
            "curated_s3_key": "curated/job_postings/dt=2026-03-24/abc-123.json",
        }

    def test_build_s3_paths_should_use_job_source_and_job_id_only_in_expected_positions(self):
        job = make_job(
            job_id="custom-id-999",
            source="crawler",
            collected_at="2026-01-02T03:04:05Z",
        )

        result = worker.build_s3_paths(job)

        assert result["raw_s3_key"].endswith("/custom-id-999.html")
        assert result["processed_s3_key"].endswith("/custom-id-999.json")
        assert result["curated_s3_key"].endswith("/custom-id-999.json")
        assert "source=crawler" in result["raw_s3_key"]
        assert "source=crawler" in result["processed_s3_key"]
        assert "source=crawler" not in result["curated_s3_key"]
        assert "dt=2026-01-02" in result["raw_s3_key"]
        assert "dt=2026-01-02" in result["processed_s3_key"]
        assert "dt=2026-01-02" in result["curated_s3_key"]

    def test_build_s3_paths_should_raise_for_invalid_collected_at(self):
        job = make_job(collected_at="not-a-date")

        with pytest.raises(ValueError):
            worker.build_s3_paths(job)


class TestSslPolicy:
    @pytest.mark.parametrize(
        ("url", "expected"),
        [
            ("https://www.saramin.co.kr/zf_user/jobs/view", True),
            ("https://saramin.co.kr/zf_user/jobs/view", True),
            ("https://www.wanted.co.kr/wd/242151", False),
            ("https://groupby.kr/positions/9644", False),
            ("https://www.jobkorea.co.kr/Recruit/GI_Read/48818694", False),
            ("https://www.catch.co.kr/NCS/RecruitInfoDetails/543587", False),
        ],
    )
    def test_should_disable_ssl_verify_should_match_domain_policy(self, url, expected):
        assert worker.should_disable_ssl_verify(url) is expected


class TestRetryLimitPolicy:
    def test_has_exceeded_retry_limit_should_be_false_below_limit(self, monkeypatch):
        monkeypatch.setattr(worker, "MAX_RETRY_COUNT", 3)

        assert worker.has_exceeded_retry_limit(make_job(retry_count=0)) is False
        assert worker.has_exceeded_retry_limit(make_job(retry_count=1)) is False
        assert worker.has_exceeded_retry_limit(make_job(retry_count=2)) is False

    def test_has_exceeded_retry_limit_should_be_true_at_or_above_limit(self, monkeypatch):
        monkeypatch.setattr(worker, "MAX_RETRY_COUNT", 3)

        assert worker.has_exceeded_retry_limit(make_job(retry_count=3)) is True
        assert worker.has_exceeded_retry_limit(make_job(retry_count=4)) is True


class TestContentHash:
    def test_compute_content_hash_should_return_same_hash_for_same_text(self):
        text = "same content"

        first = worker.compute_content_hash(text)
        second = worker.compute_content_hash(text)

        assert first == second
        assert isinstance(first, str)
        assert len(first) == 64

    def test_compute_content_hash_should_return_different_hash_for_different_text(self):
        first = worker.compute_content_hash("content A")
        second = worker.compute_content_hash("content B")

        assert first != second

    def test_compute_content_hash_should_be_sensitive_to_whitespace_changes(self):
        first = worker.compute_content_hash("hello world")
        second = worker.compute_content_hash("hello  world")

        assert first != second


class TestFetchErrorClassification:
    def test_classify_fetch_error_should_classify_connect_timeout(self):
        error = requests.exceptions.ConnectTimeout("connect timeout")
        assert worker.classify_fetch_error(error) == "connect_timeout"

    def test_classify_fetch_error_should_classify_read_timeout(self):
        error = requests.exceptions.ReadTimeout("read timeout")
        assert worker.classify_fetch_error(error) == "read_timeout"

    def test_classify_fetch_error_should_classify_ssl_error(self):
        error = requests.exceptions.SSLError("certificate verify failed")
        assert worker.classify_fetch_error(error) == "ssl_error"

    @pytest.mark.parametrize(
        ("status_code", "expected"),
        [
            (400, "http_4xx_400"),
            (404, "http_4xx_404"),
            (429, "http_4xx_429"),
            (500, "http_5xx_500"),
            (503, "http_5xx_503"),
        ],
    )
    def test_classify_fetch_error_should_classify_http_status_ranges(self, status_code, expected):
        error = requests.exceptions.HTTPError("http error", response=DummyResponse(status_code))
        assert worker.classify_fetch_error(error) == expected

    def test_classify_fetch_error_should_fallback_to_http_error_without_status(self):
        error = requests.exceptions.HTTPError("http error", response=None)
        assert worker.classify_fetch_error(error) == "http_error"

    @pytest.mark.parametrize(
        "message",
        [
            "Failed to resolve 'invalid-url-test-1234.com'",
            "Name or service not known",
            "Temporary failure in name resolution; failed to resolve host",
        ],
    )
    def test_classify_fetch_error_should_classify_dns_error_from_connection_error_message(self, message):
        error = requests.exceptions.ConnectionError(message)
        assert worker.classify_fetch_error(error) == "dns_error"

    def test_classify_fetch_error_should_classify_generic_connection_error(self):
        error = requests.exceptions.ConnectionError("connection refused")
        assert worker.classify_fetch_error(error) == "connection_error"

    def test_classify_fetch_error_should_fallback_to_exception_type_name_for_unknown_errors(self):
        error = RuntimeError("unexpected")
        assert worker.classify_fetch_error(error) == "RuntimeError"


class TestFetchPolicy:
    def test_fetch_html_should_use_ssl_policy_and_timeout(self, monkeypatch):
        captured: dict = {}

        class FakeSession:
            def get(self, url, timeout, headers, verify):
                captured["url"] = url
                captured["timeout"] = timeout
                captured["headers"] = headers
                captured["verify"] = verify

                class FakeResponse:
                    text = "<html>ok</html>"

                    @staticmethod
                    def raise_for_status():
                        return None

                return FakeResponse()

        html = worker.fetch_html(FakeSession(), "https://www.wanted.co.kr/wd/242151")

        assert html == "<html>ok</html>"
        assert captured["url"] == "https://www.wanted.co.kr/wd/242151"
        assert captured["timeout"] == (3, 10)
        assert "Mozilla/5.0" in captured["headers"]["User-Agent"]
        assert captured["verify"] is True

    def test_fetch_html_should_disable_ssl_verification_only_for_allowed_domains(self):
        captured: dict = {}

        class FakeSession:
            def get(self, url, timeout, headers, verify):
                captured["verify"] = verify

                class FakeResponse:
                    text = "<html>saramin</html>"

                    @staticmethod
                    def raise_for_status():
                        return None

                return FakeResponse()

        worker.fetch_html(FakeSession(), "https://www.saramin.co.kr/zf_user/jobs/view")
        assert captured["verify"] is False

    def test_fetch_html_should_propagate_http_errors(self):
        class FakeSession:
            def get(self, url, timeout, headers, verify):
                class FakeResponse:
                    @staticmethod
                    def raise_for_status():
                        raise requests.exceptions.HTTPError(
                            "server error",
                            response=DummyResponse(503),
                        )

                return FakeResponse()

        with pytest.raises(requests.exceptions.HTTPError):
            worker.fetch_html(FakeSession(), "https://example.com")


class RecordingSession:
    """재요청이 실제로 일어났는지 기록하는 세션 스텁.

    refetch 는 실패하면 원본 html 을 돌려주므로, 반환값만 보면 "요청을 건너뛴 것"과
    "요청했다가 실패한 것"이 구분되지 않는다. 호출 자체를 기록해야 조기 반환을 검증할 수 있다.
    """

    def __init__(self):
        self.calls = []

    def get(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        raise RuntimeError("이 테스트에서는 네트워크 요청이 일어나면 안 된다")


class TestCanonicalRefetchPolicy:
    def test_refetch_canonical_url_should_return_original_html_when_domain_not_configured(self):
        session = RecordingSession()
        original_html = "<html>original</html>"

        result = worker.refetch_canonical_url(
            session=session,
            url="https://www.wanted.co.kr/wd/242151",
            html=original_html,
        )

        assert result == original_html
        assert session.calls == []

    def test_refetch_canonical_url_should_return_original_html_when_canonical_missing(self, monkeypatch):
        session = RecordingSession()
        original_html = "<html>no canonical</html>"
        monkeypatch.setattr(worker, "extract_canonical_url", lambda html: None)

        result = worker.refetch_canonical_url(
            session=session,
            url="https://www.saramin.co.kr/zf_user/jobs/view",
            html=original_html,
        )

        assert result == original_html
        assert session.calls == []

    def test_refetch_canonical_url_should_return_original_html_when_canonical_same_as_url(self, monkeypatch):
        session = RecordingSession()
        original_html = "<html>same canonical</html>"
        url = "https://www.saramin.co.kr/zf_user/jobs/view"
        monkeypatch.setattr(worker, "extract_canonical_url", lambda html: url)

        result = worker.refetch_canonical_url(
            session=session,
            url=url,
            html=original_html,
        )

        assert result == original_html
        assert session.calls == []

    def test_refetch_canonical_url_should_fetch_canonical_and_return_new_html(self, monkeypatch):
        canonical_url = "https://www.saramin.co.kr/zf_user/jobs/relay/view?rec_idx=123"
        monkeypatch.setattr(worker, "extract_canonical_url", lambda html: canonical_url)

        captured: dict = {}

        class FakeSession:
            def get(self, url, timeout, headers, verify):
                captured["url"] = url
                captured["timeout"] = timeout
                captured["verify"] = verify

                class FakeResponse:
                    text = "<html>canonical html</html>"

                    @staticmethod
                    def raise_for_status():
                        return None

                return FakeResponse()

        result = worker.refetch_canonical_url(
            session=FakeSession(),
            url="https://www.saramin.co.kr/zf_user/jobs/view",
            html="<html>old html</html>",
        )

        assert result == "<html>canonical html</html>"
        assert captured["url"] == canonical_url
        assert captured["timeout"] == (3, 10)
        assert captured["verify"] is False

    def test_refetch_canonical_url_should_fallback_to_original_html_on_refetch_failure(self, monkeypatch):
        canonical_url = "https://www.saramin.co.kr/zf_user/jobs/relay/view?rec_idx=123"
        monkeypatch.setattr(worker, "extract_canonical_url", lambda html: canonical_url)

        class FakeSession:
            def get(self, url, timeout, headers, verify):
                raise requests.exceptions.ConnectTimeout("timeout")

        original_html = "<html>old html</html>"

        result = worker.refetch_canonical_url(
            session=FakeSession(),
            url="https://www.saramin.co.kr/zf_user/jobs/view",
            html=original_html,
        )

        assert result == original_html


class TestDocumentShape:
    def test_build_processed_document_should_return_required_fields(self):
        job = make_job(job_id="job-1", source="seed", url="https://example.com")
        s3_paths = {
            "raw_s3_key": "raw/job_postings/source=seed/dt=2026-03-24/job-1.html",
            "processed_s3_key": "processed/job_postings/source=seed/dt=2026-03-24/job-1.json",
            "curated_s3_key": "curated/job_postings/dt=2026-03-24/job-1.json",
        }
        html = "<html>" + ("a" * 1000) + "</html>"

        result = worker.build_processed_document(job, s3_paths, html)

        assert result["job_id"] == "job-1"
        assert result["source"] == "seed"
        assert result["url"] == "https://example.com"
        assert result["raw_s3_key"] == s3_paths["raw_s3_key"]
        assert result["html_length"] == len(html)
        assert result["text_preview"] == html[:500]
        assert result["processed_at"].endswith("Z")

    def test_build_curated_document_should_include_core_business_fields(self, monkeypatch):
        job = make_job(job_id="job-1", source="seed", url="https://example.com")
        s3_paths = {
            "raw_s3_key": "raw/job_postings/source=seed/dt=2026-03-24/job-1.html",
            "processed_s3_key": "processed/job_postings/source=seed/dt=2026-03-24/job-1.json",
            "curated_s3_key": "curated/job_postings/dt=2026-03-24/job-1.json",
        }

        monkeypatch.setattr(
            worker,
            "extract_fields_by_domain",
            lambda url, html: {
                "company_name": "Example Corp",
                "title": "Backend Engineer",
                "location": "Seoul",
                "employment_type": "FULL_TIME",
                "experience_level": "3 years",
                "description_text": "Build backend services",
            },
        )

        result = worker.build_curated_document(job, s3_paths, "<html>ignored</html>")

        assert result["posting_id"] == "job-1"
        assert result["source"] == "seed"
        assert result["original_url"] == "https://example.com"
        assert result["company_name"] == "Example Corp"
        assert result["title"] == "Backend Engineer"
        assert result["location"] == "Seoul"
        assert result["employment_type"] == "FULL_TIME"
        assert result["experience_level"] == "3 years"
        assert result["description_text"] == "Build backend services"
        assert result["skills"] == "[]"
        assert result["content_hash"] == worker.compute_content_hash("Build backend services")
        assert result["raw_s3_key"] == s3_paths["raw_s3_key"]
        assert result["processed_s3_key"] == s3_paths["processed_s3_key"]
        assert result["curated_s3_key"] == s3_paths["curated_s3_key"]

    def test_build_curated_document_should_trim_very_long_description(self, monkeypatch):
        long_text = "x" * 5000
        monkeypatch.setattr(
            worker,
            "extract_fields_by_domain",
            lambda url, html: {
                "company_name": "Example Corp",
                "title": "Backend Engineer",
                "location": None,
                "employment_type": None,
                "experience_level": None,
                "description_text": long_text,
            },
        )

        result = worker.build_curated_document(
            make_job(),
            {
                "raw_s3_key": "raw.html",
                "processed_s3_key": "processed.json",
                "curated_s3_key": "curated.json",
            },
            "<html>ignored</html>",
        )

        assert len(result["description_text"]) == 4000

class TestConsumerConfiguration:
    """컨슈머 설정은 시나리오 테스트가 create_consumer 를 통째로 대체하기 때문에
    한 번도 검증된 적이 없었다. 설정 자체를 보는 테스트가 따로 필요하다.
    """

    @staticmethod
    def _capture_config(monkeypatch) -> dict:
        captured = {}

        class FakeKafkaConsumer:
            def __init__(self, config):
                captured.update(config)

        monkeypatch.setattr(worker, "Consumer", FakeKafkaConsumer)
        worker.create_consumer()
        return captured

    def test_consumer_must_disable_auto_commit(self, monkeypatch):
        """자동 커밋이 켜져 있으면 단계별 수동 커밋 설계가 무력화된다.

        librdkafka 는 enable.auto.commit 기본값이 true 이고, 기본 설정에서는
        poll() 이 메시지를 애플리케이션에 넘기는 순간 오프셋이 저장된 뒤
        auto.commit.interval.ms(기본 5초) 마다 백그라운드로 커밋된다.
        처리 성공 여부를 보지 않으므로, fetch 가 느린 사이(타임아웃이 연결 3초 +
        읽기 10초라 최대 13초) 워커가 죽으면 처리되지 않은 메시지의 오프셋이
        이미 커밋돼 있다. at-least-once 로 설계한 파이프라인이 실제로는
        at-most-once 로 동작해 메시지가 조용히 사라진다.

        같은 저장소의 replay_dlq_to_original.py 와 consumer_dlq_to_bigquery.py 는
        둘 다 명시적으로 끄고 있다. 워커만 빠져 있던 것은 판단이 아니라 누락이다.
        """
        config = self._capture_config(monkeypatch)

        assert config.get("enable.auto.commit") is False

    def test_consumer_must_keep_earliest_offset_reset(self, monkeypatch):
        """오프셋 정보가 없을 때 처음부터 읽어야 수집분을 건너뛰지 않는다."""
        config = self._capture_config(monkeypatch)

        assert config["auto.offset.reset"] == "earliest"

    def test_consumer_must_set_group_id_and_bootstrap(self, monkeypatch):
        config = self._capture_config(monkeypatch)

        assert config["group.id"] == worker.GROUP_ID
        assert config["bootstrap.servers"] == worker.KAFKA_BOOTSTRAP
