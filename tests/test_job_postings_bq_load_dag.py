"""s3_to_bq_job_postings_stg DAG 의 적재 로직 테스트.

DAG 파일은 그대로 import 할 수 없다. 저장소 루트의 airflow/ 디렉토리가 실제 Airflow
패키지를 가리고, google-cloud-bigquery 는 venv 에 설치돼 있지 않다. 두 의존성의
스텁은 tests/conftest.py 가 깔아두고, 여기서는 파일 경로로 직접 로드한다.

DAG 파일을 손대지 않고 테스트를 붙이기 위한 선택이다. 로직을 별도 모듈로 분리하면
스텁이 필요 없어지지만, 그러면 Airflow 배포 시 모듈이 함께 가도록 배포 설정을
맞춰야 한다 — 배포 경로가 정해지기 전까지는 구조를 건드리지 않는 편이 낫다.
"""

import importlib.util
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAG_PATH = PROJECT_ROOT / "airflow" / "dags" / "s3_to_bq_job_postings_stg.py"


def _load_dag_module():
    """airflow / google.cloud.bigquery 스텁은 tests/conftest.py 가 미리 깔아둔다."""
    spec = importlib.util.spec_from_file_location("stg_dag_under_test", DAG_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


dag_module = _load_dag_module()


class FakeQueryJob:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return self._rows


class FakeBqClient:
    """query 호출을 기록하고, 지정하면 예외를 던지는 가짜 BigQuery 클라이언트."""

    def __init__(self, rows=None, raise_on_query: Exception | None = None):
        self.rows = rows or []
        self.raise_on_query = raise_on_query
        self.queries = []

    def query(self, query):
        self.queries.append(query)
        if self.raise_on_query:
            raise self.raise_on_query
        return FakeQueryJob(self.rows)


class TestCheckpointReadFailure:
    """체크포인트 조회가 실패했을 때 '모른다'를 '없다'로 바꾸면 안 된다.

    already_loaded_keys 가 모든 예외를 삼키고 빈 집합을 돌려주면, 호출부는 그것을
    "여태 적재한 파일이 하나도 없다"로 읽는다. 그러면 S3 의 전체 파일이 새 파일로
    판정돼 히스토리 전체가 BigQuery 에 다시 들어간다. 중복 방지 장치가 고장 났을 때
    막는 쪽이 아니라 통과시키는 쪽으로 열려 있는 fail-open 이다.

    적재가 한 시간 늦는 것보다 전량 중복이 훨씬 비싸므로 fail-closed 가 맞다.
    DAG 에 retries=2 가 걸려 있어 일시적 오류라면 재시도에서 회복된다.
    """

    def test_checkpoint_query_failure_must_raise_not_return_empty(self, monkeypatch):
        client = FakeBqClient(raise_on_query=RuntimeError("BigQuery unavailable"))
        monkeypatch.setattr(
            dag_module.bigquery, "Client", lambda project=None: client, raising=False
        )

        with pytest.raises(RuntimeError):
            dag_module.already_loaded_keys("proj", "ds", "etl_loaded_s3_keys_job_postings")

    def test_checkpoint_query_success_returns_loaded_keys(self, monkeypatch):
        client = FakeBqClient(rows=[{"s3_key": "curated/a.json"}, {"s3_key": "curated/b.json"}])
        monkeypatch.setattr(
            dag_module.bigquery, "Client", lambda project=None: client, raising=False
        )

        result = dag_module.already_loaded_keys("proj", "ds", "etl_loaded_s3_keys_job_postings")

        assert result == {"curated/a.json", "curated/b.json"}


class TestCheckpointFailureDoesNotReloadEverything:
    """이 테스트가 fail-open 의 실제 피해를 재현한다.

    체크포인트 조회만 실패시키고, 그 결과 BigQuery 에 몇 건이 적재되는지를 본다.
    """

    @staticmethod
    def _setup(monkeypatch, *, checkpoint_error: Exception | None):
        monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
        monkeypatch.setenv("S3_BUCKET", "test-bucket")

        existing_keys = [f"curated/job_postings/dt=2026-08-13/job-{i}.json" for i in range(500)]

        monkeypatch.setattr(dag_module, "ensure_bq_table", lambda *a, **k: None)
        monkeypatch.setattr(dag_module, "ensure_checkpoint_table", lambda *a, **k: None)
        monkeypatch.setattr(dag_module, "list_s3_keys", lambda bucket, prefix: existing_keys)
        monkeypatch.setattr(
            dag_module, "download_json", lambda bucket, key: {"posting_id": key}
        )

        appended = []
        monkeypatch.setattr(
            dag_module,
            "append_rows_to_bq",
            lambda project, dataset, table, rows: appended.append((table, len(rows))),
        )

        client = FakeBqClient(
            rows=[{"s3_key": k} for k in existing_keys],
            raise_on_query=checkpoint_error,
        )
        monkeypatch.setattr(
            dag_module.bigquery, "Client", lambda project=None: client, raising=False
        )

        return appended, existing_keys

    def test_all_keys_already_loaded_should_append_nothing(self, monkeypatch):
        """대조군 — 체크포인트가 정상이면 새 파일이 없으므로 아무것도 안 넣는다."""
        appended, _ = self._setup(monkeypatch, checkpoint_error=None)

        dag_module.load_s3_to_bq()

        assert appended == []

    def test_checkpoint_failure_must_not_reload_the_entire_history(self, monkeypatch):
        """체크포인트를 못 읽었다고 500건을 다시 넣으면 안 된다.

        태스크를 실패시켜야 한다. Airflow 가 재시도하고, 계속 안 되면 사람에게
        알림이 간다. 조용히 잘못하는 것보다 시끄럽게 멈추는 편이 낫다.
        """
        appended, existing_keys = self._setup(
            monkeypatch, checkpoint_error=RuntimeError("BigQuery unavailable")
        )

        with pytest.raises(RuntimeError):
            dag_module.load_s3_to_bq()

        assert appended == [], (
            f"체크포인트 조회 실패에도 {sum(n for _, n in appended)}건을 적재했다 — "
            f"전체 {len(existing_keys)}건이 중복될 수 있다"
        )
