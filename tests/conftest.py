"""테스트 전역 준비.

google-cloud-bigquery 는 requirements 에 선언돼 있지만 venv 에 설치돼 있지 않고,
저장소 루트의 airflow/ 디렉토리가 실제 Airflow 패키지 이름을 가린다. 두 의존성을
쓰는 모듈(DAG, DLQ 소비자)을 import 하려면 가짜 모듈을 미리 sys.modules 에
넣어야 한다.

conftest.py 에 두는 이유는 순서 때문이다. pytest 는 테스트 모듈보다 conftest 를
먼저 불러오므로, 어떤 테스트 파일이 먼저 수집되든 스텁이 이미 준비돼 있다.
개별 테스트 파일에서 스텁을 깔면 수집 순서에 따라 깨진다.

confluent_kafka 와 boto3 는 실제로 설치돼 있으므로 스텁하지 않는다 — 가짜로
바꾸면 워커 테스트의 충실도가 떨어진다.
"""

import sys
import types


class FakeDAG:
    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakePythonOperator:
    def __init__(self, *args, **kwargs):
        self.kwargs = kwargs


def _module(name: str, **attrs) -> types.ModuleType:
    """속성을 채운 가짜 모듈을 만든다 (setattr 로 넣어 타입 체커 잡음을 피한다)."""
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    return module


def _install_airflow_stub() -> None:
    python_op = _module("airflow.operators.python", PythonOperator=FakePythonOperator)
    operators = _module("airflow.operators", python=python_op)
    airflow = _module("airflow", DAG=FakeDAG, operators=operators)

    sys.modules["airflow"] = airflow
    sys.modules["airflow.operators"] = operators
    sys.modules["airflow.operators.python"] = python_op


def _install_bigquery_stub() -> None:
    """이름만 채운다. Client 등 실제로 쓰이는 것은 테스트가 monkeypatch 로 덮는다."""
    def noop(*_args, **_kwargs):
        return None

    bigquery = _module(
        "google.cloud.bigquery",
        Client=object,
        SchemaField=noop,
        DatasetReference=noop,
        Table=noop,
        TimePartitioning=noop,
        TimePartitioningType=types.SimpleNamespace(DAY="DAY"),
    )
    cloud = _module("google.cloud", bigquery=bigquery)
    google = _module("google", cloud=cloud)

    sys.modules["google"] = google
    sys.modules["google.cloud"] = cloud
    sys.modules["google.cloud.bigquery"] = bigquery


_install_airflow_stub()
_install_bigquery_stub()
