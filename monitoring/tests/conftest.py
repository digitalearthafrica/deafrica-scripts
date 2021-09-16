from pathlib import Path

import pytest
from urlpath import URL

REGION = "af-south-1"
TEST_BUCKET_NAME = "test-bucket"
SQS_QUEUE_NAME = "test-queue"
TEST_DATA_DIR = Path(__file__).absolute().parent / "data"
REPORT_FILE = "2021-08-17_update.txt.gz"
FAKE_STAC_FILE = "fake_stac.json"
REPORT_FOLDER = "status-report"
COGS_REGION = "us-west-2"


@pytest.fixture(autouse=True)
def setup_env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)


@pytest.fixture
def local_report_update_file():
    return TEST_DATA_DIR / REPORT_FILE


@pytest.fixture
def fake_stac_file():
    return TEST_DATA_DIR / FAKE_STAC_FILE


@pytest.fixture
def s3_report_file():
    s3_report_path = URL(REPORT_FOLDER)
    return s3_report_path / REPORT_FILE


@pytest.fixture
def s3_report_path():
    return URL(f"s3://{TEST_BUCKET_NAME}") / URL(REPORT_FOLDER)
