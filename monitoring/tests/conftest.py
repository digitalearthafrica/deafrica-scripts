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

# INVENTORY
INVENTORY_BUCKET_COGS = "test-cogs-inventory-bucket"
INVENTORY_BUCKET = "test-inventory-bucket"
INVENTORY_FOLDER = "test"
INVENTORY_DATA_FILE = "data_file.csv.gz"
INVENTORY_MANIFEST_FILE = "manifest.json"
INVENTORY_MISSING_DATA_FILE = "missing_data_file.csv.gz"
INVENTORY_MISSING_DATA_MANIFEST_FILE = "missing_data_manifest.json"


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


@pytest.fixture
def inventory_manifest_file():
    return TEST_DATA_DIR / "inventory" / INVENTORY_MANIFEST_FILE


@pytest.fixture
def inventory_missing_data_manifest_file():
    return TEST_DATA_DIR / "inventory" / INVENTORY_MISSING_DATA_MANIFEST_FILE


@pytest.fixture
def inventory_data_file():
    return TEST_DATA_DIR / "inventory" / INVENTORY_DATA_FILE


@pytest.fixture
def inventory_missing_data_file():
    return TEST_DATA_DIR / "inventory" / INVENTORY_MISSING_DATA_FILE


@pytest.fixture
def s3_inventory_manifest_file():
    return (
        URL(INVENTORY_FOLDER)
        / URL(INVENTORY_BUCKET)
        / "2021-09-17T00-00Z"
        / INVENTORY_MANIFEST_FILE
    )


@pytest.fixture
def s3_inventory_data_file():
    return (
        URL(INVENTORY_FOLDER)
        / URL(INVENTORY_BUCKET)
        / URL("data")
        / INVENTORY_DATA_FILE
    )
