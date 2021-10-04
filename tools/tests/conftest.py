from pathlib import Path

import pytest
from urlpath import URL

REGION = "af-south-1"
TEST_BUCKET_NAME = "test-bucket"
SQS_QUEUE_NAME = "test-queue"
SQS_DEADLETTER_QUEUE_NAME = "test-queue-deadletter"
TEST_DATA_DIR = Path(__file__).absolute().parent / "data"
REPORT_FOLDER = "status-report"
COGS_REGION = "us-west-2"

# INVENTORY
INVENTORY_BUCKET_SOURCE_NAME = "test-cogs-inventory-bucket"
INVENTORY_BUCKET_NAME = "test-inventory-bucket"
INVENTORY_FOLDER = "test"
INVENTORY_DATA_FILE = "data_file.csv.gz"
INVENTORY_MANIFEST_FILE = "manifest.json"


@pytest.fixture(autouse=True)
def setup_env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", REGION)


@pytest.fixture
def s3_report_path():
    return URL(f"s3://{TEST_BUCKET_NAME}") / URL(REPORT_FOLDER)


@pytest.fixture
def s3_inventory_manifest_file():
    return (
        URL(INVENTORY_FOLDER)
        / URL(INVENTORY_BUCKET_NAME)
        / "2021-09-17T00-00Z"
        / INVENTORY_MANIFEST_FILE
    )


@pytest.fixture
def s3_inventory_data_file():
    return (
        URL(INVENTORY_FOLDER)
        / URL(INVENTORY_BUCKET_NAME)
        / URL("data")
        / INVENTORY_DATA_FILE
    )
