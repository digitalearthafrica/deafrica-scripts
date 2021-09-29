from pathlib import Path

import pytest
from urlpath import URL

REGION = "af-south-1"
TEST_BUCKET_NAME = "test-bucket"
SQS_QUEUE_NAME = "test-queue"
SQS_DEADLETTER_QUEUE_NAME = "test-queue-deadletter"
TEST_DATA_DIR = Path(__file__).absolute().parent / "data"
REPORT_FILE = "2021-08-17_update.txt.gz"
FAKE_STAC_FILE = "fake_stac.json"
FAKE_LANDSAT_8_BULK_FILE = "fake_landsat_8_bulk_file.csv.gz"
FAKE_LANDSAT_GAP_REPORT = "landsat_gap_report.txt.gz"
REPORT_FOLDER = "status-report"
COGS_REGION = "us-west-2"
CHIRPS_REGION = "ap-southeast-2"

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
def local_report_update_file():
    return TEST_DATA_DIR / REPORT_FILE


@pytest.fixture
def fake_stac_file():
    return TEST_DATA_DIR / FAKE_STAC_FILE


@pytest.fixture
def s3_s2_report_file():
    return URL(REPORT_FOLDER) / REPORT_FILE


@pytest.fixture
def s3_report_path():
    return URL(f"s3://{TEST_BUCKET_NAME}") / URL(REPORT_FOLDER)


@pytest.fixture
def inventory_s2_manifest_file():
    return TEST_DATA_DIR / "inventory_s2" / INVENTORY_MANIFEST_FILE


@pytest.fixture
def inventory_s2_data_file():
    return TEST_DATA_DIR / "inventory_s2" / INVENTORY_DATA_FILE


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


@pytest.fixture
def fake_landsat_bulk_file():
    return TEST_DATA_DIR / FAKE_LANDSAT_8_BULK_FILE


@pytest.fixture
def inventory_landsat_manifest_file():
    return TEST_DATA_DIR / "inventory_landsat" / INVENTORY_MANIFEST_FILE


@pytest.fixture
def inventory_landsat_data_file():
    return TEST_DATA_DIR / "inventory_landsat" / INVENTORY_DATA_FILE


@pytest.fixture
def landsat_gap_report():
    return TEST_DATA_DIR / FAKE_LANDSAT_GAP_REPORT


@pytest.fixture
def s3_landsat_gap_report():
    return URL(REPORT_FOLDER) / FAKE_LANDSAT_GAP_REPORT
