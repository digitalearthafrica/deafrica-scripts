from pathlib import Path
from urlpath import URL

import boto3
import pytest
from moto import mock_s3, mock_sqs
from odc.aws.queue import publish_message
from tools.monitoring.tools.check_dead_queues import (
    check_deadletter_queues,
    get_dead_queues,
)
from tools.monitoring.tools.utils import find_latest_report, read_report

REGION = "af-south-1"
TEST_BUCKET_NAME = "test-bucket"
TEST_DATA_DIR = Path(__file__).absolute().parent / "data"
REPORT_FILE = "2021-08-17_update.txt.gz"


@pytest.fixture(autouse=True)
def setup_env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "af-south-1")


@mock_sqs
def test_get_dead_queues(monkeypatch):
    resource = boto3.resource("sqs")
    resource.create_queue(QueueName="deafrica-test-queue-deadletter")
    resource.create_queue(QueueName="deafrica-test-queue2-deadletter")
    resource.create_queue(QueueName="deafrica-test-queue3-deadletter")
    resource.create_queue(QueueName="deafrica-test-queue")

    dead_queues = get_dead_queues()

    assert len(dead_queues) == 3


@mock_sqs
def test_get_find_msg_dead_queues(monkeypatch):
    resource = boto3.resource("sqs")
    queue1 = resource.create_queue(QueueName="deafrica-test-queue-deadletter")
    resource.create_queue(QueueName="deafrica-test-queue2-deadletter")
    queue3 = resource.create_queue(QueueName="deafrica-test-queue3-deadletter")
    resource.create_queue(QueueName="deafrica-test-queue")

    message = "Message Body to dead queue"
    publish_message(
        queue=queue1,
        message=message,
    )
    publish_message(
        queue=queue3,
        message=message,
    )

    dead_queues = get_dead_queues()

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        check_deadletter_queues(dead_queues=dead_queues)
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 1


@mock_sqs
def test_get_no_msg_dead_queues(monkeypatch):
    resource = boto3.resource("sqs")
    resource.create_queue(QueueName="deafrica-test-queue-deadletter")
    resource.create_queue(QueueName="deafrica-test-queue2-deadletter")
    resource.create_queue(QueueName="deafrica-test-queue3-deadletter")
    resource.create_queue(QueueName="deafrica-test-queue")

    dead_queues = get_dead_queues()

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        check_deadletter_queues(dead_queues=dead_queues)
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 0


@mock_s3
def test_find_latest_report(monkeypatch, update_report_file: Path, s3_report_file: URL):
    s3_client = boto3.client("s3", region_name=REGION)
    s3_client.create_bucket(
        Bucket=TEST_BUCKET_NAME,
        CreateBucketConfiguration={
            "LocationConstraint": REGION,
        },
    )

    s3_client.upload_file(
        str(update_report_file),
        TEST_BUCKET_NAME,
        str(s3_report_file),
    )

    last_report = find_latest_report(
        report_folder_path=f"s3://{TEST_BUCKET_NAME}/{s3_report_file.parent}"
    )
    assert last_report is not None and last_report != []


@mock_s3
def test_not_found_latest_report(monkeypatch, s3_report_file: URL):
    s3_client = boto3.client("s3", region_name=REGION)
    s3_client.create_bucket(
        Bucket=TEST_BUCKET_NAME,
        CreateBucketConfiguration={
            "LocationConstraint": REGION,
        },
    )

    with pytest.raises(RuntimeError):
        find_latest_report(
            report_folder_path=f"s3://{TEST_BUCKET_NAME}/{s3_report_file.parent}"
        )


@mock_s3
def test_read_report(monkeypatch, update_report_file: Path, s3_report_file: URL):
    s3_client = boto3.client("s3", region_name=REGION)
    s3_client.create_bucket(
        Bucket=TEST_BUCKET_NAME,
        CreateBucketConfiguration={
            "LocationConstraint": REGION,
        },
    )

    s3_client.upload_file(str(update_report_file), TEST_BUCKET_NAME, str(s3_report_file))
    s3_path = f"s3://{TEST_BUCKET_NAME}/{s3_report_file.path}"

    values = read_report(report_path=s3_path)
    assert len(values) == 8

    # Test with limit
    values = read_report(report_path=s3_path, limit=2)
    assert len(values) == 2


@pytest.fixture
def update_report_file():
    return TEST_DATA_DIR / REPORT_FILE


@pytest.fixture
def s3_report_file():
    s3_report_path = URL(f"report/")
    return s3_report_path / REPORT_FILE
