import boto3
import pytest
from moto import mock_sqs, mock_s3
from odc.aws.queue import publish_message

from check_dead_queues import get_dead_queues, check_deadletter_queues
from utils import find_latest_report, read_report

REGION = "af-south-1"
S2_BUCKET_NAME = 'deafrica-sentinel-2'
REPORT_FOLDER_PATH = 'status-report/'


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

    message = 'Message Body to dead queue'
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
def test_find_latest_report(monkeypatch):
    s3_client = boto3.client("s3", region_name=REGION)
    s3_client.create_bucket(
        Bucket=S2_BUCKET_NAME,
        CreateBucketConfiguration={
            'LocationConstraint': REGION,
        }
    )

    file_name = '2021-08-17_update.txt.gz'
    s3_client.upload_file(
        f'../tests/data/{file_name}',
        S2_BUCKET_NAME,
        f"{REPORT_FOLDER_PATH}{file_name}"
    )

    last_report = find_latest_report(report_folder_path=f"s3://{S2_BUCKET_NAME}/{REPORT_FOLDER_PATH}")
    assert last_report is not None and last_report != []


@mock_s3
def test_not_found_latest_report(monkeypatch):
    s3_client = boto3.client("s3", region_name=REGION)
    s3_client.create_bucket(
        Bucket=S2_BUCKET_NAME,
        CreateBucketConfiguration={
            'LocationConstraint': REGION,
        }
    )

    with pytest.raises(RuntimeError):
        find_latest_report(report_folder_path=f"s3://{S2_BUCKET_NAME}/{REPORT_FOLDER_PATH}")


@mock_s3
def test_read_report(monkeypatch):
    s3_client = boto3.client("s3", region_name=REGION)
    s3_client.create_bucket(
        Bucket=S2_BUCKET_NAME,
        CreateBucketConfiguration={
            'LocationConstraint': REGION,
        }
    )

    file_name = '2021-08-17_update.txt.gz'
    s3_client.upload_file(
        f'../tests/data/{file_name}',
        S2_BUCKET_NAME,
        f"{REPORT_FOLDER_PATH}{file_name}"
    )

    values = read_report(report_path=f"s3://{S2_BUCKET_NAME}/{REPORT_FOLDER_PATH}{file_name}")
    assert len(values) == 8

    # Test with limit
    values = read_report(report_path=f"s3://{S2_BUCKET_NAME}/{REPORT_FOLDER_PATH}{file_name}", limit=2)
    assert len(values) == 2
