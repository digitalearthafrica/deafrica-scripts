import boto3
import pytest
from moto import mock_sqs

REGION = "af-south-1"


@pytest.fixture(autouse=True)
def setup_env(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "af-south-1")


@mock_sqs
def test_get_dead_queues(monkeypatch):
    sns_client = boto3.client("sqs", region_name=REGION)
    sns_client.create_queue(QueueName="deafrica-test-queue-deadletter")
    sns_client.create_queue(QueueName="deafrica-test-queue2-deadletter")
    sns_client.create_queue(QueueName="deafrica-test-queue3-deadletter")
    sns_client.create_queue(QueueName="deafrica-test-queue")

    from deafrica_automation_tools.check_dead_queues import get_dead_queues

    dead_queues = get_dead_queues(region=REGION)

    assert len(dead_queues) == 3


@mock_sqs
def test_get_find_msg_dead_queues(monkeypatch):
    sqs_client = boto3.client('sqs', region_name=REGION)
    queue1 = sqs_client.create_queue(QueueName="deafrica-test-queue-deadletter")
    queue2 = sqs_client.create_queue(QueueName="deafrica-test-queue2-deadletter")
    queue3 = sqs_client.create_queue(QueueName="deafrica-test-queue3-deadletter")
    queue4 = sqs_client.create_queue(QueueName="deafrica-test-queue")

    message = 'Message Body to dead queue'
    sqs_client.send_message(
        QueueUrl=queue1['QueueUrl'],
        MessageBody=message,
    )
    sqs_client.send_message(
        QueueUrl=queue3['QueueUrl'],
        MessageBody=message,
    )

    from deafrica_automation_tools.check_dead_queues import get_dead_queues, check_deadletter_queues

    dead_queues = get_dead_queues(REGION)

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        check_deadletter_queues(dead_queues=dead_queues, region=REGION)
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 1


@mock_sqs
def test_get_no_msg_dead_queues(monkeypatch):
    sqs_client = boto3.client('sqs', region_name=REGION)
    sqs_client.create_queue(QueueName="deafrica-test-queue-deadletter")
    sqs_client.create_queue(QueueName="deafrica-test-queue2-deadletter")
    sqs_client.create_queue(QueueName="deafrica-test-queue3-deadletter")
    sqs_client.create_queue(QueueName="deafrica-test-queue")

    from deafrica_automation_tools.check_dead_queues import get_dead_queues, check_deadletter_queues

    dead_queues = get_dead_queues(REGION)

    with pytest.raises(SystemExit) as pytest_wrapped_e:
        check_deadletter_queues(dead_queues=dead_queues, region=REGION)
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 0

