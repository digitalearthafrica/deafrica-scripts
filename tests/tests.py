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
    queue1 = sns_client.create_queue(QueueName="deafrica-test-queue-deadletter")
    queue2 = sns_client.create_queue(QueueName="deafrica-test-queue2-deadletter")
    queue3 = sns_client.create_queue(QueueName="deafrica-test-queue3-deadletter")
    queue4 = sns_client.create_queue(QueueName="deafrica-test-queue")

    from deafrica_automation_tools.check_dead_queues import get_dead_queues

    dead_queues = get_dead_queues(region=REGION)

    assert len(dead_queues) == 3


@mock_sqs
def test_get_dead_queues(monkeypatch):
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
    check_deadletter_queues(dead_queues=dead_queues, region=REGION)
