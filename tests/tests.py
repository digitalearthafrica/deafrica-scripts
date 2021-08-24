import boto3
import pytest
from moto import mock_sqs
from odc.aws.queue import publish_messages, get_queue, publish_message

from deafrica_automation_tools.check_dead_queues import get_dead_queues, check_deadletter_queues

REGION = "af-south-1"


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

