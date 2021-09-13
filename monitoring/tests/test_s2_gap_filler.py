import gzip
from pathlib import Path
from random import randrange
from unittest.mock import patch

import boto3
import pytest
from click.testing import CliRunner
from monitoring.tests.conftest import (
    FAKE_STAC_FILE,
    REGION,
    REPORT_FOLDER,
    SQS_QUEUE_NAME,
    TEST_BUCKET_NAME,
)
from monitoring.tools import s2_gap_filler
from moto import mock_s3, mock_sqs
from odc.aws.queue import get_queue
from urlpath import URL


@mock_s3
@mock_sqs
def test_publish_message_s2_gap_filler(
    monkeypatch, update_report_file: Path, fake_stac_file: Path
):
    sqs_client = boto3.client("sqs", region_name="af-south-1")
    sqs_client.create_queue(QueueName=SQS_QUEUE_NAME)

    s3_client = boto3.client("s3", region_name=REGION)
    s3_client.create_bucket(
        Bucket=TEST_BUCKET_NAME,
        CreateBucketConfiguration={
            "LocationConstraint": REGION,
        },
    )

    files = [
        scene_path.strip()
        for scene_path in gzip.open(open(str(update_report_file), "rb"))
        .read()
        .decode("utf-8")
        .split("\n")
        if scene_path
    ]

    for i in range(len(files)):
        s3_client.upload_file(
            str(fake_stac_file), TEST_BUCKET_NAME, f"{i}/{FAKE_STAC_FILE}"
        )

    with patch.object(s2_gap_filler, "SENTINEL_2_SYNC_SQS_NAME", SQS_QUEUE_NAME):
        s2_gap_filler.publish_message(files=files)
        queue = get_queue(queue_name=SQS_QUEUE_NAME)
        number_of_msgs = queue.attributes.get("ApproximateNumberOfMessages")
        assert int(number_of_msgs) == 8


@mock_s3
@mock_sqs
@pytest.mark.skip(reason="This test is not working")
def test_publish_message_s2_gap_filler_cli(
    monkeypatch, update_report_file: Path, fake_stac_file: Path, s3_report_file: URL
):
    """
    Test for random numbers of limits (between 1-10) for a random numbers of workers workers (between 1-30).
    """
    sqs_client = boto3.client("sqs", region_name="af-south-1")
    sqs_client.create_queue(QueueName=SQS_QUEUE_NAME)

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

    files = [
        scene_path.strip()
        for scene_path in gzip.open(open(str(update_report_file), "rb"))
        .read()
        .decode("utf-8")
        .split("\n")
        if scene_path
    ]

    for i in range(len(files)):
        s3_client.upload_file(
            str(fake_stac_file), TEST_BUCKET_NAME, f"{i}/{FAKE_STAC_FILE}"
        )

    s3_report_path = URL(f"s3://{TEST_BUCKET_NAME}") / URL(REPORT_FOLDER)

    with patch.object(s2_gap_filler, "SENTINEL_2_SYNC_SQS_NAME", SQS_QUEUE_NAME):
        with patch.object(s2_gap_filler, "S3_BUKET_PATH", str(s3_report_path)):
            runner = CliRunner()
            max_workers = randrange(1, 6)
            max_limit = randrange(1, 10)
            for limit in range(max_limit):
                for idx in range(max_workers):
                    runner.invoke(
                        s2_gap_filler.cli,
                        [str(idx), str(max_workers), "--limit", str(limit)],
                    )

                queue = get_queue(queue_name=SQS_QUEUE_NAME)
                number_of_msgs = queue.attributes.get("ApproximateNumberOfMessages")

                # total of messages sent won't be bigger than 8 so even with more workers and
                # higher limits the process must send a max of 8 messages len(files) == 8

                # if limit is 0 it returns error
                if limit == 0:
                    assert int(number_of_msgs) == 0

                # if limit bigger than 0 and smaller than the number max of messages
                if max_limit <= len(files):
                    assert int(number_of_msgs) == limit

                # if limit bigger than 8
                if max_limit > len(files):
                    assert int(number_of_msgs) == len(files)

                sqs_client.purge_queue(QueueUrl=queue.url)
