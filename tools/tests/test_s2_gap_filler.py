import gzip
from random import randrange
from unittest.mock import patch

import boto3
from click.testing import CliRunner
from moto import mock_s3, mock_sqs
from odc.aws.queue import get_queue

from tools.monitoring import s2_gap_filler
from tools.tests.conftest import *


@mock_s3
@mock_sqs
def test_publish_message_s2_gap_filler(
    monkeypatch,
    local_report_update_file,
    fake_stac_file: Path,
    s3_report_path: URL,
    s3_report_file: URL,
):
    sqs_client = boto3.client("sqs", region_name=REGION)
    sqs_client.create_queue(QueueName=SQS_QUEUE_NAME)

    s3_client = boto3.client("s3", region_name=COGS_REGION)
    s3_client.create_bucket(
        Bucket=TEST_BUCKET_NAME,
        CreateBucketConfiguration={
            "LocationConstraint": COGS_REGION,
        },
    )

    s3_client.upload_file(
        str(local_report_update_file),
        TEST_BUCKET_NAME,
        str(s3_report_file),
    )

    files = [
        scene_path.strip()
        for scene_path in gzip.open(open(str(local_report_update_file), "rb"))
        .read()
        .decode("utf-8")
        .split("\n")
        if scene_path
    ]

    for i in range(len(files)):
        s3_client.upload_file(
            str(fake_stac_file), TEST_BUCKET_NAME, f"{i}/{FAKE_STAC_FILE}"
        )

    with patch.object(s2_gap_filler, "S3_BUCKET_PATH", str(s3_report_path)):
        s2_gap_filler.send_messages(
            limit=None,
            max_workers=1,
            idx=0,
            queue_name=SQS_QUEUE_NAME,
            slack_url=None,
        )

        queue = get_queue(queue_name=SQS_QUEUE_NAME)
        number_of_msgs = queue.attributes.get("ApproximateNumberOfMessages")
        assert int(number_of_msgs) == 8


@mock_s3
@mock_sqs
def test_s2_gap_filler_cli(
    monkeypatch,
    local_report_update_file,
    fake_stac_file: Path,
    s3_report_file: URL,
    s3_report_path: URL,
):
    """
    Test for random numbers of limits (between 1-10) for a random numbers of workers workers (between 1-30).
    """
    sqs_client = boto3.client("sqs", region_name=REGION)
    sqs_client.create_queue(QueueName=SQS_QUEUE_NAME)

    s3_client = boto3.client("s3", region_name=COGS_REGION)
    s3_client.create_bucket(
        Bucket=TEST_BUCKET_NAME,
        CreateBucketConfiguration={
            "LocationConstraint": COGS_REGION,
        },
    )

    s3_client.upload_file(
        str(local_report_update_file),
        TEST_BUCKET_NAME,
        str(s3_report_file),
    )

    files = [
        scene_path.strip()
        for scene_path in gzip.open(open(str(local_report_update_file), "rb"))
        .read()
        .decode("utf-8")
        .split("\n")
        if scene_path
    ]

    for i in range(len(files)):
        s3_client.upload_file(
            str(fake_stac_file), TEST_BUCKET_NAME, f"{i}/{FAKE_STAC_FILE}"
        )

    with patch.object(s2_gap_filler, "S3_BUCKET_PATH", str(s3_report_path)):
        runner = CliRunner()
        max_workers = randrange(1, 6)
        max_limit = randrange(1, 10)
        for limit in range(max_limit):
            for idx in range(max_workers):
                runner.invoke(
                    s2_gap_filler.cli,
                    [
                        str(idx),
                        str(max_workers),
                        str(SQS_QUEUE_NAME),
                        "--limit",
                        str(limit),
                    ],
                )

            queue = get_queue(queue_name=SQS_QUEUE_NAME)
            number_of_msgs = queue.attributes.get("ApproximateNumberOfMessages")

            # total of messages sent won't be bigger than 8 so even with more workers and
            # higher limits the process must send a max of 8 messages len(files) == 8

            # if limit is 0 it returns error
            if limit == 0:
                assert int(number_of_msgs) == 0

            # if limit bigger than 0 and smaller than the number max of messages
            elif limit < len(files):
                assert int(number_of_msgs) == limit

            # if limit bigger than 8
            elif limit >= len(files):
                assert int(number_of_msgs) == len(files)

            sqs_client.purge_queue(QueueUrl=queue.url)

        print(f"max_limit {max_limit} - max_workers {max_workers}")
