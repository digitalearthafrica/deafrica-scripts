import gzip
from pathlib import Path
from unittest.mock import patch
from click.testing import CliRunner

import boto3
from moto import mock_s3, mock_sqs
from odc.aws.queue import get_queue

from monitoring.tools.utils import split_list_equally
from tools.tests.conftest import (
    FAKE_STAC_FILE,
    REGION,
    SQS_QUEUE_NAME,
    TEST_BUCKET_NAME,
)
from tools.monitoring.tools import s2_gap_filler


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
def test_publish_message_s2_gap_filler_cli(
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
        list_string = split_list_equally(list_to_split=files, num_inter_lists=2)

        for files_str_list in list_string:
            runner = CliRunner()
            runner.invoke(s2_gap_filler.cli, [files_str_list])

        queue = get_queue(queue_name=SQS_QUEUE_NAME)
        number_of_msgs = queue.attributes.get("ApproximateNumberOfMessages")
        assert int(number_of_msgs) == 8
