from pathlib import Path
from unittest.mock import patch

import boto3
import pandas as pd
import pytest
from moto import mock_sqs, mock_s3
from odc.aws.queue import get_queue
from urlpath import URL

from tools.monitoring import landsat_gap_filler
from tools.monitoring.landsat_gap_filler import (
    build_messages,
    post_messages,
    fill_the_gap,
)
from tools.tests.conftest import SQS_QUEUE_NAME, REGION, TEST_BUCKET_NAME


def test_build_messages(landsat_gap_report: Path):
    missing_scene_paths = set(
        pd.read_csv(
            landsat_gap_report,
            header=None,
        ).values.ravel()
    )
    returned_list = build_messages(missing_scene_paths, False)

    assert len(returned_list["message_list"]) == 28
    for value in returned_list["message_list"]:
        assert value.get("Message", False)
        assert value["Message"].get("landsat_product_id", False)
        assert value["Message"].get("s3_location", False)
        assert value["Message"].get("update_stac") is False


@mock_sqs
def test_post_messages(landsat_gap_report: Path):
    resource = boto3.resource("sqs")
    resource.create_queue(QueueName=SQS_QUEUE_NAME)

    missing_scene_paths = set(
        pd.read_csv(
            landsat_gap_report,
            header=None,
        ).values.ravel()
    )
    messages_to_send = build_messages(missing_scene_paths, False)

    post_messages(
        message_list=messages_to_send["message_list"], queue_name=SQS_QUEUE_NAME
    )


@mock_sqs
@mock_s3
def test_generate_buckets_diff(
    landsat_gap_report: Path, s3_report_path: URL, s3_landsat_gap_report: URL
):
    sqs_client = boto3.client("sqs", region_name=REGION)
    sqs_client.create_queue(QueueName=SQS_QUEUE_NAME)

    s3_client = boto3.client("s3", region_name=REGION)
    s3_client.create_bucket(
        Bucket=TEST_BUCKET_NAME,
        CreateBucketConfiguration={
            "LocationConstraint": REGION,
        },
    )

    # Upload fake gap report
    s3_client.upload_file(
        str(landsat_gap_report),
        TEST_BUCKET_NAME,
        str(s3_landsat_gap_report),
    )

    print(list(boto3.resource("s3").Bucket(TEST_BUCKET_NAME).objects.all()))
    with patch.object(landsat_gap_filler, "S3_BUCKET_PATH", str(s3_report_path)):
        # No differences
        fill_the_gap(landsat="landsat_5", sync_queue_name=SQS_QUEUE_NAME)
        queue = get_queue(queue_name=SQS_QUEUE_NAME)
        number_of_msgs = queue.attributes.get("ApproximateNumberOfMessages")
        assert int(number_of_msgs) == 28


@mock_sqs
@mock_s3
def test_exceptions(
    landsat_gap_report: Path, s3_report_path: URL, s3_landsat_gap_report: URL
):
    sqs_client = boto3.client("sqs", region_name=REGION)
    sqs_client.create_queue(QueueName=SQS_QUEUE_NAME)

    s3_client = boto3.client("s3", region_name=REGION)
    s3_client.create_bucket(
        Bucket=TEST_BUCKET_NAME,
        CreateBucketConfiguration={
            "LocationConstraint": REGION,
        },
    )

    # Upload fake gap report
    s3_client.upload_file(
        str(landsat_gap_report),
        TEST_BUCKET_NAME,
        str(s3_landsat_gap_report),
    )

    print(list(boto3.resource("s3").Bucket(TEST_BUCKET_NAME).objects.all()))
    with patch.object(landsat_gap_filler, "S3_BUCKET_PATH", str(s3_report_path)):
        # String Limit
        with pytest.raises(ValueError):
            fill_the_gap(
                landsat="landsat_5",
                sync_queue_name=SQS_QUEUE_NAME,
                scenes_limit="string test",
            )

        # Fake slack notification
        with pytest.raises(Exception):
            fill_the_gap(
                landsat="landsat_5",
                sync_queue_name=SQS_QUEUE_NAME,
                notification_url="fake_notification",
            )
