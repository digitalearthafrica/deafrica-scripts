import json
import os
from unittest.mock import patch

import boto3
import pytest
from moto import mock_s3, mock_sqs
from odc.aws.queue import get_queue

from deafrica.monitoring import landsat_gap_filler
from deafrica.monitoring.landsat_gap_filler import (
    build_messages,
    fill_the_gap,
    post_messages,
)
from deafrica.tests.conftest import (
    REGION,
    REPORT_FOLDER,
    SQS_QUEUE_NAME,
    TEST_BUCKET_NAME,
    TEST_DATA_DIR,
)

# from urlpath import URL


DATA_FOLDER = "landsat"
FAKE_LANDSAT_GAP_REPORT = "landsat_5_2021-08-16_gap_report_update.json"
LANDSAT_GAP_REPORT = TEST_DATA_DIR / DATA_FOLDER / FAKE_LANDSAT_GAP_REPORT
S3_LANDSAT_GAP_REPORT = os.path.join(REPORT_FOLDER, FAKE_LANDSAT_GAP_REPORT)


def test_build_messages():
    missing_dict = json.loads(open(str(LANDSAT_GAP_REPORT), "rb").read())
    missing_scene_paths = [
        scene_path.strip() for scene_path in missing_dict["missing"] if scene_path
    ]
    returned_list = build_messages(missing_scene_paths, False)

    assert len(returned_list["message_list"]) == 28
    for value in returned_list["message_list"]:
        assert value.get("Message", False)
        assert value["Message"].get("landsat_product_id", False)
        assert value["Message"].get("s3_location", False)
        assert value["Message"].get("update_stac") is False


@mock_sqs
def test_post_messages():
    resource = boto3.resource("sqs")
    resource.create_queue(QueueName=SQS_QUEUE_NAME)

    missing_dict = json.loads(open(str(LANDSAT_GAP_REPORT), "rb").read())
    missing_scene_paths = [
        scene_path.strip() for scene_path in missing_dict["missing"] if scene_path
    ]
    messages_to_send = build_messages(missing_scene_paths, False)

    post_messages(
        message_list=messages_to_send["message_list"], queue_name=SQS_QUEUE_NAME
    )


@mock_sqs
@mock_s3
def test_generate_buckets_diff(s3_report_path: str):
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
        str(LANDSAT_GAP_REPORT),
        TEST_BUCKET_NAME,
        S3_LANDSAT_GAP_REPORT,
    )

    print(list(boto3.resource("s3").Bucket(TEST_BUCKET_NAME).objects.all()))
    with patch.object(landsat_gap_filler, "S3_BUCKET_PATH", s3_report_path):
        # No differences
        fill_the_gap(landsat="landsat_5", sync_queue_name=SQS_QUEUE_NAME)
        queue = get_queue(queue_name=SQS_QUEUE_NAME)
        number_of_msgs = queue.attributes.get("ApproximateNumberOfMessages")
        assert int(number_of_msgs) == 28


@mock_sqs
@mock_s3
def test_exceptions(s3_report_path: str):
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
        str(LANDSAT_GAP_REPORT),
        TEST_BUCKET_NAME,
        S3_LANDSAT_GAP_REPORT,
    )

    print(list(boto3.resource("s3").Bucket(TEST_BUCKET_NAME).objects.all()))
    with patch.object(landsat_gap_filler, "S3_BUCKET_PATH", s3_report_path):
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