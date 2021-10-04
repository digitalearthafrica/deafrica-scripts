import json
from random import randrange
from unittest.mock import patch

import boto3
from click.testing import CliRunner
from moto import mock_s3, mock_sqs
from odc.aws.queue import get_queue
from urlpath import URL

from tools.monitoring import s2_gap_filler
from tools.tests.conftest import (
    REGION,
    SQS_QUEUE_NAME,
    COGS_REGION,
    TEST_BUCKET_NAME,
    TEST_DATA_DIR,
)

DATA_FOLDER = "sentinel_2"
S2_JSON_FILE = "2021-08-17_gap_report_update.json"
S2_FAKE_STAC_FILE = "fake_stac.json"
S3_S2_REPORT_FILE = URL("status-report") / S2_JSON_FILE
LOCAL_REPORT_UPDATE_FILE = TEST_DATA_DIR / DATA_FOLDER / S2_JSON_FILE
FAKE_STAC_FILE_PATH = TEST_DATA_DIR / DATA_FOLDER / S2_FAKE_STAC_FILE


@mock_s3
@mock_sqs
def test_publish_message_s2_gap_filler(s3_report_path: URL):
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
        str(LOCAL_REPORT_UPDATE_FILE),
        TEST_BUCKET_NAME,
        str(S3_S2_REPORT_FILE),
    )

    missing_dict = json.loads(open(str(LOCAL_REPORT_UPDATE_FILE), "rb").read())

    files = [scene_path.strip() for scene_path in missing_dict["missing"] if scene_path]

    [
        s3_client.upload_file(
            str(FAKE_STAC_FILE_PATH), TEST_BUCKET_NAME, f"{i}/{S2_FAKE_STAC_FILE}"
        )
        for i in range(len(files))
    ]

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
def test_s2_gap_filler_cli(s3_report_path: URL):
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
        str(LOCAL_REPORT_UPDATE_FILE),
        TEST_BUCKET_NAME,
        str(S3_S2_REPORT_FILE),
    )

    missing_dict = json.loads(open(str(LOCAL_REPORT_UPDATE_FILE), "rb").read())

    files = [scene_path.strip() for scene_path in missing_dict["missing"] if scene_path]

    [
        s3_client.upload_file(
            str(FAKE_STAC_FILE_PATH), TEST_BUCKET_NAME, f"{i}/{S2_FAKE_STAC_FILE}"
        )
        for i in range(len(files))
    ]

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
