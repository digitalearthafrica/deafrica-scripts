from unittest.mock import patch

import boto3
from moto import mock_s3

from monitoring.tests.conftest import *
from monitoring.tools import s2_gap_report
from monitoring.tools.s2_gap_report import (
    get_and_filter_cogs_keys,
    generate_buckets_diff,
)


@mock_s3
def test_get_and_filter_cogs_keys(
    monkeypatch,
    inventory_data_file: Path,
    s3_inventory_data_file: URL,
    inventory_manifest_file: Path,
    s3_inventory_manifest_file: URL,
):
    s3_client = boto3.client("s3", region_name=COGS_REGION)
    s3_client.create_bucket(
        Bucket=INVENTORY_BUCKET,
        CreateBucketConfiguration={
            "LocationConstraint": COGS_REGION,
        },
    )

    # Upload inventory manifest
    s3_client.upload_file(
        str(inventory_manifest_file),
        INVENTORY_BUCKET,
        str(s3_inventory_manifest_file),
    )

    # Upload inventory data
    s3_client.upload_file(
        str(inventory_data_file),
        INVENTORY_BUCKET,
        str(s3_inventory_data_file),
    )

    print(list(boto3.resource("s3").Bucket("test-inventory-bucket").objects.all()))

    s3_inventory_path = URL(
        f"s3://{INVENTORY_BUCKET}/{INVENTORY_FOLDER}/{INVENTORY_BUCKET}/"
    )

    with patch.object(
        s2_gap_report, "SENTINEL_COGS_INVENTORY_PATH", str(s3_inventory_path)
    ), patch.object(s2_gap_report, "COGS_FOLDER_NAME", str(INVENTORY_FOLDER)):
        scenes_list = get_and_filter_cogs_keys()
        assert len(scenes_list) == 6


@mock_s3
def test_generate_buckets_diff(
    monkeypatch,
    inventory_data_file: Path,
    s3_inventory_data_file: URL,
    inventory_manifest_file: Path,
    s3_inventory_manifest_file: URL,
):
    s3_client_cogs = boto3.client("s3", region_name=COGS_REGION)
    s3_client_cogs.create_bucket(
        Bucket=INVENTORY_BUCKET_COGS,
        CreateBucketConfiguration={
            "LocationConstraint": COGS_REGION,
        },
    )

    # Upload inventory manifest
    s3_client_cogs.upload_file(
        str(inventory_manifest_file),
        INVENTORY_BUCKET_COGS,
        str(s3_inventory_manifest_file),
    )

    # Upload inventory data
    s3_client_cogs.upload_file(
        str(inventory_data_file),
        INVENTORY_BUCKET_COGS,
        str(s3_inventory_data_file),
    )

    print(list(boto3.resource("s3").Bucket("test-cogs-inventory-bucket").objects.all()))

    s3_client = boto3.client("s3", region_name=REGION)
    s3_client.create_bucket(
        Bucket=INVENTORY_BUCKET,
        CreateBucketConfiguration={
            "LocationConstraint": REGION,
        },
    )

    # Upload inventory manifest
    s3_client.upload_file(
        str(inventory_manifest_file),
        INVENTORY_BUCKET,
        str(s3_inventory_manifest_file),
    )

    # Upload inventory data
    s3_client.upload_file(
        str(inventory_data_file),
        INVENTORY_BUCKET,
        str(s3_inventory_data_file),
    )

    print(list(boto3.resource("s3").Bucket("test-inventory-bucket").objects.all()))

    s3_inventory_path = URL(
        f"s3://{INVENTORY_BUCKET}/{INVENTORY_FOLDER}/{INVENTORY_BUCKET}/"
    )

    s3_cogs_inventory_path = URL(
        f"s3://{INVENTORY_BUCKET_COGS}/{INVENTORY_FOLDER}/{INVENTORY_BUCKET}/"
    )

    status_report_path = URL(f"s3://{INVENTORY_BUCKET}/{REPORT_FOLDER}/")

    with patch.object(
        s2_gap_report, "SENTINEL_COGS_INVENTORY_PATH", str(s3_cogs_inventory_path)
    ), patch.object(
        s2_gap_report, "SENTINEL_2_INVENTORY_PATH", str(s3_inventory_path)
    ), patch.object(
        s2_gap_report, "SENTINEL_2_STATUS_REPORT_PATH", str(status_report_path)
    ), patch.object(
        s2_gap_report, "COGS_FOLDER_NAME", str(INVENTORY_FOLDER)
    ):
        # No differences
        generate_buckets_diff()
        assert (
            len(
                s3_client.list_objects_v2(
                    Bucket=INVENTORY_BUCKET, Prefix=REPORT_FOLDER
                ).get("Contents", [])
            )
            == 1
        )
