from unittest.mock import patch

import boto3
from moto import mock_s3

from tools.monitoring.landsat_gap_report import (
    get_and_filter_keys_from_files,
    get_and_filter_keys
)
from tools.monitoring import landsat_gap_report
from tools.tests.conftest import *


def test_get_and_filter_keys_from_files(
        monkeypatch,
        fake_landsat_bulk_file: Path,
):
    keys = get_and_filter_keys_from_files(fake_landsat_bulk_file)
    assert len(keys) == 20


@mock_s3
def test_get_and_filter_keys(
        monkeypatch,
        inventory_landsat_manifest_file,
        s3_inventory_data_file: URL,
        inventory_landsat_data_file,
        s3_inventory_manifest_file: URL,
):
    s3_client = boto3.client("s3", region_name=REGION)
    s3_client.create_bucket(
        Bucket=INVENTORY_BUCKET_NAME,
        CreateBucketConfiguration={
            "LocationConstraint": REGION,
        },
    )

    # Upload inventory manifest
    s3_client.upload_file(
        str(inventory_landsat_manifest_file),
        INVENTORY_BUCKET_NAME,
        str(s3_inventory_manifest_file),
    )

    # Upload inventory data
    s3_client.upload_file(
        str(inventory_landsat_data_file),
        INVENTORY_BUCKET_NAME,
        str(s3_inventory_data_file),
    )

    print(list(boto3.resource("s3").Bucket("test-inventory-bucket").objects.all()))

    s3_inventory_path = URL(
        f"s3://{INVENTORY_BUCKET_NAME}/{INVENTORY_FOLDER}/{INVENTORY_BUCKET_NAME}/"
    )

    with patch.object(landsat_gap_report, "LANDSAT_INVENTORY_PATH", str(s3_inventory_path)):
        keys = get_and_filter_keys("landsat_5")
        assert len(keys) == 1
