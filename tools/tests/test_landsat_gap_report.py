from pathlib import Path
from unittest.mock import patch, PropertyMock

import boto3
from click.testing import CliRunner
from moto import mock_s3
from urlpath import URL

from tools.monitoring.landsat_gap_report import (
    get_and_filter_keys_from_files,
    get_and_filter_keys,
    cli,
)
from tools.tests.conftest import (
    REGION,
    INVENTORY_BUCKET_NAME,
    INVENTORY_FOLDER,
    TEST_BUCKET_NAME,
)


def test_get_and_filter_keys_from_files(fake_landsat_bulk_file: Path):
    keys = get_and_filter_keys_from_files(fake_landsat_bulk_file)
    assert len(keys) == 20


@mock_s3
def test_get_and_filter_keys(
    inventory_landsat_manifest_file: Path,
    s3_inventory_data_file: URL,
    inventory_landsat_data_file: Path,
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

    with patch(
        "tools.monitoring.landsat_gap_report.LANDSAT_INVENTORY_PATH", s3_inventory_path
    ):
        keys = get_and_filter_keys("landsat_5")
        assert len(keys) == 1


@mock_s3
def test_landsat_gap_report_cli(
    inventory_landsat_manifest_file: Path,
    s3_inventory_data_file: URL,
    inventory_landsat_data_file: Path,
    s3_inventory_manifest_file: URL,
    fake_landsat_bulk_file: Path,
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

    s3_client2 = boto3.client("s3", region_name=REGION)
    s3_client2.create_bucket(
        Bucket=TEST_BUCKET_NAME,
        CreateBucketConfiguration={
            "LocationConstraint": REGION,
        },
    )

    with patch(
        "tools.monitoring.landsat_gap_report.LANDSAT_INVENTORY_PATH", s3_inventory_path
    ), patch(
        "tools.monitoring.landsat_gap_report.download_file_to_tmp",
        new_callable=PropertyMock,
        return_value=fake_landsat_bulk_file,
    ):

        runner = CliRunner()
        runner.invoke(
            cli,
            [
                "landsat_5",
                TEST_BUCKET_NAME,
            ],
        )

        bucket_objs = list(boto3.resource("s3").Bucket(TEST_BUCKET_NAME).objects.all())
        assert len(bucket_objs) == 1
        assert "landsat_5" in bucket_objs[0].key
