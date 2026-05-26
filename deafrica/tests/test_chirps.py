import boto3
import moto
import pytest
from botocore.exceptions import ClientError

from deafrica.data.chirps import download_and_cog_chirps
from deafrica.tests.conftest import TEST_DATA_DIR

TEST_BUCKET_NAME = "test-bucket"
TEST_REGION = "ap-southeast-2"

YEAR = "2018"
MONTH = "09"
DAY = "09"


def get_test_file(is_daily: bool, use_gz: bool = True) -> str:
    """Return full path to local test file."""
    chirps_dir = TEST_DATA_DIR / "chirps"
    if is_daily:
        if use_gz:
            return str(chirps_dir / "chirps-v2.0.2018.09.09.tif.gz")
        else:
            return str(chirps_dir / "chirps-v2.0.2018.08.08.tif")
    else:
        if use_gz:
            return str(chirps_dir / "chirps-v2.0.2018.09.tif.gz")
        else:
            return str(chirps_dir / "chirps-v2.0.2018.08.tif")


@moto.mock_s3
def test_one_month():
    s3_client, s3_dst = bucket_create()
    local_file = get_test_file(is_daily=False, use_gz=True)

    download_and_cog_chirps(
        YEAR, MONTH, s3_dst,
        overwrite=True,
        test_local_file=local_file
    )
    check_s3_paths(s3_client, f"chirps-v2.0_{YEAR}.{MONTH}")


@moto.mock_s3
def test_one_day():
    s3_client, s3_dst = bucket_create()
    local_file = get_test_file(is_daily=True, use_gz=True)

    download_and_cog_chirps(
        YEAR, MONTH, s3_dst,
        day=DAY,
        overwrite=True,
        test_local_file=local_file
    )
    check_s3_paths(s3_client, f"{YEAR}/{MONTH}/chirps-v2.0_{YEAR}.{MONTH}.{DAY}")


@moto.mock_s3
def test_one_month_non_gz():
    s3_client, s3_dst = bucket_create()
    local_file = get_test_file(is_daily=False, use_gz=False)

    download_and_cog_chirps(
        YEAR, MONTH, s3_dst,
        overwrite=True,
        test_local_file=local_file
    )
    check_s3_paths(s3_client, f"chirps-v2.0_{YEAR}.{MONTH}")


@moto.mock_s3
def test_one_day_non_gz():
    s3_client, s3_dst = bucket_create()
    local_file = get_test_file(is_daily=True, use_gz=False)

    download_and_cog_chirps(
        YEAR, MONTH, s3_dst,
        day=DAY,
        overwrite=True,
        test_local_file=local_file
    )
    check_s3_paths(s3_client, f"{YEAR}/{MONTH}/chirps-v2.0_{YEAR}.{MONTH}.{DAY}")


def bucket_create():
    try:
        s3_client = boto3.client("s3", region_name=TEST_REGION)
        s3_client.create_bucket(
            Bucket=TEST_BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": TEST_REGION},
        )
    except ClientError:
        pass
    return s3_client, f"s3://{TEST_BUCKET_NAME}"


def check_s3_paths(s3_client, path):
    out_data = f"{path}.tif"
    out_stac = f"{path}.stac-item.json"
    assert s3_client.head_object(Bucket=TEST_BUCKET_NAME, Key=out_data)
    assert s3_client.head_object(Bucket=TEST_BUCKET_NAME, Key=out_stac)
