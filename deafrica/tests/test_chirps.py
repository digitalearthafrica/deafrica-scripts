import boto3
import moto
import pytest
from deafrica.data.chirps import (
    DAILY_URL_TEMPLATE,
    MONTHLY_URL_TEMPLATE,
    download_and_cog_chirps,
)
from deafrica.tests.conftest import TEST_DATA_DIR

from botocore.exceptions import ClientError


TEST_BUCKET_NAME = "test-bucket"
TEST_REGION = "ap-southeast-2"

YEAR = "2018"
MONTH = "09"
DAY = "09"


@moto.mock_s3
def test_one_month(remote_file_month):
    s3_client = boto3.client("s3", region_name=TEST_REGION)
    s3_client.create_bucket(
        Bucket=TEST_BUCKET_NAME,
        CreateBucketConfiguration={
            "LocationConstraint": TEST_REGION,
        },
    )

    s3_dst = f"s3://{TEST_BUCKET_NAME}"

    download_and_cog_chirps(YEAR, MONTH, s3_dst, overwrite=True)

    out_data = f"chirps-v2.0_{YEAR}.{MONTH}.tif"
    out_stac = f"chirps-v2.0_{YEAR}.{MONTH}.stac-item.json"

    assert s3_client.head_object(Bucket=TEST_BUCKET_NAME, Key=out_data)
    assert s3_client.head_object(Bucket=TEST_BUCKET_NAME, Key=out_stac)


def test_one_day(remote_file_day):
    try:
        s3_client = boto3.client("s3", region_name=TEST_REGION)
        s3_client.create_bucket(
            Bucket=TEST_BUCKET_NAME,
            CreateBucketConfiguration={
                "LocationConstraint": TEST_REGION,
            },
        )
    except ClientError:
        pass

    s3_dst = f"s3://{TEST_BUCKET_NAME}"

    download_and_cog_chirps(YEAR, MONTH, s3_dst, day=DAY, overwrite=True)

    out_data = f"chirps-v2.0_{YEAR}.{MONTH}.{DAY}.tif"
    out_stac = f"chirps-v2.0_{YEAR}.{MONTH}.{DAY}.stac-item.json"

    assert s3_client.head_object(Bucket=TEST_BUCKET_NAME, Key=out_data)
    assert s3_client.head_object(Bucket=TEST_BUCKET_NAME, Key=out_stac)


@pytest.fixture
def remote_file_month(httpserver):
    in_file = "chirps-v2.0.2018.09.tif.gz"
    local_file = TEST_DATA_DIR / "chirps" / in_file
    test_url = MONTHLY_URL_TEMPLATE.format(in_file=in_file)
    httpserver.expect_request(test_url).respond_with_data(open(local_file, "rb").read())
    yield httpserver.url_for(test_url)


@pytest.fixture
def remote_file_day(httpserver):
    in_file = "chirps-v2.0.2018.09.09.tif.gz"
    local_file = TEST_DATA_DIR / "chirps" / in_file
    test_url = DAILY_URL_TEMPLATE.format(in_file=in_file, year=YEAR)
    httpserver.expect_request(test_url).respond_with_data(open(local_file, "rb").read())
    yield httpserver.url_for(test_url)
