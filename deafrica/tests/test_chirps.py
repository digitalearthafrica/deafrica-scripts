import boto3
import moto
import pytest
from deafrica.data.chirps import URL_TEMPLATE, download_and_cog_chirps
from deafrica.tests.conftest import TEST_DATA_DIR


@pytest.mark.xfail(reason="Rasterio isn't able to retrieve file from URL")
@moto.mock_s3
def test_one_full(remote_file):
    TEST_BUCKET_NAME = "test-bucket"
    TEST_REGION = "ap-southeast-2"

    s3_client = boto3.client("s3", region_name=TEST_REGION)
    s3_client.create_bucket(
        Bucket=TEST_BUCKET_NAME,
        CreateBucketConfiguration={
            "LocationConstraint": TEST_REGION,
        },
    )

    year = "2018"
    month = "09"

    s3_dst = f"s3://{TEST_BUCKET_NAME}"

    download_and_cog_chirps(year, month, s3_dst, overwrite=True)

    out_data = f"chirps-v2.0_{year}.{month}.tif"
    out_stac = f"chirps-v2.0_{year}.{month}.stac-item.json"

    assert s3_client.head_object(Bucket=TEST_BUCKET_NAME, Key=out_data)
    assert s3_client.head_object(Bucket=TEST_BUCKET_NAME, Key=out_stac)


@pytest.fixture
def remote_file(httpserver):
    in_file = "chirps-v2.0.2018.09.tif.gz"
    local_file = TEST_DATA_DIR / "chirps" / in_file
    test_url = URL_TEMPLATE.format(in_file=in_file)
    httpserver.expect_request(test_url).respond_with_data(open(local_file, "rb").read())
    yield httpserver.url_for(test_url)
