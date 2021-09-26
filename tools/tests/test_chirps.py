import boto3
import moto
from tools.data.chirps import download_and_cog_chirps


@moto.mock_s3
def test_one_full():

    TEST_BUCKET_NAME = "fake-bucket"
    REGION = "ap-southeast-2"
    s3_client = boto3.client("s3", region_name=REGION)
    s3_client.create_bucket(
        Bucket=TEST_BUCKET_NAME,
        CreateBucketConfiguration={
            "LocationConstraint": REGION,
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
