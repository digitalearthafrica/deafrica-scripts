from unittest.mock import patch

import boto3
import moto

from tools.data.chirps import download_and_cog_chirps
from tools.tests.conftest import *


@patch("tools.data.chirps.build_in_data")
@moto.mock_s3
def test_one_full(mock_build_in_data, chirps_file):
    s3_client = boto3.client("s3", region_name=CHIRPS_REGION)
    s3_client.create_bucket(
        Bucket=TEST_BUCKET_NAME,
        CreateBucketConfiguration={
            "LocationConstraint": CHIRPS_REGION,
        },
    )

    year = "2018"
    month = "09"

    s3_dst = f"s3://{TEST_BUCKET_NAME}"

    mock_build_in_data.return_value = str(chirps_file)
    download_and_cog_chirps(year, month, s3_dst, overwrite=True)

    out_data = f"chirps-v2.0_{year}.{month}.tif"
    out_stac = f"chirps-v2.0_{year}.{month}.stac-item.json"

    assert s3_client.head_object(Bucket=TEST_BUCKET_NAME, Key=out_data)
    assert s3_client.head_object(Bucket=TEST_BUCKET_NAME, Key=out_stac)
