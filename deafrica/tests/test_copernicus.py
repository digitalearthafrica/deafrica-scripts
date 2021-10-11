import boto3
import moto

from deafrica.data.copernicus import download_and_cog_copernicus


# @pytest.mark.xfail(reason="Rasterio isn't able to retrieve file from URL")
@moto.mock_s3
def test_one_full(remote_file):
    test_bucket_name = "test-bucket"
    test_region = "ap-southeast-2"

    s3_client = boto3.client("s3", region_name=test_region)
    s3_client.create_bucket(
        Bucket=test_bucket_name,
        CreateBucketConfiguration={
            "LocationConstraint": test_region,
        },
    )

    year = "2018"

    s3_dst = f"s3://{test_bucket_name}"

    download_and_cog_copernicus(year, s3_dst, overwrite=True)

    print(list(boto3.resource("s3").Bucket(test_bucket_name).objects.all()))
    # assert s3_client.head_object(Bucket=test_bucket_name, Key=out_data)
    # assert s3_client.head_object(Bucket=test_bucket_name, Key=out_stac)
