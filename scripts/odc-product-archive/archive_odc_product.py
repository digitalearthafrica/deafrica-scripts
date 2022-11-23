
import datacube
import pandas as pd
import sys
import boto3
import os

s3 = boto3.client('s3')
dc = datacube.Datacube()


s3 = boto3.resource(
    service_name='s3',
    region_name=os.environ["AWS_DEFAULT_REGION"],
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
)

# store dataset list
datasets_list = []

# Empty array to store datasets ids
dataset_ids = []

# input product name
PRODUCT_NAME = sys.argv[1]

# search datasets using product name
try:

    datasets_list = dc.find_datasets(product=PRODUCT_NAME)

    # Storing dataset ids
    for dataset_id in datasets_list:
        dataset_ids.append(dataset_id.id)
except:
    print("Product name "+PRODUCT_NAME + " does not exist")
    sys.exit(1)

# check datasets
if not dataset_ids:
    print("No datasets to archive................................................")
else:
    df = pd.DataFrame(dataset_ids)
    s3.Bucket("s3://deafrica-landsat/status-report/archived/").upload_file(Filename=PRODUCT_NAME + "_archived.csv", Key=PRODUCT_NAME + "_archived.csv")
    dc.index.datasets.archive(dataset_ids)

print("Done")