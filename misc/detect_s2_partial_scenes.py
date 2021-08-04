"""
Generate a report on scenes in deafrica-sentinel-2 bucket which have incomplete data
E.g command: python generate_report.py "s3://deafrica-sentinel-2-inventory/deafrica-sentinel-2/deafrica-sentinel-2-inventory/2020-11-24T00-00Z/manifest.json"
                                        africa_account report.txt

"""

from datetime import datetime
from pathlib import Path
import boto3
import csv
import gzip
import click
import json
import pandas as pd
from tqdm import tqdm


MANIFEST_SUFFIX = "manifest.json"
SRC_BUCKET_NAME = "deafrica-sentinel-2"
INVENTORY_BUCKET_NAME = "s3://deafrica-sentinel-2-inventory/"


@click.command()
@click.argument("manifest-file")
@click.argument("output-filepath")
def generate_report(manifest_file, output_filepath):
    """
    Compare Sentinel-2 buckets in US and Africa and detect differences
    A report containing missing keys will be written to output folder
    """
    s3 = boto3.client("s3", region_name="af-south-1")
    manifest_file = manifest_file

    def read_manifest():
        bucket, key = manifest_file.replace("s3://", "").split("/", 1)
        s3_clientobj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(s3_clientobj["Body"].read().decode("utf-8"))

    manifest = read_manifest()
    df = pd.Series()
    counter = 0
    for obj in tqdm(manifest["files"]):
        bucket = "deafrica-sentinel-2-inventory"
        gzip_obj = s3.get_object(Bucket=bucket, Key=obj["key"])
        inventory_df = pd.read_csv(
            gzip_obj["Body"],
            names=["bucket", "key", "size", "time"],
            compression="gzip",
            header=None,
        )
        # second column is the object key
        inventory_df["key"] = inventory_df["key"].map(lambda a: Path(a).parent)
        count = inventory_df.groupby("key").count()["size"]
        partial_inventory = count[count != 18]
        df = df.append(partial_inventory)
        # aggregate across files
        df = df.groupby(df.index).sum()
        df = df[df != 18]

    print(f"{len(df)} partial scenes found in {SRC_BUCKET_NAME}")

    output_file = open(output_filepath, "w")
    df.index = [
        "s3://sentinel-cogs/" + str(x) + "/" + x.name + ".json" for x in df.index
    ]
    df.index = df.index.astype(str)
    output_file.write("\n".join(df.index))


if __name__ == "__main__":
    generate_report()
