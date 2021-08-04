import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from odc.aws import inventory, s3_head_object

os.environ["AWS_ACCESS_KEY_ID"] = ""
os.environ["AWS_SECRET_ACCESS_KEY"] = ""
os.environ["AWS_S3_ENDPOINT"] = "s3.af-south-1.amazonaws.com"
os.environ["AWS_DEFAULT_REGION"] = "af-south-1"


def double_slash_report():
    """
    """
    s3 = inventory.s3_client(
        profile=None,
        creds=None,
        region_name='af-south-1',
        session=None,
        aws_unsigned=True,
        use_ssl=True,
        cache=False,
    )
    manifest = inventory.find_latest_manifest(
        prefix='s3://deafrica-landsat-inventory/deafrica-landsat/deafrica-landsat-inventory/',
        s3=s3
    )
    inventory_list = inventory.list_inventory(
        manifest=manifest,
        s3=s3,
        n_threads=200,
    )

    double_slash_list = []
    for namespace in inventory_list:
        if hasattr(namespace, 'Key') and '//' in namespace.Key:
            yield namespace.Key


def create_txt(path_list, file_name):
    f = open(f"{file_name}.txt", "a+")
    [f.write(f"{path}\n") for path in path_list]
    f.close()


def check_keys(path_list):
    s3 = inventory.s3_client(
        profile=None,
        creds=None,
        region_name='af-south-1',
        session=None,
        aws_unsigned=True,
        use_ssl=True,
        cache=False,
    )
    to_remove_paths = []

    with ThreadPoolExecutor(max_workers=400) as executor:
        tasks = [
            executor.submit(
                s3_head_object,
                f's3://deafrica-landsat/{path}',
                s3,
            )
            for path in path_list
        ]

        [to_remove_paths.append(future.result()) for future in as_completed(tasks) if future.result()]

        create_txt(to_remove_paths, 'to_remove')


def get_all_versions(bucket, filename):
    s3 = boto3.client('s3')
    keys = ["Versions", "DeleteMarkers"]
    results = []
    for k in keys:
        response = s3.list_object_versions(Bucket=bucket, Prefix=filename)[k]
        to_delete = [r["VersionId"] for r in response if r["Key"] == filename]
        results.extend(to_delete)

    print(f"results: {results}")

    return results


def remove_object(bucket, path_list):
    s3 = boto3.client('s3')

    for file_path in path_list:
        [
            s3.delete_object(Bucket=bucket, Key=file_path, VersionId=version)
            for version in get_all_versions(bucket, file_path)
        ]


if __name__ == "__main__":
    path_list = double_slash_report()
    check_keys(path_list)
    # Just use if you are sure of what you are doing!!!
    # f = open(f"misc/to_remove.txt", "r+")
    # remove_object('deafrica-landsat', [line.replace('\n', '') for line in f.readlines()])

