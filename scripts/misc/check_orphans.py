import json
import os

from odc.aws import s3_fetch, s3_client, s3_ls_dir, s3_ls

os.environ["AWS_ACCESS_KEY_ID"] = ""
os.environ["AWS_SECRET_ACCESS_KEY"] = ""
os.environ["AWS_S3_ENDPOINT"] = "s3.us-west-2.amazonaws.com"
os.environ["AWS_DEFAULT_REGION"] = "us-west-2"

S3_BUCKET_PATH = "s3://deafrica-landsat/status-report/"


def get_orphans():
    s3 = s3_client(
        region_name="af-south-1",
        use_ssl=True,
        cache=False,
    )

    print("Finding Orphans")
    report_files = list(s3_ls_dir(uri=S3_BUCKET_PATH, s3=s3))

    report_files.sort()

    orphan_landsat8 = [
        orphan_file for orphan_file in report_files if "landsat_8" in orphan_file
    ][-1]
    orphan_landsat7 = [
        orphan_file for orphan_file in report_files if "landsat_7" in orphan_file
    ][-1]
    orphan_landsat5 = [
        orphan_file for orphan_file in report_files if "landsat_5" in orphan_file and "json" in orphan_file
    ][-1]

    list_orphan_paths = []
    for orphan in [orphan_landsat7, orphan_landsat5, orphan_landsat8]:
        print(f"Finding {orphan}")
        file = s3_fetch(
            url=orphan,
            s3=s3,
            range=None,
        )

        dict_file = json.loads(file.decode('utf8').replace("'", '"'))
        list_orphan_paths.extend(set(dict_file.get("orphan")))

    return list_orphan_paths


def check_keys(path_list):
    print("Checking keys")

    exist = []
    not_exist = []
    for path in path_list:
        usgs = path.replace("s3://deafrica-landsat", "s3://usgs-landsat")
        returned = set(s3_ls(usgs, s3=None, **{"RequestPayer": "requester"}))
        if returned:
            exist.append(usgs)
        else:
            not_exist.append(usgs)

    print(f"EXIST : {exist}")
    print(f"NoEXIST : {not_exist}")


if __name__ == "__main__":
    check_keys(path_list=get_orphans())
