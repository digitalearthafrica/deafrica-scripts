"""
Read landsat gap reports for Landsat 5, Landsat 7 and Landsat 8 and check orphan scenes for cleanup
"""
import json
from datetime import datetime
from odc.aws import s3_client, s3_fetch, s3_ls, s3_dump, s3_ls_dir

DEAFRICA_AWS_REGION = "af-south-1"
DEAFRICA_LANDSAT_BUCKET_NAME = "deafrica-landsat"
DEAFRICA_REPORT_PATH = f"s3://{DEAFRICA_LANDSAT_BUCKET_NAME}/status-report/"

USGS_AWS_REGION = "us-west-2"
USGS_S3_BUCKET_NAME = "usgs-landsat"

PUBLISH_TO_S3 = False


def get_orphans():
    s3 = s3_client(region_name=DEAFRICA_AWS_REGION)

    print("Finding Orphans")
    report_files = list(s3_ls_dir(uri=DEAFRICA_REPORT_PATH, s3=s3))
    report_files_json = [
        report_file for report_file in report_files if report_file.endswith(".json")
    ]

    # fetch the latest report: Landsat 5, Landsat 7 and Landsat 8
    report_files_json.sort()
    landsat_8_report = [
        report_file for report_file in report_files_json if "landsat_8" in report_file
    ][-1]
    landsat_7_report = [
        report_file for report_file in report_files_json if "landsat_7" in report_file
    ][-1]
    landsat_5_report = [
        report_file for report_file in report_files_json if "landsat_5" in report_file
    ][-1]

    # collect orphan paths
    list_orphan_paths = []
    for report in [landsat_5_report, landsat_7_report, landsat_8_report]:
        print(f"collect orphan scenes from {report}")
        file = s3_fetch(url=report, s3=s3)
        dict_file = json.loads(file)
        list_orphan_paths.extend(set(dict_file.get("orphan")))

    return list_orphan_paths


def check_scene_exist_in_source(path: str):
    """
    check scene exists in usgs source bucket
    """
    s3 = s3_client(region_name=USGS_AWS_REGION)
    usgs_path = path.replace(
        f"s3://{DEAFRICA_LANDSAT_BUCKET_NAME}", f"s3://{USGS_S3_BUCKET_NAME}"
    )

    returned = set(s3_ls(usgs_path, s3=s3, **{"RequestPayer": "requester"}))
    if returned:
        return True

    return False


def publish_to_s3(data: list, output_filename: str, content_type: str = "text/plain"):
    """
    write report to s3
    """
    s3 = s3_client(region_name=DEAFRICA_AWS_REGION)
    s3_dump(
        data=data,
        url=str(DEAFRICA_REPORT_PATH / output_filename),
        s3=s3,
        ContentType=content_type,
    )
    print(f"Report can be accessed from {DEAFRICA_REPORT_PATH / output_filename}")


if __name__ == "__main__":
    orphan_paths = get_orphans()
    print(f"orphaned_scenes 10 first keys of {len(orphan_paths)}: {list(orphan_paths[0:10])}")

    cleanup_orphan_paths = []
    for orphan_scene_path in list(orphan_paths[0:24237]):
        if not check_scene_exist_in_source(orphan_scene_path):
            cleanup_orphan_paths.append(orphan_scene_path)

    print(
        f"total orphan scenes to cleanup {len(cleanup_orphan_paths)} out of {len(orphan_paths)}"
    )

    # write report
    date_string = datetime.now().strftime("%Y-%m-%d")
    output_file = f"landsat_orphan_cleanup_{date_string}.txt"
    report_data = "\n".join(cleanup_orphan_paths)
    if PUBLISH_TO_S3:
        publish_to_s3(report_data, output_file)
    else:
        with open(output_file, "w") as f:
            f.write(report_data)
