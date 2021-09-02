import gzip
import json
import logging
import math

from odc.aws import s3_client, s3_ls_dir, s3_fetch

log = logging.getLogger()
console = logging.StreamHandler()
log.addHandler(console)


def find_latest_report(report_folder_path: str) -> str:
    """
    Function to find the latest gap report
    :return:(str) return the latest report file name
    """

    s3 = s3_client(region_name="af-south-1")

    report_files = list(s3_ls_dir(uri=report_folder_path, s3=s3))

    if not report_files:
        raise RuntimeError("Report not found!")

    report_files.sort()

    log.info(f"Last report {report_files[-1]}")

    return report_files[-1]


def read_report(report_path: str, limit=None):
    """
    read the gap report
    """

    if "update" in report_path:
        log.info("FORCED UPDATE FLAGGED!")

    s3 = s3_client(region_name="af-south-1")
    missing_scene_file_gzip = s3_fetch(url=report_path, s3=s3)

    missing_scene_paths = [
        scene_path.strip()
        for scene_path in gzip.decompress(missing_scene_file_gzip)
        .decode("utf-8")
        .split("\n")
        if scene_path
    ]

    log.info(f"Limited: {int(limit) if limit else 'No limit'}")

    if limit:
        missing_scene_paths = missing_scene_paths[: int(limit)]

    log.info(f"Number of scenes found {len(missing_scene_paths)}")
    log.info(f"Example scenes: {missing_scene_paths[0:10]}")

    return missing_scene_paths


def split_list_equally(list_to_split: list, num_inter_lists: int):
    """
    Split a big list in smaller lists in a big strings that separate items with a space
    """
    if num_inter_lists < 1:
        raise Exception("max_items_per_line needs to be greater than 0")

    max_list_items = math.ceil(len(list_to_split) / num_inter_lists)
    return [
        list_to_split[i : i + max_list_items]
        for i in range(0, len(list_to_split), max_list_items)
    ]
