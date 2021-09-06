import gzip
import logging
import math

import requests
from odc.aws import s3_client, s3_fetch, s3_ls_dir


def setup_logging() -> logging.Logger:
    """Set up a simple logger"""
    log = logging.getLogger()
    console = logging.StreamHandler()
    log.addHandler(console)
    return log


def send_slack_notification(url: str, title: str, message: str):
    """
    Sends a slack notification.
    :param url: (str) Slack webhook url
    :param title: (str) Slack notification title
    :param message: (str) Slack notification message in markdown
    """

    content = {
        "text": f"{title}",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"{message}",
                },
            }
        ],
    }

    response = requests.post(url, json=content)

    # Raise exception if response is not 200
    response.raise_for_status()


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

    return report_files[-1]


def read_report(report_path: str, limit=None):
    """
    read the gap report
    """

    s3 = s3_client(region_name="af-south-1")
    missing_scene_file_gzip = s3_fetch(url=report_path, s3=s3)

    missing_scene_paths = [
        scene_path.strip()
        for scene_path in gzip.decompress(missing_scene_file_gzip)
        .decode("utf-8")
        .split("\n")
        if scene_path
    ]

    if limit:
        missing_scene_paths = missing_scene_paths[: int(limit)]

    return missing_scene_paths


def split_list_equally(list_to_split: list, num_inter_lists: int):
    """
    Split list_to_split in equally balanced lists among num_inter_lists
    """
    if num_inter_lists < 1:
        raise Exception("max_items_per_line needs to be greater than 0")

    max_list_items = math.ceil(len(list_to_split) / num_inter_lists)
    return [
        list_to_split[i : i + max_list_items]
        for i in range(0, len(list_to_split), max_list_items)
    ]
