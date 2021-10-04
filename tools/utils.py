import gzip
import json
import logging
import math
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import click
import requests
from odc.aws import s3_client, s3_fetch, s3_ls_dir


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """Set up a simple logger"""
    log = logging.getLogger(__name__)
    console = logging.StreamHandler()
    log.addHandler(console)
    log.setLevel(level)
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


def find_latest_report(
    report_folder_path: str, contains: str = None, not_contains: str = None
) -> str:
    """
    Function to find the latest gap report
    :return:(str) return the latest report file name
    """

    s3 = s3_client(region_name="af-south-1")

    report_files = list(s3_ls_dir(uri=report_folder_path, s3=s3))

    if contains is not None:
        report_files = [report for report in report_files if contains in report]

    if not_contains is not None:
        report_files = [report for report in report_files if not_contains not in report]

    report_files.sort()

    if not report_files:
        raise RuntimeError("Report not found!")

    return report_files[-1]


def read_report_missing_scenes(report_path: str, limit=None):
    """
    read the gap report
    """

    s3 = s3_client(region_name="af-south-1")
    report_json = s3_fetch(url=report_path, s3=s3)
    report_dict = json.loads(report_json)

    if report_dict.get("missing", None) is None:
        raise Exception("Missing scenes not found")

    missing_scene_paths = [
        scene_path.strip() for scene_path in report_dict["missing"] if scene_path
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


def convert_str_to_date(date: str):
    """
    Function to convert a date in a string format into a datetime YYYY/MM/DD.

    :param date: (str) Date in a string format
    :return: (datetime) return datetime of a string date. The time will always be 0.
    """
    try:
        return datetime.strptime(date, "%Y/%m/%d").date()
    except ValueError:
        try:
            return datetime.strptime(date, "%Y-%m-%d").date()
        except ValueError as error:
            raise error


def time_process(start: float):
    """
    Times the process
    :param start:
    :return:
    """
    t_sec = round(time.time() - start)
    (t_min, t_sec) = divmod(t_sec, 60)
    (t_hour, t_min) = divmod(t_min, 60)

    return f"{t_hour} hour: {t_min} min: {t_sec} sec"


def download_file_to_tmp(url: str, file_name: str, always_return_path: bool = True):
    """
    Function to check if a specific file is already downloaded based on its size,
    if not downloaded, it will download the file from the informed server.
    The file will be saved in the local machine/container under the /tmp/ folder,
    so the OS will delete that accordingly with its pre-defined configurations.
    Warning: The server shall have enough free storage.

    :param url:(String) URL path for the file server
    :param file_name: (String) File name which will be downloaded
    :param always_return_path:(bool) Returns the path even if already updated
    :return: (String) File path where it was downloaded. Hardcoded for /tmp/
    """

    logging.info("Start downloading files")

    url = urlparse(f"{url}{file_name}")
    file_path = Path(f"/tmp/{file_name}")

    # check if file exists and comparing size against cloud file
    if file_path.exists():

        logging.info(f"File already found on {file_path}")

        file_size = file_path.stat().st_size
        head = requests.head(url.geturl())

        if hasattr(head, "headers") and head.headers.get("Content-Length"):
            server_file_size = head.headers["Content-Length"]
            logging.info(
                f"Comparing sizes between local saved file and server hosted file,"
                f" local file size : {file_size} server file size: {server_file_size}"
            )

            if int(file_size) == int(server_file_size):
                logging.info("Already updated!!")
                return file_path if always_return_path else None

    logging.info(f"Downloading file {file_name} to {file_path}")
    downloaded = requests.get(url.geturl(), stream=True)
    file_path.write_bytes(downloaded.content)

    logging.info(f"{file_name} Downloaded!")
    return file_path


def test_http_return(returned):
    """
    Test API response
    :param returned:
    :return:
    """
    if hasattr(returned, "status_code") and returned.status_code != 200:
        url = returned.url if hasattr(returned, "url") else "Not informed"
        content = returned.content if hasattr(returned, "content") else "Not informed"
        text = returned.text if hasattr(returned, "text") else "Not informed"
        status_code = (
            returned.status_code if hasattr(returned, "status_code") else "Not informed"
        )
        reason = returned.reason if hasattr(returned, "reason") else "Not informed"
        raise Exception(
            f"API return is not 200: \n"
            f"-url: {url} \n"
            f"-content: {content} \n"
            f"-text: {text} \n"
            f"-status_code: {status_code} \n"
            f"-reason: {reason} \n"
        )


# A whole bunch of generic Click options
slack_url = click.option(
    "--slack_url",
    help="Slack url to use to send a notification",
    default=None,
)

update_stac = click.option(
    "--update_stac",
    is_flag=True,
    default=False,
    help="Will fill a special report within all scenes from the source",
)
limit = click.option(
    "--limit",
    "-l",
    help="Limit the number of messages to transfer.",
    default=None,
)
