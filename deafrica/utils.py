from __future__ import annotations

import json
import logging
import math
import os
import re
import time
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urlparse

import fsspec
import gcsfs
import requests
import s3fs
import yaml
from fsspec.implementations.local import LocalFileSystem
from gcsfs import GCSFileSystem
from odc.aws import s3_client, s3_fetch, s3_ls_dir, s3_url_parse
from s3fs.core import S3FileSystem

# GDAL format: [ulx, uly, lrx, lry]
AFRICA_BBOX = [-26.36, 38.35, 64.50, -47.97]
AFRICA_EXTENT_URL = "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/africa-extent-bbox.json"


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


def is_s3_path(path: str) -> bool:
    o = urlparse(path)
    if o.scheme in ["s3"]:
        return True
    else:
        return False


def is_gcsfs_path(path: str) -> bool:
    o = urlparse(path)
    if o.scheme in ["gcs", "gs"]:
        return True
    else:
        return False


def is_url(path: str) -> bool:
    o = urlparse(path)
    if o.scheme in ["http", "https"]:
        return True
    else:
        return False


def get_filesystem(
    path: str,
    anon: bool = True,
) -> S3FileSystem | LocalFileSystem | GCSFileSystem:
    if is_s3_path(path=path):
        fs = s3fs.S3FileSystem(
            anon=anon, s3_additional_kwargs={"ACL": "bucket-owner-full-control"}
        )
    elif is_gcsfs_path(path=path):
        if anon:
            fs = gcsfs.GCSFileSystem(token="anon")
        else:
            fs = gcsfs.GCSFileSystem()
    else:
        fs = fsspec.filesystem("file")
    return fs


def check_file_exists(path: str) -> bool:
    fs = get_filesystem(path=path, anon=True)
    if fs.exists(path) and fs.isfile(path):
        return True
    else:
        return False


def check_directory_exists(path: str) -> bool:
    fs = get_filesystem(path=path, anon=True)
    if fs.exists(path) and fs.isdir(path):
        return True
    else:
        return False


def check_file_extension(path: str, accepted_file_extensions: list[str]) -> bool:
    _, file_extension = os.path.splitext(path)
    if file_extension.lower() in accepted_file_extensions:
        return True
    else:
        return False


def is_geotiff(path: str) -> bool:
    accepted_geotiff_extensions = [".tif", ".tiff", ".gtiff"]
    return check_file_extension(
        path=path, accepted_file_extensions=accepted_geotiff_extensions
    )


def find_geotiff_files(directory_path: str, file_name_pattern: str = ".*") -> list[str]:
    file_name_pattern = re.compile(file_name_pattern)

    fs = get_filesystem(path=directory_path, anon=True)

    geotiff_file_paths = []

    for root, dirs, files in fs.walk(directory_path):
        for file_name in files:
            if is_geotiff(path=file_name):
                if re.search(file_name_pattern, file_name):
                    geotiff_file_paths.append(os.path.join(root, file_name))
                else:
                    continue
            else:
                continue

    if is_s3_path(path=directory_path):
        geotiff_file_paths = [f"s3://{file}" for file in geotiff_file_paths]
    elif is_gcsfs_path(path=directory_path):
        geotiff_file_paths = [f"gs://{file}" for file in geotiff_file_paths]
    return geotiff_file_paths


def download_product_yaml(url: str) -> str:
    """
    Download a product definition file from a raw github url.

    Parameters
    ----------
    url : str
        URL to the product definition file

    Returns
    -------
    str
        Local file path of the downloaded product definition file

    """
    try:
        # Create output directory
        tmp_products_dir = "/tmp/products"
        if not check_directory_exists(tmp_products_dir):
            fs = get_filesystem(tmp_products_dir, anon=False)
            fs.makedirs(tmp_products_dir, exist_ok=True)
            logging.info(f"Created the directory {tmp_products_dir}")

        output_path = os.path.join(tmp_products_dir, os.path.basename(url))

        # Load product definition from url
        response = requests.get(url)
        response.raise_for_status()
        content = yaml.safe_load(response.content.decode(response.encoding))

        # Write to file.
        yaml_string = yaml.dump(
            content,
            default_flow_style=False,  # Ensures block format
            sort_keys=False,  # Keeps the original order
            allow_unicode=True,  # Ensures special characters are correctly represented
        )
        # Ensure it starts with "---"
        yaml_string = f"---\n{yaml_string}"

        with open(output_path, "w") as file:
            file.write(yaml_string)
        logging.info(f"Product definition file written to {output_path}")
        return Path(output_path).resolve()
    except Exception as e:
        logging.error(e)
        raise e


def s3_uri_to_public_url(s3_uri, region="af-south-1"):
    """Convert S3 URI to a public HTTPS URL"""
    bucket, key = s3_url_parse(s3_uri)
    return f"https://{bucket}.s3.{region}.amazonaws.com/{key}"


def get_last_modified(file_path: str):
    """Returns the Last-Modified timestamp
    of a given URL if available."""
    if is_gcsfs_path(file_path):
        url = file_path.replace("gs://", "https://storage.googleapis.com/")
    elif is_s3_path(file_path):
        url = s3_uri_to_public_url(file_path)
    else:
        url = file_path

    assert is_url(url)
    response = requests.head(url, allow_redirects=True)
    last_modified = response.headers.get("Last-Modified")
    if last_modified:
        return parsedate_to_datetime(last_modified)
    else:
        return None


def fix_assets_links(stac_file: dict) -> dict:
    """
    Fix assets' links to point from gsutil URI to
    public URL

    Parameters
    ----------
    stac_file : dict
        Stac item from converting a dataset doc to stac using
        `eodatasets3.stac.to_stac_item`

    Returns
    -------
    dict
        Updated stac_item
    """
    # Fix links in assets
    assets = stac_file["assets"]
    for measurement in assets.keys():
        measurement_url = assets[measurement]["href"]
        if is_gcsfs_path(measurement_url):
            new_measurement_url = measurement_url.replace(
                "gs://", "https://storage.googleapis.com/"
            )
            stac_file["assets"][measurement]["href"] = new_measurement_url

    return stac_file
