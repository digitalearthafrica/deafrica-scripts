from __future__ import annotations

import logging
import math
import time
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlparse

import numpy as np
import requests

# GDAL format: [ulx, uly, lrx, lry]
AFRICA_BBOX = [-26.36, 38.35, 64.50, -47.97]
AFRICA_EXTENT_BBOX_URL = "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/africa-extent-bbox.json"
AFRICA_EXTENT_URL = "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/refs/heads/master/africa-extent.json"

GOOGLE_DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.file"
FOLDER_MIME_TYPE = "application/vnd.google-apps.folder"


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


def split_tasks(
    all_tasks: list[str], max_parallel_steps: int, worker_idx: int
) -> list[str]:
    """Divide tasks across workers."""
    task_chunks = np.array_split(np.array(all_tasks), max_parallel_steps)
    task_chunks = [chunk.tolist() for chunk in task_chunks if len(chunk) > 0]
    if len(task_chunks) <= worker_idx:
        return []
    return task_chunks[worker_idx]


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


def drive_query_value(value):
    return str(value).replace("\\", "\\\\").replace("'", "\\'")


def find_drive_item(service, parent_id, name, mime_type):
    """Find an existing item in its destination, including shared drives."""
    query = (
        f"'{drive_query_value(parent_id)}' in parents and trashed = false "
        f"and name = '{drive_query_value(name)}' "
        f"and mimeType = '{drive_query_value(mime_type)}'"
    )
    page_token = None
    matches = []
    while True:
        result = (
            service.files()
            .list(
                q=query,
                fields="nextPageToken, files(id, name, webViewLink)",
                pageToken=page_token,
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
        )
        matches.extend(result.get("files", []))
        page_token = result.get("nextPageToken")
        if not page_token:
            break

    # Reuse the same item even if duplicates already exist. Do not delete history.
    return min(matches, key=lambda item: item["id"]) if matches else None


def ensure_year_folder(service, parent_id, year):
    name = str(year)
    existing = find_drive_item(service, parent_id, name, FOLDER_MIME_TYPE)
    if existing:
        return existing["id"]

    folder = (
        service.files()
        .create(
            body={"name": name, "mimeType": FOLDER_MIME_TYPE, "parents": [parent_id]},
            fields="id",
            supportsAllDrives=True,
        )
        .execute()
    )
    return folder["id"]


def upsert_drive_report(service, report_path, report_name, parent_id, mime_type):
    """Replace an existing report in place so its shared link remains valid."""
    from googleapiclient.http import MediaFileUpload

    existing = find_drive_item(service, parent_id, report_name, mime_type)
    media = MediaFileUpload(str(report_path), mimetype=mime_type, resumable=False)
    if existing:
        request = service.files().update(
            fileId=existing["id"],
            media_body=media,
            fields="id, webViewLink",
            supportsAllDrives=True,
        )
    else:
        request = service.files().create(
            body={"name": report_name, "parents": [parent_id]},
            media_body=media,
            fields="id, webViewLink",
            supportsAllDrives=True,
        )
    uploaded_file = request.execute()
    print(f"Uploaded {report_name} to Google Drive file id {uploaded_file.get('id')}")
    print(f"Google Drive report link: {uploaded_file.get('webViewLink')}")
    return uploaded_file


def upload_dated_report_to_google_drive(
    report_path,
    report_name,
    google_drive_folder_id,
    google_credentials_file,
    report_date,
    mime_type,
    latest_name=None,
):
    """Save a year archive, then optionally publish its latest copy."""
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    if not google_drive_folder_id:
        raise ValueError("GOOGLE_DRIVE_FOLDER_ID is required for Google Drive upload")
    if not google_credentials_file:
        raise ValueError(
            "GOOGLE_APPLICATION_CREDENTIALS is required for Google Drive upload"
        )
    credentials_path = Path(google_credentials_file)
    if not credentials_path.exists():
        raise FileNotFoundError(
            f"Google credentials file does not exist: {credentials_path}"
        )

    year = date.fromisoformat(report_date).year
    credentials = service_account.Credentials.from_service_account_file(
        credentials_path, scopes=[GOOGLE_DRIVE_SCOPE]
    )
    service = build("drive", "v3", credentials=credentials, cache_discovery=False)
    year_folder_id = ensure_year_folder(service, google_drive_folder_id, year)
    archived_file = upsert_drive_report(
        service, report_path, report_name, year_folder_id, mime_type
    )
    # Publish latest only after the dated archive has been saved successfully.
    if latest_name:
        upsert_drive_report(
            service, report_path, latest_name, google_drive_folder_id, mime_type
        )
    return archived_file
