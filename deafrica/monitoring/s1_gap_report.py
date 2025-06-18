import datetime
import json
import logging
import os
from textwrap import dedent
from typing import Any

import click
import datacube
import geopandas as gpd
import requests
from odc.aws import s3_client, s3_dump, s3_ls_dir
from odc.aws.inventory import list_inventory
from sentinelhub import DataCollection, Geometry, SentinelHubCatalog, SHConfig
from yarl import URL

from deafrica.click_options import slack_url
from deafrica.io import find_json_files
from deafrica.logs import setup_logging
from deafrica.utils import AFRICA_EXTENT_URL, send_slack_notification

SH_CLIENT_ID = os.getenv("SH_CLIENT_ID", "")
SH_CLIENT_SECRET = os.getenv("SH_CLIENT_SECRET", "")

TILING_GRID = "https://s3.eu-central-1.amazonaws.com/sh-batch-grids/tiling-grid-3.zip"

S1_BUCKET = "s3://deafrica-sentinel-1/"
S1_STAGING_BUCKET = "s3://deafrica-sentinel-1-staging-frankfurt/"
S1_INVENTORY_PATH = "s3://deafrica-sentinel-1-inventory/deafrica-sentinel-1/deafrica-sentinel-1-inventory/"
BASE_FOLDER_NAME = "s1_rtc"
REGION_NAME = "af-south-1"

log = logging.getLogger(__name__)


def get_odc_keys() -> dict[str, str]:
    try:
        dc = datacube.Datacube()
        all_odc_vals = {}
        for val in dc.index.datasets.search_returning(
            ["uri", "indexed_time"], product=BASE_FOLDER_NAME
        ):
            all_odc_vals[val.uri.replace(S1_BUCKET, "")] = val.indexed_time
        return all_odc_vals
    except Exception:
        log.info("Error while searching for datasets in odc")
        return {}


def get_missing_and_orphan_odc_scenes() -> tuple[set[str], set[str]]:
    today = datetime.datetime.today()
    # Keys that in the destination bucket but are not indexed
    # on ODC.
    destination_keys = set(
        ns.Key
        for ns in list_inventory(
            manifest=S1_INVENTORY_PATH,
            prefix=BASE_FOLDER_NAME,
            contains="metadata.json",
            n_threads=200,
        )
    )
    all_odc_values = get_odc_keys(log)
    indexed_keys = all_odc_values.keys()
    missing_odc_scenes = set(key for key in destination_keys if key not in indexed_keys)

    # Keys that are indexed on ODC but do not exist in the
    # destination bucket
    yesterday = (today - datetime.timedelta(days=1)).date()
    orphaned_odc_scenes = set(
        key
        for key in indexed_keys
        if (key not in destination_keys and yesterday > all_odc_values[key].date())
    )

    return missing_odc_scenes, orphaned_odc_scenes


def get_staging_bucket_diff():
    source_keys = find_json_files(S1_STAGING_BUCKET, anon=False)
    source_keys = [i.replace(S1_STAGING_BUCKET, "") for i in source_keys]

    destination_keys = set(
        ns.Key
        for ns in list_inventory(
            manifest=S1_INVENTORY_PATH,
            prefix=BASE_FOLDER_NAME,
            contains="metadata.json",
            n_threads=200,
        )
    )
    missing_staged_scenes = set(
        key for key in source_keys if key not in destination_keys
    )
    return missing_staged_scenes


def write_gap_report(
    bucket_name: str,
    slack_url: str,
    missing_odc_scenes: set[str],
    orphaned_odc_scenes: set[str],
    missing_staged_scenes: set[str],
):

    today = datetime.datetime.today()
    s1_status_report_path = URL(f"s3://{bucket_name}/status-report/")
    output_filename = f"{today.strftime('%Y-%m-%d')}_gap_report.json"
    log.info(f"File will be saved in {s1_status_report_path}{output_filename}")

    missing_json = json.dumps(
        {
            "missing_odc": list(missing_odc_scenes),
            "orphan_odc": list(orphaned_odc_scenes),
            "missing_staged_scenes": list(missing_staged_scenes),
        }
    )

    client = s3_client(region_name=REGION_NAME)
    s3_dump(
        data=missing_json,
        url=str(s1_status_report_path / output_filename),
        s3=client,
        ContentType="application/json",
    )

    report_http_link = f"https://{bucket_name}.s3.af-south-1.amazonaws.com/status-report/{output_filename}"
    message = dedent(
        f"*SENTINEL 1 GAP REPORT - PDS*\n"
        f"Missing ODC Scenes: {len(missing_odc_scenes)}\n"
        f"Orphan ODC Scenes: {len(orphaned_odc_scenes)}\n"
        f"Missing Staged Scenes: {len(missing_staged_scenes)}\n"
        f"Report: {report_http_link}\n"
    )
    if slack_url:
        send_slack_notification(slack_url, "S1 Gap Report", message)
    else:
        log.info(message)


def find_missing_s1_data(bucket_name: str, slack_url: str):
    log = setup_logging()
    log.info("Task started ")
    try:
        missing_odc_scenes, orphaned_odc_scenes = get_missing_and_orphan_odc_scenes()
        missing_staged_scenes = get_staging_bucket_diff()
        write_gap_report(
            bucket_name,
            slack_url,
            missing_odc_scenes,
            orphaned_odc_scenes,
            missing_staged_scenes,
        )
    except Exception as exc:
        log.exception(exc)


@click.argument(
    "bucket_name",
    type=str,
    nargs=1,
    required=True,
    default="Bucket where the gap report will be stored",
)
@slack_url
@click.command("s1-gap-report")
def cli(
    bucket_name: str,
    slack_url: str = None,
):
    """
    Publish missing datasets
    """

    find_missing_s1_data(
        bucket_name=bucket_name,
        slack_url=slack_url,
    )
