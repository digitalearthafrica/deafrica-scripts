import json
import re
from datetime import date, datetime, timedelta
from textwrap import dedent

import click
import datacube
import pandas as pd
from odc.aws import s3_client, s3_dump
from urlpath import URL

from deafrica import __version__
from deafrica.click_options import slack_url, update_stac
from deafrica.inventory import list_inventory
from deafrica.logs import setup_logging
from deafrica.utils import (
    send_slack_notification,
)

SENTINEL_2_C1_INVENTORY_PATH = URL(
    "s3://deafrica-sentinel-2-l2a-c1-inventory/deafrica-sentinel-2-l2a-c1/deafrica-sentinel-2-l2a-c1-inventory/"
)
SOURCE_INVENTORY_PATH = URL(
    "s3://e84-earth-search-sentinel-data-inventory/e84-earth-search-sentinel-data/primary/"
)
SENTINEL_2_C1_REGION = "af-south-1"
SOURCE_REGION = "us-west-2"
BASE_FOLDER_NAME = "sentinel-2-c1-l2a"


def get_and_filter_cogs_keys():
    """
    Retrieve key list from a inventory bucket and filter
    :return:
    """

    s3 = s3_client(region_name=SOURCE_REGION)
    source_keys = list_inventory(
        manifest=f"{SOURCE_INVENTORY_PATH}",
        s3=s3,
        prefix=BASE_FOLDER_NAME,
        contains=".json",
        n_threads=200,
    )

    africa_tile_ids = set(
        pd.read_csv(
            "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/deafrica-mgrs-tiles.csv.gz",
            header=None,
        ).values.ravel()
    )

    return set(
        key.key
        for key in source_keys
        if (
            key.key.split("/")[-2].split("_")[1].lstrip("T") in africa_tile_ids
            and "tileinfo_metadata.json" not in key.key
            and "tileInfo.json" not in key.key
        )
    )


def get_odc_keys(log) -> set:
    try:
        dc = datacube.Datacube()
        all_odc_vals = {}

        for val in dc.index.datasets.search_returning(
            ["uri", "indexed_time"], product="s2_l2a_c1"
        ):
            all_odc_vals[val.uri.replace("s3://deafrica-sentinel-2-l2a-c1/", "")] = (
                val.indexed_time
            )
        return all_odc_vals
    except:
        log.info("Error while searching for datasets in odc")
        return {}


def generate_buckets_diff(
    bucket_name: str,
    update_stac: bool = False,
    notification_url: str = None,
) -> None:
    """
    Compare Sentinel-2-c1 buckets in US and Africa and detect differences
    A report containing missing keys will be written to s3://deafrica-sentinel-2-l2a-c1/status-report
    :param bucket_name: (str) Bucket where the gap report is
    :param update_stac: (bool) Define if the report will contain all scenes from the source for an update
    :param notification_url: (str) Optional slack URL in case of you want to send a slack notification
    """

    log = setup_logging()

    log.info("Task started")

    # defines where the report will be saved
    s2_c1_status_report_path = URL(f"s3://{bucket_name}/status-report/")

    environment = "DEV" if "dev" in bucket_name else "PDS"
    log.info(f"Environment {environment}")

    date_string = datetime.now().strftime("%Y-%m-%d")

    # Retrieve keys from inventory bucket
    source_keys = get_and_filter_cogs_keys()

    output_filename = "No missing scenes were found"

    if update_stac:
        log.info("FORCED UPDATE ACTIVE!")
        missing_scenes = set(
            f"s3://e84-earth-search-sentinel-data/{key}" for key in source_keys
        )
        orphaned_keys = set()

    else:
        destination_keys = set(
            ns.Key
            for ns in list_inventory(
                manifest=f"{SENTINEL_2_C1_INVENTORY_PATH}",
                prefix=BASE_FOLDER_NAME,
                contains=".json",
                n_threads=200,
            )
        )

        log.info(f"Retrieving keys from odc")
        all_odc_values = get_odc_keys(log)
        indexed_keys = all_odc_values.keys()

        # Keys that are missing, they are in the source but not in the bucket
        missing_scenes = set(
            f"s3://e84-earth-search-sentinel-data/{key}"
            for key in source_keys
            if key not in destination_keys
        )

        # Keys that are lost, they are in the bucket but not found in the source
        orphaned_keys = destination_keys.difference(source_keys)

        missing_odc_scenes = set(
            key for key in destination_keys if key not in indexed_keys
        )

        yesterday = date.today() - timedelta(days=1)

        orphaned_odc_scenes = set(
            key
            for key in indexed_keys
            if (key not in destination_keys and yesterday > all_odc_values[key].date())
        )
    s2_s3 = s3_client(region_name=SENTINEL_2_C1_REGION)

    if (
        len(missing_scenes) > 0
        or len(orphaned_keys) > 0
        or (len(missing_odc_scenes) > 0 and len(indexed_keys) > 0)
        or len(orphaned_odc_scenes) > 0
    ):
        output_filename = (
            f"{date_string}_gap_report.json"
            if not update_stac
            else URL(f"{date_string}_gap_report_update.json")
        )

        log.info(f"File will be saved in {s2_c1_status_report_path}/{output_filename}")

        missing_orphan_scenes_json = json.dumps(
            {
                "orphan": list(orphaned_keys),
                "missing": list(missing_scenes),
                "orphan_odc": list(orphaned_odc_scenes),
                "missing_odc": list(missing_odc_scenes),
            }
        )

        s3_dump(
            data=missing_orphan_scenes_json,
            url=str(URL(s2_c1_status_report_path) / output_filename),
            s3=s2_s3,
            ContentType="application/json",
        )
    report_http_link = (
        f"https://{bucket_name}.s3.{SENTINEL_2_C1_REGION}.amazonaws.com/status-report/{output_filename}"
        if len(missing_scenes) > 0
        or len(orphaned_keys) > 0
        or (len(missing_odc_scenes) > 0 and len(indexed_keys) > 0)
        or len(orphaned_odc_scenes) > 0
        else output_filename
    )

    message = dedent(
        f"*SENTINEL 2 C1 GAP REPORT - {environment}*\n"
        f"Missing Scenes: {len(missing_scenes)}\n"
        f"Orphan Scenes: {len(orphaned_keys)}\n"
        f"Missing ODC Scenes: {len(missing_odc_scenes)}\n"
        f"Orphan ODC Scenes: {len(orphaned_odc_scenes)}\n"
        f"Report: {report_http_link}\n"
    )

    log.info(message)

    if not update_stac and (
        len(missing_scenes) > 200
        or len(orphaned_keys) > 200
        or (len(missing_odc_scenes) > 200 and len(indexed_keys) > 0)
        or len(orphaned_odc_scenes) > 200
    ):
        if notification_url is not None:
            send_slack_notification(
                notification_url, "S2 C1 Gap Report - Exception", message
            )
        raise Exception(f"More than 200 scenes were found \n {message}")
    else:
        if notification_url is not None:
            send_slack_notification(
                notification_url,
                "S2 C1 Gap Report - Success",
                message + "\n Missing scenes below threshold",
            )


@click.argument(
    "bucket_name",
    type=str,
    nargs=1,
    required=True,
    default="Bucket where the gap report is",
)
@update_stac
@slack_url
@click.option("--version", is_flag=True, default=False)
@click.command("s2-c1-gap-report")
def cli(
    bucket_name: str,
    update_stac: bool = False,
    slack_url: str = None,
    version: bool = False,
):
    """
    Publish missing scenes
    """

    if version:
        click.echo(__version__)

    generate_buckets_diff(
        bucket_name=bucket_name, update_stac=update_stac, notification_url=slack_url
    )
