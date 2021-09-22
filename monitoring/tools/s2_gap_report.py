import gzip
import re
import sys
import traceback
from datetime import datetime
from textwrap import dedent

import click
import pandas as pd
from odc.aws import s3_dump, s3_client
from odc.aws.inventory import list_inventory
from urlpath import URL

from monitoring.tools.utils import send_slack_notification, setup_logging

AFRICA_TILES = "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/deafrica-mgrs-tiles.csv.gz"
SENTINEL_2_STATUS_REPORT_PATH = URL("s3://deafrica-sentinel-2/status-report/")
SENTINEL_2_DEV_STATUS_REPORT_PATH = URL("s3://deafrica-sentinel-2-dev/status-report/")
SENTINEL_2_INVENTORY_PATH = URL(
    "s3://deafrica-sentinel-2-inventory/deafrica-sentinel-2/deafrica-sentinel-2-inventory/"
)
SENTINEL_2_REGION = "af-south-1"
SENTINEL_COGS_INVENTORY_PATH = URL(
    "s3://sentinel-cogs-inventory/sentinel-cogs/sentinel-cogs/"
)
COGS_REGION = "us-west-2"
COGS_FOLDER_NAME = "sentinel-s2-l2a-cogs"


def get_and_filter_cogs_keys():
    """
    Retrieve key list from a inventory bucket and filter
    :return:
    """

    s3 = s3_client(region_name=COGS_REGION)
    source_keys = list_inventory(
        manifest=f"{SENTINEL_COGS_INVENTORY_PATH}",
        s3=s3,
        prefix=COGS_FOLDER_NAME,
        contains=".json",
        n_threads=200,
    )

    africa_tile_ids = set(
        pd.read_csv(
            AFRICA_TILES,
            header=None,
        ).values.ravel()
    )

    return set(
        key.Key
        for key in source_keys
        if (
            key.Key.split("/")[-2].split("_")[1] in africa_tile_ids
            # We need to ensure we're ignoring the old format data
            and re.match(r"sentinel-s2-l2a-cogs/\d{4}/", key.Key) is None
        )
    )


def generate_buckets_diff(
    update_stac: bool = False, dev: bool = False, slack_url: str = None
) -> None:
    """
    Compare Sentinel-2 buckets in US and Africa and detect differences
    A report containing missing keys will be written to s3://deafrica-sentinel-2/status-report
    :param update_stac: (bool) Define if the report will contain all scenes from the source for an update
    :param dev: (bool) Define if the task is running in dev environment
    :param slack_url: (str) Optional slack URL in case of you want to send a slack notification
    """

    log = setup_logging()

    log.info("Process started")

    # defines where the report will be saved
    s2_status_report_path = SENTINEL_2_STATUS_REPORT_PATH

    if dev:
        log.info("Environment DEV")
        s2_status_report_path = SENTINEL_2_DEV_STATUS_REPORT_PATH

    date_string = datetime.now().strftime("%Y-%m-%d")

    # Retrieve keys from inventory bucket
    source_keys = get_and_filter_cogs_keys()

    if update_stac:
        log.info("FORCED UPDATE ACTIVE!")
        missing_scenes = set(f"s3://sentinel-cogs/{key}" for key in source_keys)

        orphaned_keys = set()

        output_filename = URL(f"{date_string}_update.txt.gz")

    else:

        destination_keys = set(
            ns.Key
            for ns in list_inventory(
                manifest=f"{SENTINEL_2_INVENTORY_PATH}",
                prefix=COGS_FOLDER_NAME,
                contains=".json",
                n_threads=200,
            )
        )

        # Keys that are missing, they are in the source but not in the bucket
        missing_scenes = set(
            f"s3://sentinel-cogs/{key}"
            for key in source_keys
            if key not in destination_keys
        )

        # Keys that are lost, they are in the bucket but not found in the files
        orphaned_keys = destination_keys.difference(source_keys)

        output_filename = URL(f"{date_string}.txt.gz")

    log.info(f"File will be saved in {s2_status_report_path}{output_filename}")

    s2_s3 = s3_client(region_name=SENTINEL_2_REGION)
    s3_dump(
        data=gzip.compress(str.encode("\n".join(missing_scenes))),
        url=str(URL(s2_status_report_path) / output_filename),
        s3=s2_s3,
        ContentType="application/gzip",
    )

    log.info(f"10 first missing_scenes {list(missing_scenes)[0:10]}")
    log.info(f"Wrote inventory to: {str(URL(s2_status_report_path) / output_filename)}")

    if len(orphaned_keys) > 0:
        orphan_output_filename = URL(f"{date_string}_orphaned.txt")
        s3_dump(
            data=gzip.compress(str.encode("\n".join(orphaned_keys))),
            url=str(URL(s2_status_report_path) / orphan_output_filename),
            s3=s2_s3,
            ContentType="application/gzip",
        )

        log.info(f"10 first orphaned_keys {orphaned_keys[0:10]}")

        log.info(
            f"Wrote orphaned scenes to: {str(SENTINEL_2_INVENTORY_PATH)}/{str(output_filename)}"
        )

    message = dedent(
        f"*Environment*: {'DEV' if dev else 'PROD'}\n "
        f"Missing Scenes: {len(missing_scenes)}\n"
        f"Orphan Scenes: {len(orphaned_keys)}\n"
        f"Reports Saved: {s2_status_report_path}\n"
    )

    if slack_url is not None:
        send_slack_notification(slack_url, "S2 Gap Report", message)

    log.info(message)

    if not update_stac and (len(missing_scenes) > 200 or len(orphaned_keys) > 200):
        sys.exit(1)


@click.option(
    "--update_stac",
    is_flag=True,
    default=False,
    help="Defines if all stacs need to be updated",
)
@click.option(
    "--dev",
    is_flag=True,
    default=False,
    help="Defines if all stacs need to be updated",
)
@click.option(
    "--slack_url",
    help="Slack url to use to send a notification",
    default=None,
)
@click.command("s2-gap-report")
def cli(update_stac: bool = False, dev: bool = False, slack_url: str = None):
    """
    Publish missing scenes
    """
    try:
        generate_buckets_diff(update_stac=update_stac, dev=dev, slack_url=slack_url)
    except Exception as error:
        traceback.print_exc()
        raise error
