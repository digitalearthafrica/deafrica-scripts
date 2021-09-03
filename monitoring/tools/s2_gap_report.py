import gzip
import logging
import re
import sys
import traceback
from datetime import datetime

import click
import pandas as pd
from odc.aws import s3_client, s3_dump
from odc.aws.inventory import list_inventory, find_latest_manifest
from urlpath import URL

log = logging.getLogger()
console = logging.StreamHandler()
log.addHandler(console)


AFRICA_TILES = "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/deafrica-mgrs-tiles.csv.gz"
SENTINEL_2_STATUS_REPORT_PATH = URL("s3://deafrica-sentinel-2/status-report/")
SENTINEL_2_INVENTORY_PATH = URL("s3://deafrica-sentinel-2-inventory/deafrica-sentinel-2/deafrica-sentinel-2-inventory/")
SENTINEL_2_REGION = "af-south-1"
SENTINEL_COGS_INVENTORY_PATH = URL("s3://sentinel-cogs-inventory/sentinel-cogs/sentinel-cogs/")
COGS_REGION = "us-west-2"
COGS_FOLDER_NAME = "sentinel-s2-l2a-cogs"


def get_and_filter_cogs_keys():
    """
    Retrieve key list from a inventory bucket and filter
    :return:
    """
    s3 = s3_client(region_name=COGS_REGION, aws_unsigned=True)
    latest_manifest = find_latest_manifest(prefix=SENTINEL_COGS_INVENTORY_PATH, s3=s3)
    source_keys = list_inventory(
        manifest=latest_manifest,
        s3=s3,
        prefix=COGS_FOLDER_NAME,
        contains=".json",
        n_threads=200
    )

    africa_tile_ids = set(
        pd.read_csv(
            AFRICA_TILES,
            header=None,
        ).values.ravel()
    )

    return set(
        key
        for key in source_keys
        if (
                key.split("/")[-2].split("_")[1] in africa_tile_ids
                # We need to ensure we're ignoring the old format data
                and re.match(r"sentinel-s2-l2a-cogs/\d{4}/", key) is None
        )
    )


def get_and_filter_deafrica_keys():
    s3 = s3_client(region_name="af-south-1", aws_unsigned=True)

    latest_manifest = find_latest_manifest(prefix=SENTINEL_2_INVENTORY_PATH, s3=s3)

    return set(
        list_inventory(
            manifest=latest_manifest,
            s3=s3,
            prefix=COGS_FOLDER_NAME,
            contains=".json",
            n_threads=200
        )
    )


def generate_buckets_diff(update_stac: bool = False) -> None:
    """
    Compare Sentinel-2 buckets in US and Africa and detect differences
    A report containing missing keys will be written to s3://deafrica-sentinel-2/status-report
    """
    log.info("Process started")

    date_string = datetime.now().strftime("%Y-%m-%d")

    # Retrieve keys from inventory bucket
    source_keys = get_and_filter_cogs_keys()

    if update_stac:
        log.info('FORCED UPDATE ACTIVE!')
        missing_scenes = set(
            f"s3://sentinel-cogs/{key}"
            for key in source_keys
        )

        orphaned_keys = set()

        output_filename = f"{date_string}_update.txt.gz"

    else:

        destination_keys = get_and_filter_deafrica_keys()

        # Keys that are missing, they are in the source but not in the bucket
        missing_scenes = set(
            f"s3://sentinel-cogs/{key}"
            for key in source_keys
            if key not in destination_keys
        )

        # Keys that are lost, they are in the bucket but not found in the files
        orphaned_keys = destination_keys.difference(source_keys)

        output_filename = f"{date_string}.txt.gz"

    log.info(f"File will be saved in {SENTINEL_2_STATUS_REPORT_PATH}{output_filename}")

    s3_dump(
        data=gzip.compress(str.encode("\n".join(missing_scenes))),
        url=SENTINEL_2_STATUS_REPORT_PATH / output_filename,
        s3=None,
        ContentType="application/gzip"
    )

    log.info(f"10 first missing_scenes {list(missing_scenes)[0:10]}")
    log.info(f"Wrote inventory to: {SENTINEL_2_STATUS_REPORT_PATH}/{output_filename}")

    if len(orphaned_keys) > 0:
        output_filename = f"{date_string}_orphaned.txt"
        s3_dump(
            data=gzip.compress(str.encode("\n".join(orphaned_keys))),
            url=SENTINEL_2_STATUS_REPORT_PATH / output_filename,
            s3=None,
            ContentType="application/gzip"
        )

        log.info(f"10 first orphaned_keys {orphaned_keys[0:10]}")

        log.info(f"Wrote orphaned scenes to: {SENTINEL_2_INVENTORY_PATH}/{output_filename}")

    message = f"{len(missing_scenes)} scenes are missing from and {len(orphaned_keys)} scenes no longer exist in source"
    log.info(message)

    if not update_stac and (len(missing_scenes) > 200 or len(orphaned_keys) > 200):
        sys.exit(1)


@click.option("--update_stac", type=bool, default=False, help="Defines if all stacs need to be updated")
@click.command("s2-gap-report")
def cli(update_stac: bool = False):
    """
    Publish missing scenes
    """
    try:
        generate_buckets_diff(update_stac=update_stac)
    except Exception as error:
        log.exception(error)
        traceback.print_exc()
        raise error
