"""
# Generate a gap report between deafrica-landsat-dev and usgs-landsat bulk file

This DAG runs weekly and creates a gap report in the folowing location:
s3://deafrica-landsat-dev/<date>/status-report

"""

import csv
import gzip
import logging
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from textwrap import dedent

import click
import pandas as pd
from odc.aws import s3_dump, s3_client
from odc.aws.inventory import list_inventory
from urlpath import URL

from tools.utils.utils import (
    slack_url,
    update_stac,
    send_slack_notification,
    setup_logging,
    download_file_to_tmp,
    convert_str_to_date,
    time_process, )

FILES = {
    "landsat_8": "fake_landsat_8_bulk_file.csv.gz",
    "landsat_7": "LANDSAT_ETM_C2_L2.csv.gz",
    "landsat_5": "LANDSAT_TM_C2_L2.csv.gz",
}


BASE_BULK_CSV_URL = URL(
    "https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/"
)
AFRICA_GZ_PATHROWS_URL = URL(
    "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/deafrica-usgs-pathrows.csv.gz"
)
LANDSAT_INVENTORY_PATH = URL("s3://deafrica-landsat/deafrica-landsat-inventory/")
USGS_S3_BUCKET_PATH = URL(f"s3://usgs-landsat")
USGS_BASE_URL = "https://landsatlook.usgs.gov/"
USGS_INDEX_URL = f"{USGS_BASE_URL}stac-browser/"
USGS_API_MAIN_URL = f"{USGS_BASE_URL}sat-api/"
USGS_API_INDIVIDUAL_ITEM_URL = f"{USGS_API_MAIN_URL}collections/landsat-c2l2-sr/items"


def get_and_filter_keys_from_files(file_path: Path):
    """
    Read scenes from the bulk GZ file and filter
    :param file_path:
    :return:
    """

    def build_path(file_row):
        # USGS changes - for _ when generates the CSV bulk file
        identifier = file_row["Sensor Identifier"].lower().replace("_", "-")
        year_acquired = convert_str_to_date(file_row["Date Acquired"]).year

        return (
            "collection02/level-2/standard/{identifier}/{year_acquired}/"
            "{target_path}/{target_row}/{display_id}/".format(
                identifier=identifier,
                year_acquired=year_acquired,
                target_path=file_row["WRS Path"].zfill(3),
                target_row=file_row["WRS Row"].zfill(3),
                display_id=file_row["Display ID"],
            )
        )

    # Download updated Pathrows
    logging.info(f"Retrieving allowed Africa Pathrows from {AFRICA_GZ_PATHROWS_URL}")
    africa_pathrows = set(
        pd.read_csv(
            AFRICA_GZ_PATHROWS_URL,
            header=None,
        ).values.ravel()
    )

    logging.info("Reading and filtering Bulk file")
    with gzip.open(file_path, "rt") as csv_file:
        return set(
            build_path(row)
            for row in csv.DictReader(csv_file)
            if (
                # Filter to skip all LANDSAT_4
                row.get("Satellite") is not None
                and row["Satellite"] != "LANDSAT_4"
                and row["Satellite"] != "4"
                # Filter to get just day
                and (
                    row.get("Day/Night Indicator") is not None
                    and row["Day/Night Indicator"].upper() == "DAY"
                )
                # Filter to get just from Africa
                and (
                    row.get("WRS Path") is not None
                    and row.get("WRS Row") is not None
                    and int(f"{row['WRS Path'].zfill(3)}{row['WRS Row'].zfill(3)}")
                    in africa_pathrows
                )
            )
        )


def get_and_filter_keys(landsat: str) -> set:
    """
    Retrieve key list from a inventory bucket and filter

    :param landsat:(str)
    :return:(set)
    """

    sat_prefix = None
    if landsat == "landsat_8":
        sat_prefix = "LC08"
    elif landsat == "landsat_7":
        sat_prefix = "LE07"
    elif landsat == "landsat_5":
        sat_prefix = "LT05"

    if not sat_prefix:
        raise Exception("Prefix not defined")

    list_json_keys = list_inventory(
        manifest=str(LANDSAT_INVENTORY_PATH),
        prefix="collection02",
        suffix="_stac.json",
        contains=sat_prefix,
        n_threads=200,
    )
    logging.info(f"Filtering by sat prefix {sat_prefix}")

    return set(f"{key.Key.rsplit('/', 1)[0]}/" for key in list_json_keys)


def generate_buckets_diff(
    bucket_name: str,
    landsat: str,
    file_name: str,
    update_stac: bool = False,
    notification_url: str = None,
):
    """
    Compare USGS bulk files and Africa inventory bucket detecting differences
    A report containing missing keys will be written to AFRICA_S3_BUCKET_PATH
    """
    try:
        start_timer = time.time()

        log = setup_logging()

        log.info("Task started")

        landsat_status_report_path = URL(f"s3://{bucket_name}/status-report/")
        environment = "DEV" if "dev" in bucket_name else "PDS"
        logging.info(f"Environment {environment}")

        # Create connection to the inventory S3 bucket
        logging.info(f"Retrieving keys from inventory bucket {LANDSAT_INVENTORY_PATH}")
        dest_paths = get_and_filter_keys(landsat=landsat)

        logging.info(f"INVENTORY bucket number of objects {len(dest_paths)}")
        logging.info(f"INVENTORY 10 first {list(dest_paths)[0:10]}")
        date_string = datetime.now().strftime("%Y-%m-%d")

        # Download bulk file
        logging.info("Download Bulk file")
        file_path = download_file_to_tmp(
            url=str(BASE_BULK_CSV_URL), file_name=file_name
        )

        # Retrieve keys from the bulk file
        logging.info("Filtering keys from bulk file")
        source_paths = get_and_filter_keys_from_files(file_path)

        logging.info(f"BULK FILE number of objects {len(source_paths)}")
        logging.info(f"BULK 10 First {list(source_paths)[0:10]}")

        orphan_output_filename = "No orphan scenes were found"
        output_filename = "No missing scenes were found"

        if update_stac:
            logging.info("FORCED UPDATE ACTIVE!")
            missing_scenes = source_paths
            orphaned_scenes = []

        else:
            # Keys that are missing, they are in the source but not in the bucket
            logging.info("Filtering missing scenes")
            missing_scenes = [
                str(USGS_S3_BUCKET_PATH / path)
                for path in source_paths.difference(dest_paths)
            ]

            # Keys that are orphan, they are in the bucket but not found in the files
            logging.info("Filtering orphan scenes")
            orphaned_scenes = [
                str(URL(f"s3://{bucket_name}") / path)
                for path in dest_paths.difference(source_paths)
            ]

            logging.info(f"missing_scenes 10 first keys {list(missing_scenes)[0:10]}")
            logging.info(f"orphaned_scenes 10 first keys {list(orphaned_scenes)[0:10]}")

        landsat_s3 = s3_client(region_name="af-south-1")

        if len(missing_scenes) > 0:
            output_filename = (
                f"{landsat}_{date_string}.txt.gz"
                if not update_stac
                else f"{landsat}_{date_string}_update.txt.gz"
            )

            logging.info(
                f"File will be saved in {URL(landsat_status_report_path) / output_filename}"
            )
            s3_dump(
                data=gzip.compress(str.encode("\n".join(missing_scenes))),
                url=str(URL(landsat_status_report_path) / output_filename),
                s3=landsat_s3,
                ContentType="application/gzip",
            )

            logging.info(f"Number of missing scenes: {len(missing_scenes)}")

        if len(orphaned_scenes) > 0:
            orphan_output_filename = f"{landsat}_{date_string}_orphaned.txt.gz"
            s3_dump(
                data=gzip.compress(str.encode("\n".join(orphaned_scenes))),
                url=str(URL(landsat_status_report_path) / orphan_output_filename),
                s3=landsat_s3,
                ContentType="application/gzip",
            )

            logging.info(f"Number of orphaned scenes: {len(orphaned_scenes)}")

        message = dedent(
            f"*{landsat.upper()} GAP REPORT*\n "
            f"Environment: {environment}\n "
            f"Missing Scenes: {len(missing_scenes)}\n"
            f"Orphan Scenes: {len(orphaned_scenes)}\n"
            f"Missing Scenes report Saved: {str(URL(landsat_status_report_path) / output_filename)}\n"
            f"Orphan Scenes report Saved: {str(URL(landsat_status_report_path) / orphan_output_filename)}\n"
        )

        if notification_url is not None and (
            len(missing_scenes) > 0 or len(orphaned_scenes) > 0
        ):
            send_slack_notification(notification_url, f"{landsat} Gap Report", message)

        log.info(message)
        log.info(
            f"File {file_name} processed and sent in {time_process(start=start_timer)}"
        )

        if (
            len(missing_scenes) > 200 or len(orphaned_scenes) > 200
        ) and not update_stac:
            sys.exit(1)

    except Exception as error:
        logging.error(error)
        # print traceback but does not stop execution
        traceback.print_exc()
        raise error


@click.argument(
    "bucket_name",
    type=str,
    nargs=1,
    required=True,
    default="Bucket where the gap report is",
)
@click.argument(
    "satellite",
    type=str,
    nargs=1,
    required=True,
    default="satellite to be compared, supported ones (landsat_8, landsat_7, landsat_5)",
)
@update_stac
@slack_url
@click.command("landsat-gap-report")
def cli(
    bucket_name: str, satellite: str, update_stac: bool = False, slack_url: str = None
):
    """
    Publish missing scenes
    """
    try:
        generate_buckets_diff(
            bucket_name=bucket_name,
            landsat=satellite,
            file_name=FILES.get(satellite, None),
            update_stac=update_stac,
            notification_url=slack_url,
        )
    except Exception as error:
        traceback.print_exc()
        raise error
