"""
# Generate a gap report between deafrica-landsat-dev and usgs-landsat bulk file

This DAG runs weekly and creates a gap report in the following location:
s3://deafrica-landsat-dev/status-report/<satellite_date.csv.gz>

"""

from __future__ import annotations

import gzip
import json
import os
import shutil
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from textwrap import dedent

import click
import dask.dataframe as dd
import datacube
import pandas as pd
from odc.aws import s3_client, s3_dump

from deafrica import __version__
from deafrica.utils import (
    check_file_exists,
    download_file_to_tmp,
    list_inventory,
    send_slack_notification,
    setup_logging,
    slack_url,
    time_process,
    update_stac,
)

SUPPORTED_SATELLITES = ("ls8_ls9", "ls7", "ls5")
FILES = {
    "ls8_ls9": "LANDSAT_OT_C2_L2.csv.gz",
    "ls7": "LANDSAT_ETM_C2_L2.csv.gz",
    "ls5": "LANDSAT_TM_C2_L2.csv.gz",
}

BASE_BULK_CSV_URL = (
    "https://landsat.usgs.gov/landsat/metadata_service/bulk_metadata_files/"
)


AFRICA_GZ_PATHROWS_URL = "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/deafrica-usgs-pathrows.csv.gz"


LANDSAT_INVENTORY_PATH = (
    "s3://deafrica-landsat-inventory/deafrica-landsat/deafrica-landsat-inventory/"
)

USGS_S3_BUCKET_PATH = "s3://usgs-landsat"


def get_and_filter_keys_from_files(file_path: Path):
    """
    Read scenes from the bulk GZ file and filter
    :param file_path:
    :return:
    """

    africa_pathrows = set(
        pd.read_csv(
            AFRICA_GZ_PATHROWS_URL,
            header=None,
        ).values.ravel()
    )

    # Decompress the file
    file_path = str(file_path)
    unzipped_file = file_path.rstrip(".gz")
    if not check_file_exists(unzipped_file):
        with gzip.open(file_path, "rb") as f_in:
            with open(unzipped_file, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

    # Read the csv file with dask
    csv_file = dd.read_csv(unzipped_file)

    # Apply filtering

    # Filter to skip all LANDSAT_4
    csv_file = csv_file[~csv_file["Satellite"].isin(["LANDSAT_4", "4"])]

    # Filter to get just day
    csv_file = csv_file[
        csv_file["Day/Night Indicator"].map_partitions(
            lambda s: s.str.upper().isin(["DAY"])
        )
    ]

    # Filter to get rows in Africa
    csv_file = csv_file.assign(
        str_wrs_path=csv_file["WRS Path"].map_partitions(
            lambda s: s.astype(str).str.zfill(3), meta=("str_wrs_path", "str")
        ),
        str_wrs_row=csv_file["WRS Row"].map_partitions(
            lambda s: s.astype(str).str.zfill(3), meta=("str_wrs_row", "str")
        ),
    )
    csv_file["pathrow"] = csv_file.map_partitions(
        lambda df: (df["str_wrs_path"] + df["str_wrs_row"]).astype(int),
        meta=("pathrow", "int64"),
    )
    csv_file = csv_file[csv_file["pathrow"].isin(africa_pathrows)]

    # Build path
    csv_file["identifier"] = csv_file["Sensor Identifier"].map_partitions(
        lambda s: s.str.lower().str.replace("_", "-", regex=False),
        meta=("identifier", "str"),
    )
    csv_file["year_acquired"] = dd.to_datetime(
        csv_file["Date Acquired"], errors="coerce"
    ).dt.year

    def build_path(df):
        return "collection02/level-2/standard/{}/{}/{}/{}/{}/".format(
            df["identifier"],
            df["year_acquired"],
            df["str_wrs_path"],
            df["str_wrs_row"],
            df["Display ID"],
        )

    csv_file["built_path"] = csv_file.map_partitions(
        lambda df: df.apply(build_path, axis=1), meta=("path", "str")
    )

    return set(csv_file.compute()["built_path"])


def get_and_filter_keys(satellites: tuple[str, str]) -> set:
    """
    Retrieve key list from a inventory bucket and filter

    :param satellites:tuple[str] a list of satellite names
    :return:(set)
    """

    sat_prefixes = []

    if "ls9" in satellites:
        sat_prefixes.append("LC09")
    if "ls8" in satellites:
        sat_prefixes.append("LC08")
    if "ls7" in satellites:
        sat_prefixes.append("LE07")
    if "ls5" in satellites:
        sat_prefixes.append("LT05")

    if len(sat_prefixes) == 0:
        raise ValueError(f"Invalid satellites: {satellites}")

    list_json_keys = list_inventory(
        manifest=LANDSAT_INVENTORY_PATH,
        prefix="collection02",
        suffix="_stac.json",
        multiple_contains=sat_prefixes,
        n_threads=200,
    )
    return set(f"{key.Key.rsplit('/', 1)[0]}/" for key in list_json_keys)


def get_odc_keys(satellites: tuple[str, str], log) -> set:
    try:
        dc = datacube.Datacube()
        all_odc_vals = {}
        for sat in satellites:
            for uri in dc.index.datasets.search_returning(
                ["uri", "indexed_time"], product=sat + "_sr"
            ):
                all_odc_vals[
                    uri.uri.replace("s3://deafrica-landsat/", "").rsplit("/", 1)[0]
                    + "/"
                ] = uri.indexed_time
        return all_odc_vals
    except Exception:
        log.info("Error while searching for datasets in odc")
        return {}


def generate_buckets_diff(
    bucket_name: str,
    satellites: str,
    file_name: str,
    update_stac: bool = False,
    notification_url: str = None,
):
    """
    Compare USGS bulk files and Africa inventory bucket detecting differences
    A report containing missing keys will be written to AFRICA_S3_BUCKET_PATH
    """

    log = setup_logging()

    start_timer = time.time()

    log.info("Task started")

    landsat_status_report_path = f"s3://{bucket_name}/status-report/"
    landsat_status_report_url = (
        f"https://{bucket_name}.s3.af-south-1.amazonaws.com/status-report/"
    )

    environment = "DEV" if "dev" in bucket_name else "PDS"

    title = " & ".join(satellites).replace("ls", "Landsat ")

    log.info(f"Environment {environment}")
    log.info(f"Bucket Name {bucket_name}")
    log.info(f"Satellites {satellites}")
    log.info(f"File Name {file_name}")
    log.info(f"Update all ({update_stac})")
    log.info(f"Notification URL ({notification_url})")

    # Create connection to the inventory S3 bucket
    log.info(f"Retrieving keys from inventory bucket {LANDSAT_INVENTORY_PATH}")
    dest_paths = get_and_filter_keys(satellites=satellites)
    log.info(f"INVENTORY bucket number of objects {len(dest_paths)}")
    log.info(f"INVENTORY 10 first {list(dest_paths)[0:10]}")

    date_string = datetime.now().strftime("%Y-%m-%d")

    # Download bulk file
    log.info("Download Bulk file")
    file_path = download_file_to_tmp(url=BASE_BULK_CSV_URL, file_name=file_name)

    # Retrieve keys from the bulk file
    log.info("Filtering keys from bulk file")
    source_paths = get_and_filter_keys_from_files(file_path)
    log.info(f"BULK FILE number of objects {len(source_paths)}")
    log.info(f"BULK 10 First {list(source_paths)[0:10]}")

    output_filename = "No missing scenes were found"

    if update_stac:
        log.info("FORCED UPDATE ACTIVE!")
        missing_scenes = source_paths
        orphaned_scenes = []

    else:
        # collect missing scenes
        # missing scenes = keys that are in the bulk file but missing in PDS sync bucket and/or in source bucket
        log.info("Filtering missing scenes")
        missing_scenes = [
            os.path.join(USGS_S3_BUCKET_PATH, path)
            for path in source_paths.difference(dest_paths)
        ]

        # collect orphan scenes
        # orphan scenes = keys that are in PDS sync bucket but missing in the bulk file and/or in source bucket
        log.info("Filtering orphan scenes")
        orphaned_scenes = [
            os.path.join(f"s3://{bucket_name}", path)
            for path in dest_paths.difference(source_paths)
        ]

        log.info("Retrieving keys from odc")
        all_odc_values = get_odc_keys(satellites, log)
        all_odc_keys = all_odc_values.keys()

        missing_odc_scenes = [
            os.path.join(f"s3://{bucket_name}", path)
            for path in dest_paths.difference(all_odc_keys)
        ]

        yesterday = date.today() - timedelta(days=1)

        orphaned_odc_scenes = [
            os.path.join(f"s3://{bucket_name}", path)
            for path in set(all_odc_keys).difference(dest_paths)
            if yesterday > all_odc_values[path].date()
        ]

        log.info(f"Found {len(missing_scenes)} missing scenes")
        log.info(f"missing_scenes 10 first keys {list(missing_scenes)[0:10]}")
        log.info(f"Found {len(orphaned_scenes)} orphaned scenes")
        log.info(f"orphaned_scenes 10 first keys {list(orphaned_scenes)[0:10]}")

        log.info(f"Found {len(missing_odc_scenes)} missing ODC scenes")
        log.info(f"missing_odc_scenes 10 first keys {list(missing_odc_scenes)[0:10]}")
        log.info(f"Found {len(orphaned_odc_scenes)} orphaned ODC scenes")
        log.info(f"orphaned_odc_scenes 10 first keys {list(orphaned_odc_scenes)[0:10]}")
    landsat_s3 = s3_client(region_name="af-south-1")

    if (
        len(missing_scenes) > 0
        or len(orphaned_scenes) > 0
        or len(missing_odc_scenes) > 0
        or len(orphaned_odc_scenes) > 0
    ):
        output_filename = (
            (
                f"{title}_{date_string}_gap_report.json"
                if not update_stac
                else f"{date_string}_gap_report_update.json"
            )
            .replace(" ", "_")
            .replace("_&", "")
        )

        log.info(
            f"Report file will be saved in {os.path.join(landsat_status_report_path , output_filename)}"
        )
        missing_orphan_scenes_json = json.dumps(
            {
                "orphan": orphaned_scenes,
                "missing": missing_scenes,
                "orphan_odc": orphaned_odc_scenes,
                "missing_odc": missing_odc_scenes,
            }
        )

        s3_dump(
            data=missing_orphan_scenes_json,
            url=os.path.join(landsat_status_report_path, output_filename),
            s3=landsat_s3,
            ContentType="application/json",
        )

    report_output = (
        os.path.join(landsat_status_report_url, output_filename)
        if len(missing_scenes) > 0
        or len(orphaned_scenes) > 0
        or len(missing_odc_scenes) > 0
        or len(orphaned_odc_scenes) > 0
        else output_filename
    )

    message = dedent(
        f"*{title} GAP REPORT - {environment}*\n"
        f"Missing Scenes: {len(missing_scenes)}\n"
        f"Orphan Scenes: {len(orphaned_scenes)}\n"
        f"Missing ODC Scenes: {len(missing_odc_scenes)}\n"
        f"Orphan ODC Scenes: {len(orphaned_odc_scenes)}\n"
        f"Report: {report_output}\n"
    )

    log.info(message)

    log.info(
        f"File {file_name} processed and sent in {time_process(start=start_timer)}"
    )

    if not update_stac and (len(missing_scenes) > 200 or len(orphaned_scenes) > 200):
        if notification_url is not None:
            send_slack_notification(
                notification_url, f"{satellites} Gap Report", message
            )
        raise Exception(f"More than 200 scenes were found \n {message}")


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
    default=f"Satellite to be compared, supported ones are {SUPPORTED_SATELLITES}",
)
@update_stac
@slack_url
@click.option("--version", is_flag=True, default=False)
@click.command("landsat-gap-report")
def cli(
    bucket_name: str,
    satellite: str,
    update_stac: bool = False,
    slack_url: str = None,
    version: bool = False,
):
    """
    Publish missing scenes
    """

    if version:
        click.echo(__version__)
    else:
        generate_buckets_diff(
            bucket_name=bucket_name,
            satellites=satellite.split("_"),
            file_name=FILES.get(satellite, None),
            update_stac=update_stac,
            notification_url=slack_url,
        )
