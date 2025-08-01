import datetime
import json
import logging
import os
from textwrap import dedent

import click
import datacube
import geopandas as gpd
import pandas as pd
import requests
from geojson import FeatureCollection
from odc.aws import s3_client, s3_dump, s3_ls_dir
from odc.aws.inventory import list_inventory
from sentinelhub import DataCollection, Geometry, SentinelHubCatalog, SHConfig
from yarl import URL

from deafrica.click_options import slack_url
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

missing_datasets = []
missing_datatakes = []
incomplete_datatakes = []
missing_files = []


def get_origin_data(
    grided_africa: gpd.GeoDataFrame,
    africa_geometry: Geometry,
    start_date: str,
    end_date: str,
) -> list[str]:
    config = SHConfig()
    config.sh_client_id = SH_CLIENT_ID
    config.sh_client_secret = SH_CLIENT_SECRET

    catalog = SentinelHubCatalog(config=config)

    results = list(
        catalog.search(
            DataCollection.SENTINEL1_IW,
            geometry=africa_geometry,
            time=(start_date, end_date),
            fields={
                "include": ["id", "properties.datetime", "geometry"],
                "exclude": [],
            },
        )
    )
    # add id attribute to properties
    for row in results:
        props = row["properties"]
        props["filename"] = row["id"]
    s1_results_frame = gpd.GeoDataFrame.from_features(results, crs="EPSG:4326")

    grided_results = gpd.overlay(s1_results_frame, grided_africa, how="intersection")
    grided_results = grided_results[
        grided_results.geometry.to_crs("EPSG:3857").area > 0
    ]
    return create_dataset_names(grided_results)


def get_africa_grid(africa_extent_json: FeatureCollection) -> gpd.GeoDataFrame:
    grid = gpd.read_file(TILING_GRID)
    africa_extent_frame = gpd.GeoDataFrame.from_features(
        africa_extent_json["features"], crs="EPSG:4326"
    )
    return gpd.overlay(grid, africa_extent_frame, how="intersection")


def create_dataset_names(grided_results):
    datasets = []
    for index, row in grided_results.iterrows():
        split_id = row["filename"].split("_")
        date = split_id[4][0:8]
        data_take = split_id[7]
        grid_name = row["NAME"]
        dataset = (
            "s1_rtc/"
            + grid_name
            + "/"
            + date[0:4]
            + "/"
            + date[4:6]
            + "/"
            + date[6:8]
            + "/"
            + data_take
        )
        if dataset not in datasets:
            datasets.append(dataset)
    return datasets


def check_target_data(origin_datasets, target_datatakes):
    client = s3_client(region_name=REGION_NAME)
    target_files = []
    for dataset in origin_datasets:
        results = list(s3_ls_dir(uri=S1_BUCKET + dataset, s3=client))
        if results:
            target_files.append(results)
            check_if_all_files_in_target_folder(results, dataset)
            datatake = dataset[-6:]
            if datatake not in target_datatakes:
                target_datatakes.append(datatake)
        else:
            missing_datasets.append(S1_BUCKET + dataset)
    return target_files


def load_geometry_from_json(data: FeatureCollection) -> Geometry:
    for f in data["features"]:
        return Geometry.from_geojson(f["geometry"])


def check_if_all_files_in_target_folder(name_list, name: str):
    if not any("ANGLE.tif" in name for name in name_list):
        missing_files.append(create_path_from_file(name) + "_ANGLE.tif")
    if not any("AREA.tif" in name for name in name_list):
        missing_files.append(create_path_from_file(name) + "_AREA.tif")
    if not any("MASK.tif" in name for name in name_list):
        missing_files.append(create_path_from_file(name) + "_MASK.tif")
    if not any("metadata.json" in name for name in name_list):
        missing_files.append(create_path_from_file(name) + "_metadata.json")
    if not any("metadata.xml" in name for name in name_list):
        missing_files.append(create_path_from_file(name) + "_metadata.xml")
    if not any("userdata.json" in name for name in name_list):
        missing_files.append(create_path_from_file(name) + "_userdata.json")
    if not any("VH.tif" in name for name in name_list):
        missing_files.append(create_path_from_file(name) + "_VH.tif")
    if not any("VV.tif" in name for name in name_list):
        missing_files.append(create_path_from_file(name) + "_VV.tif")


def create_path_from_file(path: str):
    splited = path.split("/")
    name = (
        S1_BUCKET
        + path
        + "/"
        + splited[0]
        + "_"
        + splited[5]
        + "_"
        + splited[1]
        + "_"
        + splited[2]
        + "_"
        + splited[3]
        + "_"
        + splited[4]
    )
    return name


def get_s1_date_ranges() -> list[tuple[str]]:
    start_date_str = "2018-01-01"
    end_date_str = datetime.datetime.today().strftime("%Y-%m-%d")

    start_date = pd.to_datetime(start_date_str)
    end_date = pd.to_datetime(end_date_str)

    # Generate the first day of each month between the two dates
    month_starts = pd.date_range(start=start_date, end=end_date, freq="MS")

    # Create date ranges: (start, end) for each month
    date_ranges = []
    for i in range(len(month_starts)):
        start = month_starts[i]
        if i + 1 < len(month_starts):
            end = month_starts[i + 1] - pd.Timedelta(days=1)
        else:
            end = end_date
        date_ranges.append((start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))

    return date_ranges


def find_missing_s1_data_from_sentinelhub() -> tuple[list, list, list, list]:
    africa_extent_json = requests.get(AFRICA_EXTENT_URL).json()
    africa_geometry = load_geometry_from_json(africa_extent_json)
    africa_grid = get_africa_grid(africa_extent_json)

    date_ranges = get_s1_date_ranges()

    target_datatakes = []
    for month_range in date_ranges:
        start_date = month_range[0]
        end_date = month_range[-1]
        month_str = datetime.datetime.strptime(start_date, "%Y-%m-%d").strftime("%B %Y")
        log.info(f"Checking S1 data for the month {month_str}")

        origin_data = get_origin_data(
            africa_grid, africa_geometry, start_date, end_date
        )
        log.info(f"Sentinel-Hub results: {len(origin_data)}")

        target_data = check_target_data(origin_data, target_datatakes)
        log.info(f"DEAfrica results: {len(target_data)}")
    if missing_datasets:
        for dataset in missing_datasets:
            datatake = dataset[-6:]
            if (datatake in target_datatakes) & (datatake not in incomplete_datatakes):
                incomplete_datatakes.append(datatake)
            elif (datatake not in target_datatakes) & (
                datatake not in missing_datatakes
            ):
                missing_datatakes.append(datatake)
    return missing_datasets, missing_files, incomplete_datatakes, missing_datatakes


def get_odc_keys() -> dict[str, str]:
    try:
        dc = datacube.Datacube()
        all_odc_vals = {}
        for val in dc.index.datasets.search_returning(
            ["uri", "indexed_time"], product=BASE_FOLDER_NAME
        ):
            all_odc_vals[val.uri.replace(S1_BUCKET, "")] = val.indexed_time
        return all_odc_vals
    except Exception as e:
        log.error(f"Error while searching for datasets in odc: {e}")
        raise


def get_missing_and_orphan_odc_scenes() -> tuple[set[str], set[str]]:
    log.info(f"Finding datasets in pds bucket {S1_BUCKET} but not indexed in ODC ...")
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
    all_odc_values = get_odc_keys()
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
    log.info("Done")
    return missing_odc_scenes, orphaned_odc_scenes


def find_missing_s1_data(
    bucket_name: str, slack_url: str, skip_sentinelhub_check: bool
):
    log = setup_logging()
    log.info("Task started ")
    try:
        missing_odc_scenes, orphaned_odc_scenes = get_missing_and_orphan_odc_scenes()

        if skip_sentinelhub_check is False:
            missing_datasets, missing_files, incomplete_datatakes, missing_datatakes = (
                find_missing_s1_data_from_sentinelhub()
            )

        log.info("Writing gap report ...")
        today = datetime.datetime.today()
        s1_status_report_path = URL(f"s3://{bucket_name}/status-report/")
        output_filename = f"{today.strftime('%Y-%m-%d')}_gap_report.json"
        log.info(f"File will be saved in {s1_status_report_path}{output_filename}")

        if skip_sentinelhub_check is False:
            gap_report_json = json.dumps(
                {
                    "missing_datasets": list(missing_datasets),
                    "missing_files": list(missing_files),
                    "incomplete_datatakes": list(incomplete_datatakes),
                    "missing_datatakes": list(missing_datatakes),
                    "missing_odc": list(missing_odc_scenes),
                    "orphan_odc": list(orphaned_odc_scenes),
                }
            )
        else:
            gap_report_json = json.dumps(
                {
                    "missing_odc": list(missing_odc_scenes),
                    "orphan_odc": list(orphaned_odc_scenes),
                }
            )

        client = s3_client(region_name=REGION_NAME)
        s3_dump(
            data=gap_report_json,
            url=str(s1_status_report_path / output_filename),
            s3=client,
            ContentType="application/json",
        )
        log.info(f"Gap report written to {s1_status_report_path}{output_filename}")

        report_http_link = f"https://{bucket_name}.s3.af-south-1.amazonaws.com/status-report/{output_filename}"

        if skip_sentinelhub_check is False:
            slack_message = dedent(
                f"*SENTINEL 1 GAP REPORT - PDS*\n"
                f"Missing Datasets: {len(missing_datasets)}\n"
                f"Missing Files: {len(missing_files)}\n"
                f"Incomplete Datatakes: {len(incomplete_datatakes)}\n"
                f"Missing Datatakes: {len(missing_datatakes)}\n"
                f"Missing ODC Scenes: {len(missing_odc_scenes)}\n"
                f"Orphan ODC Scenes: {len(orphaned_odc_scenes)}\n"
                f"Report: {report_http_link}\n"
            )
        else:
            slack_message = dedent(
                f"*SENTINEL 1 GAP REPORT - PDS*\n"
                f"Missing ODC Scenes: {len(missing_odc_scenes)}\n"
                f"Orphan ODC Scenes: {len(orphaned_odc_scenes)}\n"
                f"Report: {report_http_link}\n"
            )

        if slack_url:
            send_slack_notification(slack_url, "S1 Gap Report", slack_message)
            log.info("Slack notification sent")
        else:
            log.info(slack_message)
    except Exception as exc:
        log.exception(exc)


@click.argument(
    "bucket_name",
    type=str,
    nargs=1,
    required=True,
    default="Bucket where the gap report will be stored",
)
@click.option(
    "--skip-sentinelhub-check",
    is_flag=True,
    default=False,
    help="If True, skip checking for missing datasets, missing files, "
    "incomplete and missing datatakes from SentinelHub. "
    "Gap report only returns missing ODC scenes and orphaned ODC scenes.",
)
@slack_url
@click.command("s1-gap-report")
def cli(
    bucket_name: str,
    skip_sentinelhub_check: bool,
    slack_url: str = None,
):
    """
    Sentinel-1 gap report for s1_rtc scenes in the bucket BUCKET_NAME.
    """

    find_missing_s1_data(
        bucket_name=bucket_name,
        slack_url=slack_url,
        skip_sentinelhub_check=skip_sentinelhub_check,
    )
