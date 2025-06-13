import datetime
import json
import os
from textwrap import dedent

import click
import datacube
import geopandas as gpd
import requests
from odc.aws import s3_client, s3_dump, s3_ls_dir
from odc.aws.inventory import list_inventory
from sentinelhub import DataCollection, Geometry, SentinelHubCatalog, SHConfig
from yarl import URL

from deafrica.click_options import slack_url
from deafrica.logs import setup_logging
from deafrica.utils import (
    send_slack_notification,
)

SH_CLIENT_ID = os.getenv("SH_CLIENT_ID", "")
SH_CLIENT_SECRET = os.getenv("SH_CLIENT_SECRET", "")

BUCKET = "s3://deafrica-sentinel-1/"
REGION_NAME = "af-south-1"
AFRICA_EXTENT = "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/africa-extent.json"
TILING_GRID = "https://s3.eu-central-1.amazonaws.com/sh-batch-grids/tiling-grid-3.zip"
PERIOD = 7
SENTINEL_1_INVENTORY_PATH = "s3://deafrica-sentinel-1-inventory/deafrica-sentinel-1/deafrica-sentinel-1-inventory/"

missing_datasets = []
missing_datatakes = []
incomplete_datatakes = []
missing_files = []
missing_odc_scenes = []
orphaned_odc_scenes = []


def get_origin_data(
    grided_africa: gpd.GeoDataFrame,
    africa_geometry,
    date: str,
):
    config = SHConfig()
    config.sh_client_id = SH_CLIENT_ID
    config.sh_client_secret = SH_CLIENT_SECRET

    catalog = SentinelHubCatalog(config=config)

    results = list(
        catalog.search(
            DataCollection.SENTINEL1_IW,
            geometry=africa_geometry,
            time=date,
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


def get_africa_grid(africa_extent_json):
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
        results = list(s3_ls_dir(uri=BUCKET + dataset, s3=client))
        if results:
            target_files.append(results)
            check_if_all_files_in_target_folder(results, dataset)
            datatake = dataset[-6:]
            if datatake not in target_datatakes:
                target_datatakes.append(datatake)
        else:
            missing_datasets.append(BUCKET + dataset)
    return target_files


def load_json_from_geometry(data):
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
        BUCKET
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


def sendNotification(slack_url, report_http_link):
    message = dedent(
        f"*SENTINEL 1 GAP REPORT - PDS*\n"
        f"Missing Datasets: {len(missing_datasets)}\n"
        f"Missing Files: {len(missing_files)}\n"
        f"Incomplete Datatakes: {len(incomplete_datatakes)}\n"
        f"Missing Datatakes: {len(missing_datatakes)}\n"
        f"Missing ODC Scenes: {len(missing_odc_scenes)}\n"
        f"Orphan ODC Scenes: {len(orphaned_odc_scenes)}\n"
        f"Report: {report_http_link}\n"
    )
    send_slack_notification(slack_url, "S1 Gap Report", message)


def get_odc_keys(log) -> set:
    try:
        dc = datacube.Datacube()
        all_odc_vals = {}

        for val in dc.index.datasets.search_returning(
            ["uri", "indexed_time"], product="s1_rtc"
        ):
            all_odc_vals[val.uri.replace("s3://deafrica-sentinel-1/", "")] = (
                val.indexed_time
            )
        return all_odc_vals
    except Exception:
        log.info("Error while searching for datasets in odc")
        return {}


def find_missing_s1_data(bucket_name: str, slack_url: str):
    log = setup_logging()
    log.info("Task started ")
    today = datetime.datetime.today()
    s1_status_report_path = URL(f"s3://{bucket_name}/status-report/")
    try:
        africa_extent_json = requests.get(AFRICA_EXTENT).json()
        africa_grid = get_africa_grid(africa_extent_json)

        target_datatakes = []
        for i in range(0, PERIOD):
            date = today - datetime.timedelta(days=PERIOD - i + 1)
            date_str = date.strftime("%Y-%m-%d")
            log.info("Checking S1 data for date: " + date_str)

            africa_geometry = load_json_from_geometry(africa_extent_json)
            origin_data = get_origin_data(africa_grid, africa_geometry, date_str)
            log.info("Sentinel-Hub results: " + str(len(origin_data)))

            target_data = check_target_data(origin_data, target_datatakes)
            log.info("DEAfrica results: " + str(len(target_data)))
        if missing_datasets:
            for dataset in missing_datasets:
                datatake = dataset[-6:]
                if (datatake in target_datatakes) & (
                    datatake not in incomplete_datatakes
                ):
                    incomplete_datatakes.append(datatake)
                elif (datatake not in target_datatakes) & (
                    datatake not in missing_datatakes
                ):
                    missing_datatakes.append(datatake)

        # Keys that in the destination bucket but are not indexed
        # on ODC.
        client = s3_client(region_name=REGION_NAME)
        destination_keys = set(
            ns.Key
            for ns in list_inventory(
                manifest=SENTINEL_1_INVENTORY_PATH,
                prefix="s1_rtc",
                contains="metadata.json",
                n_threads=200,
            )
        )
        all_odc_values = get_odc_keys(log)
        indexed_keys = all_odc_values.keys()
        missing_odc_scenes = set(
            key for key in destination_keys if key not in indexed_keys
        )

        # Keys that are indexed on ODC but do not exist in the
        # destination bucket
        yesterday = (today - datetime.timedelta(days=1)).date()
        orphaned_odc_scenes = set(
            key
            for key in indexed_keys
            if (key not in destination_keys and yesterday > all_odc_values[key].date())
        )

        # Write report
        output_filename = f"{today.strftime('%Y-%m-%d')}_gap_report.json"
        log.info(f"File will be saved in {s1_status_report_path}{output_filename}")

        missing_json = json.dumps(
            {
                "missing_datasets": list(missing_datasets),
                "missing_files": list(missing_files),
                "incomplete_datatakes": list(incomplete_datatakes),
                "missing_datatakes": list(missing_datatakes),
                "missing_odc": list(missing_odc_scenes),
                "orphan_odc": list(orphaned_odc_scenes),
            }
        )

        s3_dump(
            data=missing_json,
            url=str(URL(s1_status_report_path) / output_filename),
            s3=client,
            ContentType="application/json",
        )

        if slack_url:
            report_http_link = f"https://{bucket_name}.s3.af-south-1.amazonaws.com/status-report/{output_filename}"
            sendNotification(slack_url, report_http_link)
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
