#!/usr/bin/env python3

import json
import os
import shutil
from logging import Logger
from pathlib import Path
from shapely.geometry import Polygon
from odc.aws import s3_dump, s3_head_object
import requests
import pystac
from rio_stac import create_stac_item
from deafrica.utils import odc_uuid, setup_logging
import click


AFRICA_EXTENT = "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/africa-extent.json"

MAX_X = 64
MIN_X = -26
MAX_Y = 38
MIN_Y = -54


def make_directory(directory: Path, log: Logger):
    if not directory.exists():
        log.info(f"Creating directory {directory}")
        directory.mkdir(parents=True)


def delete_directory(directory: Path, log: Logger):
    log.info("Deleting directory...")
    if directory.exists():
        log.info(f"Deleting directory {directory}")
        shutil.rmtree(directory)


def download_file(url, file_name, log: Logger):
    log.info("Downloading file...")
    with requests.get(url, stream=True, allow_redirects=True) as r:
        with open(file_name, "wb") as f:
            shutil.copyfileobj(r.raw, f)


def is_tile_over_africa(workdir, source_url, folder_name, africa_polygon, log: Logger):
    filename = f"{folder_name}_stac.json"

    location = f"{source_url}{filename}"
    json_file = workdir / filename
    try:
        download_file(location, json_file, log)
        with open(json_file) as f:
            data = json.load(f)
            polygon = data["geometry"]["coordinates"][0]
            poly = Polygon(polygon)
            return poly.intersects(africa_polygon)
    except Exception:
        return False


def download_tif(workdir, source_url, folder_name, tif_files, log: Logger):
    filename = f"{folder_name}.tif"
    location = f"{source_url}{filename}"
    tif_file = workdir / filename

    try:
        if not tif_file.exists():
            log.info(f"Downloading file: {location}")
            download_file(location, tif_file, log)
        else:
            log.info("Skipping download, file already exists")
        tif_files.append(tif_file)
        return tif_file
    except Exception:
        log.info("File failed to download... skipping")
        exit(0)


def get_version(edition):
    if edition == "2015":
        return "v2"
    elif edition == "2019":
        return "v1"
    elif edition == "evolution":
        return "v1"


def get_source_main_folder_name(edition):
    if edition == "2015":
        return f"WSF{edition}"
    elif edition == "2019":
        return f"WSF{edition}"
    elif edition == "evolution":
        return "WSF_EVO"


def get_source_url(edition, main_folder_name, folder_name):
    if edition == "2019":
        return f"https://download.geoservice.dlr.de/{main_folder_name}/files/"
    else:
        return f"https://download.geoservice.dlr.de/{main_folder_name}/files/{folder_name}/"


def upload_to_s3(s3_destination, files, log: Logger):
    for file in files:
        out_name = os.path.basename(file)
        dest = f"S3://{s3_destination}/{out_name}"
        log.info(f"Uploading file to {dest}")
        content_type = "image/tiff"
        s3_dump(
            data=open(file, "rb").read(),
            url=dest,
            ACL="bucket-owner-full-control",
            ContentType=content_type,
        )


def write_stac(
    s3_destination: str, folder_names: str, edition: str, tile: str, log: Logger, path
) -> str:
    stac_href = f"s3://{s3_destination}/{folder_names[0]}.stac-item.json"
    filepaths = []
    for folder_name in folder_names:
        filepaths.append(f"s3://{s3_destination}/{folder_name}.tif")
    print(f"Creating STAC file: {stac_href}")

    shortname = "wsf"
    product_name = f"wsf_{edition}"
    asset_name = f"wsf{edition}"

    if edition == "evolution":
        start_date = "1985-01-01T00:00:00.000Z"
        end_date = "2015-12-31T23:59:59.999Z"
        bandpaths = {
            "wsfevolution": filepaths[0],
            "idc_score": filepaths[1],
        }
    else:
        start_date = f"{edition}-01-01T00:00:00Z"
        end_date = f"{edition}-12-31T23:59:59Z"
        bandpaths = {
            {asset_name}: filepaths[0],
        }
    properties = {
        "odc:product": product_name,
        "odc:region_code": tile,
        "start_datetime": start_date,
        "end_datetime": end_date,
    }

    assets = {}
    for name, s3_path in bandpaths.items():
        href = s3_path
        assets[name] = pystac.Asset(
            href=href, media_type=pystac.MediaType.COG, roles=["data"]
        )
    item = create_stac_item(
        path,
        id=str(odc_uuid(shortname, "1", [], year=edition, tile=tile)),
        properties=properties,
        assets=assets,
        with_proj=True,
    )
    item.set_self_href(stac_href)
    log.info("Storing stac item")
    s3_dump(
        json.dumps(item.to_dict(), indent=2),
        item.self_href,
        ContentType="application/json",
        ACL="bucket-owner-full-control",
    )
    log.info(f"STAC written to {item.self_href}")


def processTile(
    edition,
    tile,
    base_dir,
    s3_destination,
    update_metadata: bool,
    africa_polygon,
    log: Logger,
):
    workdir = base_dir / tile / "wrk"
    tif_files = []
    folder_names = []

    version = get_version(edition)
    main_folder_name = get_source_main_folder_name(edition)
    folder_name = f"WSF{edition}_{version}_{tile}"
    folder_names.append(folder_name)
    source_url = get_source_url(edition, main_folder_name, folder_name)

    s3_destination = f"{s3_destination}/{tile}"

    stac_href = f"s3://{s3_destination}/{folder_name}.stac-item.json"
    log.info(f"checking if json exists in bucket: {stac_href}")

    if s3_head_object(stac_href) is not None and not update_metadata:
        log.info(f"{stac_href} already exists, skipping")
        return
    elif update_metadata:
        file_href = f"s3://{s3_destination}/{folder_name}.tif"
        log.info(file_href)

        if s3_head_object(file_href) is not None:
            log.info(f"{file_href} exists, updating metadata only")
            splited_s3 = s3_destination.split("/")
            s3_main_folder = splited_s3[0]
            s3_product_folder = splited_s3[1]

            path = f"https://{s3_main_folder}.s3.af-south-1.amazonaws.com/{s3_product_folder}/{tile}/{folder_name}.tif"
            write_stac(s3_destination, folder_names, edition, tile, log, path)
            return
        else:
            log.info(f"{file_href} does not exist, continuing with data creation.")
    try:
        log.info(f"Starting up process for tile {tile}")
        make_directory(workdir, log)
        if is_tile_over_africa(workdir, source_url, folder_name, africa_polygon, log):
            log.info(f"Tile {tile} exists")
            download_tif(workdir, source_url, folder_name, tif_files, log)
            if edition == "evolution":
                evo_folder_name = f"IDC_Score_{tile}"
                download_tif(
                    workdir,
                    get_source_url(edition, main_folder_name, "idcscore"),
                    evo_folder_name,
                    tif_files,
                    log,
                )
                folder_names.append(evo_folder_name)
            if tif_files[0]:
                upload_to_s3(s3_destination, tif_files, log)
                write_stac(
                    s3_destination, folder_names, edition, tile, log, tif_files[0]
                )
        delete_directory(base_dir / tile, log)
    except Exception:
        log.info(f"Job failed for tile {tile}")
        exit(1)


def run(
    edition: str,
    base_dir: Path,
    s3_destination: str,
    update_metadata: bool,
    log: Logger,
):
    json = requests.get(AFRICA_EXTENT).json()
    africa_polygon = Polygon(json["features"][0]["geometry"]["coordinates"][0])

    for x in range(MIN_X, MAX_X, 2):
        for y in range(MIN_Y, MAX_Y, 2):
            tile = f"{x}_{y}"
            processTile(
                edition,
                tile,
                base_dir,
                s3_destination,
                update_metadata,
                africa_polygon,
                log,
            )


@click.command("download-wsf")
@click.option(
    "--edition",
    "-e",
    required=True,
    help="Edition of the WSF, like '2015' or 'evolution'",
)
@click.option(
    "--workdir",
    "-w",
    default="/tmp/download",
    help="The directory to download files to",
)
@click.option("--s3-bucket", "-s", required=False, help="The S3 bucket to upload to")
@click.option("--s3-path", "-p", required=False, help="The S3 path to upload to")
@click.option(
    "--update-metadata",
    "-u",
    is_flag=True,
    help="Update only metadata if the data already exists.",
)
def cli(edition, workdir, s3_bucket, update_metadata, s3_path):
    """
    Example command:

    download-wsf --e 2015 -w /tmp/download -s example-bucket -p wsf
    """
    log = setup_logging()

    s3_destination = s3_bucket.rstrip("/").lstrip("s3://") + "/" + s3_path.rstrip("/")

    run(edition, Path(workdir), s3_destination, update_metadata, log)
