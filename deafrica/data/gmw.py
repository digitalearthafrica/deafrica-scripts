"""
Download the Global Mangrove Watch (1996 - 2020) Version 3.0 Dataset
for a year from Zenodo, merge and convert to Cloud Optimized Geotiff,
and push to an S3 bucket

Datasource: https://zenodo.org/records/6894273
"""

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from subprocess import STDOUT, check_output
from zipfile import ZipFile

import click
import geopandas as gpd
import pystac
import requests
from odc.aws import s3_dump
from pystac import Item
from rio_stac import create_stac_item
from urlpath import URL

from deafrica.utils import odc_uuid, send_slack_notification, setup_logging, slack_url

VALID_YEARS = [
    "1996",
    "2007",
    "2008",
    "2009",
    "2010",
    "2015",
    "2016",
    "2017",
    "2018",
    "2019",
    "2020",
]
SOURCE_URL_PATH = URL("https://zenodo.org/records/6894273/files/")
FILE_NAME = "gmw_v3_{year}_gtiff.zip"
LOCAL_DIR = Path(os.getcwd())
AFRICA_EXTENT_URL = "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/africa-extent-bbox.json" # noqa E501

# Set log level to info
log = setup_logging()


def download_and_unzip_gmw(year: str) -> list[str]:
    """
    Download and unzip the Global Mangrove Watch (GMW) files
    for a year.

    Parameters
    ----------
    year : str
        Year for which to download Global Mangrove Watch data.

    Returns
    -------
    list[str]
        GMW TIF files downloaded.
    """
    url = SOURCE_URL_PATH / FILE_NAME.format(year=year)

    local_filename = LOCAL_DIR / FILE_NAME.format(year=year)

    if not os.path.exists(local_filename):
        with requests.get(url, stream=True, allow_redirects=True) as r:
            with open(local_filename, "wb") as f:
                shutil.copyfileobj(r.raw, f)
    else:
        log.info(f"Skipping download, {local_filename} already exists!")

    with ZipFile(local_filename) as z:
        z.extractall()
        gmw_tiles = [f for f in z.namelist() if f.endswith(".tif")]
    return gmw_tiles


def get_gmw_africa_tiles() -> set[str]:
    """
    Get a set of the labels for Global Mangrove Watch tiles over Africa.
    Returns
    -------
    set[str]
        Labels for Global Mangrove Watch tiles over Africa.
    """
    africa_extent = gpd.read_file(AFRICA_EXTENT_URL).to_crs("EPSG:4326")
    gmw_tiles_url = SOURCE_URL_PATH / "gmw_v3_tiles.geojson"
    gmw_tiles = gpd.read_file(str(gmw_tiles_url)).to_crs("EPSG:4326")
    gmw_africa_tiles = set(
        africa_extent.sjoin(gmw_tiles, how="inner", predicate="intersects")[
            "tile"
        ].values
    )
    return gmw_africa_tiles


def create_and_upload_stac(cog_file: Path, s3_dst: str, year) -> Item:
    out_path = URL(f"{s3_dst}/{year}/")

    log.info("Item base creation")
    item = create_stac_item(
        str(cog_file),
        id=str(odc_uuid("gmw", "3.0", [cog_file.name.replace("tif", "")])),
        with_proj=True,
        input_datetime=datetime(int(year), 12, 31),
        properties={
            "odc:product": "gmw",
            "start_datetime": f"{year}-01-01T00:00:00Z",
            "end_datetime": f"{year}-12-31T23:59:59Z",
        },
    )

    log.info("links creation")
    item.set_self_href(str(out_path / f"gmw_{year}_stac-item.json"))
    item.add_links(
        [
            pystac.Link(
                target=str(SOURCE_URL_PATH / FILE_NAME.format(year=year)),
                title="Source file",
                rel=pystac.RelType.DERIVED_FROM,
                media_type="application/zip",
            )
        ]
    )

    out_data = out_path / cog_file.name
    # Remove asset created by create_stac_item and add our own
    del item.assets["asset"]
    item.assets["mangrove"] = pystac.Asset(
        href=str(out_data),
        title="gmw-v3.0",
        media_type=pystac.MediaType.COG,
        roles=["data"],
    )

    log.info(f"Item created {item.to_dict()}")
    
    # Error _RefResolutionError: Unresolvable JSON pointer: 'definitions/link'
    # not sure why.
    # log.info(f"Item validated {item.validate()}")

    log.info(f"Dump the data to S3 {str(cog_file)}")
    s3_dump(
        data=open(str(cog_file), "rb").read(),
        url=str(out_data),
        ACL="bucket-owner-full-control",
        ContentType="image/tiff",
    )
    log.info(f"File written to {out_data}")

    log.info("Write STAC to S3")
    s3_dump(
        data=json.dumps(item.to_dict(), indent=2),
        url=item.self_href,
        ACL="bucket-owner-full-control",
        ContentType="application/json",
    )
    log.info(f"STAC written to {item.self_href}")

    return item


def gmw_download_stac_cog(year: str, s3_dst: str, slack_url: str = None) -> None:
    """
    Mangrove download, COG and STAC process

    """
    try:
        if year not in VALID_YEARS:
            raise ValueError(
                f"Chosen year {year} is not valid, "
                f"please choose from one of {VALID_YEARS}"
            )
        log.info(f"Starting GMW downloader for year {year}")
        gmw_africa_tiles = get_gmw_africa_tiles()
        gmw_files = download_and_unzip_gmw(year=year)
        gmw_africa_files = [
            file
            for file in gmw_files
            if any(label in file for label in gmw_africa_tiles)
        ]

        tiff_list = LOCAL_DIR / "files_to_merge.txt"
        with open(tiff_list, "w") as file:
            for item in gmw_africa_files:
                file.write(item + "\n")

        output_file = LOCAL_DIR / f"gmw_v3_{year}.tiff"
        log.info(f"Output TIF file is {output_file}")

        log.info("Start gdal_merge.py")
        cmd = (
            f"gdal_merge.py -o {str(output_file)} -of GTiff -ot Byte "
            "-ps 0.0002 0.0002 -ul_lr -26.36 38.35 64.50 -47.97 -v -a_nodata 0 "
            "--optfile files_to_merge.txt"
        )
        check_output(cmd, stderr=STDOUT, shell=True)
        log.info(f"File {output_file} merged successfully")

        log.info("Creating cloud optimised GeoTIFF ...")
        cloud_optimised_file = LOCAL_DIR / f"deafrica_gmw_{year}.tif"
        cmd = (
            "CHECK_DISK_FREE_SPACE=FALSE rio cogeo create --overview-resampling "
            f"nearest {output_file} {cloud_optimised_file}"
        )
        check_output(cmd, stderr=STDOUT, shell=True)
        log.info(f"File {cloud_optimised_file} cloud optimised successfully")

        create_and_upload_stac(cog_file=cloud_optimised_file, s3_dst=s3_dst, year=year)

        # All done!
        log.info(f"Completed work on {s3_dst}/{year}")
    except Exception as e:
        message = f"Failed to handle GMW {FILE_NAME.format(year=year)} with error {e}"
        if slack_url is not None:
            send_slack_notification(slack_url, "GMW", message)
        log.exception(message)

        exit(1)


@click.command("download-gmw")
@click.option("--year", required=True)
@click.option("--s3_dst", default="s3://deafrica-data-dev-af/gmw/")
@slack_url
def cli(year, s3_dst, slack_url):
    """
    Available years are
    • 1996
    • 2007
    • 2008
    • 2009
    • 2010
    • 2015
    • 2016
    • 2017
    • 2018
    • 2019
    • 2020
    """

    gmw_download_stac_cog(year=year, s3_dst=s3_dst, slack_url=slack_url)
