import json
import os
from datetime import datetime
from pathlib import Path
from subprocess import check_output, STDOUT

import click
import pystac
from odc.aws import s3_dump
from odc.index import odc_uuid
from pystac import Item
from rio_stac import create_stac_item
from urlpath import URL
from deafrica.utils import (
    setup_logging,
    slack_url,
    send_slack_notification,
)

VALID_YEARS = ["1996", "2007", "2008", "2009", "2010", "2015", "2016"]
LOCAL_DIR = Path(os.getcwd())
SOURCE_URL_PATH = URL("https://wcmc.io/")
FILE_NAME = "GMW_{year}"

# Set log level to info
log = setup_logging()


def download_and_unzip_gmw(local_filename: str) -> str:
    import requests
    import shutil
    from zipfile import ZipFile

    url = SOURCE_URL_PATH / local_filename

    with requests.get(url, stream=True, allow_redirects=True) as r:
        with open(local_filename, "wb") as f:
            shutil.copyfileobj(r.raw, f)

    with ZipFile(local_filename) as z:
        z.extractall()

    return [f for f in z.namelist() if f.endswith(".shp")][0]


def create_and_upload_stac(cog_file: Path, s3_dst: str, year) -> Item:
    out_path = URL(f"{s3_dst}/{year}/")

    log.info("Item base creation")
    item = create_stac_item(
        str(cog_file),
        id=str(odc_uuid("gmw", "2.0", [cog_file.name.replace("tif", "")])),
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
        title="gmw-v1.0",
        media_type=pystac.MediaType.COG,
        roles=["data"],
    )

    log.info(f"Item created {item.to_dict()}")
    log.info(f"Item validated {item.validate()}")

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

    gmw_shp = ""

    try:
        if year not in VALID_YEARS:
            raise ValueError(
                f"Chosen year {year} is not valid, please choose from one of {VALID_YEARS}"
            )

        log.info(f"Starting GMW downloader for year {year}")

        log.info("download extents if needed")
        gmw_shp = f"GMW_001_GlobalMangroveWatch_{year}/01_Data/GMW_{year}_v2.shp"
        local_filename = FILE_NAME.format(year=year)
        if not os.path.exists(gmw_shp):
            gmw_shp = download_and_unzip_gmw(local_filename=local_filename)

        local_extracted_file_path = LOCAL_DIR / gmw_shp

        output_file = LOCAL_DIR / gmw_shp.replace(".shp", ".tif")
        log.info(f"Output TIF file is {output_file}")
        log.info(f"Extracted SHP file is {local_extracted_file_path}")
        log.info("Start gdal_rasterize")
        cmd = (
            "gdal_rasterize "
            "-a_nodata 0 "
            "-ot Byte "
            "-a pxlval "
            "-of GTiff "
            "-tr 0.0002 0.0002 "
            f"{local_extracted_file_path} {output_file} "
            "-te -26.36 -47.97 64.50 38.35"
        )
        check_output(cmd, stderr=STDOUT, shell=True)

        log.info(f"File {output_file} rasterized successfully")

        # Create cloud optimised GeoTIFF
        cloud_optimised_file = LOCAL_DIR / f"deafrica_gmw_{year}.tif"
        cmd = (
            f"rio cogeo create --overview-level 0 {output_file} {cloud_optimised_file}"
        )
        check_output(cmd, stderr=STDOUT, shell=True)

        log.info(f"File {cloud_optimised_file} cloud optimised successfully")

        create_and_upload_stac(cog_file=cloud_optimised_file, s3_dst=s3_dst, year=year)

        # All done!
        log.info(f"Completed work on {s3_dst}/{year}")

    except Exception as e:
        message = f"Failed to handle GMW {gmw_shp} with error {e}"

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
    """

    gmw_download_stac_cog(year=year, s3_dst=s3_dst, slack_url=slack_url)
