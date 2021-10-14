import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from subprocess import check_output, STDOUT, CalledProcessError

import click
import pystac
from odc.aws import s3_dump
from odc.index import odc_uuid
from pystac import Item
from pystac.utils import datetime_to_str
from rio_stac import create_stac_item
from urlpath import URL

from deafrica.utils import setup_logging, io_timer

VALID_YEARS = ["1996", "2007", "2008", "2009", "2010", "2015", "2016"]
LOCAL_DIR = Path(os.getcwd())
SOURCE_URL_PATH = URL(f"https://wcmc.io/")
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
            "odc:processing_datetime": datetime_to_str(datetime.now()),
            "odc:product": "gmw_yearly",
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

    log.info("assets creation")
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

    log.info("Dump the data to S3")
    s3_dump(str(cog_file), str(out_data), ACL="bucket-owner-full-control")
    log.info(f"File written to {out_data}")

    log.info(f"Write STAC to S3")
    s3_dump(
        json.dumps(item.to_dict(), indent=2),
        item.self_href,
        ContentType="application/json",
        ACL="bucket-owner-full-control",
    )
    log.info(f"STAC written to {item.self_href}")

    return item


def gmw_download_stac_cog(year: str, s3_dst: str) -> None:
    """
    Mangrove download, COG and STAC process

    """

    if year not in VALID_YEARS:
        raise ValueError(
            f"Informed year {year} not valid, please choose among {VALID_YEARS}"
        )

    log.info(f"Starting GMW downloader for year {year}")

    log.info(f"download extents if needed")
    gmw_shp = f"GMW_001_GlobalMangroveWatch_{year}/01_Data/GMW_{year}_v2.shp"
    local_filename = FILE_NAME.format(year=year)
    if not os.path.exists(gmw_shp):
        gmw_shp = download_and_unzip_gmw(local_filename=local_filename)

    local_extracted_file_path = LOCAL_DIR / gmw_shp
    try:
        output_file = LOCAL_DIR / gmw_shp.replace(".shp", ".tif")
        log.info(f"Output TIF file is {output_file}")
        log.info(f"Extracted SHP file is {local_extracted_file_path}")
        log.info(f"Start gdal_rasterize")
        cmd = (
            f"gdal_rasterize "
            f"-a_nodata 0 "
            f"-ot Byte "
            f"-a pxlval "
            f"-of GTiff "
            f"-tr 0.001 0.001 "
            f"{local_extracted_file_path} "
            f"{output_file} "
            f"-te -26.359944882003788 -47.96476498374171 64.4936701740102 38.34459242512347"
        )
        check_output(cmd, stderr=STDOUT, shell=True)

        io_timer(file_path=output_file, log=log)

        log.info(f"File {output_file} rasterized successfully")

        # create cloud optimised geotif
        cloud_optimised_file = LOCAL_DIR / f"deafrica_gmw_{year}.tif"
        cmd = (
            f"rio cogeo create --overview-level 0 {output_file} {cloud_optimised_file}"
        )
        check_output(cmd, stderr=STDOUT, shell=True)

        io_timer(file_path=cloud_optimised_file)

        log.info(f"File {cloud_optimised_file} cloud optimised successfully")

    except CalledProcessError as ex:
        raise ex

    create_and_upload_stac(cog_file=cloud_optimised_file, s3_dst=s3_dst, year=year)

    # All done!
    log.info(f"Completed work on {s3_dst}/{year}")


@click.command("download-gmw")
@click.option("--year", required=True)
@click.option("--s3_dst", default="s3://deafrica-data-dev-af/gmw_yealy/")
def cli(year, s3_dst):
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

    gmw_download_stac_cog(year=year, s3_dst=s3_dst)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Select a year to download.")
    else:
        gmw_download_stac_cog(sys.argv[1], "s3://deafrica-data-dev-af/gmw_yealy/")
