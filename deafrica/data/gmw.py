import json
import os
import sys
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

from deafrica.utils import setup_logging

LOCAL_DIR = Path(__file__).absolute().parent
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


def create_and_upload_stac(cog_file: str, s3_dst: str, year) -> Item:
    out_path = URL(f"{s3_dst}/{year}/")
    file_name = cog_file.replace("tif", "")

    log.info("Item base creation")
    item = create_stac_item(
        file_name,
        id=str(odc_uuid("gmw", "2.0", [file_name])),
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
    item.set_self_href(out_path / f"gmw_{year}_stac-item.json")
    item.add_links(
        [
            pystac.Link(
                target=SOURCE_URL_PATH / FILE_NAME.format(year=year),
                title="Source file",
                rel=pystac.RelType.DERIVED_FROM,
                media_type="application/zip",
            )
        ]
    )

    log.info("assets creation")
    del item.assets["asset"]

    item.assets["rainfall"] = pystac.Asset(
        href=out_path / file_name,
        title="gmw-v1.0",
        media_type=pystac.MediaType.COG,
        roles=["data"],
    )

    log.info(f"Item created {item.to_dict()}")

    log.info("Dump the data to S3")
    out_data = out_path / cog_file
    s3_dump(file_name, out_data, ACL="bucket-owner-full-control")
    log.info(f"File written to {out_data}")

    log.info("Write STAC to S3")
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
    log.info(f"Starting GMW downloader for year {year}")

    log.info(f"download extents if needed")
    gmw_shp = f"GMW_001_GlobalMangroveWatch_{year}/01_Data/GMW_{year}_v2.shp"
    local_filename = FILE_NAME.format(year=year)
    if not os.path.exists(gmw_shp):
        gmw_shp = download_and_unzip_gmw(local_filename=local_filename)

    try:
        output_file = gmw_shp.replace(".shp", ".tif")
        cmd = f"gdal_rasterize -a_nodata 0 -ot Byte -a pxlval -of GTiff -tr 0.001 0.001 {gmw_shp} {output_file} -te -26.359944882003788 -47.96476498374171 64.4936701740102 38.34459242512347"
        check_output(cmd, stderr=STDOUT, shell=True)

        # create cloud optimised geotif
        cloud_optimised_file = f"deafrica_gmw_{year}.tif"
        cmd = (
            f"rio cogeo create --overview-level 0 {output_file} {cloud_optimised_file}"
        )
        check_output(cmd, stderr=STDOUT, shell=True)

    except CalledProcessError as ex:
        raise ex

    create_and_upload_stac(cog_file=cloud_optimised_file, s3_dst=s3_dst, year=year)

    log.info("Write all files zipped to S3")
    out_zip_data = URL(s3_dst) / year / f"{local_filename}.zip"
    s3_dump(
        open(LOCAL_DIR / local_filename),
        out_zip_data,
        ContentType="application/zip",
        ACL="bucket-owner-full-control",
    )
    log.info(f"ZIP written to {out_zip_data}")

    # All done!
    log.info(f"Completed work on {s3_dst}/{year}")


@click.command("download-gmw")
@click.option("--year", default="2020")
@click.option("--s3_dst", default="s3://deafrica-data-dev-af/gmw_yealy/")
def cli(year, s3_dst):
    """
    Available years are
    • GMW 1996
    • GMW 2007
    • GMW 2008
    • GMW 2009
    • GMW 2010
    • GMW 2015
    • GMW 2016
    """

    gmw_download_stac_cog(year=year, s3_dst=s3_dst)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Select a year to download.")
    else:
        gmw_download_stac_cog(sys.argv[1], "s3://deafrica-data-dev-af/gmw_yealy/")
