import calendar
import json
from datetime import datetime

import click
import pystac
from odc.aws import s3_dump, s3_head_object
from odc.index import odc_uuid
from pystac.utils import datetime_to_str
from rasterio.io import MemoryFile
from rio_cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from rio_stac import create_stac_item
from deafrica.utils import (
    setup_logging,
    slack_url,
    send_slack_notification,
)


MONTHLY_URL_TEMPLATE = (
    "https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_monthly/tifs/{in_file}"
)
DAILY_URL_TEMPLATE = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_daily/tifs/p05/{year}/{in_file}"

# Set log level to info
log = setup_logging()

log.info("Starting CHIRPS downloader")


def download_and_cog_chirps(
    year: str,
    month: str,
    s3_dst: str,
    day: str = None,
    overwrite: bool = False,
    slack_url: str = None,
):
    # Cleaning and sanity checks
    s3_dst = s3_dst.rstrip("/")

    # Set up file strings
    if day is not None:
        # Set up a daily process
        in_file = f"chirps-v2.0.{year}.{month}.{day}.tif.gz"
        in_href = DAILY_URL_TEMPLATE.format(year=year, in_file=in_file)
        in_data = f"/vsigzip//vsicurl/{in_href}"

        out_data = f"{s3_dst}/{year}/{month}/chirps-v2.0_{year}.{month}.{day}.tif"
        out_stac = (
            f"{s3_dst}/{year}/{month}/chirps-v2.0_{year}.{month}.{day}.stac-item.json"
        )

        start_datetime = f"{year}-{month}-{day}T00:00:00Z"
        end_datetime = f"{year}-{month}-{day}T23:59:59Z"
        product_name = "rainfall_chirps_daily"

        # Completely ridiculous change handling
        if int(year) >= 2021 and int(month) >= 6:
            # Someone turned off gzipping
            in_data = in_data.replace("/vsigzip/", "").replace(".gz", "")
    else:
        # Set up a monthly process
        in_file = f"chirps-v2.0.{year}.{month}.tif.gz"
        in_href = MONTHLY_URL_TEMPLATE.format(in_file=in_file)
        in_data = f"/vsigzip//vsicurl/{in_href}"

        out_data = f"{s3_dst}/chirps-v2.0_{year}.{month}.tif"
        out_stac = f"{s3_dst}/chirps-v2.0_{year}.{month}.stac-item.json"

        _, end = calendar.monthrange(int(year), int(month))
        start_datetime = f"{year}-{month}-01T00:00:00Z"
        end_datetime = f"{year}-{month}-{end}T23:59:59Z"
        product_name = "rainfall_chirps_monthly"

        # Set to 15 for the STAC metadata
        day = 15

    try:
        # Check if file already exists
        log.info(f"Working on {in_file}")
        if not overwrite and s3_head_object(out_stac) is not None:
            log.warning(f"File {out_stac} already exists. Skipping.")
            return

        # COG and STAC
        with MemoryFile() as mem_dst:
            # Creating the COG, with a memory cache and no download. Shiny.
            cog_translate(
                in_data,
                mem_dst.name,
                cog_profiles.get("deflate"),
                in_memory=True,
                nodata=-9999,
            )
            # Creating the STAC document with appropriate date range
            _, end = calendar.monthrange(int(year), int(month))
            item = create_stac_item(
                mem_dst,
                id=str(odc_uuid("chirps", "2.0", [in_file])),
                with_proj=True,
                input_datetime=datetime(int(year), int(month), int(day)),
                properties={
                    "odc:processing_datetime": datetime_to_str(datetime.now()),
                    "odc:product": product_name,
                    "start_datetime": start_datetime,
                    "end_datetime": end_datetime,
                },
            )
            item.set_self_href(out_stac)
            # Manually redo the asset
            del item.assets["asset"]
            item.assets["rainfall"] = pystac.Asset(
                href=out_data,
                title="CHIRPS-v2.0",
                media_type=pystac.MediaType.COG,
                roles=["data"],
            )
            # Let's add a link to the source
            item.add_links(
                [
                    pystac.Link(
                        target=in_href,
                        title="Source file",
                        rel=pystac.RelType.DERIVED_FROM,
                        media_type="application/gzip",
                    )
                ]
            )

            # Dump the data to S3
            mem_dst.seek(0)
            log.info(f"Writing DATA to: {out_data}")
            s3_dump(mem_dst, out_data, ACL="bucket-owner-full-control")
            # Write STAC to S3
            log.info(f"Writing STAC to: {out_stac}")
            s3_dump(
                json.dumps(item.to_dict(), indent=2),
                out_stac,
                ContentType="application/json",
                ACL="bucket-owner-full-control",
            )
            # All done!
            log.info(f"Completed work on {in_file}")

    except Exception as e:
        message = f"Failed to handle {in_file} with error {e}"

        if slack_url is not None:
            send_slack_notification(slack_url, "Chirps Rainfall Monthly", message)
        log.exception(message)

        exit(1)


@click.command("download-chirps-daily")
@click.option("--year", default="2020")
@click.option("--month", default="01")
@click.option("--day", default="01")
@click.option("--s3_dst", default="s3://deafrica-data-dev-af/rainfall_chirps_monthy/")
@click.option("--overwrite", is_flag=True, default=False)
@slack_url
def cli_daily(year, month, day, s3_dst, overwrite, slack_url):
    """
    Download CHIRPS Africa daily tifs, COG, copy to
    S3 bucket.

    GeoTIFFs are copied from here:
        https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_monthly/tifs/p05/

    Example:
    download-chirps-daily
        --s3_dst s3://deafrica-data-dev-af/rainfall_chirps_monthy/
        --year 1983
        --month 01
        --day 01

    Available years are 1981-2021.
    """

    download_and_cog_chirps(
        year=year,
        month=month,
        day=day,
        s3_dst=s3_dst,
        overwrite=overwrite,
        slack_url=slack_url,
    )


@click.command("download-chirps")
@click.option("--year", default="2020")
@click.option("--month", default="01")
@click.option("--s3_dst", default="s3://deafrica-data-dev-af/rainfall_chirps_monthy/")
@click.option("--overwrite", is_flag=True, default=False)
@slack_url
def cli_monthly(year, month, s3_dst, overwrite, slack_url):
    """
    Download CHIRPS Africa monthly tifs, COG, copy to
    S3 bucket.

    GeoTIFFs are copied from here:
        https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_monthly/tifs/

    Run with:
        for m in {01..12};
        do download-chirps
        --s3_dst s3://deafrica-data-dev-af/rainfall_chirps_monthy/
        --year 1983
        --month $m;
        done

    to download a whole year.

    Available years are 1981-2021.
    """

    download_and_cog_chirps(
        year=year,
        month=month,
        s3_dst=s3_dst,
        overwrite=overwrite,
        slack_url=slack_url,
    )
