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

DAILY_URL_TEMPLATE = (
    "https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_daily/tifs/p05/{year}/{in_file}"
)

# Set log level to info
log = setup_logging()

log.info("Starting CHIRPS downloader")


def download_and_cog_chirps(
    year: str,
    month: str,
    s3_monthly_dst: str,
    s3_daily_dst: str,
    overwrite: bool = False,
    slack_url: str = None,
    daily: bool = False,
):
    # Set up file strings for monthly data
    in_monthly_file = f"chirps-v2.0.{year}.{month}.tif.gz"
    in_monthly_href = MONTHLY_URL_TEMPLATE.format(in_file=in_monthly_file)
    in_monthly_data = f"/vsigzip//vsicurl/{in_monthly_href}"

    s3_monthly_dst = s3_monthly_dst.rstrip("/")
    out_monthly_data = f"{s3_monthly_dst}/chirps-v2.0_{year}.{month}.tif"
    out_monthly_stac = f"{s3_monthly_dst}/chirps-v2.0_{year}.{month}.stac-item.json"

    # Add monthly file to list to operate on
    # List will be extended with daily files if daily flag used
    in_file_list = [in_monthly_file]
    in_href_list = [in_monthly_href]
    in_data_list = [in_monthly_data]
    out_data_list = [out_monthly_data]
    out_stac_list = [out_monthly_stac]

    # Set up file strings for daily data
    if daily:
        _, n_days = calendar.monthrange(int(year), int(month))
        in_daily_file = [f"chirps-v2.0.{year}.{month}.{day:02d}.tif.gz" for day in range(1, n_days+1)]
        in_daily_href = [DAILY_URL_TEMPLATE.format(year=year, in_file=in_file) for in_file in in_daily_file]
        in_daily_data = [f"/vsigzip//vsicurl/{in_href}" for in_href in in_daily_href]

        s3_daily_dst = s3_daily_dst.rstrip("/")
        out_daily_data = [f"{s3_daily_dst}/{year}/chirps-v2.0_{year}.{month}.{day:02d}.tif" for day in range(1, n_days+1)]
        out_daily_stac = [f"{s3_daily_dst}/{year}/chirps-v2.0_{year}.{month}.{day:02d}.stac-item.json" for day in range(1, n_days+1)]

        in_file_list.extend(in_daily_file)
        in_data_list.extend(in_daily_data)
        in_href_list.extend(in_daily_href)
        out_data_list.extend(out_daily_data)
        out_stac_list.extend(out_daily_stac)

    for i, in_file in enumerate(in_file_list):

        try:
            # Check if file already exists
            log.info(f"Working on {in_file}")
            if not overwrite and s3_head_object(out_stac_list[i]):
                log.warning(f"File {out_stac_list[i]} already exists. Skipping.")
                return

            # COG and STAC
            with MemoryFile() as mem_dst:
                # Creating the COG, with a memory cache and no download. Shiny.
                cog_translate(
                    in_data_list[i],
                    mem_dst.name,
                    cog_profiles.get("deflate"),
                    in_memory=True,
                    nodata=-9999,
                )
                # Creating the STAC document with appropriate date range
                # Use different logic if monthly or daily. Monthly will always be the first file in the list.
                if i == 0:
                    # MONTHLY
                    _, end = calendar.monthrange(int(year), int(month))
                    item = create_stac_item(
                        mem_dst,
                        id=str(odc_uuid("chirps", "2.0", [in_file])),
                        with_proj=True,
                        input_datetime=datetime(int(year), int(month), 15),
                        properties={
                            "odc:processing_datetime": datetime_to_str(datetime.now()),
                            "odc:product": "rainfall_chirps_monthly",
                            "start_datetime": f"{year}-{month}-01T00:00:00Z",
                            "end_datetime": f"{year}-{month}-{end}T23:59:59Z",
                        },
                    )
                else:
                    # DAILY
                    day = i
                    item = create_stac_item(
                        mem_dst,
                        id=str(odc_uuid("chirps", "2.0", [in_file])),
                        with_proj=True,
                        input_datetime=datetime(int(year), int(month), day),
                        properties={
                            "odc:processing_datetime": datetime_to_str(datetime.now()),
                            "odc:product": "rainfall_chirps_daily",
                            "start_datetime": f"{year}-{month}-{day:02d}T00:00:00Z",
                            "end_datetime": f"{year}-{month}-{day:02d}T23:59:59Z",
                        },
                    )
                item.set_self_href(out_stac_list[i])
                # Manually redo the asset
                del item.assets["asset"]
                item.assets["rainfall"] = pystac.Asset(
                    href=out_data_list[i],
                    title="CHIRPS-v2.0",
                    media_type=pystac.MediaType.COG,
                    roles=["data"],
                )
                # Let's add a link to the source
                item.add_links(
                    [
                        pystac.Link(
                            target=in_href_list[i],
                            title="Source file",
                            rel=pystac.RelType.DERIVED_FROM,
                            media_type="application/gzip",
                        )
                    ]
                )

                # Dump the data to S3
                mem_dst.seek(0)
                s3_dump(mem_dst, in_href_list, ACL="bucket-owner-full-control")
                log.info(f"File written to {in_href_list}")
                # Write STAC to S3
                s3_dump(
                    json.dumps(item.to_dict(), indent=2),
                    out_stac_list[i],
                    ContentType="application/json",
                    ACL="bucket-owner-full-control",
                )
                log.info(f"STAC written to {out_stac_list[i]}")
                # All done!
                log.info(f"Completed work on {in_file}")

        except Exception as e:
            message = f"Failed to handle {in_file} with error {e}"

            if slack_url is not None:
                send_slack_notification(slack_url, "Chirps Rainfall Monthly", message)
            log.exception(message)

            exit(1)


@click.command("download-chirps")
@click.option("--year", default="2020")
@click.option("--month", default="01")
@click.option("--daily", is_flag=True, default=False)
@click.option("--s3_monthly_dst", default="s3://deafrica-data-dev-af/rainfall_chirps_monthy/")
@click.option("--s3_daily_dst", default="s3://deafrica-data-dev-af/rainfall_chirps_daily/")
@click.option("--overwrite", is_flag=True, default=False)
@slack_url
def cli(year, month, daily, s3_monthly_dst, s3_daily_dst, overwrite, slack_url):
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
        s3_monthly_dst=s3_monthly_dst,
        s3_daily_dst=s3_daily_dst,
        overwrite=overwrite,
        slack_url=slack_url,
        daily=daily,
    )


# years = [str(i) for i in range(1981, 2022)]
# months = [str(i).zfill(2) for i in range(1,13)]
if __name__ == "__main__":
    cli()
