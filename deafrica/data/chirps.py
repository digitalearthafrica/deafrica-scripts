import calendar
import json
import os
import sys
import tempfile
import time
from datetime import datetime
from typing import Optional, Tuple

import click
import pystac
import requests
from odc.apps.dc_tools._docs import odc_uuid
from odc.aws import s3_dump, s3_head_object
from pystac.utils import datetime_to_str
from rasterio.io import MemoryFile
from rio_cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from rio_stac import create_stac_item

from deafrica.click_options import slack_url
from deafrica.logs import setup_logging
from deafrica.utils import send_slack_notification

MONTHLY_URL_TEMPLATE = (
    "https://data.chc.ucsb.edu/products/CHIRPS/v2.0/africa_monthly/tifs/{in_file}"
)
DAILY_URL_TEMPLATE = "https://data.chc.ucsb.edu/products/CHIRPS/v2.0/africa_daily/tifs/p05/{year}/{in_file}"
CHIRPS_USER_AGENT = "Mozilla/5.0"
# One GET per attempt. These pauses sit between retries of that same GET.
DOWNLOAD_RETRY_PAUSES_SECONDS = (15, 30, 60)

# Set log level to info
log = setup_logging()

log.info("Starting CHIRPS downloader")


def check_values(
    year: str, month: str, day: Optional[str]
) -> Tuple[str, str, Optional[str]]:
    # Assert that the year should be 4 characters long
    assert len(year) == 4, "Year should be 4 characters long"

    # Ensure the month is zero padded
    if len(month) == 1:
        month = f"0{month}"

    # As is the day, if it is provided
    if day is not None and len(day) == 1:
        day = f"0{day}"

    return year, month, day


def source_dataset_path(path: str) -> str:
    """GDAL path for a local CHIRPS file. Gzip is inflated locally."""
    if path.endswith(".gz"):
        return f"/vsigzip/{path}"
    return path


def download_chirps_source(href: str) -> Optional[str]:
    """Download one CHIRPS file with a single GET per attempt.

    Returns a local temp path, or None when the server replies 404.
    Retries a 429 or 5xx response. Does not send Range requests.
    """
    headers = {"User-Agent": CHIRPS_USER_AGENT}
    attempts = len(DOWNLOAD_RETRY_PAUSES_SECONDS) + 1
    status_code = None

    for attempt in range(attempts):
        if attempt:
            pause = DOWNLOAD_RETRY_PAUSES_SECONDS[attempt - 1]
            log.warning(f"{href} returned {status_code}, retrying in {pause}s")
            time.sleep(pause)

        try:
            response = requests.get(
                href,
                headers=headers,
                timeout=120,
                stream=True,
                allow_redirects=True,
            )
        except requests.exceptions.RequestException as exc:
            log.error(f"Failed to download {href}: {exc}")
            if attempt == attempts - 1:
                sys.exit(1)
            status_code = "error"
            continue

        status_code = response.status_code
        if status_code == 404:
            response.close()
            log.info(f"Download {href} -> 404")
            return None
        if status_code == 429 or status_code >= 500:
            response.close()
            log.info(f"Download {href} -> {status_code}")
            if attempt == attempts - 1:
                log.error(f"{href} returned {status_code} after retries")
                sys.exit(1)
            continue

        local_path = None
        try:
            response.raise_for_status()
            suffix = ".tif.gz" if href.endswith(".tif.gz") else ".tif"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as handle:
                local_path = handle.name
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)
        except Exception:
            if local_path is not None and os.path.exists(local_path):
                os.remove(local_path)
            raise
        finally:
            response.close()

        log.info(f"Download {href} -> {status_code}")
        return local_path

    log.error(f"Failed to download {href}")
    sys.exit(1)


def download_and_cog_chirps(
    year: str,
    month: str,
    s3_dst: str,
    day: str = None,
    overwrite: bool = False,
    slack_url: str = None,
    test_local_file: str = None,  # For offline testing
):
    # Cleaning and sanity checks
    s3_dst = s3_dst.rstrip("/")

    # Set up file strings. The remote file is chosen later with one GET.
    is_daily = day is not None
    if is_daily:
        # Set up a daily process
        in_file = f"chirps-v2.0.{year}.{month}.{day}.tif.gz"
        plain_file = f"chirps-v2.0.{year}.{month}.{day}.tif"
        in_href = DAILY_URL_TEMPLATE.format(year=year, in_file=in_file)

        file_base = f"{s3_dst}/{year}/{month}/chirps-v2.0_{year}.{month}.{day}"
        out_data = f"{file_base}.tif"
        out_stac = f"{file_base}.stac-item.json"

        start_datetime = f"{year}-{month}-{day}T00:00:00Z"
        end_datetime = f"{year}-{month}-{day}T23:59:59Z"
        product_name = "rainfall_chirps_daily"
    else:
        # Set up a monthly process
        in_file = f"chirps-v2.0.{year}.{month}.tif.gz"
        plain_file = f"chirps-v2.0.{year}.{month}.tif"
        in_href = MONTHLY_URL_TEMPLATE.format(in_file=in_file)

        file_base = f"{s3_dst}/chirps-v2.0_{year}.{month}"
        out_data = f"{file_base}.tif"
        out_stac = f"{file_base}.stac-item.json"

        _, end = calendar.monthrange(int(year), int(month))
        start_datetime = f"{year}-{month}-01T00:00:00Z"
        end_datetime = f"{year}-{month}-{end}T23:59:59Z"
        product_name = "rainfall_chirps_monthly"

        # Set to 15 for the STAC metadata
        day = 15

    downloaded_path = None
    try:
        if test_local_file is not None:
            in_data = source_dataset_path(test_local_file)
            log.info(f"TEST MODE: Using local test file {test_local_file}")
        else:
            if not overwrite and s3_head_object(out_stac) is not None:
                log.info(f"Working on {in_file}")
                log.warning(f"File {out_stac} already exists. Skipping.")
                return

            downloaded_path = download_chirps_source(in_href)
            if downloaded_path is None:
                log.warning("Couldn't find the gzipped file, trying the .tif")
                in_file = plain_file
                if is_daily:
                    in_href = DAILY_URL_TEMPLATE.format(year=year, in_file=in_file)
                else:
                    in_href = MONTHLY_URL_TEMPLATE.format(in_file=in_file)
                downloaded_path = download_chirps_source(in_href)
                if downloaded_path is None:
                    log.error("Couldn't find the .tif file either, aborting")
                    sys.exit(1)
            in_data = source_dataset_path(downloaded_path)

        log.info(f"Working on {in_file}")

        # COG and STAC
        with MemoryFile() as mem_dst:
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
    finally:
        if downloaded_path is not None and os.path.exists(downloaded_path):
            os.remove(downloaded_path)


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
        https://data.chc.ucsb.edu/products/CHIRPS/v2.0/africa_daily/tifs/p05/

    Example:
    download-chirps-daily
        --s3_dst s3://deafrica-data-dev-af/rainfall_chirps_monthy/
        --year 1983
        --month 01
        --day 01

    Available years are 1981-2021.
    """

    year, month, day = check_values(year, month, day)

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
        https://data.chc.ucsb.edu/products/CHIRPS/v2.0/africa_monthly/tifs/

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

    year, month, _ = check_values(year, month)

    download_and_cog_chirps(
        year=year,
        month=month,
        s3_dst=s3_dst,
        overwrite=overwrite,
        slack_url=slack_url,
    )
