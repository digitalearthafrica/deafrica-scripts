import json
import logging
from datetime import datetime

import click
import pystac
from odc.aws import s3_dump, s3_head_object
from odc.index import odc_uuid
from rasterio.io import MemoryFile
from rio_cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from rio_stac import create_stac_item

from monitoring.tools.utils import setup_logging

# Set log level to info
log = setup_logging()

log.info("Starting CHIRPS downloader")


def download_and_cog_chirps(
    year: str, month: str, s3_dst: str, overwrite: bool = False
):
    # Set up file strings
    filename = f"chirps-v2.0.{year}.{month}.tif.gz"
    in_data = f"/vsigzip//vsicurl/https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_monthly/tifs/{filename}"

    s3_dst = s3_dst.rstrip("/")
    out_data = f"{s3_dst}/chirps-v2.0_{year}.{month}.tif"
    out_stac = f"{s3_dst}/chirps-v2.0_{year}.{month}.stac-item.json"

    try:
        # Check if file already exists
        if not overwrite and s3_head_object(out_stac):
            log.warning(f"File {out_stac} already exists. Skipping.")
            return

        # COG and STAC
        with MemoryFile() as mem_dst:
            log.info("Creating COG in memory...")
            cog_translate(
                in_data,
                mem_dst.name,
                cog_profiles.get("deflate"),
                in_memory=True,
                nodata=-9999,
            )
            log.info("Creating STAC...")
            item = create_stac_item(
                mem_dst,
                id=str(odc_uuid("chirps", "2.0", [filename])),
                with_proj=True,
                input_datetime=datetime(int(year), int(month), 1),
                properties={"odc:product": "rainfall_chirps"},
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

            log.info("Dumping files to S3")
            # Dump the data to S3
            mem_dst.seek(0)
            s3_dump(mem_dst, out_data, ACL="bucket-owner-full-control")
            # Write STAC
            s3_dump(
                json.dumps(item.to_dict(), indent=2),
                out_stac,
                ContentType="application/json",
                ACL="bucket-owner-full-control",
            )
            log.info(f"Successfully completed {filename}")

    except Exception as e:
        log.exception(f"Failed to handle {filename} with error {e}")
        exit(1)


@click.command("download-chirps")
@click.option("--year", default="2020")
@click.option("--month", default="01")
@click.option("--s3_dst", default="s3://deafrica-data-dev-af/chirps_rainfall/")
@click.option("--overwrite", is_flag=True, default=False)
def cli(year, month, s3_dst, overwrite):
    """
    Download CHIRPS Africa monthly tifs, COG, copy to
    S3 bucket.

    GeoTIFFs are copied from here:
        https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_monthly/tifs/

    Run with:
        for m in {01..12};
        do download-chirps
        --s3_dst s3://deafrica-data-dev-af/chirps_rainfall/
        --year 1983
        --month $m;
        done

    to download a whole year.

    Available years are 1981-2021.
    """

    download_and_cog_chirps(year=year, month=month, s3_dst=s3_dst, overwrite=overwrite)


# years = [str(i) for i in range(1981, 2022)]
# months = [str(i).zfill(2) for i in range(1,13)]
if __name__ == "__main__":
    cli()
