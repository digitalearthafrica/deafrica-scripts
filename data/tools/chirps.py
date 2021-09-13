import json
import logging
from datetime import datetime

import click
from odc.aws import s3_dump, s3_head_object
from rasterio.io import MemoryFile
from rio_cogeo import cog_translate
from rio_cogeo.profiles import cog_profiles
from rio_stac import create_stac_item
import pystac

from odc.index import odc_uuid


def download_and_cog_chirps(
    year: str, month: str, s3_dst: str, overwrite: bool = False
):
    # Set up file strings
    filename = f"chirps-v2.0.{year}.{month}.tif.gz"
    in_data = f"/vsigzip//vsicurl/https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_monthly/tifs/{filename}"

    out_data = f"{s3_dst}/chirps-v2.0_{year}.{month}.tif"
    out_stac = f"{s3_dst}/chirps-v2.0_{year}.{month}.stac-item.json"

    try:
        # Check if file already exists
        if not overwrite and s3_head_object(out_stac):
            logging.info(f"{out_stac} already exists. Skipping.")
            return

        # COG and STAC
        with MemoryFile() as mem_dst:
            logging.info("Creating COG in memory...")
            cog_translate(
                in_data, mem_dst.name, cog_profiles.get("deflate"), in_memory=True
            )
            logging.info("Creating STAC...")
            item = create_stac_item(
                mem_dst,
                id=str(odc_uuid("chirps", "2.0", [filename])),
                with_proj=True,
                input_datetime=datetime(int(year), int(month), 1),
            )
            item.set_self_href(out_stac)
            # Manually redo the asset
            del item.assets["asset"]
            item.assets["data"] = pystac.Asset(
                href=out_data,
                title="CHIRPS-v2.0",
                media_type=pystac.MediaType.COG,
                roles=["data"],
            )

            logging.info("Dumping files to S3")
            # Dump the data to S3
            s3_dump(mem_dst, out_data)
            # Write STAC
            s3_dump(json.dumps(item.to_dict(), indent=2), out_stac)

    except Exception as e:
        logging.exception(f"Failed to handle {filename} with error {e}")
        exit(1)


@click.command("download-chirps-rainfall")
@click.option(
    "--s3_dst", default="s3://deafrica-input-datasets/chirps_rainfall_monthly/"
)
@click.option("--overwrite", is_flag=True, default=False)
def cli(year, month, s3_dst, overwrite):
    """
    Download CHIRPS Africa monthly tifs, COG, copy to
    S3 bucket.

    geotifs are copied from here:
        https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_monthly/tifs/
    """

    download_and_cog_chirps(year=year, month=month, s3_dst=s3_dst, overwrite=overwrite)


# years = [str(i) for i in range(1981, 2022)]
# months = [str(i).zfill(2) for i in range(1,13)]
if __name__ == "__main__":
    cli()
