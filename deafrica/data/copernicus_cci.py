import json
import zipfile
from os import environ
from pathlib import Path
from tempfile import TemporaryDirectory

import cdsapi
import click
import pystac
import xarray as xr
from datacube.utils.cog import write_cog
from datacube.utils.geometry import assign_crs
from deafrica.utils import AFRICA_BBOX, setup_logging
from odc.aws import s3_dump, s3_head_object
from odc.index import odc_uuid
from rio_stac import create_stac_item
from urlpath import URL

"""
Download ESA Climate Change Initiative 300m Landcover from
the Climate Data Store, convert to CLoud Optimized Geotiff,
and push to an S3 bucket

Datasource: https://cds.climate.copernicus.eu/cdsapp#!/dataset/satellite-land-cover
"""

PRODUCT_NAME = "cci_landcover"

# CDSAPI_URL = "https://cds.climate.copernicus.eu/api/v2"
# CDSAPI_KEY = "user-or-organisation-key"

# values may be set in ~/.cdsapirc
if not environ.get("CDSAPI_URL"):
    # environ["CDSAPI_URL"] = CDSAPI_URL
    raise ValueError("CDSAPI_URL not set")

if not environ.get("CDSAPI_KEY"):
    # environ["CDSAPI_KEY"] = CDSAPI_KEY
    raise ValueError("CDSAPI_KEY not set")


def get_version_from_year(year: str) -> str:
    """Utility function to help assign the correct version info.
    Also helps to validate year input values

    Version 2.0.7cds provides the LC maps for the years 1992-2015
    Version 2.1.1 for the years 2016-2019.
    Both versions are produced with the same processing chain.
    """
    try:
        year_num = int(year)
        # Only allow from 1992 to 2020, inclusive
        if year_num not in range(1992, 2021):
            raise ValueError("Supplied date is outside of available range")
        if 1992 <= year_num <= 2015:
            return "v2.0.7cds"
        if 2016 <= year_num <= 2020:
            return "v2.1.1"
    except ValueError as e:
        raise e


def download_cci_lc(year: str, s3_dst: str, workdir: str, overwrite: bool = False):
    log = setup_logging()
    assets = {}
    out_stac = URL(s3_dst) / year / f"{PRODUCT_NAME}_{year}.stac-item.json"

    cci_lc_version = get_version_from_year(year)
    name = f"C3S-LC-L4-LCCS-Map-300m-P1Y-{year}-{cci_lc_version}.zip"

    workdir = Path(workdir)
    if not workdir.exists():
        workdir.mkdir(parents=True, exist_ok=True)

    if s3_head_object(str(out_stac)) is not None and not overwrite:
        log.info(f"{out_stac} exists, skipping")
        return

    # Create a temporary directory to work with
    with TemporaryDirectory(prefix=str(workdir)) as tmpdir:
        log.info(f"Working on {year}")

        dest_url = URL(s3_dst) / year / f"{PRODUCT_NAME}_{year}_LCCS_300m.tif"

        if s3_head_object(str(dest_url)) is None or overwrite:
            log.info(f"Downloading {year}")
            try:
                local_file = Path(tmpdir) / str(name)
                if not local_file.exists():
                    # Download the file
                    c = cdsapi.Client()

                    # We could also retrieve the object metadata from the CDS.
                    # e.g. f = c.retrieve("series",{params}) | f.location = URL to download
                    c.retrieve(
                        "satellite-land-cover",
                        {
                            "format": "zip",
                            "variable": "all",
                            "version": cci_lc_version,
                            "year": str(year),
                        },
                        local_file,
                    )

                    log.info(f"Downloaded file to {local_file}")
                else:
                    log.info(
                        f"File {local_file} exists, continuing without downloading"
                    )

                # Unzip the file
                log.info(f"Unzipping {local_file}")
                with zipfile.ZipFile(local_file, "r") as zip_ref:
                    zip_ref.extractall(workdir)
                unzipped = local_file.with_suffix(".nc")

                # Process data
                ds = xr.open_dataset(unzipped)
                # Subset to Africa
                ulx, uly, lrx, lry = AFRICA_BBOX
                # Note: lats are upside down!
                ds_small = ds.sel(lat=slice(uly, lry), lon=slice(ulx, lrx))
                ds_small = assign_crs(ds_small, crs="epsg:4326")

                # Create cog (in memory - :mem: returns bytes object)
                mem_dst = write_cog(
                    ds_small.lccs_class,
                    ":mem:",
                    nodata=0,
                    overview_resampling="nearest",
                )

                # Write to s3
                s3_dump(mem_dst, str(dest_url), ACL="bucket-owner-full-control")
                log.info(f"File written to {dest_url}")

            except Exception:
                log.exception(f"Failed to process {name}")
                exit(1)
        else:
            log.info(f"{dest_url} exists, skipping")

        assets["classification"] = pystac.Asset(
            href=str(dest_url), roles=["data"], media_type=pystac.MediaType.COG
        )

    # Write STAC document
    source_doc = (
        "https://cds.climate.copernicus.eu/cdsapp#!/dataset/satellite-land-cover"
    )
    item = create_stac_item(
        str(dest_url),
        id=str(odc_uuid("Copernicus Land Cover", cci_lc_version, [source_doc])),
        assets=assets,
        with_proj=True,
        properties={
            "odc:product": PRODUCT_NAME,
            "start_datetime": f"{year}-01-01T00:00:00Z",
            "end_datetime": f"{year}-12-31T23:59:59Z",
        },
    )
    item.add_links(
        [
            pystac.Link(
                target=source_doc,
                title="Source",
                rel=pystac.RelType.DERIVED_FROM,
                media_type="text/html",
            )
        ]
    )
    s3_dump(
        json.dumps(item.to_dict(), indent=2),
        str(out_stac),
        ContentType="application/json",
        ACL="bucket-owner-full-control",
    )
    log.info(f"STAC written to {out_stac}")


@click.command("download-cop-cci")
@click.option("--year", default="2019")
@click.option("--s3_dst", default=f"s3://deafrica-data-dev-af/{PRODUCT_NAME}/")
@click.option("--overwrite", is_flag=True, default=False)
@click.option(
    "--workdir",
    "-w",
    default="/tmp/download/",
    help="The directory to download files to",
)
def cli(year, s3_dst, overwrite, workdir):
    """Process CII Landcover data

    Args:
        year: valid year in YYYY format. default 2019
        s3_dst: destination bucket url
        overwrite: set to true to skip existing outputs in s3
    """

    download_cci_lc(year=year, s3_dst=s3_dst, overwrite=overwrite, workdir=workdir)


if __name__ == "__main__":
    cli()
