import cdsapi
from os import environ

import json
from pathlib import Path
from tempfile import TemporaryDirectory

import click
import pystac
from deafrica.utils import setup_logging
from odc.aws import s3_dump, s3_head_object
from odc.index import odc_uuid
from rio_stac import create_stac_item
from urlpath import URL
import xarray as xr
from datacube.utils.geometry import assign_crs
from datacube.utils.cog import write_cog

"""
Download ESA Climate Change Initiative 300m Landcover from
the Climate Data Store, convert to CLoud Optimized Geotiff,
and push to an S3 bucket

Datasource: https://cds.climate.copernicus.eu/cdsapp#!/dataset/satellite-land-cover
"""

PRODUCT_NAME = "cci_landcover"
EXTENTS = {
    "max_lat": 40.0,
    "min_lat": -35.0,
    "max_lon": 55.0,
    "min_lon": -20.0,
}

area_extents = [
    EXTENTS["max_lat"],
    EXTENTS["min_lon"],
    EXTENTS["min_lat"],
    EXTENTS["max_lon"],
]

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
        if year_num not in range(1992, 2020):
            raise ValueError("Supplied date is outside of available range")
        if 1992 <= year_num <= 2015:
            return "v2.0.7cds"
        if 2016 <= year_num <= 2020:
            return "v2.1.1"
    except ValueError as e:
        raise e


def download_cci_lc(year: str, s3_dst: str, workdir: Path, overwrite: bool = False):
    log = setup_logging()
    assets = {}
    out_stac = URL(s3_dst) / year / f"{PRODUCT_NAME}_{year}.stac-item.json"

    cci_lc_version = get_version_from_year(year)
    name = f"C3S-LC-L4-LCCS-Map-300m-P1Y-{year}-{cci_lc_version}.nc"

    if s3_head_object(str(out_stac)) is not None and not overwrite:
        log.info(f"{out_stac} exists, skipping")
        return

    # Create a temporary directory to work with
    with TemporaryDirectory(prefix=workdir) as tmpdir:
        log.info(f"Working on {year}")

        dest_url = URL(s3_dst) / year / f"{PRODUCT_NAME}_{year}_LCCS_300m.tif"

        if s3_head_object(str(dest_url)) is None or overwrite:
            log.info(f"Downloading {year}")

            try:
                local_file = Path(tmpdir) / str(name)
                # Download the file
                c = cdsapi.Client()

                # we can also retrieve the object metadata from the CDS.
                # e.g. f = c.retrieve("series",{params}) | f.location = URL to download
                c.retrieve(
                    "satellite-land-cover",
                    {
                        "format": "netcdf",
                        "variable": "lccs_class",
                        "version": [cci_lc_version],
                        "year": [year],
                        "area": area_extents,
                    },
                    local_file,
                )

                log.info(f"Downloaded file to {local_file}")

                # Process data
                ds = xr.open_dataset(local_file)
                # Subset to Africa
                # ds_small = ds.sel(lat=slice(-35.0, 40.0), lon=slice(-20.0, 55.0))
                ds_small = ds.where(
                    (ds.lat >= EXTENTS["min_lat"])
                    & (ds.lat <= EXTENTS["max_lat"])
                    & (ds.lon >= EXTENTS["min_lon"])
                    & (ds.lon <= EXTENTS["max_lon"]),
                    drop=True,
                )
                ds_small = assign_crs(ds_small, crs="epsg:4326")

                # Create cog (in memory - :mem: returns bytes object)
                mem_dst = write_cog(
                    ds_small, ":mem:", nodata=255, use_windowed_writes=True
                )
                # mem_dst = to_cog(ds_small)
                mem_dst.seek(0)

                # Write to s3
                s3_dump(mem_dst, str(dest_url), ACL="bucket-owner-full-control")
                log.info(f"File written to {dest_url}")

            except Exception:
                log.exception(f"Failed to process {name}")
                exit(1)
        else:
            log.info(f"{dest_url} exists, skipping")

        assets[name] = pystac.Asset(
            href=str(dest_url), roles=["data"], media_type=pystac.MediaType.COG
        )

    # Write STAC document from the last-written file
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


@click.command("download-cci")
@click.option("--year", default="2019")
@click.option("--s3_dst", default=f"s3://deafrica-data-dev-af/{PRODUCT_NAME}/")
@click.option("--overwrite", is_flag=True, default=False)
@click.option(
    "--workdir",
    "-w",
    default="/tmp/download",
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
