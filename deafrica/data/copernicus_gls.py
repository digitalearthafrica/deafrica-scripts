import json
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory

import click
import pystac
import requests
from deafrica.utils import setup_logging, AFRICA_BBOX
from odc.aws import s3_dump, s3_head_object
from deafrica.utils import odc_uuid
from osgeo import gdal
from rasterio import MemoryFile
from rio_cogeo import cog_profiles, cog_translate
from rio_stac import create_stac_item
from urlpath import URL

# 2015
# https://zenodo.org/record/3939038/files/PROBAV_LC100_global_v3.0.1_2015-base_Bare-CoverFraction-layer_EPSG-4326.tif

# 2016-2018
# https://zenodo.org/record/3518036/files/PROBAV_LC100_global_v3.0.1_2017-conso_Bare-CoverFraction-layer_EPSG-4326.tif

# 2019
# https://zenodo.org/record/3939050/files/PROBAV_LC100_global_v3.0.1_2019-nrt_Bare-CoverFraction-layer_EPSG-4326.tif

BASE_URL = "https://zenodo.org/record/{record_id}/files/PROBAV_LC100_global_v3.0.1_{year_key}_{file}_EPSG-4326.tif"

YEARS = {
    "2015": ["2015-base", "3939038"],
    "2016": ["2016-conso", "3518026"],
    "2017": ["2017-conso", "3518036"],
    "2018": ["2018-conso", "3518038"],
    "2019": ["2019-nrt", "3939050"],
}

FILES = {
    "bare_cover_fraction": "Bare-CoverFraction-layer",
    "crops_cover_fraction": "Crops-CoverFraction-layer",
    "builtup_cover_fraction": "BuiltUp-CoverFraction-layer",
    "grass_cover_fraction": "Grass-CoverFraction-layer",
    "mosslichen_cover_fraction": "MossLichen-CoverFraction-layer",
    "permanentwater_cover_fraction": "PermanentWater-CoverFraction-layer",
    "seasonalwater_cover_fraction": "SeasonalWater-CoverFraction-layer",
    "shrub_cover_fraction": "Shrub-CoverFraction-layer",
    "snow_cover_fraction": "Snow-CoverFraction-layer",
    "tree_cover_fraction": "Tree-CoverFraction-layer",
    # Change confidence is only available for some years... can't include it.
    # "change_confidence": "Change-Confidence-layer",
    "data_density": "DataDensityIndicator",
    "classification": "Discrete-Classification-map",
    "classification_probability": "Discrete-Classification-proba",
    "forest_type": "Forest-Type-layer",
}

DO_NEAREST = set(["classification", "forest_type"])

PRODUCT_NAME = "cgls_landcover"


def download_file(url, file_name):
    with requests.get(url, stream=True, allow_redirects=True) as r:
        with open(file_name, "wb") as f:
            shutil.copyfileobj(r.raw, f)


def translate_file_deafrica_extent(file_name: Path):
    # Use Rasterio to do a translate on the file so it's limited to Africa
    small_file = file_name.with_suffix(".small.tif")
    ds = gdal.Open(str(file_name))
    # [ulx, uly, lrx, lry]
    ds = gdal.Translate(str(small_file), ds, projWin=AFRICA_BBOX)
    ds = None
    return small_file


def download_gls(year: str, s3_dst: str, workdir: Path, overwrite: bool = False):
    log = setup_logging()
    assets = {}
    out_stac = URL(s3_dst) / year / f"{PRODUCT_NAME}_{year}.stac-item.json"

    if s3_head_object(str(out_stac)) is not None and not overwrite:
        log.info(f"{out_stac} exists, skipping")
        return

    # Download the files
    for name, file in FILES.items():
        # Create a temporary directory to work with
        with TemporaryDirectory(prefix=workdir) as tmpdir:
            log.info(f"Working on {file}")
            url = URL(
                BASE_URL.format(
                    record_id=YEARS[year][1], year_key=YEARS[year][0], file=file
                )
            )

            dest_url = URL(s3_dst) / year / f"{PRODUCT_NAME}_{year}_{name}.tif"

            if s3_head_object(str(dest_url)) is None or overwrite:
                log.info(f"Downloading {url}")

                try:
                    local_file = Path(tmpdir) / str(url.name)
                    # Download the file
                    download_file(url, local_file)

                    log.info(f"Downloaded file to {local_file}")
                    local_file_small = translate_file_deafrica_extent(local_file)
                    log.info(f"Clipped Africa out and saved to {local_file_small}")
                    resampling = "nearest" if name in DO_NEAREST else "bilinear"

                    # Create a COG in memory and upload to S3
                    with MemoryFile() as mem_dst:
                        # Creating the COG, with a memory cache and no download. Shiny.
                        cog_translate(
                            local_file_small,
                            mem_dst.name,
                            cog_profiles.get("deflate"),
                            in_memory=True,
                            nodata=255,
                            overview_resampling=resampling,
                        )
                        mem_dst.seek(0)
                        s3_dump(mem_dst, str(dest_url), ACL="bucket-owner-full-control")
                        log.info(f"File written to {dest_url}")
                except Exception:
                    log.exception(f"Failed to process {url}")
                    exit(1)
            else:
                log.info(f"{dest_url} exists, skipping")

            assets[name] = pystac.Asset(
                href=str(dest_url), roles=["data"], media_type=pystac.MediaType.COG
            )

    # Write STAC document from the last-written file
    source_doc = f"https://zenodo.org/record/{YEARS[year][1]}"
    item = create_stac_item(
        str(dest_url),
        id=str(odc_uuid("Copernicus Global Land Cover", "3.0.1", [source_doc])),
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


@click.command("download-gls")
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
    """ """

    download_gls(year=year, s3_dst=s3_dst, overwrite=overwrite, workdir=workdir)


if __name__ == "__main__":
    cli()
