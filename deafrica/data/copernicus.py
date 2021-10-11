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
from urlpath import URL

from deafrica.utils import setup_logging

DICT_CODE_YEAR = {
    "2019": {"id": "3939050", "code": "nrt"},
    "2018": {"id": "3518038", "code": "conso"},
    "2017": {"id": "3518036", "code": "conso"},
    "2016": {"id": "3518026", "code": "conso"},
    "2015": {"id": "3939038", "code": "base"},
}

FILES = [
    "PROBAV_LC100_global_v3.0.1_{year}-{code}_Bare-CoverFraction-layer_EPSG-4326.tif",
    "PROBAV_LC100_global_v3.0.1_{year}-{code}_BuiltUp-CoverFraction-layer_EPSG-4326.tif",
    "PROBAV_LC100_global_v3.0.1_{year}-{code}_Change-Confidence-layer_EPSG-4326.tif",
    "PROBAV_LC100_global_v3.0.1_{year}-{code}_Crops-CoverFraction-layer_EPSG-4326.tif",
    "PROBAV_LC100_global_v3.0.1_{year}-{code}_DataDensityIndicator_EPSG-4326.tif",
    "PROBAV_LC100_global_v3.0.1_{year}-{code}_Discrete-Classification-map_EPSG-4326.tif",
    "PROBAV_LC100_global_v3.0.1_{year}-{code}_Discrete-Classification-proba_EPSG-4326.tif",
    "PROBAV_LC100_global_v3.0.1_{year}-{code}_Forest-Type-layer_EPSG-4326.tif",
    "PROBAV_LC100_global_v3.0.1_{year}-{code}_Grass-CoverFraction-layer_EPSG-4326.tif",
    "PROBAV_LC100_global_v3.0.1_{year}-{code}_MossLichen-CoverFraction-layer_EPSG-4326.tif",
    "PROBAV_LC100_global_v3.0.1_{year}-{code}_PermanentWater-CoverFraction-layer_EPSG-4326.tif",
    "PROBAV_LC100_global_v3.0.1_{year}-{code}_SeasonalWater-CoverFraction-layer_EPSG-4326.tif",
    "PROBAV_LC100_global_v3.0.1_{year}-{code}_Shrub-CoverFraction-layer_EPSG-4326.tif",
    "PROBAV_LC100_global_v3.0.1_{year}-{code}_Snow-CoverFraction-layer_EPSG-4326.tif",
    "PROBAV_LC100_global_v3.0.1_{year}-{code}_Tree-CoverFraction-layer_EPSG-4326.tif",
]

# Set log level to info
log = setup_logging()

log.info("Starting CHIRPS downloader")


def build_file_list(year: str):

    if year not in DICT_CODE_YEAR.keys():
        raise ValueError(
            f"Year {year} not supported! Please choose a year among {[k for k in DICT_CODE_YEAR.keys()]}"
        )

    codes = DICT_CODE_YEAR[year]
    for link in FILES:
        # Eg. https://zenodo.org/record/3518038/files/PROBAV_LC100_global_v3.0.1_2018-conso_Bare-CoverFraction-layer_EPSG-4326.tif
        yield URL("https://zenodo.org/record/") / codes.get(
            "id"
        ) / "files" / link.format(year=year, code=codes.get("code"))


def download_and_cog_copernicus(year: str, s3_dst: str, overwrite: bool = False):

    for file_path in build_file_list(year):

        s3_dst = URL(s3_dst)
        out_data = s3_dst / file_path.name
        out_stac = s3_dst / file_path.name.replace(
            file_path.name.split(".")[-1], "json"
        )

        try:
            # Check if file already exists
            log.info(f"Working on {file_path}")
            if not overwrite and s3_head_object(str(out_stac)):
                log.warning(f"File {out_stac} already exists. Skipping.")
                return

            # COG and STAC
            with MemoryFile() as mem_dst:
                # Creating the COG, with a memory cache and no download. Shiny.
                cog_translate(
                    file_path,
                    mem_dst.name,
                    cog_profiles.get("deflate"),
                    in_memory=True,
                    nodata=-9999,
                )
                # Creating the STAC document with appropriate date range
                item = create_stac_item(
                    mem_dst,
                    id=str(odc_uuid("copernicus", "1.0", [file_path.name])),
                    with_proj=True,
                    input_datetime=datetime(int(year), 1, 15),
                    properties={
                        "odc:processing_datetime": datetime_to_str(datetime.now()),
                        "odc:product": "rainfall_chirps_monthly",
                        "start_datetime": f"{year}-{1}-01T00:00:00Z",
                        "end_datetime": f"{year}-{12}-{31}T23:59:59Z",
                    },
                )
                item.set_self_href(out_stac)
                # Manually redo the asset
                # del item.assets["asset"]
                # item.assets["rainfall"] = pystac.Asset(
                #     href=out_data,
                #     title="CHIRPS-v2.0",
                #     media_type=pystac.MediaType.COG,
                #     roles=["data"],
                # )
                # Let's add a link to the source
                item.add_links(
                    [
                        pystac.Link(
                            target=file_path,
                            title="Source file",
                            rel=pystac.RelType.DERIVED_FROM,
                            media_type="application/gzip",
                        )
                    ]
                )

                # Dump the data to S3
                mem_dst.seek(0)
                s3_dump(mem_dst, out_data, ACL="bucket-owner-full-control")
                log.info(f"File written to {out_data}")
                # Write STAC to S3
                s3_dump(
                    json.dumps(item.to_dict(), indent=2),
                    out_stac,
                    ContentType="application/json",
                    ACL="bucket-owner-full-control",
                )
                log.info(f"STAC written to {out_stac}")
                # All done!
                log.info(f"Completed work on {file_path}")

        except Exception as e:
            log.exception(f"Failed to handle {file_path} with error {e}")
            exit(1)


@click.command("download-copernicus")
@click.option("--year", default="2020")
@click.option("--s3_dst", default="s3://deafrica-data-dev-af/copernicus-yearly/")
@click.option("--overwrite", is_flag=True, default=False)
def cli(year, s3_dst, overwrite):
    """
    Available years are 2015-2019.
    """

    download_and_cog_copernicus(year=year, s3_dst=s3_dst, overwrite=overwrite)


if __name__ == "__main__":
    years = [str(i) for i in range(2015, 2019)]

    for year in years:
        download_and_cog_copernicus(
            year=year, s3_dst="s3://deafrica-data-dev-af/copernicus-yearly/"
        )
