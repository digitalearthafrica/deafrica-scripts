"""
Download the ESA WorldCereal 10 m 2021 v100 products from Zenodo,
convert to Cloud Optimized Geotiff, and push to an S3 bucket.

Datasource: https://zenodo.org/records/7875105
"""

import os
import posixpath
from subprocess import STDOUT, check_output
from zipfile import ZipFile

import click
import geopandas as gpd
from odc.aws import s3_dump

from deafrica.data.esa_worldcereal.constants import (
    LOCAL_DOWNLOAD_DIR,
    NO_CONFIDENCE_PRODUCTS,
    VALID_PRODUCTS,
    VALID_SEASONS,
    VALID_YEAR,
    WORLDCEREAL_AEZ_URL,
)
from deafrica.data.esa_worldcereal.geotiff import parse_geotiff_url
from deafrica.io import (
    check_directory_exists,
    check_file_exists,
    download_file_from_url,
    get_filesystem,
    is_s3_path,
    join_url,
)
from deafrica.logs import setup_logging
from deafrica.utils import AFRICA_EXTENT_URL

log = setup_logging()


def get_africa_aez_ids():
    """
    Get the Agro-ecological zone (AEZ) ids for the zones in Africa.

    Returns:
        set[str]: Agro-ecological zone (AEZ) ids for the zones in Africa
    """
    # Get the AEZ ids for Africa
    africa_extent = gpd.read_file(AFRICA_EXTENT_URL).to_crs("EPSG:4326")

    worldcereal_aez = gpd.read_file(WORLDCEREAL_AEZ_URL).to_crs("EPSG:4326")

    africa_worldcereal_aez_ids = worldcereal_aez.sjoin(
        africa_extent, predicate="intersects", how="inner"
    )["aez_id"].to_list()

    to_remove = [17135, 17166, 34119, 40129, 46171, 43134, 43170]

    africa_worldcereal_aez_ids = [
        str(i) for i in africa_worldcereal_aez_ids if i not in to_remove
    ]
    africa_worldcereal_aez_ids = set(africa_worldcereal_aez_ids)

    return africa_worldcereal_aez_ids


def download_and_unzip_data(zip_url: str):
    """
    Download and extract the selected World Cereal product GeoTIFFs.

    Args:
        zip_url (str): URL for the World Cereal product zip file to download.
    """
    if not check_directory_exists(LOCAL_DOWNLOAD_DIR):
        fs = get_filesystem(LOCAL_DOWNLOAD_DIR, anon=False)
        fs.makedirs(LOCAL_DOWNLOAD_DIR, exist_ok=True)

    zip_filename = posixpath.basename(zip_url).split(".zip")[0] + ".zip"
    local_zip_path = os.path.join(LOCAL_DOWNLOAD_DIR, zip_filename)

    if check_file_exists(local_zip_path):
        log.info(f"{local_zip_path} already exists! Skipping download ...")
    else:
        log.info(f"Downloading {zip_url} to {local_zip_path} ...")
        local_zip_path = download_file_from_url(
            url=zip_url, output_file_path=local_zip_path, chunks=100
        )
        log.info("Download complete!")

    africa_aez_ids = get_africa_aez_ids()

    log.info(f"Extracting the AEZ-based GeoTIFF files from {local_zip_path} ...")
    with ZipFile(local_zip_path) as zip_ref:
        # All files in zip
        all_aez_geotiffs = [
            file for file in zip_ref.namelist() if file.endswith(".tif")
        ]
        # Filter to Africa extent
        africa_aez_geotiffs = [
            file
            for file in all_aez_geotiffs
            if os.path.basename(file).split("_")[0] in africa_aez_ids
        ]
        # Extract
        local_aez_geotiffs = []
        for file in africa_aez_geotiffs:
            local_file_path = os.path.join(LOCAL_DOWNLOAD_DIR, file)

            # Check if the file already exists
            if os.path.exists(local_file_path):
                local_aez_geotiffs.append(local_file_path)
                continue

            # Extract file
            zip_ref.extract(member=file, path=LOCAL_DOWNLOAD_DIR)
            local_aez_geotiffs.append(local_file_path)
    log.info(f"Downloaded {len(local_aez_geotiffs)} AEZ-based GeoTIFF files")

    # Clean up
    if check_file_exists(local_zip_path):
        os.remove(local_zip_path)

    return local_aez_geotiffs


def create_and_upload_cog(geotiff_path: str, cog_output_path: str):
    """
    Create COG from GeoTIFF.
    """
    if is_s3_path(cog_output_path):
        # Temporary directory to store the cogs before uploading to s3
        local_cog_dir = os.path.join(LOCAL_DOWNLOAD_DIR, "cogs")
        if not check_directory_exists(local_cog_dir):
            fs = get_filesystem(local_cog_dir, anon=False)
            fs.makedirs(local_cog_dir, exist_ok=True)

        # Create a COG and save to local disk
        local_cog_file = os.path.join(
            local_cog_dir, posixpath.basename(cog_output_path)
        )
        cmd = f"rio cogeo create --overview-resampling nearest {geotiff_path} {local_cog_file}"
        check_output(cmd, stderr=STDOUT, shell=True)
        log.info(f"{geotiff_path} converted to COG {local_cog_file} successfully")

        # Upload COG to s3
        log.info(f"Upload {local_cog_file} to S3 {cog_output_path}")
        s3_dump(
            data=open(local_cog_file, "rb").read(),
            url=cog_output_path,
            ACL="bucket-owner-full-control",
            ContentType="image/tiff",
        )
        log.info("Upload to s3 complete!")
    else:
        cmd = f"rio cogeo create --overview-resampling nearest {geotiff_path} {cog_output_path}"
        check_output(cmd, stderr=STDOUT, shell=True)
        log.info(f"{geotiff_path} converted to COG {cog_output_path} successfully")


@click.command(
    "download-esa-worldcereal-cogs",
    help="Download ESA WorldCereal product cogs for AEZ regions within Africa's bounding box.",
    no_args_is_help=True,
)
@click.option(
    "--season", required=True, type=click.Choice(VALID_SEASONS, case_sensitive=False)
)
@click.option(
    "--product", required=True, type=click.Choice(VALID_PRODUCTS, case_sensitive=False)
)
@click.option(
    "--cog-output-dir",
    type=str,
    help="Directory to write the cog files to",
)
@click.option("--overwrite/--no-overwrite", default=False, show_default=True)
def download_cogs(
    season,
    product,
    cog_output_dir,
    overwrite,
):
    """
    Download the ESA WorldCereal 10 m 2021 v100 products from Zenodo,
    convert to Cloud Optimized Geotiff, and push to an S3 bucket.
    """
    if season not in VALID_SEASONS:
        raise ValueError(f"Invalid season selected: {season}")

    if product not in VALID_PRODUCTS:
        raise ValueError(f"Invalid product selected: {product}")

    # Downoad the classifcation geotiffs for the product
    classification_zip_url = f"https://zenodo.org/records/7875105/files/WorldCereal_{VALID_YEAR}_{season}_{product}_classification.zip?download=1"

    local_classification_geotiffs = download_and_unzip_data(classification_zip_url)

    log.info("Processing classification geotiffs")
    for idx, local_classification_geotiff in enumerate(local_classification_geotiffs):
        log.info(
            f"Processing geotiff {local_classification_geotiff} {idx + 1}/{len(local_classification_geotiffs)}"
        )
        filename = os.path.basename(local_classification_geotiff)
        aez_id, _, _, startdate, enddate, band_name = parse_geotiff_url(
            local_classification_geotiff
        )

        output_cog_parent_dir = join_url(
            cog_output_dir, product, season, aez_id, VALID_YEAR
        )
        output_cog_path = join_url(output_cog_parent_dir, filename)
        if not overwrite:
            if check_file_exists(output_cog_path):
                log.info(f"{output_cog_path} exists! Skipping ...")
                continue

        if not check_directory_exists(output_cog_parent_dir):
            fs = get_filesystem(output_cog_parent_dir, anon=False)
            fs.makedirs(output_cog_parent_dir, exist_ok=True)

        create_and_upload_cog(local_classification_geotiff, output_cog_path)

    if product not in NO_CONFIDENCE_PRODUCTS:
        # Download the confidence geotiffs for the product
        confidence_zip_url = f"https://zenodo.org/records/7875105/files/WorldCereal_{VALID_YEAR}_{season}_{product}_confidence.zip?download=1"

        local_confidence_geotiffs = download_and_unzip_data(confidence_zip_url)

        log.info("Processing confidence geotiffs")
        for idx, local_confidence_geotiff in enumerate(local_confidence_geotiffs):
            log.info(
                f"Processing geotiff {local_confidence_geotiff} {idx + 1}/{len(local_confidence_geotiffs)}"
            )
            filename = os.path.basename(local_confidence_geotiff)
            aez_id, _, _, startdate, enddate, band_name = parse_geotiff_url(
                local_confidence_geotiff
            )

            output_cog_parent_dir = join_url(
                cog_output_dir, product, season, aez_id, VALID_YEAR
            )
            output_cog_path = join_url(output_cog_parent_dir, filename)
            if not overwrite:
                if check_file_exists(output_cog_path):
                    log.info(f"{output_cog_path} exists! Skipping ...")
                    continue

            if not check_directory_exists(output_cog_parent_dir):
                fs = get_filesystem(output_cog_parent_dir, anon=False)
                fs.makedirs(output_cog_parent_dir, exist_ok=True)

            create_and_upload_cog(local_confidence_geotiff, output_cog_path)
