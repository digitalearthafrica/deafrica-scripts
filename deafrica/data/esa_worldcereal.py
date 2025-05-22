"""
Download the ESA WorldCereal 10 m 2021 v100 products from Zenodo,
convert to Cloud Optimized Geotiff, and push to an S3 bucket.

Datasource: https://zenodo.org/records/7875105
"""

import json
import os
import re
import shutil
import sys
from datetime import datetime
from pathlib import Path
from subprocess import STDOUT, check_output
from zipfile import ZipFile

import click
import geopandas as gpd
import numpy as np
import requests
import rioxarray
from eodatasets3.images import ValidDataMethod
from eodatasets3.model import DatasetDoc
from eodatasets3.serialise import to_path  # noqa F401
from eodatasets3.stac import to_stac_item
from odc.apps.dc_tools._docs import odc_uuid
from odc.aws import s3_dump

from deafrica.easi_assemble import EasiPrepare
from deafrica.io import (
    check_directory_exists,
    check_file_exists,
    download_product_yaml,
    find_geotiff_files,
    get_filesystem,
    is_gcsfs_path,
    is_s3_path,
    is_url,
)
from deafrica.logs import setup_logging
from deafrica.utils import (
    AFRICA_EXTENT_URL,
)

WORLDCEREAL_AEZ_URL = "https://zenodo.org/records/7875105/files/WorldCereal_AEZ.geojson"
VALID_YEARS = ["2021"]
VALID_SEASONS = [
    "tc-annual",
    "tc-wintercereals",
    "tc-springcereals",
    "tc-maize-main",
    "tc-maize-second",
]
VALID_PRODUCTS = [
    "activecropland",
    "irrigation",
    "maize",
    "springcereals",
    "temporarycrops",
    "wintercereals",
]
LOCAL_DOWNLOAD_DIR = "tmp/worldcereal_data"

# Set log level to info
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
        log.info(f"Created the directory {LOCAL_DOWNLOAD_DIR}")

    zip_filename = os.path.basename(zip_url).split(".zip")[0] + ".zip"
    local_zip_path = os.path.join(LOCAL_DOWNLOAD_DIR, zip_filename)

    # Download the zip file.
    if not os.path.exists(local_zip_path):
        with requests.get(zip_url, stream=True, allow_redirects=True) as r:
            with open(local_zip_path, "wb") as f:
                shutil.copyfileobj(r.raw, f)
    else:
        log.info(f"Skipping download, {local_zip_path} already exists!")

    africa_aez_ids = get_africa_aez_ids()

    # Extract the AEZ-based GeoTIFF files
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

            # TODO: Remove file path check
            # Check if the file already exists
            if os.path.exists(local_file_path):
                local_aez_geotiffs.append(local_file_path)
                continue

            # Extract file
            zip_ref.extract(member=file, path=LOCAL_DOWNLOAD_DIR)
            local_aez_geotiffs.append(local_file_path)

    log.info(f"Download complete! \nDownloaded {len(local_aez_geotiffs)} geotiffs")

    return local_aez_geotiffs


def create_and_upload_cog(img_path: str, output_path: str):
    """
    Create COG from GeoTIFF.
    """
    if is_s3_path(output_path):
        # Temporary directory to store the cogs before uploading to s3
        local_cog_dir = os.path.join(LOCAL_DOWNLOAD_DIR, "cogs")
        if not check_directory_exists(local_cog_dir):
            fs = get_filesystem(local_cog_dir, anon=False)
            fs.makedirs(local_cog_dir, exist_ok=True)

        # Create a COG and save to local disk
        cloud_optimised_file = os.path.join(
            local_cog_dir, os.path.basename(output_path)
        )
        cmd = f"rio cogeo create --overview-resampling nearest {img_path} {cloud_optimised_file}"
        check_output(cmd, stderr=STDOUT, shell=True)
        log.info(f"File {cloud_optimised_file} cloud optimised successfully")

        # Uppload COG to s3
        log.info(f"Upload {cloud_optimised_file} to S3 {output_path}")
        s3_dump(
            data=open(str(cloud_optimised_file), "rb").read(),
            url=output_path,
            ACL="bucket-owner-full-control",
            ContentType="image/tiff",
        )
        log.info(f"File written to {output_path}")
    else:
        cmd = f"rio cogeo create --overview-resampling nearest {img_path} {output_path}"
        check_output(cmd, stderr=STDOUT, shell=True)
        log.info(f"File {output_path} cloud optimised successfully")


def prepare_dataset(
    dataset_path: str | Path,
    product_yaml: str | Path,
    output_path: str = None,
) -> DatasetDoc:
    """
    Prepare an eo3 metadata file for SAMPLE data product.
    @param dataset_path: Path to the geotiff to create dataset metadata for.
    @param product_yaml: Path to the product definition yaml file.
    @param output_path: Path to write the output metadata file.

    :return: DatasetDoc
    """
    ## Initialise and validate inputs
    # Creates variables (see EasiPrepare for others):
    # - p.dataset_path
    # - p.product_name
    p = EasiPrepare(dataset_path, product_yaml, output_path)

    ## File format of preprocessed data
    # e.g. cloud-optimised GeoTiff (= GeoTiff)
    file_format = "GeoTIFF"
    extension = "tif"

    ## Check the p.dataset_path
    # Use a glob or a file PATTERN.
    # Customise depending on the expected dir/file names and p.dataset_path
    files = find_geotiff_files(str(p.dataset_path))
    if not files:
        return False, f"Product ID does not match expected form: {p.dataset_path}"

    ## IDs and Labels

    # AEZ-based GeoTIFF files inside are named according to following convention
    # {AEZ_id}_{season}_{product}_{startdate}_{enddate}_{classification|confidence}.tif
    AEZ_id, season, product, startdate, enddate, _ = (
        os.path.basename(files[0]).removesuffix(f".{extension}").split("_")
    )

    # Unique dataset name, probably parsed from p.dataset_path or a filename
    unique_name = f"{AEZ_id}_{season}_{product}_{startdate}_{enddate}"

    # Can not have '.' in label
    unique_name_replace = re.sub("\.", "_", unique_name)
    label = f"{unique_name_replace}-{p.product_name}"  # noqa F841
    # p.label = label

    # product_name is added by EasiPrepare().init()
    p.product_uri = f"https://explorer.digitalearth.africa/product/{p.product_name}"

    # The version of the source dataset
    p.dataset_version = "v1.0.0"

    # Unique dataset UUID built from the unique Product ID
    p.dataset_id = odc_uuid(p.product_name, p.dataset_version, [unique_name])

    ## Satellite, Instrument and Processing level

    # High-level name for the source data (satellite platform or project name).
    # Comma-separated for multiple platforms.
    p.platform = "ESA WorldCereal project"
    #  Instrument name, optional
    # p.instrument = 'OPTIONAL'
    # Organisation that produces the data.
    # URI domain format containing a '.'
    p.producer = "https://vito.be/"
    # ODC/EASI identifier for this "family" of products, optional
    # p.product_family = 'OPTIONAL'
    # Helpful but not critical
    p.properties["odc:file_format"] = file_format
    p.properties["odc:product"] = p.product_name

    ## Scene capture and Processing

    # Use attributes from the classification measurement geotiff instead
    # of the confidence measurement
    band_regex = rf"([^_]+)\.{extension}$"
    measurement_map = p.map_measurements_to_paths(band_regex)
    for measurement_name, file_location in measurement_map.items():
        if measurement_name == "classification":
            attrs = rioxarray.open_rasterio(str(file_location)).attrs

    # Searchable datetime of the dataset, datetime object
    p.datetime = datetime.strptime(attrs["start_date"], "%Y-%m-%d")
    # Searchable start and end datetimes of the dataset, datetime objects
    p.datetime_range = (
        datetime.strptime(attrs["start_date"], "%Y-%m-%d"),
        datetime.strptime(attrs["end_date"], "%Y-%m-%d").replace(
            hour=23, minute=59, second=59
        ),
    )
    # When the source dataset was created by the producer, datetime object
    p.processed = datetime.strptime(attrs["creation_time"], "%Y-%m-%d %H:%M:%S")

    ## Geometry
    # Geometry adds a "valid data" polygon for the scene, which helps bounding box searching in ODC
    # Either provide a "valid data" polygon or calculate it from all bands in the dataset
    # ValidDataMethod.thorough = Vectorize the full valid pixel mask as-is
    # ValidDataMethod.filled = Fill holes in the valid pixel mask before vectorizing
    # ValidDataMethod.convex_hull = Take convex-hull of valid pixel mask before vectorizing
    # ValidDataMethod.bounds = Use the image file bounds, ignoring actual pixel values
    # p.geometry = Provide a "valid data" polygon rather than read from the file, shapely.geometry.base.BaseGeometry()
    # p.crs = Provide a CRS string if measurements GridSpec.crs is None, "epsg:*" or WKT
    p.valid_data_method = ValidDataMethod.bounds

    ## Scene metrics, as available

    # The "region" of acquisition, if applicable
    p.region_code = str(attrs["AEZ_ID"])
    # p.properties["eo:gsd"] = 'FILL'  # Nominal ground sample distance or spatial resolution
    # p.properties["eo:cloud_cover"] = 'OPTIONAL'
    # p.properties["eo:sun_azimuth"] = 'OPTIONAL'
    # p.properties["eo:sun_zenith"] = 'OPTIONAL'

    ## Product-specific properties, OPTIONAL
    # For examples see eodatasets3.properties.Eo3Dict().KNOWN_PROPERTIES
    # p.properties[f'{custom_prefix}:algorithm_version'] = ''
    # p.properties[f'{custom_prefix}:doi'] = ''
    # p.properties[f'{custom_prefix}:short_name'] = ''
    # p.properties[f'{custom_prefix}:processing_system'] = ''

    ## Add measurement paths
    for measurement_name, file_location in measurement_map.items():
        log.debug(f"Measurement map: {measurement_name} > {file_location}")
        p.note_measurement(measurement_name, file_location, relative_to_metadata=False)
    return p.to_dataset_doc(validate_correctness=True, sort_measurements=True)


@click.command()
@click.option(
    "--year",
    required=True,
    default="2021",
    type=click.Choice(VALID_YEARS, case_sensitive=False),
)
@click.option(
    "--season", required=True, type=click.Choice(VALID_SEASONS, case_sensitive=False)
)
@click.option(
    "--product", required=True, type=click.Choice(VALID_PRODUCTS, case_sensitive=False)
)
@click.option(
    "--output-dir",
    type=str,
    default="s3://deafrica-data-dev-af/esa_worldcereal_sample/",
    help="Directory to write the cropped COG files to",
)
@click.option("--overwrite/--no-overwrite", default=False)
def download_esa_worldcereal_cogs(year, season, product, output_dir, overwrite):
    """
    Download the ESA WorldCereal 10 m 2021 v100 products from Zenodo,
    convert to Cloud Optimized Geotiff, and push to an S3 bucket.

    Naming convention of the ZIP files is as follows:
        WorldCereal_{year}_{season}_{product}_{classification|confidence}.zip

    The actual AEZ-based GeoTIFF files inside each ZIP are named according to following
    convention:
        {AEZ_id}_{season}_{product}_{startdate}_{enddate}_{classification|confidence}.tif
    """

    if season not in VALID_SEASONS:
        raise ValueError(f"Invalid season selected: {season}")

    if product not in VALID_PRODUCTS:
        raise ValueError(f"Invalid product selected: {product}")

    if year not in VALID_YEARS:
        raise ValueError(f"Invalid year selected: {year}")

    # Download the classifcation geotiffs for the product
    classification_zip_url = f"https://zenodo.org/records/7875105/files/WorldCereal_{year}_{season}_{product}_classification.zip?download=1"

    log.info("Processing classification geotiffs")

    local_classification_geotiffs = download_and_unzip_data(classification_zip_url)
    for idx, local_classification_geotiff in enumerate(local_classification_geotiffs):
        log.info(
            f"Processing geotiff {local_classification_geotiff} {idx+1}/{len(local_classification_geotiffs)}"
        )

        filename = os.path.splitext(os.path.basename(local_classification_geotiff))[0]
        aez_id, season_, product_, startdate, enddate, product_type = filename.split(
            "_"
        )

        # Define output files
        output_cog_path = os.path.join(
            output_dir, product, season, aez_id, year, f"{filename}.tif"
        )
        if not overwrite:
            if check_file_exists(output_cog_path):
                log.info(f"{output_cog_path} exists! Skipping ...")
                continue

        # Create the required parent directories
        output_cog_parent_dir = os.path.dirname(output_cog_path)
        if not check_directory_exists(output_cog_parent_dir):
            fs = get_filesystem(output_cog_parent_dir, anon=False)
            fs.makedirs(output_cog_parent_dir, exist_ok=True)

        create_and_upload_cog(local_classification_geotiff, output_cog_path)

    # Download the confidence geotiffs for the product
    confidence_zip_url = f"https://zenodo.org/records/7875105/files/WorldCereal_{year}_{season}_{product}_confidence.zip?download=1"

    log.info("Processing confidence geotiffs")

    local_confidence_geotiffs = download_and_unzip_data(confidence_zip_url)
    for idx, local_confidence_geotiff in enumerate(local_confidence_geotiffs):
        log.info(
            f"Processing geotiff {local_confidence_geotiff} {idx+1}/{len(local_confidence_geotiffs)}"
        )

        filename = os.path.splitext(os.path.basename(local_confidence_geotiff))[0]
        aez_id, season_, product_, startdate, enddate, product_type = filename.split(
            "_"
        )

        # Define output files
        output_cog_path = os.path.join(
            output_dir, product, season, aez_id, year, f"{filename}.tif"
        )
        if not overwrite:
            if check_file_exists(output_cog_path):
                log.info(f"{output_cog_path} exists! Skipping ...")
                continue

        # Create the required parent directories
        output_cog_parent_dir = os.path.dirname(output_cog_path)
        if not check_directory_exists(output_cog_parent_dir):
            fs = get_filesystem(output_cog_parent_dir, anon=False)
            fs.makedirs(output_cog_parent_dir, exist_ok=True)

        create_and_upload_cog(local_confidence_geotiff, output_cog_path)


@click.command()
@click.option(
    "--product-name",
    type=str,
    help="Name of the product to generate the stac item files for",
)
@click.option(
    "--product-yaml",
    type=str,
    help="File path or URL to the product definition yaml file",
)
@click.option(
    "--geotiffs-dir",
    type=str,
    default="s3://deafrica-data-dev-af/esa_worldcereal_sample/",
    help="File path to the directory containing the COG files",
)
@click.option(
    "--stac-output-dir",
    type=str,
    default="s3://deafrica-data-dev-af/esa_worldcereal_sample/",
    help="Directory to write the stac files to",
)
@click.option("--overwrite/--no-overwrite", default=False)
@click.option(
    "--max-parallel-steps",
    default=1,
    type=int,
    help="Maximum number of parallel steps/pods to have in the workflow.",
)
@click.option(
    "--worker-idx",
    default=0,
    type=int,
    help="Sequential index which will be used to define the range of geotiffs the pod will work with.",
)
def create_esa_worldcereal_stac(
    product_name: str,
    product_yaml: str,
    geotiffs_dir: str,
    stac_output_dir: str,
    overwrite: bool,
    max_parallel_steps: int,
    worker_idx: int,
):
    """
    Create stac files for products from the ESA WorldCereal 10 m 2021 v100.

    The actual AEZ-based GeoTIFF files are named according to following
    convention:
        {AEZ_id}_{season}_{product}_{startdate}_{enddate}_{classification|confidence}.tif
    """

    # Validate products
    valid_product_names = [
        "esa_worldcereal_wintercereals",
    ]
    if product_name not in valid_product_names:
        raise NotImplementedError(
            f"Stac file generation has not been implemented for ESA World Cereal product {product_name}"
        )

    # Set to temporary dir as output metadata yaml files are not required.
    metadata_output_dir = "tmp/metadata_docs/esa_worldcereal"

    if is_s3_path(metadata_output_dir):
        raise RuntimeError("Metadata files require to be written to a local directory")

    # Path to product yaml
    if not is_s3_path(product_yaml):
        if is_url(product_yaml):
            product_yaml = download_product_yaml(product_yaml)
    else:
        NotImplemented("Product yaml is expected to be a local file or url not s3 path")

    # Geotiffs directory
    if geotiffs_dir:
        # Each dataset path is a folder with 2 geotiffs one for the classification measurement
        # and one for the confidence measurement
        all_geotiff_files = find_geotiff_files(geotiffs_dir)
        all_dataset_paths = list(set([os.path.dirname(i) for i in all_geotiff_files]))
        log.info(f"Found {len(all_dataset_paths)} datasets")
    else:
        raise ValueError(
            "No file path to the directory containing the COG files provided"
        )

    # Split files equally among the workers
    task_chunks = np.array_split(np.array(all_dataset_paths), max_parallel_steps)
    task_chunks = [chunk.tolist() for chunk in task_chunks]
    task_chunks = list(filter(None, task_chunks))

    # In case of the index being bigger than the number of positions in the array, the extra POD isn't necessary
    if len(task_chunks) <= worker_idx:
        log.warning(f"Worker {worker_idx} Skipped!")
        sys.exit(0)

    log.info(f"Executing worker {worker_idx}")

    dataset_paths = task_chunks[worker_idx]

    log.info(f"Generating stac files for the product {product_name}")

    for idx, dataset_path in enumerate(dataset_paths):
        log.info(
            f"Generating stac file for {dataset_path} {idx+1}/{len(dataset_paths)}"
        )

        # File system Path() to the dataset
        # or gsutil URI prefix  (gs://bucket/key) to the dataset.
        if not is_s3_path(dataset_path) and not is_gcsfs_path(dataset_path):
            dataset_path = Path(dataset_path).resolve()
        else:
            dataset_path = dataset_path

        # Find the measurement geotiff files in the dataset path
        measurement_files = find_geotiff_files(dataset_path)
        # AEZ-based GeoTIFF files inside are named according to following convention
        # {AEZ_id}_{season}_{product}_{startdate}_{enddate}_{classification|confidence}.tif
        AEZ_id, season, product, startdate, enddate, _ = (
            os.path.basename(measurement_files[0]).removesuffix(".tif").split("_")
        )

        # Get the year from the dataset_path.
        file_path_parts = os.path.normpath(dataset_path).split(os.sep)
        file_path_parts.reverse()
        year, *_ = file_path_parts

        # Expected file and dir structure
        tile_id = f"{AEZ_id}_{season}_{product}_{startdate}_{enddate}"
        metadata_output_path = Path(
            os.path.join(
                metadata_output_dir,
                product,
                season,
                AEZ_id,
                year,
                f"{tile_id}.odc-metadata.yaml",
            )
        ).resolve()
        stac_item_destination_url = os.path.join(
            stac_output_dir, product, season, AEZ_id, year, f"{tile_id}.stac-item.json"
        )

        # Check if the stac item exists
        if not overwrite:
            if check_file_exists(stac_item_destination_url):
                log.info(
                    f"{stac_item_destination_url} exists! Skipping stac file generation for {dataset_path}"
                )
                continue

        # Create the required parent directories
        metadata_output_parent_dir = os.path.dirname(metadata_output_path)
        if not check_directory_exists(metadata_output_parent_dir):
            fs = get_filesystem(metadata_output_parent_dir, anon=False)
            fs.makedirs(metadata_output_parent_dir, exist_ok=True)
            log.info(f"Created the directory {metadata_output_parent_dir}")

        stac_item_parent_dir = os.path.dirname(stac_item_destination_url)
        if not check_directory_exists(stac_item_parent_dir):
            fs = get_filesystem(stac_item_parent_dir, anon=False)
            fs.makedirs(stac_item_parent_dir, exist_ok=True)
            log.info(f"Created the directory {stac_item_parent_dir}")

        dataset_doc = prepare_dataset(
            dataset_path=dataset_path,
            product_yaml=product_yaml,
            output_path=metadata_output_path,
        )

        # Write the dataset doc to file
        to_path(metadata_output_path, dataset_doc)
        log.info(f"Wrote dataset to {metadata_output_path}")

        # Convert dataset doc to stac item
        stac_item = to_stac_item(
            dataset=dataset_doc,
            stac_item_destination_url=str(stac_item_destination_url),
        )

        # Write stac item
        if is_s3_path(stac_item_destination_url):
            s3_dump(
                data=json.dumps(stac_item, indent=2),
                url=stac_item_destination_url,
                ACL="bucket-owner-full-control",
                ContentType="application/json",
            )
        else:
            with open(stac_item_destination_url, "w") as file:
                json.dump(
                    stac_item, file, indent=2
                )  # `indent=4` makes it human-readable

        log.info(f"STAC item written to {stac_item_destination_url}")
