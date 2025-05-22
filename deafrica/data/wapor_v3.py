"""
Generate stac files for the WaPOR version 3.0 Datasets
"""

import calendar
import collections
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import click
import numpy as np
import pandas as pd
import requests
from dateutil.relativedelta import relativedelta
from eodatasets3.images import ValidDataMethod
from eodatasets3.model import DatasetDoc
from eodatasets3.serialise import to_path  # noqa F401
from eodatasets3.stac import to_stac_item
from odc.aws import s3_dump

from deafrica.easi_assemble import EasiPrepare
from deafrica.logs import setup_logging
from deafrica.utils import (
    check_directory_exists,
    check_file_exists,
    download_product_yaml,
    find_geotiff_files,
    fix_assets_links,
    get_filesystem,
    get_last_modified,
    is_gcsfs_path,
    is_s3_path,
    is_url,
    odc_uuid,
)

# Set log level to info
log = setup_logging()


def get_WaPORv3_info(url: str) -> pd.DataFrame:
    """
    Get information on WaPOR v3 data from the api url.
    WaPOR v3 variables are stored in `mapsets`, which in turn contain
    `rasters` that contain the data for a particular date or period.

    Parameters
    ----------
    url : str
        URL to get information from
    Returns
    -------
    pd.DataFrame
        A table of the mapset attributes found.
    """
    data = {"links": [{"rel": "next", "href": url}]}

    output_dict = collections.defaultdict(list)
    while "next" in [x["rel"] for x in data["links"]]:
        url_ = [x["href"] for x in data["links"] if x["rel"] == "next"][0]
        response = requests.get(url_)
        response.raise_for_status()
        data = response.json()["response"]
        for item in data["items"]:
            for key in list(item.keys()):
                if key == "links":
                    output_dict[key].append(item[key][0]["href"])
                else:
                    output_dict[key].append(item[key])

    output_df = pd.DataFrame(output_dict)

    if "code" in output_df.columns:
        output_df.sort_values("code", inplace=True)
        output_df.reset_index(drop=True, inplace=True)
    return output_df


def get_mapset_rasters_from_api(wapor_v3_mapset_code: str) -> list[str]:
    base_url = (
        "https://data.apps.fao.org/gismgr/api/v2/catalog/workspaces/WAPOR-3/mapsets"
    )
    wapor_v3_mapset_url = os.path.join(base_url, wapor_v3_mapset_code, "rasters")
    wapor_v3_mapset_rasters = get_WaPORv3_info(wapor_v3_mapset_url)[
        "downloadUrl"
    ].to_list()
    return wapor_v3_mapset_rasters


def get_mapset_rasters_from_gsutil_uri(wapor_v3_mapset_code: str) -> list[str]:
    base_url = "gs://fao-gismgr-wapor-3-data/DATA/WAPOR-3/MAPSET/"
    wapor_v3_mapset_url = os.path.join(base_url, wapor_v3_mapset_code)
    wapor_v3_mapset_rasters = find_geotiff_files(directory_path=wapor_v3_mapset_url)
    return wapor_v3_mapset_rasters


def get_mapset_rasters(wapor_v3_mapset_code: str) -> list[str]:
    try:
        wapor_v3_mapset_rasters = get_mapset_rasters_from_api(wapor_v3_mapset_code)
    except Exception:
        wapor_v3_mapset_rasters = get_mapset_rasters_from_gsutil_uri(
            wapor_v3_mapset_code
        )
    log.info(
        f"Found {len(wapor_v3_mapset_rasters)} rasters for the mapset {wapor_v3_mapset_code}"
    )
    return wapor_v3_mapset_rasters


def get_dekad(year: str | int, month: str | int, dekad_label: str) -> tuple:
    """
    Get the start date of the dekad that a date belongs to and the time range
    for the dekad.
    Every month has three dekads, such that the first two dekads
    have 10 days (i.e., 1-10, 11-20), and the third is comprised of the
    remaining days of the month.

    Parameters
    ----------
    year: int | str
        Year of the dekad
    month: int | str
        Month of the dekad
    dekad_label: str
        Label indicating whether the date falls in the 1st, 2nd or 3rd dekad
        in a month

    Returns
    -------
    tuple
        The start date of the dekad and the time range for the dekad.
    """
    if isinstance(year, str):
        year = int(year)

    if isinstance(month, str):
        month = int(month)

    first_day = datetime(year, month, 1)
    last_day = datetime(year, month, calendar.monthrange(year, month)[1])

    d1_start_date, d2_start_date, d3_start_date = pd.date_range(
        start=first_day, end=last_day, freq="10D", inclusive="left"
    )
    if dekad_label == "D1":
        start_datetime = d1_start_date.to_pydatetime()
        end_datetime = (d2_start_date - relativedelta(days=1)).to_pydatetime()
        end_datetime = end_datetime.replace(hour=23, minute=59, second=59)
    elif dekad_label == "D2":
        start_datetime = d2_start_date.to_pydatetime()
        end_datetime = (d3_start_date - relativedelta(days=1)).to_pydatetime()
        end_datetime = end_datetime.replace(hour=23, minute=59, second=59)
    elif dekad_label == "D3":
        start_datetime = d3_start_date.to_pydatetime()
        end_datetime = last_day.replace(hour=23, minute=59, second=59)

    return start_datetime, (start_datetime, end_datetime)


def get_month(year: str | int, month: str) -> tuple:
    """
    Get the start date of the month that a date belongs to and the time range
    for the month.

    Parameters
    ----------
    year: int | str
        Year
    month: int | str
        Month


    Returns
    -------
    tuple
        The start date of the month and the time range for the month.
    """
    if isinstance(year, str):
        year = int(year)

    if isinstance(month, str):
        month = int(month)

    start_datetime = datetime(year, month, 1)

    last_day = datetime(year, month, calendar.monthrange(year, month)[1])
    end_datetime = last_day.replace(hour=23, minute=59, second=59)

    return start_datetime, (start_datetime, end_datetime)


def prepare_dataset(
    dataset_path: str | Path,
    product_yaml: str | Path,
    output_path: str = None,
) -> DatasetDoc:
    ## File format of data
    # e.g. cloud-optimised GeoTiff (= GeoTiff)
    file_format = "GeoTIFF"
    file_extension = ".tif"

    tile_id = os.path.basename(dataset_path).removesuffix(file_extension)

    ## Initialise and validate inputs
    # Creates variables (see EasiPrepare for others):
    # - p.dataset_path
    # - p.product_name
    # The output_path and tile_id are use to create a dataset unique filename
    # for the output metadata file.
    # Variable p is a dictionary of metadata and measurements to be written
    # to the output metadata file.
    # The code will populate p with the metadata and measurements and then call
    # p.write_eo3() to write the output metadata file.
    p = EasiPrepare(dataset_path, product_yaml, output_path)

    ## IDs and Labels should be dataset and Product unique
    # Unique dataset name, probably parsed from p.dataset_path or a filename
    unique_name = f"{tile_id}"
    # Can not have '.' in label
    unique_name_replace = re.sub("\.", "_", unique_name)
    label = f"{unique_name_replace}-{p.product_name}"  # noqa F841
    # p.label = label # Optional
    # product_name is added by EasiPrepare().init()
    p.product_uri = f"https://explorer.digitalearth.africa/product/{p.product_name}"
    # The version of the source dataset
    p.dataset_version = "v3.0"
    # Unique dataset UUID built from the unique Product ID
    p.dataset_id = odc_uuid(p.product_name, p.dataset_version, [unique_name])

    ## Satellite, Instrument and Processing level
    # High-level name for the source data (satellite platform or project name).
    # Comma-separated for multiple platforms.
    p.platform = "WaPORv3"
    # p.instrument = 'SAMPLETYPE'  #  Instrument name, optional
    # Organisation that produces the data.
    # URI domain format containing a '.'
    p.producer = "www.fao.org"
    # ODC/EASI identifier for this "family" of products, optional
    # p.product_family = 'FAMILY_STUFF'
    p.properties["odc:file_format"] = file_format  # Helpful but not critical
    p.properties["odc:product"] = p.product_name

    ## Scene capture and Processing

    # Datetime derived from file name
    try:
        year, month, dekad_label = tile_id.split(".")[-1].split("-")
    except ValueError:
        year, month = tile_id.split(".")[-1].split("-")
        dekad_label = None

    if dekad_label:
        input_datetime, time_range = get_dekad(year, month, dekad_label)
    else:
        input_datetime, time_range = get_month(year, month)
    # Searchable datetime of the dataset, datetime object
    p.datetime = input_datetime
    # Searchable start and end datetimes of the dataset, datetime objects
    p.datetime_range = time_range
    # When the source dataset was created by the producer, datetime object
    processed_dt = get_last_modified(dataset_path)
    if processed_dt:
        p.processed = processed_dt

    ## Geometry
    # Geometry adds a "valid data" polygon for the scene, which helps bounding box searching in ODC
    # Either provide a "valid data" polygon or calculate it from all bands in the dataset
    # Some techniques are more accurate than others, but all are valid. You may need to use coarser methods if the data
    # is particularly noisy or sparse.
    # ValidDataMethod.thorough = Vectorize the full valid pixel mask as-is
    # ValidDataMethod.filled = Fill holes in the valid pixel mask before vectorizing
    # ValidDataMethod.convex_hull = Take convex-hull of valid pixel mask before vectorizing
    # ValidDataMethod.bounds = Use the image file bounds, ignoring actual pixel values
    # p.geometry = Provide a "valid data" polygon rather than read from the file, shapely.geometry.base.BaseGeometry()
    # p.crs = Provide a CRS string if measurements GridSpec.crs is None, "epsg:*" or WKT
    p.valid_data_method = ValidDataMethod.bounds

    ## Product-specific properties, OPTIONAL
    # For examples see eodatasets3.properties.Eo3Dict().KNOWN_PROPERTIES
    # p.properties[f'{custom_prefix}:algorithm_version'] = ''
    # p.properties[f'{custom_prefix}:doi'] = ''
    # p.properties[f'{custom_prefix}:short_name'] = ''
    # p.properties[f'{custom_prefix}:processing_system'] = 'SomeAwesomeProcessor' # as an example

    ## Add measurement paths
    # This simple loop will go through all the measurements and determine their grids, the valid data polygon, etc
    # and add them to the dataset.
    # For LULC there is only one measurement, land_cover_class
    if p.product_name == "wapor_soil_moisture":
        measurement_name = "relative_soil_moisture"
    elif p.product_name == "wapor_monthly_npp":
        measurement_name = "net_primary_production"

    p.note_measurement(measurement_name, dataset_path, relative_to_metadata=False)

    return p.to_dataset_doc(validate_correctness=True, sort_measurements=True)


@click.command("create-wapor-v3-stac")
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
    "--stac-output-dir",
    type=str,
    default="s3://deafrica-data-dev-af/wapor-v3/",
    help="Directory to write the stac files docs to",
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
def create_wapor_v3_stac(
    product_name: str,
    product_yaml: str,
    stac_output_dir: str,
    overwrite: bool,
    max_parallel_steps: int,
    worker_idx: int,
):
    valid_product_names = ["wapor_soil_moisture", "wapor_monthly_npp"]
    if product_name not in valid_product_names:
        raise NotImplementedError(
            f"Stac file generation has not been implemented for {product_name}"
        )

    # Set to temporary dir as output metadata yaml files are not required.
    metadata_output_dir = "tmp/metadata_docs"
    if product_name not in os.path.basename(metadata_output_dir.rstrip("/")):
        metadata_output_dir = os.path.join(metadata_output_dir, product_name)

    if is_s3_path(metadata_output_dir):
        raise RuntimeError("Metadata files require to be written to a local directory")

    # Path to product yaml
    if not is_s3_path(product_yaml):
        if is_url(product_yaml):
            product_yaml = download_product_yaml(product_yaml)
    else:
        NotImplemented("Product yaml is expected to be a local file or url not s3 path")

    # Directory to write the stac files to
    if product_name not in os.path.basename(stac_output_dir.rstrip("/")):
        stac_output_dir = os.path.join(stac_output_dir, product_name)

    # WaPOR version 3 mapset code for the product
    if product_name == "wapor_soil_moisture":
        mapset_code = "L2-RSM-D"
    elif product_name == "wapor_monthly_npp":
        mapset_code = "L2-NPP-M"

    all_geotiff_files = get_mapset_rasters(mapset_code)
    # Use a gsutil URI instead of the public URL
    all_geotiff_files = [
        i.replace("https://storage.googleapis.com/", "gs://") for i in all_geotiff_files
    ]

    # Split files equally among the workers
    task_chunks = np.array_split(np.array(all_geotiff_files), max_parallel_steps)
    task_chunks = [chunk.tolist() for chunk in task_chunks]
    task_chunks = list(filter(None, task_chunks))

    # In case of the index being bigger than the number of positions in the array, the extra POD isn't necessary
    if len(task_chunks) <= worker_idx:
        log.warning(f"Worker {worker_idx} Skipped!")
        sys.exit(0)

    log.info(f"Executing worker {worker_idx}")

    geotiffs = task_chunks[worker_idx]

    log.info(f"Generating stac files for the product {product_name}")

    for idx, geotiff in enumerate(geotiffs):
        log.info(f"Generating stac file for {geotiff} {idx+1}/{len(geotiffs)}")

        # File system Path() to the dataset
        # or gsutil URI prefix  (gs://bucket/key) to the dataset.
        if not is_s3_path(geotiff) and not is_gcsfs_path(geotiff):
            dataset_path = Path(geotiff).resolve()
        else:
            dataset_path = geotiff

        tile_id = os.path.basename(dataset_path).removesuffix(".tif")

        try:
            year, month, _ = tile_id.split(".")[-1].split("-")
        except ValueError:
            year, month = tile_id.split(".")[-1].split("-")

        metadata_output_path = Path(
            os.path.join(
                metadata_output_dir, year, month, f"{tile_id}.odc-metadata.yaml"
            )
        ).resolve()
        stac_item_destination_url = os.path.join(
            stac_output_dir, year, month, f"{tile_id}.stac-item.json"
        )

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
        # to_path(metadata_output_path, dataset_doc)
        # log.info(f"Wrote dataset to {metadata_output_path}")

        # Convert dataset doc to stac item
        stac_item = to_stac_item(
            dataset=dataset_doc,
            stac_item_destination_url=str(stac_item_destination_url),
        )

        # Fix links in stac item
        stac_item = fix_assets_links(stac_item)

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
