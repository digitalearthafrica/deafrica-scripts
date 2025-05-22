"""
Create per dataset metadata (stac files) for the ESA WorldCereal 10 m
2021 v100 products.
"""

import json
import logging
import os
import posixpath
import sys
from pathlib import Path

import click
import numpy as np
from eodatasets3.serialise import to_path
from eodatasets3.stac import to_stac_item

from deafrica.data.esa_worldcereal.constants import (
    VALID_YEAR,
)
from deafrica.data.esa_worldcereal.geotiff import (
    get_dataset_tile_id,
    parse_dataset_tile_id,
)
from deafrica.data.esa_worldcereal.prepare_metadata import prepare_dataset
from deafrica.io import (
    check_directory_exists,
    check_file_exists,
    find_geotiff_files,
    get_filesystem,
    is_local_path,
    join_url,
)
from deafrica.logs import setup_logging


def get_stac_item_destination_url(output_dir: str, dataset_tile_id: str) -> str:
    """
    Construct the file path for the dataset STAC document.

    Parameters
    ----------
    output_dir : str
        Directory to write the STAC document to.
    dataset_tile_id : str
        Unique tile ID for a single dataset.

    Returns
    -------
    str
        File path to write the dataset STAC document to.
    """
    aez_id, season, product, startdate, enddate = parse_dataset_tile_id(dataset_tile_id)
    parent_dir = join_url(output_dir, product, season, aez_id, VALID_YEAR)
    file_name = f"{dataset_tile_id}.stac-item.json"

    if not check_directory_exists(parent_dir):
        fs = get_filesystem(parent_dir, anon=False)
        fs.makedirs(parent_dir, exist_ok=True)

    stac_item_destination_url = join_url(parent_dir, file_name)
    return stac_item_destination_url


def get_eo3_dataset_doc_file_path(
    output_dir: str, dataset_tile_id: str, write_eo3_dataset_doc: bool
) -> str:
    """Construct the file path for the dataset's eo3 metadata document.

    Parameters
    ----------
    output_dir : str
        Directory to write the eo3 metadata document to.
    dataset_tile_id : str
        Unique tile ID for a single dataset.
    write_eo3_dataset_doc : bool
        If True create the parent directory for the document.

    Returns
    -------
    str
        File path for the dataset's eo3 metadata document.
    """
    aez_id, season, product, startdate, enddate = parse_dataset_tile_id(dataset_tile_id)
    parent_dir = join_url(output_dir, product, season, aez_id, VALID_YEAR)
    file_name = f"{dataset_tile_id}.odc-metadata.yaml"

    if write_eo3_dataset_doc:
        if not check_directory_exists(parent_dir):
            fs = get_filesystem(parent_dir, anon=False)
            fs.makedirs(parent_dir, exist_ok=True)

    eo3_dataset_doc_file_path = join_url(parent_dir, file_name)
    return eo3_dataset_doc_file_path


@click.command(
    "create-esa-worldcereal-stac",
    help="Create per dataset metadata (stac files) for ESA WorldCereal 10m 2021 v100 products",
    no_args_is_help=True,
)
@click.option(
    "--cogs-dir",
    type=str,
    help="Directory containing the datasets to generate metadata for",
)
@click.option(
    "--product-yaml",
    type=str,
    help="File path or URL to the product definition yaml file",
)
@click.option(
    "--stac-output-dir",
    type=str,
    help="Directory to write the stac files docs to",
)
@click.option("--overwrite/--no-overwrite", default=False, show_default=True)
@click.option(
    "--max-parallel-steps",
    default=1,
    show_default=True,
    type=int,
    help="Maximum number of parallel steps/pods to have in the workflow.",
)
@click.option(
    "--worker-idx",
    default=0,
    show_default=True,
    type=int,
    help="Sequential index which will be used to define the range of geotiffs the pod will work with.",
)
@click.option(
    "--write-eo3/--no-write-eo3",
    default=False,
    show_default=True,
    help="Whether to write eo3 dataset documents before they are converted to stac.",
)
def create_stac_files(
    cogs_dir: str,
    product_yaml: str,
    stac_output_dir: str,
    overwrite: bool,
    max_parallel_steps: int,
    worker_idx: int,
    write_eo3: bool,
):
    """
    Create stac files for products from the ESA WorldCereal 10 m 2021 v100 product suiite.

    The actual AEZ-based GeoTIFF files are named according to following
    convention:
        {AEZ_id}_{season}_{product}_{startdate}_{enddate}_{classification|confidence}.tif
    """
    # Setup logging level
    setup_logging()

    log = logging.getLogger(__name__)

    # Find all the geotiffs
    # Files have the structure
    # s3://<bucket>/<product_name>/<x>/<y>/<year>/<month>/c_gls_<Acronym>_<YYYYMMDDHHmm>_<AREA>_<SENSOR>_<Version>_<x><y>_<subdataset_variable>.tif
    all_geotiffs = find_geotiff_files(cogs_dir)
    if is_local_path(cogs_dir):
        all_dataset_paths = list(set(os.path.dirname(i) for i in all_geotiffs))
    else:
        all_dataset_paths = list(set(posixpath.dirname(i) for i in all_geotiffs))
    all_dataset_paths.sort()
    log.info(f"Found {len(all_dataset_paths)} datasets")

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

    log.info(f"Generating stac files for {len(all_dataset_paths)} datasets")
    failed_tasks = []
    for idx, dataset_path in enumerate(dataset_paths):
        try:
            log.info(
                f"Generating stac file for {dataset_path} {idx + 1}/{len(dataset_paths)}"
            )

            # Get the measurement geotiffs that belong to the dataset.
            measurement_files = list(filter(lambda x: dataset_path in x, all_geotiffs))
            measurement_files.sort()

            dataset_tile_id = get_dataset_tile_id(measurement_files[0])

            if is_local_path(dataset_path):
                dataset_path = Path(dataset_path).resolve()

            stac_item_destination_url = get_stac_item_destination_url(
                stac_output_dir, dataset_tile_id
            )
            if not overwrite:
                if check_file_exists(stac_item_destination_url):
                    log.info(
                        f"{stac_item_destination_url} exists! Skipping stac file generation for {dataset_path}"
                    )
                    continue

            # Dataset docs
            dataset_doc_output_path = get_eo3_dataset_doc_file_path(
                "tmp", dataset_tile_id, write_eo3
            )

            dataset_doc = prepare_dataset(
                dataset_tile_id, dataset_path, product_yaml, dataset_doc_output_path
            )

            if write_eo3:
                to_path(Path(dataset_doc_output_path), dataset_doc)

            # Convert dataset doc to stac item
            stac_item = to_stac_item(
                dataset=dataset_doc,
                stac_item_destination_url=str(stac_item_destination_url),
            )

            # Write stac file to disk.
            fs = get_filesystem(str(stac_item_destination_url), anon=False)
            with fs.open(str(stac_item_destination_url), "w") as f:
                json.dump(stac_item, f, indent=2)  # `indent=4` makes it human-readable

        except Exception as error:
            log.exception(error)
            log.error(
                f"Failed to generate metedata file for dataset {str(dataset_path)}"
            )
            failed_tasks.append(str(dataset_path))

    if failed_tasks:
        failed_tasks_json_array = json.dumps(failed_tasks)

        tasks_directory = "/tmp/"
        failed_tasks_output_file = join_url(tasks_directory, "failed_tasks")

        fs = get_filesystem(path=tasks_directory, anon=False)

        if not check_directory_exists(path=tasks_directory):
            fs.mkdirs(path=tasks_directory, exist_ok=True)
            log.info(f"Created directory {tasks_directory}")

        with fs.open(failed_tasks_output_file, "a") as file:
            file.write(failed_tasks_json_array + "\n")
        log.info(f"Failed tasks written to {failed_tasks_output_file}")

        raise RuntimeError(f"{len(failed_tasks)} tasks failed")
