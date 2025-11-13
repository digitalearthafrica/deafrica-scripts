"""
Create per dataset metadata (stac files) for the Copernicus Global Land Service -
Lake Water Quality datasets.
"""

import json
import sys
import warnings

import click

from deafrica.data.cgls_lwq.filename_parser import get_stac_url
from deafrica.data.cgls_lwq.prepare_metadata import prepare_dataset
from deafrica.io import (
    check_directory_exists,
    check_file_exists,
    find_geotiff_files,
    get_filesystem,
    get_parent_dir,
    join_url,
)
from deafrica.logs import setup_logging
from deafrica.utils import split_tasks


@click.command(
    "create-cgls-lwq-stac",
    no_args_is_help=True,
)
@click.option(
    "--overwrite/--no-overwrite",
    default=False,
    show_default=True,
    help=(
        "If overwrite is True tasks that have already been processed " "will be rerun. "
    ),
)
@click.argument(
    "datasets-dir",
    type=str,
)
@click.argument(
    "product-yaml",
    type=str,
)
@click.argument(
    "max-parallel-steps",
    type=int,
)
@click.argument(
    "worker-idx",
    type=int,
)
def create_stac_files(
    overwrite: bool,
    datasets_dir: str,
    product_yaml: str,
    max_parallel_steps: int,
    worker_idx: int,
):
    """Generate STAC metadata files for the CGLS Lake Water Quality ODC
    product defined by the product definition file located at PRODUCT_YAML and
    whose datasets are located in the directory DATASETS_DIR.

    MAX_PARALLEL_STEPS indicates the total number of parallel workers
    processing tasks, and WORKER_IDX indicates the index of this worker
    (0-indexed).
    """
    # Setup logging level
    log = setup_logging()

    # Find all the geotiffs
    # Files have the structure
    # s3://<bucket>/<product_name>/<x>/<y>/<year>/<month>/c_gls_<Acronym>_<YYYYMMDDHHmm>_<AREA>_<SENSOR>_<Version>_<x><y>_<subdataset_variable>.tif
    all_geotiffs = find_geotiff_files(datasets_dir)
    all_dataset_paths = list(set(get_parent_dir(i) for i in all_geotiffs))
    del all_geotiffs
    all_dataset_paths.sort()
    log.info(f"Found {len(all_dataset_paths)} datasets")

    datasets_to_run = split_tasks(all_dataset_paths, max_parallel_steps, worker_idx)

    if not datasets_to_run:
        log.warning(f"Worker {worker_idx} has no datasets to process. Exiting.")
        sys.exit(0)

    log.info(f"Worker {worker_idx} processing {len(datasets_to_run)} datasets")

    failed_tasks = []
    for idx, dataset_path in enumerate(datasets_to_run):
        log.info(
            f"Generating stac file for {dataset_path} {idx + 1}/{len(datasets_to_run)}"
        )
        output_stac_url = get_stac_url(dataset_path)

        exists = check_file_exists(output_stac_url)
        if not overwrite and exists:
            log.info(
                f"{output_stac_url} exists! Skipping processing dataset {dataset_path}"
            )
            continue
        else:

            try:
                # Generate STAC metadata
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=UserWarning)
                    log.info("Creating metadata STAC file ...")
                    stac_file_url = prepare_dataset(  # noqa F841
                        dataset_path=dataset_path, product_yaml=product_yaml
                    )
            except Exception as e:
                log.error(
                    f"Failed to generate STAC file for dataset {dataset_path}: {e}"
                )
                failed_tasks.append(dataset_path)
                continue

    # Handle failed tasks
    if failed_tasks:
        failed_tasks_json_array = json.dumps(failed_tasks)

        tasks_directory = "/tmp/"
        failed_tasks_output_file = join_url(tasks_directory, "failed_tasks")

        fs = get_filesystem(path=tasks_directory, anon=False)
        if not check_directory_exists(path=tasks_directory):
            fs.mkdirs(path=tasks_directory, exist_ok=True)

        with fs.open(failed_tasks_output_file, "a") as file:
            file.write(failed_tasks_json_array + "\n")
        log.error(f"Failed tasks: {failed_tasks_json_array}")
        log.info(f"Failed tasks written to {failed_tasks_output_file}")
        sys.exit(1)
    else:
        log.info(f"Worker {worker_idx} completed successfully!")
        sys.exit(0)


if __name__ == "__main__":
    create_stac_files()
