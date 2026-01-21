import json
import sys
from datetime import datetime

import click
import pandas as pd
from datacube import Datacube

from deafrica.io import check_directory_exists, get_filesystem, get_parent_dir, join_url
from deafrica.logs import setup_logging
from deafrica.utils import split_tasks


@click.command(
    "archive-scenes",
    no_args_is_help=True,
)
@click.argument(
    "report-path",
    type=str,
)
@click.argument(
    "output-dir",
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
@click.option(
    "--dry-run", is_flag=True, help="Show what would be done without making changes"
)
def cli(
    report_path: str,
    output_dir: str,
    max_parallel_steps: int,
    worker_idx: int,
    dry_run: bool = False,
):
    """
    Archive and purge datasets whose metadata document file path is listed in the REPORT_PATH text file and write a status report to the OUTPUT_DIR directory.

    MAX_PARALLEL_STEPS: The total number of parallel workers or pods
    expected in the workflow. This value is used to divide the list of
    scenes to be processed among the available workers.

    WORKER_IDX: The sequential index (0-indexed) of the current worker.
    This index determines which subset of scenes the current worker will
    process.

    >Note this script requires delete permissions on the ODC database.
    """

    log = setup_logging()

    fs = get_filesystem(report_path, anon=True)

    with fs.open(report_path, "r") as f:
        all_dataset_uris = [line.rstrip("\n") for line in f]

    dataset_uris = split_tasks(all_dataset_uris, max_parallel_steps, worker_idx)

    if not dataset_uris:
        log.warning(f"Worker {worker_idx} has no scenes to process. Exiting.")
        sys.exit(0)

    log.info(f"Worker {worker_idx} processing {len(dataset_uris)} scenes")

    dc = Datacube()

    archived_info = []
    failed_to_archive = []
    failed_to_purge = []
    for idx, ds_uri in enumerate(dataset_uris):
        log.info(f"Processing datasets for {ds_uri} : {idx + 1} of {len(dataset_uris)}")

        datasets = list(
            dc.index.datasets.get_datasets_for_location(ds_uri, mode="exact")
        )
        if not datasets:
            log.warning(f"No datasets found for location {ds_uri}. Skipping.")
            continue
        else:
            for ds in datasets:
                product = ds.product.name
                ds_id = str(ds.id)
                if dry_run:
                    log.info(f"[Dry Run] Would archive and purge dataset {ds_id}")
                    continue
                else:
                    try:
                        dc.index.datasets.archive([ds_id])
                    except Exception as e:
                        log.error(f"Failed to archive dataset {ds_id}: {e}")
                        failed_to_archive.append(ds_uri)
                        continue
                    else:
                        try:
                            dc.index.datasets.purge([ds_id])
                        except Exception as e:
                            log.error(f"Failed to purge dataset {ds_id}: {e}")
                            failed_to_purge.append(ds_uri)
                        else:
                            row = {
                                "dataset-id": ds_id,
                                "product": product,
                                "location": ds_uri,
                            }
                            archived_info.append(row)

    if archived_info:
        output_csv_file = join_url(
            output_dir,
            "status-report",
            f"archived_{datetime.now().strftime('%Y-%m-%d')}_worker_{worker_idx}.csv",
        )
        fs = get_filesystem(output_csv_file, anon=False)

        parent_dir = get_parent_dir(output_csv_file)
        if not check_directory_exists(parent_dir):
            fs.makedirs(parent_dir, exist_ok=True)

        archived_df = pd.DataFrame(archived_info)

        with fs.open(output_csv_file, mode="w") as f:
            archived_df.to_csv(f, index=False)
        log.info(f"{len(archived_info)} datasets archived and purged.")
        log.info(f"Archived and purged datasets report written to {output_csv_file}")

    if failed_to_archive or failed_to_purge:
        log.error(
            f"Worker {worker_idx} completed with {len(failed_to_archive)} failed archives and {len(failed_to_purge)} failed purges."
        )
        if failed_to_archive:
            tmp_dir = "/tmp/"
            output_file = join_url(tmp_dir, "failed_to_archive")

            fs = get_filesystem(path=tmp_dir, anon=False)
            if not check_directory_exists(path=tmp_dir):
                fs.makedirs(path=tmp_dir, exist_ok=True)

            with fs.open(output_file, "a") as file:
                file.write(json.dumps(failed_to_archive) + "\n")
            log.info(f"Failed to archive scenes written to {output_file}")
        if failed_to_purge:
            tmp_dir = "/tmp/"
            output_file = join_url(tmp_dir, "failed_to_purge")

            fs = get_filesystem(path=tmp_dir, anon=False)
            if not check_directory_exists(path=tmp_dir):
                fs.makedirs(path=tmp_dir, exist_ok=True)

            with fs.open(output_file, "a") as file:
                file.write(json.dumps(failed_to_purge) + "\n")
            log.info(f"Failed to purge scenes written to {output_file}")
        sys.exit(1)
    else:
        sys.exit(0)
