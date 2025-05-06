import subprocess
import sys
from textwrap import dedent

import click
from datacube import Datacube
from odc.aws import s3_url_parse
from tqdm import tqdm

from deafrica import __version__
from deafrica.utils import (
    find_latest_report,
    limit,
    read_report_missing_odc_scenes,
    send_slack_notification,
    setup_logging,
    slack_url,
    split_list_equally,
)

S3_BUCKET_PATH = "s3://deafrica-sentinel-2/status-report/"


def index_missing_odc_scenes(
    idx: int,
    product_name: str,
    max_workers: int = 1,
    limit: int = None,
    slack_url: str = None,
) -> None:
    """
    Index a list of missing ODC scenes

    params:
        limit: (int) optional limit of messages to be read from the report
        max_workers: (int) total number of pods used for the task. This number is used to
            split the number of scenes equally among the PODS
        idx: (int) sequential index which will be used to define the range of scenes that the POD will work with
        slack_url: (str) Optional slack URL in case of you want to send a slack notification

    returns:
        None.
    """
    log = setup_logging()

    dc = Datacube()

    if product_name not in dc.list_products()["name"].to_list():
        assert NotImplementedError(f"product {product_name} not available in datacube")

    latest_report = find_latest_report(
        report_folder_path=S3_BUCKET_PATH,
        not_contains="orphaned",
        contains="gap_report",
    )

    log.info("working")
    log.info(f"Latest report: {latest_report}")

    log.info(f"Limited: {int(limit) if limit else 'No limit'}")
    log.info(f"Number of workers: {max_workers}")

    files = read_report_missing_odc_scenes(report_path=latest_report, limit=limit)

    log.info(f"Number of missing ODC scenes found {len(files)}")
    log.info(f"Example scenes: {files[0:10]}")

    # Split scenes equally among the workers
    split_list_scenes = split_list_equally(
        list_to_split=files, num_inter_lists=int(max_workers)
    )

    # In case of the index being bigger than the number of positions in the array, the extra POD isn' necessary
    if len(split_list_scenes) <= idx:
        log.warning(f"Worker {idx} Skipped!")
        sys.exit(0)

    log.info(f"Executing worker {idx}")

    bucket_name = s3_url_parse(S3_BUCKET_PATH)[0]
    scene_paths = [f"s3://{bucket_name}/{scene}" for scene in split_list_scenes[idx]]

    failed = []
    error_list = []
    indexed = []

    for scene in tqdm(
        iterable=scene_paths, total=len(scene_paths), desc="Indexing missing odc scenes"
    ):
        cmd = [
            "s3-to-dc-v2",
            "--stac",
            "--no-sign-request",
            "--update-if-exists",
            "--allow-unsafe",
            scene,
            product_name,
        ]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
        except Exception as exc:
            failed.append(scene)
            log.error(exc)
            error_list.append(exc)
        else:
            if result.returncode == 0:
                indexed.append(scene)
            else:
                failed.append(scene)
                log.error(result.stderr)

    environment = "DEV" if "dev" in bucket_name else "PDS"
    error_flag = ":red_circle:" if len(failed) > 0 else ""

    message = dedent(
        f"{error_flag}*Sentinel 2 Collection 0 GAP Filler (worker {idx}) - {environment}*\n"
        f"Total missing ODC scenes: {len(files)}\n"
        f"Attempted missing ODC scenes to index: {len(scene_paths)}\n"
        f"Failed missing ODC scenes to index: {len(split_list_scenes[idx]) - len(indexed)}\n"
        f"Indexed missing ODC scenes: {indexed}\n"
        f"Failed to index missing ODC scenes: {failed}\n"
    )

    if slack_url is not None:
        send_slack_notification(slack_url, "S2 Collection 0 Gap Filler", message)

    log.info(message)

    if len(failed) > 0:
        sys.exit(1)


@click.command("s2-index-missing-odc")
@click.argument("idx", type=int, nargs=1, required=True)
@click.argument("max_workers", type=int, nargs=1, default=1)
@click.argument("product_name", type=str, nargs=1, default="s2_l2a")
@limit
@slack_url
@click.option("--version", is_flag=True, default=False)
def cli(
    idx: int,
    max_workers: int = 1,
    product_name: str = "s2_l2a",
    limit: int = None,
    slack_url: str = None,
    version: bool = False,
):
    """
    Index missing ODC missing scenes.

    params:
        idx: (int) sequential index which will be used to define the range of scenes that the POD will work with
        max_workers: (int) total number of pods used for the task. This number is used to
            split the number of scenes equally among the PODS
        product_name (str): Product name being indexed. default is s2_l2a.
        limit: (str) optional limit of messages to be read from the report
        slack_url: (str) Slack notification channel hook URL
        version: (bool) echo the scripts version

    """
    if version:
        click.echo(__version__)

    valid_product_name = ["s2_l2a"]
    if product_name not in valid_product_name:
        raise ValueError(f"Product name must be on of {valid_product_name}")

    if limit is not None:
        try:
            limit = int(limit)
        except ValueError:
            raise ValueError(f"Limit {limit} is not valid")

        if limit < 1:
            raise ValueError(f"Limit {limit} lower than 1.")

    index_missing_odc_scenes(
        idx=idx,
        max_workers=max_workers,
        product_name=product_name,
        limit=limit,
        slack_url=slack_url,
    )
