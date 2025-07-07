import json
import logging
import sys

import boto3
import botocore
import click
from odc.aws import s3_url_parse
from yarl import URL

from deafrica import __version__
from deafrica.click_options import limit, slack_url
from deafrica.logs import setup_logging
from deafrica.monitoring.gap_report import (
    find_latest_report,
    read_report_missing_odc_scenes,
)
from deafrica.utils import split_list_equally

S1_BUCKET = "s3://deafrica-sentinel-1/"
S1_BUCKET_REGION = "af-south-1"
S1_GAP_REPORT_DIR = str(URL(S1_BUCKET) / "status-report/")

log = logging.getLogger(__name__)


@click.command("s1-gap-filler", no_args_is_help=True)
@click.argument("worker-idx", type=int, nargs=1, required=True)
@click.argument("max-workers", type=int, nargs=1, required=True)
@click.argument("sns-topic-arn", type=str, nargs=1, required=True)
@limit
@slack_url
@click.option("--version", is_flag=True, default=False)
@click.option("--dryrun", is_flag=True, default=False)
def cli(
    worker_idx: int,
    max_workers: int,
    sns_topic_arn: str,
    limit: int,
    slack_url: str,
    version: bool,
    dryrun: bool,
):
    log = setup_logging()

    if version:
        click.echo(__version__)

    if limit is not None:
        try:
            limit = int(limit)
        except ValueError:
            raise ValueError(f"Limit {limit} is not valid")
        if limit < 1:
            raise ValueError(f"Limit {limit} lower than 1.")

    if dryrun:
        log.info("dryrun, messages not sent")

    latest_report = find_latest_report(
        report_folder_path=S1_GAP_REPORT_DIR,
        not_contains="orphaned",
        contains="gap_report",
    )
    log.info(f"Latest report: {latest_report}")

    log.info(f"Limited: {int(limit) if limit else 'No limit'}")
    log.info(f"Number of workers: {max_workers}")

    files = read_report_missing_odc_scenes(report_path=latest_report, limit=limit)

    log.info(f"Number of scenes found {len(files)}")
    log.info(f"Example scenes: {files[0:10]}")

    # Split scenes equally among the workers
    split_list_scenes = split_list_equally(
        list_to_split=files, num_inter_lists=int(max_workers)
    )

    # In case of the index being bigger than the number of positions in
    # the array, the extra POD isn' necessary
    if len(split_list_scenes) <= worker_idx:
        log.warning(f"Worker {worker_idx} Skipped!")
        sys.exit(0)

    log.info(f"Executing worker {worker_idx}")

    scenes = split_list_scenes[worker_idx]

    log.info(f"Processing {len(scenes)}")

    sns_client = boto3.client("sns", region_name=S1_BUCKET_REGION)
    bucket_name = s3_url_parse(S1_BUCKET)[0]
    failed_tasks = []
    for scene in scenes:
        try:
            message_payload = {
                "Records": [
                    {"s3": {"bucket": {"name": bucket_name}, "object": {"key": scene}}}
                ]
            }
            response = sns_client.publish(
                TopicArn=sns_topic_arn,
                Message=json.dumps(message_payload),
            )
        except botocore.exceptions.ClientError as error:
            log.error(error)
            failed_tasks.append(scene)
        else:
            message_id = response["ResponseMetadata"]["RequestId"]
            log.info(
                f"{message_id} Success - SNS " f"for {URL(S1_BUCKET) / scene} sent"
            )

    if failed_tasks:
        raise RuntimeError(f"Failed to process the tasks: {', '.join(failed_tasks)}")
