import json
import logging
import ntpath
import os
import sys
from textwrap import dedent
from typing import Dict, Optional

import click
import rasterio
import requests
from odc.aws import s3_client, s3_fetch
from odc.aws.queue import get_queue, publish_messages
from rasterio.session import AWSSession

from deafrica import __version__
from deafrica.click_options import limit, slack_url
from deafrica.logs import setup_logging
from deafrica.utils import (
    find_latest_report,
    read_report_missing_scenes,
    send_slack_notification,
    split_list_equally,
)

SOURCE_REGION = "us-west-2"
S3_BUCKET_PATH = "s3://deafrica-sentinel-2-l2a-c1/status-report/"

import warnings

# supress a FutureWarning from pyproj
warnings.simplefilter(action="ignore", category=FutureWarning)


def get_common_message_attributes(stac_doc: Dict, product_name: str) -> Dict:
    """
    param
        stac_doc (dict): STAC dict
        product_name (str): product name. e.g. s2_l2a

    return:
        (dict): common message attributes dict
    """
    msg_attributes = {
        "product": {
            "Type": "String",
            "Value": product_name,
        }
    }

    date_time = stac_doc.get("properties").get("datetime")
    if date_time:
        msg_attributes["datetime"] = {
            "Type": "String",
            "Value": date_time,
        }
        msg_attributes["start_datetime"] = {
            "Type": "String",
            "Value": date_time,
        }
        msg_attributes["end_datetime"] = {
            "Type": "String",
            "Value": date_time,
        }

    cloud_cover = stac_doc.get("properties").get("eo:cloud_cover")
    if cloud_cover:
        msg_attributes["cloudcover"] = {
            "Type": "Number",
            "Value": str(cloud_cover),
        }

    maturity = stac_doc.get("properties").get("dea:dataset_maturity")
    if maturity:
        msg_attributes["maturity"] = {
            "Type": "String",
            "Value": maturity,
        }

    bbox = stac_doc.get("bbox")
    if bbox and len(bbox) > 3:
        msg_attributes["bbox.ll_lon"] = {
            "Type": "Number",
            "Value": str(bbox[0]),
        }
        msg_attributes["bbox.ll_lat"] = {
            "Type": "Number",
            "Value": str(bbox[1]),
        }
        msg_attributes["bbox.ur_lon"] = {
            "Type": "Number",
            "Value": str(bbox[2]),
        }
        msg_attributes["bbox.ur_lat"] = {
            "Type": "Number",
            "Value": str(bbox[3]),
        }

    return msg_attributes


def prepare_message(
    scene_paths: list, product_name: str, log: Optional[logging.Logger] = None
):
    """
    Prepare a single message for each STAC file.

    yields:
        message: SNS message with STAC document as payload.
    """

    s3 = s3_client(region_name=SOURCE_REGION)

    message_id = 0
    for s3_path in scene_paths:
        try:
            # read the provided STAC document
            contents = s3_fetch(url=s3_path, s3=s3)
            src_stac_doc = json.loads(contents)

            # Handle formatting shifting changes from upstream metadata and collections,
            # so they can be transformed into a STAC document along with message attributes
            # for the SNS message, to be indexed into a consistent DEAfrica product.
            if product_name == "s2_l2a_c1":
                stac_metadata = src_stac_doc
                attributes = get_common_message_attributes(stac_metadata, product_name)

            message = {
                "Id": str(message_id),
                "MessageBody": json.dumps(
                    {
                        "Message": json.dumps(stac_metadata),
                        "MessageAttributes": attributes,
                    }
                ),
            }
            message_id += 1
            yield message
        except Exception as exc:
            if log:
                log.error(f"Error generating message for : {s3_path}")
                log.error(f"{exc}")


def send_messages(
    idx: int,
    queue_name: str,
    max_workers: int = 1,
    product_name: str = "s2_l2a_c1",
    limit: int = None,
    slack_url: str = None,
    dryrun: bool = False,
) -> None:
    """
    Publish a list of missing scenes to an specific queue

    params:
        limit: (int) optional limit of messages to be read from the report
        max_workers: (int) total number of pods used for the task. This number is used to
            split the number of scenes equally among the PODS
        idx: (int) sequential index which will be used to define the range of scenes that the POD will work with
        queue_name: (str) queue for formatted messages to be sent to
        slack_url: (str) Optional slack URL in case of you want to send a slack notification
        dryrun: (bool) if true do not send messages. used for testing.

    returns:
        None.
    """
    log = setup_logging()

    if dryrun:
        log.info("dryrun, messages not sent")

    latest_report = find_latest_report(
        report_folder_path=S3_BUCKET_PATH,
        not_contains="orphaned",
        contains="gap_report",
    )

    log.info("working")
    log.info(f"Latest report: {latest_report}")

    if "update" in latest_report:
        log.info("FORCED UPDATE FLAGGED!")

    log.info(f"Limited: {int(limit) if limit else 'No limit'}")
    log.info(f"Number of workers: {max_workers}")

    files = read_report_missing_scenes(report_path=latest_report, limit=limit)

    log.info(f"Number of scenes found {len(files)}")
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

    messages = prepare_message(
        scene_paths=split_list_scenes[idx], product_name=product_name, log=log
    )

    queue = get_queue(queue_name=queue_name)

    batch = []
    failed = 0
    sent = 0
    error_list = []
    for message in messages:
        try:
            batch.append(message)
            if len(batch) == 10:
                if not dryrun:
                    publish_messages(queue=queue, messages=batch)
                batch = []
                sent += 10
        except Exception as exc:
            failed += 1
            error_list.append(exc)
            batch = []

    if len(batch) > 0:
        if not dryrun:
            publish_messages(queue=queue, messages=batch)
        sent += len(batch)

    environment = "DEV" if "dev" in queue_name else "PDS"
    error_flag = ":red_circle:" if failed > 0 else ""

    message = dedent(
        f"{error_flag}*Sentinel 2 GAP Filler (worker {idx}) - {environment}*\n"
        f"Total messages: {len(files)}\n"
        f"Attempted worker messages prepared: {len(split_list_scenes[idx])}\n"
        f"Failed messages prepared: {len(split_list_scenes[idx]) - sent}\n"
        f"Sent Messages: {sent}\n"
        f"Failed Messages: {failed}\n"
    )
    if (slack_url is not None) and (not dryrun):
        send_slack_notification(slack_url, "S2 Gap Filler", message)

    log.info(message)

    if failed > 0:
        sys.exit(1)


@click.command("s2-c1-gap-filler")
@click.argument("idx", type=int, nargs=1, required=True)
@click.argument("max_workers", type=int, nargs=1, default=1)
@click.argument(
    "sync_queue_name",
    type=str,
    nargs=1,
    default="deafrica-pds-sentinel-2-l2a-c1-sync-scene",
)
@click.argument("product_name", type=str, nargs=1, default="s2_l2a_c1")
@limit
@slack_url
@click.option("--version", is_flag=True, default=False)
@click.option("--dryrun", is_flag=True, default=False)
def cli(
    idx: int,
    max_workers: int = 1,
    sync_queue_name: str = "deafrica-pds-sentinel-2-l2a-c1-sync-scene",
    product_name: str = "s2_l2a_c1",
    limit: int = None,
    slack_url: str = None,
    version: bool = False,
    dryrun: bool = False,
):
    """
    Publish missing scenes. Messages are backfilled for missing products. Missing products will
    therefore be synced and indexed as originally intended.

    params:
        idx: (int) sequential index which will be used to define the range of scenes that the POD will work with
        max_workers: (int) total number of pods used for the task. This number is used to
            split the number of scenes equally among the PODS
        sync_queue_name: (str) Sync queue name
        product_name (str): Product name being indexed. default is s2_l2a_c1.
        limit: (str) optional limit of messages to be read from the report
        slack_url: (str) Slack notification channel hook URL
        version: (bool) echo the scripts version
        dryrun: (bool) if true do not send messages. used for testing.

    """
    if version:
        click.echo(__version__)

    valid_product_name = ["s2_l2a_c1"]
    if product_name not in valid_product_name:
        raise ValueError(f"Product name must be on of {valid_product_name}")

    if limit is not None:
        try:
            limit = int(limit)
        except ValueError:
            raise ValueError(f"Limit {limit} is not valid")

        if limit < 1:
            raise ValueError(f"Limit {limit} lower than 1.")

    # send the right range of scenes for this worker
    send_messages(
        idx=idx,
        queue_name=sync_queue_name,
        max_workers=max_workers,
        product_name=product_name,
        limit=limit,
        slack_url=slack_url,
        dryrun=dryrun,
    )
