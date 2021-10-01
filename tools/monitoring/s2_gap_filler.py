"""
"""

import json
import logging
import sys
from textwrap import dedent
from typing import Dict, Optional

import click
from odc.aws import s3_fetch, s3_client
from odc.aws.queue import get_queue, publish_messages

from tools.utils import (
    find_latest_report,
    read_report,
    split_list_equally,
    send_slack_notification,
    setup_logging,
    slack_url,
)

PRODUCT_NAME = "s2_l2a"
SOURCE_REGION = "us-west-2"
S3_BUCKET_PATH = "s3://deafrica-sentinel-2/status-report/"


def get_common_message_attributes(stac_doc: Dict) -> Dict:
    """
    Returns common message attributes dict
    :param stac_doc: STAC dict
    :return: common message attributes dict
    """
    msg_attributes = {
        "product": {
            "Type": "String",
            "Value": PRODUCT_NAME,
        }
    }

    date_time = stac_doc.get("properties").get("datetime")
    if date_time:
        msg_attributes["datetime"] = {
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


def prepare_message(scene_paths: list, log: Optional[logging.Logger] = None):
    """
    Prepare a single message for each stac file
    """

    s3 = s3_client(region_name=SOURCE_REGION)

    message_id = 0
    for s3_path in scene_paths:
        try:
            contents = s3_fetch(url=s3_path, s3=s3)
            contents_dict = json.loads(contents)

            attributes = get_common_message_attributes(contents_dict)

            message = {
                "Id": str(message_id),
                "MessageBody": json.dumps(
                    {
                        "Message": json.dumps(contents_dict),
                        "MessageAttributes": attributes,
                    }
                ),
            }
            message_id += 1
            yield message
        except Exception as exc:
            if log:
                log.error(f"{s3_path} does not exist - {exc}")


def send_messages(
    idx: int,
    queue_name: str,
    max_workers: int = 2,
    limit: int = None,
    slack_url: str = None,
) -> None:
    """
    Publish a list of missing scenes to an specific queue and by the end of that it's able to notify slack the result

    :param limit: (int) optional limit of messages to be read from the report
    :param max_workers: (int) total number of pods used for the task. This number is used to split the number of scenes
    equally among the PODS
    :param idx: (int) sequential index which will be used to define the range of scenes that the POD will work with
    :param queue_name: (str) queue to be sens to
    :param slack_url: (str) Optional slack URL in case of you want to send a slack notification
    """
    log = setup_logging()

    latest_report = find_latest_report(
        report_folder_path=S3_BUCKET_PATH, not_contains="orphaned"
    )

    if "update" in latest_report:
        log.info("FORCED UPDATE FLAGGED!")

    log.info(f"Limited: {int(limit) if limit else 'No limit'}")
    log.info(f"Number of workers: {max_workers}")

    files = read_report(report_path=latest_report, limit=limit)

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
    messages = prepare_message(scene_paths=split_list_scenes[idx], log=log)

    queue = get_queue(queue_name=queue_name)

    batch = []
    failed = 0
    sent = 0
    error_list = []
    for message in messages:
        try:
            batch.append(message)
            if len(batch) == 10:
                publish_messages(queue=queue, messages=batch)
                batch = []
                sent += 10
        except Exception as exc:
            failed += 1
            error_list.append(exc)
            batch = []

    if len(batch) > 0:
        publish_messages(queue=queue, messages=batch)
        sent += len(batch)

    environment = "DEV" if "dev" in queue_name else "PDS"
    error_flag = ":red_circle:" if failed > 0 else ""

    message = dedent(
        f"{error_flag}*Sentinel 2 GAP Filler - {environment}*\n"
        f"Sent Messages: {sent}\n"
        f"Failed Messages: {failed}\n"
    )
    if slack_url is not None:
        send_slack_notification(slack_url, "S2 Gap Filler", message)

    log.info(message)

    if failed > 0:
        sys.exit(1)


@click.command("s2-gap-filler")
@click.argument("idx", type=int, nargs=1, required=True)
@click.argument("max_workers", type=int, nargs=1, default=2)
@click.argument(
    "sync_queue_name", type=str, nargs=1, default="deafrica-pds-sentinel-2-sync-scene"
)
@click.option(
    "--limit",
    "-l",
    help="Limit the number of messages to transfer.",
    default=None,
)
@slack_url
def cli(
    idx: int,
    max_workers: int = 2,
    sync_queue_name: str = "deafrica-pds-sentinel-2-sync-scene",
    limit: int = None,
    slack_url: str = None,
):
    """
    Publish missing scenes

    idx: (int) sequential index which will be used to define the range of scenes that the POD will work with

    max_workers: (int) total number of pods used for the task. This number is used to split the number of scenes
    equally among the PODS

    sync_queue_name: (str) Sync queue name

    limit: (str) optional limit of messages to be read from the report

    slack_url: (str) Slack notification channel hook URL
    """

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
        limit=limit,
        max_workers=max_workers,
        slack_url=slack_url,
    )
