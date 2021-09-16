"""
"""

import json
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict

import click
from monitoring.tools.utils import (
    find_latest_report,
    read_report,
    split_list_equally,
)
from monitoring.tools.utils import send_slack_notification, setup_logging
from odc.aws import s3_fetch, s3_head_object, s3_client
from odc.aws.queue import get_queue, publish_messages

PRODUCT_NAME = "s2_l2a"
COGS_REGION = "us-west-2"
S3_BUKET_PATH = "s3://deafrica-sentinel-2/status-report/"


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


def prepare_message(s3_path):
    """
    Prepare a single message for each stac file
    """

    s3 = s3_client(region_name=COGS_REGION)

    if s3_head_object(url=s3_path, s3=s3) is None:
        raise ValueError(f"{s3_path} does not exist")

    contents = s3_fetch(url=s3_path, s3=s3)
    contents_dict = json.loads(contents)

    attributes = get_common_message_attributes(contents_dict)

    message = {
        "MessageBody": json.dumps(
            {"Message": json.dumps(contents_dict), "MessageAttributes": attributes}
        ),
    }
    return message


def publish_message(files: list, queue_name: str, slack_url: str = None) -> str:
    """ """
    max_workers = 300
    # counter for files that no longer exist
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(prepare_message, s3_path) for s3_path in files]
        failed = 0
        sent = 0
        batch = []
        error_list = []
        message_id = 0
        queue = get_queue(queue_name=queue_name)
        for future in as_completed(futures):
            try:
                message_dict = future.result()
                message_dict["Id"] = str(message_id)
                message_id += 1
                batch.append(message_dict)
                if len(batch) == 10:
                    publish_messages(queue=queue, messages=batch)
                    batch = []
                    sent += 10
            except Exception as exc:
                failed += 1
                error_list.append(exc)

    if len(batch) > 0:
        publish_messages(queue=queue, messages=batch)
        sent += len(batch)

    if failed > 0:
        msg = f":red_circle: Total of {failed} files failed, Total of sent messages {sent}"
        if slack_url is not None:
            send_slack_notification(slack_url, "S2 Gap Filler", msg)
        raise ValueError(f"{msg} - {set(error_list)}")

    msg = f"Total messages sent {sent}"
    if slack_url is not None:
        send_slack_notification(slack_url, "S2 Gap Filler", msg)
    return msg


@click.command("s2-gap-filler")
@click.argument("idx", type=int, nargs=1, required=True)
@click.argument("max_workers", type=int, nargs=1, default=2)
@click.argument("sync_queue_name", type=str, nargs=1, default="deafrica-pds-sentinel-2-sync-scene")
@click.option(
    "--limit",
    "-l",
    help="Limit the number of messages to transfer.",
    default=None,
)
@click.option(
    "--slack_url",
    help="Slack url to use to send a notification",
    default=None,
)
def cli(
    idx: int,
    max_workers: int = 2,
    sync_queue_name: str = "deafrica-pds-sentinel-2-sync-scene",
    limit: int = None,
    slack_url: str = None,
):
    """
    Publish missing scenes
    """

    log = setup_logging()

    try:

        if limit is not None:
            try:
                limit = int(limit)
            except ValueError:
                raise ValueError(f"Limit {limit} is not valid")

            if limit < 1:
                raise ValueError(f"Limit {limit} lower than 1.")

        latest_report = find_latest_report(report_folder_path=S3_BUKET_PATH)

        if "update" in latest_report:
            log.info("FORCED UPDATE FLAGGED!")

        log.info(f"Limited: {int(limit) if limit else 'No limit'}")

        files = read_report(report_path=latest_report, limit=limit)

        log.info(f"Number of scenes found {len(files)}")
        log.info(f"Example scenes: {files[0:10]}")

        split_list_scenes = split_list_equally(
            list_to_split=files, num_inter_lists=int(max_workers)
        )

        if len(split_list_scenes) <= idx:
            log.warning("Worker Skipped!")
            sys.exit(0)

        returned = publish_message(
            files=split_list_scenes[idx],
            queue_name=sync_queue_name,
            slack_url=slack_url,
        )
        log.info(returned)
    except Exception as error:
        log.exception(error)
        traceback.print_exc()
        raise error


if __name__ == "__main__":
    cli()
