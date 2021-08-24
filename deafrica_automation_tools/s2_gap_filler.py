"""
"""

import json
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict

import click
from odc.aws import s3_fetch, s3_head_object
from odc.aws.queue import get_queue, publish_messages

from deafrica_automation_tools.utils import read_report, find_latest_report

PRODUCT_NAME = "s2_l2a"
S3_BUKET_PATH = 's3://deafrica-sentinel-2/status-report/'
SENTINEL_2_SYNC_SQS_NAME = "deafrica-pds-sentinel-2-sync-scene"


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

    if s3_head_object(url=s3_path) is not None:
        raise ValueError(f"{s3_path} does not exist")

    contents = s3_fetch(url=s3_path, s3=None)
    contents_dict = json.loads(contents)

    attributes = get_common_message_attributes(contents_dict)

    message = {
        "MessageBody": json.dumps(
            {
                "Message": json.dumps(contents_dict),
                "MessageAttributes": attributes
            }
        ),
    }
    return message


def publish_message(files):
    """
    """
    max_workers = 300
    # counter for files that no longer exist
    failed = 0
    sent = 0

    batch = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(prepare_message, s3_path)
            for s3_path in files
        ]

        message_id = 0
        queue = get_queue(queue_name=SENTINEL_2_SYNC_SQS_NAME)
        for future in as_completed(futures):
            try:
                message_dict = future.result()
                message_dict["Id"] = str(message_id)
                batch.append(message_dict)
                if len(batch) == 10:
                    publish_messages(queue=queue, messages=batch)
                    batch = []
                    sent += 10
            except Exception as exc:
                failed += 1
                logging.info(f"File no longer exists: {exc}")

    if len(batch) > 0:
        publish_messages(queue=queue, messages=batch)
        sent += len(batch)
    if failed > 0:
        raise ValueError(f"Total of {failed} files failed, Total of sent messages {sent}")
    logging.info(f"Total of sent messages {sent}")


@click.command("s2-gap-filler")
def cli():
    """
    """
    try:
        latest_report = find_latest_report(report_folder_path=S3_BUKET_PATH)
        files = read_report(report_path=latest_report)
        publish_message(files=files)

    except Exception as error:
        logging.exception(error)
        # print traceback but does not stop execution
        traceback.print_exc()
        raise error


if __name__ == "__main__":
    cli()
