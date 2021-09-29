"""
# Read report and generate messages to fill missing scenes

"""
import json
import logging
import sys
import traceback
from typing import Optional

import click
from odc.aws.queue import publish_messages, get_queue

from tools.utils.utils import (
    find_latest_report,
    read_report,
    send_slack_notification,
    setup_logging,
    slack_url,
)

S3_BUCKET_PATH = "s3://deafrica-landsat/status-report/"


def post_messages(
    message_list, queue_name: str, log: Optional[logging.Logger] = None
) -> dict:
    """
    Publish messages

    :param message_list:(list) list of messages
    :param queue_name: (str) queue to be sens to
    :param log: (str) log
    :return:(None)
    """

    count = 0
    messages = []
    error_list = []
    failed = 0
    sent = 0
    queue = get_queue(queue_name=queue_name)

    logging.info("Sending messages")
    for message_dict in message_list:
        try:
            message = {
                "Id": str(count),
                "MessageBody": str(json.dumps(message_dict)),
            }

            messages.append(message)
            count += 1

            # Send 10 messages per time
            if count % 10 == 0:
                publish_messages(queue, messages)
                messages = []
                sent += 10
        except Exception as exc:
            failed += 1
            error_list.append(exc)
            messages = []

    # Post the last messages if there are any
    if len(messages) > 0:
        sent += len(messages)
        publish_messages(queue, messages)

    msg = f"Total messages sent {sent}"
    if failed > 0:
        msg = f":red_circle: Total of {failed} files failed, Total of sent messages {sent}"
        if log:
            log.error(f"{set(error_list)}")

    return {"msg": msg, "fail": failed > 0}


def build_message(missing_scene_paths, update_stac):
    """ """
    message_list = []
    for path in missing_scene_paths:
        landsat_product_id = str(path.strip("/").split("/")[-1])
        if not landsat_product_id:
            raise Exception(f"It was not possible to build product ID from path {path}")
        message_list.append(
            {
                "Message": {
                    "landsat_product_id": landsat_product_id,
                    "s3_location": str(path),
                    "update_stac": update_stac,
                }
            }
        )
    return message_list


def fill_the_gap(
    landsat: str,
    sync_queue_name: str,
    scenes_limit: Optional[int] = None,
    notification_url: str = None,
) -> None:
    """
    Function to retrieve the latest gap report and create messages to the filter queue process.

    :param landsat:(str) satellite name
    :param sync_queue_name:(str) Queue name
    :param scenes_limit:(int) limit of how many scenes will be filled
    :param notification_url:(str) Slack notification URL
    :return:(None)
    """
    log = setup_logging()

    log.info(f"Satellite: {landsat}")
    log.info(f"Queue: {sync_queue_name}")
    log.info(f"Limited: {int(scenes_limit) if scenes_limit else 'No limit'}")
    log.info(f"Notification URL: {notification_url}")

    latest_report = find_latest_report(report_folder_path=S3_BUCKET_PATH)

    if not latest_report:
        logging.error("Report not found")
        raise RuntimeError("Report not found!")

    update_stac = False
    if "update" in latest_report:
        log.info("FORCED UPDATE FLAGGED!")
        update_stac = True

    log.info("Reading missing scenes from the report")

    missing_scene_paths = read_report(report_path=latest_report, limit=scenes_limit)

    log.info(f"Number of scenes found {len(missing_scene_paths)}")
    log.info(f"Example scenes: {missing_scene_paths[0:10]}")

    messages_to_send = build_message(
        missing_scene_paths=missing_scene_paths, update_stac=update_stac
    )

    log.info("Publishing messages")
    result = post_messages(
        message_list=messages_to_send, queue_name=sync_queue_name, log=log
    )

    log.info(result["msg"])
    if result["fail"]:
        if slack_url is not None:
            send_slack_notification(slack_url, "Landsat Gap Filler", result["msg"])
        sys.exit(1)


@click.argument(
    "satellite",
    type=str,
    nargs=1,
    required=True,
    default="satellite to be compared, supported ones (landsat_8, landsat_7, landsat_5)",
)
@click.argument(
    "sync_queue_name",
    type=str,
    nargs=1,
    required=True,
)
@click.option(
    "--limit",
    "-l",
    help="Limit the number of messages to transfer.",
    default=None,
)
@slack_url
@click.command("landsat-gap-filler")
def cli(
    satellite: str,
    sync_queue_name: str = "deafrica-pds-sentinel-2-sync-scene",
    limit: int = None,
    slack_url: str = None,
):
    """
    Publish missing scenes
    """

    fill_the_gap(
        landsat=satellite,
        sync_queue_name=sync_queue_name,
        scenes_limit=int(limit),
        notification_url=slack_url,
    )
