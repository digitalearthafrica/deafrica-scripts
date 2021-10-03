"""
# Read report and generate messages to fill missing scenes

"""
import json
import logging
import sys
from textwrap import dedent
from typing import Optional

import click
from odc.aws.queue import publish_messages, get_queue

from tools.utils import (
    find_latest_report,
    read_report,
    send_slack_notification,
    setup_logging,
    slack_url,
    limit,
)

S3_BUCKET_PATH = "s3://deafrica-landsat/status-report/"


def post_messages(message_list, queue_name: str) -> dict:
    """
    Publish messages

    :param message_list:(list) list of messages
    :param queue_name: (str) queue to be sens to

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

    return {"failed": failed, "sent": sent}


def build_messages(missing_scene_paths, update_stac):
    """ """
    message_list = []
    error_list = []
    for path in missing_scene_paths:
        landsat_product_id = str(path.strip("/").split("/")[-1])
        if not landsat_product_id:
            error_list.append(
                f"It was not possible to build product ID from path {path}"
            )
        message_list.append(
            {
                "Message": {
                    "landsat_product_id": landsat_product_id,
                    "s3_location": str(path),
                    "update_stac": update_stac,
                }
            }
        )

    return {"message_list": message_list, "failed": error_list}


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
    log.info(f"Limit: {scenes_limit if scenes_limit else 'No limit'}")
    log.info(f"Notification URL: {notification_url}")

    environment = "DEV" if "dev" in sync_queue_name else "PDS"

    latest_report = find_latest_report(
        report_folder_path=S3_BUCKET_PATH, contains=landsat, not_contains="orphaned"
    )

    if not latest_report:
        raise RuntimeError("Report not found!")

    update_stac = False
    if "update" in latest_report:
        log.info("FORCED UPDATE FLAGGED!")
        update_stac = True

    log.info("Reading missing scenes from the report")

    missing_scene_paths = read_report(report_path=latest_report, limit=scenes_limit)

    log.info(f"Number of scenes found {len(missing_scene_paths)}")
    log.info(f"Example scenes: {missing_scene_paths[0:10]}")

    returned = build_messages(
        missing_scene_paths=missing_scene_paths, update_stac=update_stac
    )

    messages_to_send = returned["message_list"]

    log.info("Publishing messages")
    result = post_messages(message_list=messages_to_send, queue_name=sync_queue_name)

    error_flag = (
        ":red_circle:" if result["failed"] > 0 or len(returned["failed"]) > 0 else ""
    )

    extra_issues = "\n".join(returned["failed"])
    message = dedent(
        f"{error_flag}*Landsat GAP Filler - {environment}*\n"
        f"Sent Messages: {result['sent']}\n"
        f"Failed Messages: {int(result['failed']) + len(returned['failed'])}\n"
        f"Failed sending: {int(result['failed'])}\n"
        f"Other issues presented: {extra_issues}"
    )

    log.info(message)
    if notification_url is not None and result["sent"] > 0:
        send_slack_notification(notification_url, "Landsat Gap Filler", message)

    if (int(result["failed"]) + len(returned["failed"])) > 0:
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
@limit
@slack_url
@click.command("landsat-gap-filler")
def cli(
    satellite: str,
    sync_queue_name: str = "sync_queue_name",
    limit: int = None,
    slack_url: str = None,
):
    """
    Publish missing scenes
    """

    if limit is not None:
        try:
            limit = int(limit)
        except ValueError:
            raise ValueError(f"Limit {limit} is not valid")

        if limit < 1:
            raise ValueError(f"Limit {limit} lower than 1.")

    fill_the_gap(
        landsat=satellite,
        sync_queue_name=sync_queue_name,
        scenes_limit=limit,
        notification_url=slack_url,
    )
