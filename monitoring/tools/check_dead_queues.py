import logging
import sys
from textwrap import dedent

import click as click
from odc.aws.queue import get_queues

from monitoring.tools.utils import send_slack_notification, setup_logging

from typing import Optional


def check_deadletter_queues(
    slack_url: Optional[str] = None, log: Optional[logging.Logger] = None
):
    bad_queues = []
    dead_queues = get_queues(contains="deadletter")
    for dead_queue in dead_queues:
        queue_size = int(dead_queue.attributes.get("ApproximateNumberOfMessages", 0))
        if queue_size > 0:
            bad_queues.append(f"Queue {dead_queue.url} has {queue_size} items")

    bad_queues_str = "\n".join(f" * {q}" for q in bad_queues)
    message = dedent(
        f"Found {len(bad_queues)} dead queues with messages:\n{bad_queues_str}"
    )

    if len(bad_queues) > 0:
        if log is not None:
            log.error(message)
        # Send a Slack message
        if slack_url is not None:
            send_slack_notification(slack_url, "Dead Letter Checker", message)
        sys.exit(1)
    else:
        print(f"Found nothing, log is: {log}")
        if log is not None:
            log.info("No messages fond in any dead queue")
        sys.exit(0)


@click.command("check-dead-queue")
@click.option(
    "--slack-url", default=None, help="Slack url to use to send a notification"
)
def cli(slack_url):
    """
    Check all dead queues which the user is allowed to
    """
    log = setup_logging()

    check_deadletter_queues(slack_url=slack_url, log=log)


if __name__ == "__main__":
    cli()
