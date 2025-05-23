import logging
import sys
from textwrap import dedent
from typing import Optional

import click as click
from odc.aws.queue import get_queues

from deafrica import __version__
from deafrica.click_options import slack_url
from deafrica.logs import setup_logging
from deafrica.utils import send_slack_notification


def check_deadletter_queues(
    slack_url: Optional[str] = None, log: Optional[logging.Logger] = None
):
    bad_queue_messages = []
    dead_queues = get_queues(contains="deadletter")
    environment = "Unknown"
    for dead_queue in dead_queues:
        queue_size = int(dead_queue.attributes.get("ApproximateNumberOfMessages", 0))
        if queue_size > 0:
            queue_name = dead_queue.url.split("/")[-1]
            try:
                environment = queue_name.split("-")[1].upper()
            except Exception:
                pass
            bad_queue_messages.append(f"Queue `{queue_name}` has {queue_size} items")

    if len(bad_queue_messages) > 0:
        bad_queues_str = "\n".join(f" - {q}" for q in bad_queue_messages)
        message = dedent(
            f"*Environment*: {environment}\n "
            f"Found {len(bad_queue_messages)} dead queues with messages:\n"
            f"{bad_queues_str}"
        )
        if log is not None:
            log.error(message)
        # Send a Slack message
        if slack_url is not None:
            send_slack_notification(slack_url, "Dead Letter Checker", message)
        sys.exit(1)

    # Exit with 0 if no errors
    sys.exit(0)


@click.command("check-dead-queue")
@slack_url
@click.option("--version", is_flag=True, default=False)
def cli(slack_url, version: bool = False):
    """
    Check all dead queues which the user is allowed to
    """

    if version:
        click.echo(__version__)

    log = setup_logging()
    check_deadletter_queues(slack_url=slack_url, log=log)


if __name__ == "__main__":
    cli()
