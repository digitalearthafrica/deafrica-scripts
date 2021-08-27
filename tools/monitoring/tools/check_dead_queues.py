import logging
import sys
from textwrap import dedent

import click as click
from odc.aws.queue import get_queues


def get_dead_queues():
    return get_queues(contains="deadletter")


def check_deadletter_queues(dead_queues):
    bad_queues = []
    for dead_queue in dead_queues:
        queue_size = int(dead_queue.attributes.get("ApproximateNumberOfMessages", 0))
        if queue_size > 0:
            bad_queues.append(
                f"SQS deadletter queue {dead_queue.url} has {queue_size} items on it."
            )

    bad_queues_str = "\n".join(f" * {q}" for q in bad_queues)
    message = dedent(
        f"""
            Found {len(bad_queues)} dead queues that have messages on them.
            These are the culprits:
            {bad_queues_str}
        """
    )

    if len(bad_queues) > 0:
        logging.info(message)
        sys.exit(1)
    else:
        logging.info("No messages fond in any dead queue")
        sys.exit(0)


@click.command("check-dead-queue")
def cli():
    """
    Check all dead queues which the user is allowed to
    """

    dead_queue_set = get_dead_queues()
    check_deadletter_queues(dead_queues=dead_queue_set)


if __name__ == "__main__":
    cli()
