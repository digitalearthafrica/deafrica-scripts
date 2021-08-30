import logging
import sys
from textwrap import dedent

import click as click
from odc.aws.queue import get_queues

log = logging.getLogger()
console = logging.StreamHandler()
log.addHandler(console)


def check_deadletter_queues():
    bad_queues = []
    dead_queues = get_queues(contains="deadletter")
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
        log.error(message)
        sys.exit(1)
    else:
        log.info("No messages fond in any dead queue")
        sys.exit(0)


@click.command("check-dead-queue")
def cli():
    """
    Check all dead queues which the user is allowed to
    """

    check_deadletter_queues()


if __name__ == "__main__":
    cli()
