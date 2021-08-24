import logging
import sys
from textwrap import dedent

import boto3
from odc.aws.queue import get_messages, get_queue
import click as click


def get_dead_queues(region) -> set:
    client = boto3.client('sqs', region_name=region)
    queues = client.list_queues()

    if queues.get('QueueUrls') is None:
        logging.info("No queues were found for your user")
        sys.exit(1)

    return set(queue for queue in queues['QueueUrls'] if 'deadletter' in queue)


def check_deadletter_queues(dead_queues, region):
    client = boto3.client('sqs', region_name=region)

    bad_queues = []
    for queue in dead_queues:
        attributes_dict = client.get_queue_attributes(
            QueueUrl=queue,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        queue_size = int(attributes_dict['Attributes'].get("ApproximateNumberOfMessages"))
        if queue_size > 0:
            bad_queues.append(f"SQS deadletter queue {queue} has {queue_size} items on it.")

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
@click.argument("region", type=str, nargs=1)
def cli(region: str):
    """
    Check all dead queues which the user is allowed to
    """

    if not region:
        raise ValueError('Region parameter is required')

    dead_queue_set = get_dead_queues(region=region)
    check_deadletter_queues(dead_queues=dead_queue_set, region=region)


if __name__ == "__main__":
    cli()
