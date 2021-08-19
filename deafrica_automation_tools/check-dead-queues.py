from textwrap import dedent

import boto3
import click as click


def check_deadletter_queues():
    client = boto3.client('sqs')
    queues = client.list_queues()

    if queues.get('QueueUrls') is None:
        print("No queues found")

    bad_queues = []
    for queue in queues['QueueUrls']:
        if 'deadletter' in queue:
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
        raise Exception(message)


@click.command("check-dead-queue")
def cli():
    """
    Check all dead queues which the user is allowed to
    """
    check_deadletter_queues()


if __name__ == "__main__":
    check_deadletter_queues()
