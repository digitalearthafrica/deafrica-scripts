import boto3
import click as click
from datetime import datetime
from datetime import timedelta
from deafrica.utils import setup_logging

# Set log level to info
log = setup_logging()

ec2_resource = boto3.resource("ec2")
ct_client = boto3.client("cloudtrail")

time_now = datetime.now()
time_back = time_now - timedelta(days=90)
filters = [
    {
        "Name": "tag:kubernetes.io/cluster/deafrica-dev-eks",
        "Values": [
            "owned",
        ],
    },
    {
        "Name": "tag:kubernetes.io/created-for/pvc/namespace",
        "Values": [
            "sandbox",
        ],
    },
]


def delete_volumes(dryrun):
    """
    delete volumes if CloudTrail "AttachVolume" event returns empty in the last 90 days
    """
    count = 0
    for volume in ec2_resource.volumes.filter(Filters=filters):
        response = ct_client.lookup_events(
            LookupAttributes=[
                {"AttributeKey": "ResourceName", "AttributeValue": volume.id},
            ],
            MaxResults=10,
            StartTime=time_back,
            EndTime=time_now,
        )

        attach_events = [
            event
            for event in response["Events"]
            if event.get("EventName") == "AttachVolume"
        ]

        try:
            if len(attach_events) == 0:
                log.info(
                    f"Deleting Volume {volume.id} ({volume.size} GiB) -> {volume.state}"
                )
                count = count + 1
                if not dryrun:
                    volume.delete()
                    log.info(f"Volume successfully deleted")
            else:
                log.info(
                    f"Skip Volume {volume.id} ({volume.size} GiB) -> {volume.state}"
                )
        except:
            log.exception(
                f"Failed to Delete Volume {volume.id} ({volume.size} GiB) -> {volume.state}"
            )
            pass

    log.info(f"Total Volumes Deleted -> {count}")


@click.command("delete-sandbox-volumes")
@click.option(
    "--dryrun",
    is_flag=True,
    help="Do not run delete, just print the action",
)
def cli(dryrun):
    """
    Delete sandbox unused volumes using CloudTrail events
    """
    delete_volumes(dryrun)
