import boto3
import click as click
from datetime import datetime
from datetime import timedelta
from deafrica.utils import setup_logging
from kubernetes import config, client

# Set log level to info
log = setup_logging()

try:
    config.load_incluster_config()
except config.ConfigException:
    try:
        config.load_kube_config()
    except config.ConfigException:
        log.exception("Could not configure kubernetes python client")

configuration = client.Configuration()

# create an instance of the API class
k8s_api = client.CoreV1Api(client.ApiClient(configuration))
k8s_namespace = "sandbox"

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
            k8s_namespace,
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
            if len(attach_events) == 0 and volume.state == "available":
                # Delete k8s pvc that deletes unused volume
                for tags in volume.tags:
                    if tags["Key"] == "kubernetes.io/created-for/pvc/name":
                        pvc_name = tags["Value"]
                        log.info(
                            f"Deleting PVC {pvc_name} associated to -> {volume.id} ({volume.size} GiB) -> {volume.state})"
                        )
                        if not dryrun:
                            k8s_api.delete_namespaced_persistent_volume_claim(
                                pvc_name, k8s_namespace
                            )
                        count += 1
            else:
                log.info(
                    f"Skip Volume {volume.id} ({volume.size} GiB) -> {volume.state}"
                )
        except Exception as e:
            log.exception(
                f"Failed to Delete Volume {volume.id} ({volume.size} GiB) -> {volume.state}: {e}"
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
