import boto3
import click as click
from datetime import datetime
from datetime import timedelta
from deafrica.utils import setup_logging
from kubernetes import config, client

# Set log level to info
log = setup_logging()


def delete_volumes(cluster_name, dryrun):
    """
    Cleanup sandbox unused Volumes, k8s PVs & PVCs by
    looking into CloudTrail "AttachVolume" events over the past 90 days
    """
    # configure kubernetes API client
    try:
        config.load_incluster_config()
    except config.ConfigException:
        try:
            config.load_kube_config()
        except config.ConfigException:
            log.exception("Could not configure kubernetes python client")
    k8s_api = client.CoreV1Api()
    k8s_namespace = "sandbox"

    # configure boto3 client
    ec2_resource = boto3.resource("ec2")
    ct_client = boto3.client("cloudtrail")

    time_now = datetime.now()
    time_back = time_now - timedelta(days=90)
    filters = [
        {
            "Name": f"tag:kubernetes.io/cluster/{cluster_name}",
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
        {
            "Name": "tag:Name",
            "Values": [
                "kubernetes-dynamic-pvc-*",
            ],
        },
    ]
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
            # Cleanup Volume, k8s PV & PVC
            if len(attach_events) == 0 and volume.state == "available":
                delete(dryrun, k8s_api, k8s_namespace, volume)
                count += 1
            else:
                pv_name, pvc_name = get_user_claim(volume)
                log.info(
                    f"Skip PVC {pvc_name}, PV {pv_name} and EBS volume {volume.id} ({volume.size} GiB) -> {volume.state})"
                )
        except Exception as e:
            log.exception(
                f"Failed to Delete Volume {volume.id} ({volume.size} GiB) -> {volume.state}: {e}"
            )
            pass

    log.info(f"Total Volumes Deleted -> {count}")


def delete(dryrun, k8s_api, k8s_namespace, volume):
    pv_name, pvc_name = get_user_claim(volume)
    log.info(
        f"Deleting PVC {pvc_name}, PV {pv_name} and EBS volume {volume.id} ({volume.size} GiB) -> {volume.state})"
    )
    if not dryrun:
        # cleanup k8s PVC/PV
        if (
            len(
                [
                    pvc
                    for pvc in k8s_api.list_namespaced_persistent_volume_claim(
                        k8s_namespace
                    ).items
                    if pvc.spec.volume_name == pv_name
                ]
            )
            > 0
        ):
            log.info(f"Delete PVC: {volume.id}")
            k8s_api.delete_namespaced_persistent_volume_claim(pvc_name, k8s_namespace)
        elif (
            len(
                [
                    pv
                    for pv in k8s_api.list_persistent_volume().items
                    if pv.metadata.name == pv_name
                ]
            )
            > 0
        ):
            log.info(f"Delete PV: {pv_name}")
            k8s_api.delete_persistent_volume(pv_name)

        # cleanup volume
        # NOTE: k8s ebs storageclass volume reclaimPolicy:retain so explicit cleanup required
        log.info(f"Delete volume: {volume.id}")
        volume.delete()
        log.info("Deletion Completed Successfully")


def get_user_claim(volume):
    pvc_name = ""
    pv_name = ""
    for tags in volume.tags:
        if tags["Key"] == "kubernetes.io/created-for/pvc/name":
            pvc_name = tags["Value"]
        if tags["Key"] == "kubernetes.io/created-for/pv/name":
            pv_name = tags["Value"]
    return pv_name, pvc_name


@click.command("delete-sandbox-volumes")
@click.option(
    "--cluster-name",
    default="deafrica-dev-eks",
    help="Provide a cluster name e.g. deafrica-dev-eks",
)
@click.option(
    "--dryrun",
    is_flag=True,
    help="Do not run delete, just print the action",
)
def cli(cluster_name, dryrun):
    """
    Delete sandbox unused volumes using CloudTrail events
    """
    delete_volumes(cluster_name, dryrun)