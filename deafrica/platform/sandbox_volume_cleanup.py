import boto3
import click as click
from datetime import datetime
from datetime import timedelta
import json
from kubernetes import config, client

from deafrica.utils import setup_logging

log = setup_logging()


def delete_volumes(namespace, dryrun, ebs_tag_filter_debug, tojson):
    """
    Cleanup sandbox unused Volumes, k8s PVs & PVCs by
    looking into CloudTrail "AttachVolume and "DetachVolume" events over the past 90 days
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

    # configure boto3 client
    ec2_resource = boto3.resource("ec2")
    ct_client = boto3.client("cloudtrail")

    time_now = datetime.now()
    time_back = time_now - timedelta(days=90)
    filters = [
        {
            "Name": "tag:kubernetes.io/created-for/pvc/namespace",
            "Values": [
                namespace,
            ],
        },
        {
            "Name": "tag:kubernetes.io/created-for/pvc/name",
            "Values": [
                ebs_tag_filter_debug,
            ],
        },
    ]

    del_count = 0
    ignore_count = 0
    fail_count = 0

    if tojson:
        jsondata = []  # array to store data to write file

    log.info(f"dryrun : {dryrun}")
    for volume in ec2_resource.volumes.filter(Filters=filters):
        # cloud logs only go back 90 days.
        # call last 90 days explicitly incase this changes
        response = ct_client.lookup_events(
            LookupAttributes=[
                {"AttributeKey": "ResourceName", "AttributeValue": volume.id},
            ],
            MaxResults=100,
            StartTime=time_back,
            EndTime=time_now,
        )

        # check both attach and detach events
        attach_events = [
            event
            for event in response["Events"]
            if event.get("EventName") in ["AttachVolume", "DetachVolume"]
        ]

        try:
            # collect some properties
            pv_name, pvc_name = get_user_claim(volume)
            props = {
                "action": None,  # set below, one of [DELETE, DRY_RUN_DELETE, IGNORE]
                "volume_id": volume.id,
                "volume_size": volume.size,
                "volume_state": volume.state,
                "volume_created": volume.create_time.strftime("%Y/%m/%d, %H:%M:%S"),
                "volume_last_attach_detach": None,  # None implies > 90 days. Replaced below if logs exist
                "volume_last_attach_detach_days": None,
                "pv_name": pv_name,
                "pvc_name": pvc_name,
            }

            # Cleanup Volume, k8s PV & PVC
            if len(attach_events) == 0 and volume.state == "available":
                # no attachments in last 90 days and ebs is not in use
                props["action"] = "DELETE" if not dryrun else "DRY_RUN_DELETE"
                log.info(log_string(props))
                if not dryrun:
                    delete(k8s_api, namespace, volume, props)
                del_count += 1

            else:
                # attachments in last 90d or ebs in use
                props["action"] = "IGNORE"
                props["volume_last_attach_detach"] = attach_events[0][
                    "EventTime"
                ].strftime("%Y/%m/%d, %H:%M:%S")
                props["volume_last_attach_detach_days"] = abs(
                    attach_events[0]["EventTime"].replace(tzinfo=None) - time_now
                ).days
                log.info(log_string(props))
                ignore_count += 1

        except Exception as e:
            props["action"] = "FAILED_TO_DELETE"
            log.warning(log_string(props))
            log.exception(e)
            fail_count += 1
            pass

        if tojson:
            jsondata.append(props)

    if dryrun:
        log.info(f"Volumes not deleted on dryrun")
    log.info(f"Total Volumes Deleted -> {del_count}")
    log.info(f"Total Volumes Ignored -> {ignore_count}")
    log.info(f"Total Failed Volume Deletion -> {fail_count}")

    if tojson:
        log.info(f"Saving output to {tojson}")
        with open(tojson, "w") as outfile:
            json.dump(jsondata, outfile)


def delete(k8s_api, namespace, volume, props):
    # cleanup k8s PVC/PV
    if (
        len(
            [
                pvc
                for pvc in k8s_api.list_namespaced_persistent_volume_claim(
                    namespace
                ).items
                if pvc.spec.volume_name == props["pv_name"]
            ]
        )
        > 0
    ):
        log.info(f"Delete PVC: {props['pvc_name']}")
        k8s_api.delete_namespaced_persistent_volume_claim(props["pvc_name"], namespace)
    if (
        len(
            [
                pv
                for pv in k8s_api.list_persistent_volume().items
                if pv.metadata.name == props["pv_name"]
            ]
        )
        > 0
    ):
        log.info(f"Delete PV: {props['pv_name']}")
        k8s_api.delete_persistent_volume(props["pv_name"])

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


def log_string(props):
    """
    create a string to print for logging
    """
    keys = sorted(props.keys())
    print_str = "".join([f"{key} : {props[key]}, " for key in keys])
    return print_str


@click.command("delete-sandbox-volumes")
@click.option(
    "--namespace",
    default="sandbox",
    help="Provide a namespace. default sandbox",
)
@click.option(
    "--dryrun",
    is_flag=True,
    help="Do not run delete, just print the action",
)
@click.option(
    "--ebs-tag-filter-debug",
    default="*",
    help="""
        Add an extra degug filter on default sandbox filter.  
        Filter to delete specific ebs volumes. Default gets all in sandbox namespace.
        The filter is run on ebs tag:kubernetes.io/created-for/pvc/name.
        e.g. claim-alex-2ebradley-* will limit the volume search to such strings.
        useful for testing on specific volumes. 
        """,
)
@click.option(
    "--tojson",
    default="",
    help="Name of .json file for debug. Write ebs actions to a json file.",
)
def cli(namespace, dryrun, ebs_tag_filter_debug, tojson):
    """
    Delete sandbox unused volumes using CloudTrail events
    """
    delete_volumes(namespace, dryrun, ebs_tag_filter_debug, tojson)
