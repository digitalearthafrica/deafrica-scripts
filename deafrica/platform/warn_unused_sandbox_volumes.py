import boto3
import click as click
from datetime import datetime
from datetime import timedelta
import os

from deafrica.utils import setup_logging

log = setup_logging()


class HTTPError(Exception):
    pass


def get_all_cognito_users(cognito_client, cluster_name):
    """get a list of all deafrica users from cognito"""
    log.info("Getting users from cognito")
    users = []
    next_page = None
    if cluster_name == "deafrica-prod-af-eks":
        kwargs = {"UserPoolId": "us-west-2_v9nJrst3o"}  # prod
    elif cluster_name == "deafrica-dev-eks":
        kwargs = {"UserPoolId": "us-west-2_MuvLWwMvg"}  # dev
    else:
        raise NameError(f"cognito group for cluster unknown : {cluster_name}")

    users_remain = True
    while users_remain:
        if next_page:
            kwargs["PaginationToken"] = next_page
        response = cognito_client.list_users(**kwargs)
        users.extend(response["Users"])
        next_page = response.get("PaginationToken", None)
        users_remain = next_page is not None
    return users


def find_cognito_user(all_cognito_users, pvc_email):
    """find cognito user based on pvc email"""
    for user in all_cognito_users:
        email_address = None
        name = None
        for attribute in user["Attributes"]:
            if attribute["Name"] == "email":
                email_address = attribute["Value"]
        if email_address == pvc_email:
            for attribute in user["Attributes"]:
                if attribute["Name"] == "name":
                    name = attribute["Value"]
            break

    return name, email_address


def get_user_claim(volume):
    """get the pvc and pv from volume tags"""
    pvc_name = ""
    pv_name = ""
    for tags in volume.tags:
        if tags["Key"] == "kubernetes.io/created-for/pvc/name":
            pvc_name = tags["Value"]
        if tags["Key"] == "kubernetes.io/created-for/pv/name":
            pv_name = tags["Value"]
    return pv_name, pvc_name


def log_string(props):
    """create a string to print for logging"""
    keys = sorted(props.keys())
    print_str = "".join([f"{key} : {props[key]}, " for key in keys])
    return print_str


def send_warning_email(
    ses_client,
    cluster_name,
    days_to_delete,
    BccAddresses,
    CcAddresses=[],
    ToAddresses=[],
    SourceAdress="systems@digitalearthafrica.org",
):
    """
    send an email warning for impending deletion.
    """

    if cluster_name == "deafrica-prod-af-eks":
        env_str1 = " "
        env_str2 = " "
    elif cluster_name == "deafrica-dev-eks":
        # add strings for the development server for internal users
        env_str1 = " Internal Development "
        env_str2 = " at https://sandbox.dev.digitalearth.africa "

    response = ses_client.send_email(
        Destination={
            "BccAddresses": BccAddresses,
            "CcAddresses": CcAddresses,
            "ToAddresses": ToAddresses,
        },
        Message={
            "Body": {
                "Html": {
                    "Charset": "utf-8",
                    "Data": f"Good Day. \n Your{env_str1}Digital Earth Africa Sandbox Volume will be scheduled for deletion in {days_to_delete} days! Please login to your DE Africa Sandbox{env_str2}to prevent your data being lost.",
                },
                "Text": {
                    "Charset": "utf-8",
                    "Data": f"Good Day. \n Your{env_str1}Digital Earth Africa Sandbox  Volume will be scheduled for deletion in {days_to_delete} days! Please login to your DE Africa Sandbox{env_str2}to prevent your data being lost.",
                },
            },
            "Subject": {
                "Charset": "utf-8",
                "Data": f"Warning - Your{env_str1}Digital Earth Africa Sandbox Volume will be scheduled for deletion in {days_to_delete} days!",
            },
        },
        Source=SourceAdress,
    )

    if int(response["ResponseMetadata"]["HTTPStatusCode"]) != 200:
        raise HTTPError(f"{days_to_delete} day warning email failed to send")
    else:
        log.info(f"{days_to_delete} day warning email successfully sent")


def warn_unused_sandbox_volumes(cluster_name, cron_schedule, dryrun):

    log.info(f"dryrun : {dryrun}")
    # configure boto3 clients
    ec2_resource = boto3.resource("ec2")
    ct_client = boto3.client("cloudtrail")
    ses_client = boto3.client("ses")

    k8s_namespace = "sandbox"
    daysback = 90

    time_now = datetime.now()
    time_back = time_now - timedelta(days=daysback)
    filters = filters = [
        {
            "Name": "tag:kubernetes.io/created-for/pvc/namespace",
            "Values": [
                k8s_namespace,
            ],
        },
        {
            "Name": "tag:kubernetes.io/created-for/pvc/name",
            "Values": [
                "*",
            ],
        },
    ]

    # get all users from cogneto
    os.environ["AWS_DEFAULT_REGION"] = "us-west-2"
    cognito_client = boto3.client("cognito-idp")
    all_cognito_users = get_all_cognito_users(cognito_client, cluster_name)

    volume_warnings = []  # collect some information about the volumes
    count = 0

    for volume in ec2_resource.volumes.filter(Filters=filters):
        count += 1
        response = ct_client.lookup_events(
            LookupAttributes=[
                {"AttributeKey": "ResourceName", "AttributeValue": volume.id},
            ],
            MaxResults=100,
            StartTime=time_back,
            EndTime=time_now,
        )

        attach_events = [
            event
            for event in response["Events"]
            if event.get("EventName") in ["AttachVolume", "DetachVolume"]
        ]

        if len(attach_events) == 0 and volume.state == "available":
            # no attachments in last 90 days and ebs is not in use
            volume_last_attach_detach = None
            volume_last_attach_detach_delta = None
        if len(attach_events) > 0:
            # attach events in the last 90 days
            volume_last_attach_detach = attach_events[0]["EventTime"]
            volume_last_attach_detach_delta = (
                datetime.now(volume_last_attach_detach.tzinfo)
                - volume_last_attach_detach
            )

        # get the persistant volume (pv) and persistant volume claim (pvc)
        pv_name, pvc_name = get_user_claim(volume)
        # get the email from the pvc
        pvc_email = pvc_name[6:].replace("-40", "@").replace("-2e", ".")
        # find the user for email
        user, user_email = find_cognito_user(all_cognito_users, pvc_email)

        # get number of days to delete (i.e. when volume is 90 days unused)
        time_to_delete = (
            timedelta(days=90) - volume_last_attach_detach_delta
            if volume_last_attach_detach is not None
            else timedelta(days=0)
        )

        # note, the deletion script may be run on a different schedule (e.g. monthly)
        # volumes will therefore last longer than the warning states. hence warning acts as a minimum
        if cron_schedule == "daily":
            # emails are being sent daily, therefore we collect users with exactly
            # 5 and 30 days until deletion
            if int(time_to_delete.days) == 5:
                action = "5 Day Warning"
            elif int(time_to_delete.days) == 30:
                action = "30 Day Warning"
            else:
                action = None
        elif cron_schedule == "weekly":
            # emails are being sent weekly, therefore warn between (5 to <12) days for
            # 5 day warning, (30 to < 37 days) days for 30 day warning
            if (int(time_to_delete.days) >= 5) and (int(time_to_delete.days) < 12):
                action = "5 Day Warning"
            elif (int(time_to_delete.days) >= 30) and (int(time_to_delete.days) < 37):
                action = "30 Day Warning"
            else:
                action = None

        props = {
            "action": action,
            "volume_id": volume.id,
            "volume_last_attach_detach": volume_last_attach_detach,
            "time_to_delete": time_to_delete,
            "days_to_delete": time_to_delete.days,
            "user": user,
            "user_email": user_email,
        }

        volume_warnings.append(props)
        log.info(log_string(props))

    if not dryrun:

        # send a 5 day warning to users
        five_day_emails = [
            user["user_email"]
            for user in volume_warnings
            if user["action"] == "5 Day Warning"
        ]
        log.info("Users receiving 5 day warning emails")
        log.info(five_day_emails)
        send_warning_email(
            ses_client, cluster_name, days_to_delete=5, BccAddresses=five_day_emails
        )

        # send a 30 day warning to uesers
        thirty_day_emails = [
            user["user_email"]
            for user in volume_warnings
            if user["action"] == "30 Day Warning"
        ]
        log.info("Users receiving 30 day warning emails")
        log.info(thirty_day_emails)
        send_warning_email(
            ses_client, cluster_name, days_to_delete=30, BccAddresses=thirty_day_emails
        )


@click.command("warn-unused-volumes")
@click.option(
    "--cluster-name",
    default="deafrica-dev-eks",
    help="Provide the cluster name. default to dev cluster",
)
@click.option(
    "--cron-schedule",
    help="""
    How often this script is run to warn users. Protection to not spam
    users. Must be 'daily' or 'weekly'. 
    """,
)
@click.option(
    "--dryrun",
    is_flag=True,
    help="Do not send emails, just print the action",
)
def cli(cluster_name, cron_schedule, dryrun):
    """
    Warn sandbox unused volume owners via email
    """
    assert cron_schedule in [
        "daily",
        "weekly",
    ], '--cron-schedule must be "daily" or "weekly"'
    warn_unused_sandbox_volumes(cluster_name, cron_schedule, dryrun)
