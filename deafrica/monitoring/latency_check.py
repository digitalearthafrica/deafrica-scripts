"""
# Send slack notification when latency check detects higher than specified latency on Landsat 8/9 and Sentinel 1/2 scenes
"""
import json
import logging
import sys
from textwrap import dedent
from typing import Optional

import datacube
from datetime import date, datetime, timedelta, timezone

import click
import os
import boto3

from deafrica import __version__
from deafrica.utils import (
    send_slack_notification,
    setup_logging,
    slack_url,
)


def latency_check_slack(
    sensor: str,
    exceeded: str,
    notification_url: str = None,
) -> None:
    """
    Function to send a slack message reporting high latency on a given sensor
    :param sensor:(str) satellite name
    :param exceeded: (str)
    :param notification_url:(str) Slack notification URL
    :return:(None)
    """
    log = setup_logging()

    log.info(f"Satellite: {sensor}")
    log.info(f"Exceeded: {exceeded}")
    log.info(f"Notification URL: {notification_url}")

    message = dedent(f"Data Latency Checker - Latency Exceed on {sensor}!\n")
    message += f"Exceeded: {exceeded}\n"

    log.info(message)
    if notification_url is not None:
        send_slack_notification(notification_url, "Data Latency Checker", message)


def s3_latency_check(bucket_name: str, prefix: str) -> Optional[int]:
    """
    Function to check the latency of the latest object in an S3 bucket
    :param bucket_name: (str) Name of the S3 bucket
    :param prefix: (str) Prefix of the objects in the bucket
    :return: (Optional[int]) The S3 latency in days, or None if no objects found
    """
    s3 = boto3.client("s3")

    current_time = datetime.now(timezone.utc)
    latency_threshold = timedelta(days=3)

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    objects = response.get("Contents", [])

    if objects:
        latest_object = max(objects, key=lambda obj: obj["LastModified"])
        last_modified = latest_object["LastModified"]

        elapsed_time = current_time - last_modified

        if elapsed_time < latency_threshold:
            return elapsed_time

    return None


def latency_checker(
    satellite: str = 'ls9_sr',
    latency: int = 3,
    notification_slack_url: str = None,
    bucket_name: str = "deafrica-landsat",
    prefix: str = "collection02/level-2/standard/etm/2023",
) -> int:
    """
    Function to detect and send a slack message to the given URL reporting higher than specified latency on the given sensor
    :param satellite:(str) Name of satellite (product)
    :param latency:(int) Maximum latency for satellite in days
    :param notification_slack_url:(str) Slack notification URL
    :param bucket_name: (str) Name of the S3 bucket
    :param prefix: (str) Prefix of the objects in the bucket
    :return:(None)
    """

    today = date.today()
    date_n_days_ago = today - timedelta(days=latency)

    dc = datacube.Datacube()
    pl = dc.list_products()
    print(satellite)
    print(prefix)
    print(bucket_name)
    print(pl)

    if satellite in pl.name:
        central_lat = 0
        central_lon = 0
        buffer = 90
        lats = (central_lat - buffer, central_lat + buffer)
        lons = (central_lon - buffer, central_lon + buffer)

        query = {
            "x": lons,
            "y": lats,
            "time": (date_n_days_ago, today),
            "group_by": "solar_day",
        }

        ds = dc.find_datasets(product=satellite, **query)
        print("Datasets since ", date_n_days_ago, " : ", len(ds))

        s3_latency = s3_latency_check(bucket_name, prefix)

        if len(ds) <= 0 and s3_latency is not None and s3_latency > latency:
            # Latency exceeded in both Data Cube and S3 bucket
            latency_check_slack(
                sensor=satellite,
                exceeded="Latency exceeded in Data Cube and S3 bucket",
                notification_url=notification_slack_url,
            )
        elif len(ds) <= 0:
            # Latency exceeded in Data Cube
            latency_check_slack(
                sensor=satellite,
                exceeded="Latency exceeded in Data Cube",
                notification_url=notification_slack_url,
            )
        elif s3_latency is not None and s3_latency > latency:
            # Latency exceeded in S3 bucket
            latency_check_slack(
                sensor=satellite,
                exceeded="Latency exceeded in S3 bucket",
                notification_url=notification_slack_url,
            )
        else:
            print("Latency on ", satellite, " valid.")
            return 0
    else:
        print("Invalid Latency/Product!")
        return -1


@click.command("latency-check")
@click.argument(
    "prefix",
    type=str,
    nargs=1,
    required=True,
    default="collection02/level-2/standard/etm/2023",
)
@click.argument(
    "bucket-name",
    type=str,
    nargs=1,
    required=True,
    default="deafrica-landsat",
)
@click.argument(
    "latency",
    type=int,
    nargs=1,
    required=True,
    default=3,
)
@click.argument(
    "satellite",
    type=str,
    nargs=1,
    required=True,
    default="ls9_sr",
)
@slack_url
@click.option("--version", is_flag=True, default=False)
def cli(
    prefix,
    bucket_name,
    latency,
    satellite,
    slack_url,
    version,
):
    """
    Post a high latency warning message on Slack given a latency on a product or satellite

    \b
    PREFIX is the prefix of the objects in the bucket.
    BUCKET_NAME is the name of the S3 bucket.
    LATENCY is the maximum latency for the satellite or product in days. 
    SATELLITE is the name of the satellite or product.
    """

    if version:
        click.echo(__version__)
    res = latency_checker(
        satellite=satellite,
        latency=latency,
        notification_slack_url=slack_url,
        bucket_name=bucket_name,
        prefix=prefix)
