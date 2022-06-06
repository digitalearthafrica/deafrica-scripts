"""
# Send slack notification when latency check detects higher than specified latency on Landsat 8/9 and Sentinel 1/2 scenes
"""
import json
import logging
import sys
from textwrap import dedent
from typing import Optional

import datacube
from datetime import date, timedelta

import click

from deafrica import __version__
from deafrica.utils import (
    send_slack_notification,
    setup_logging,
    slack_url,
)


def latency_check_slack(
    sensor: str,
    notification_url: str = None,
) -> None:
    """
    Function to send a slack message reporting high latency on a given sensor
    :param sensor:(str) satellite name
    :param notification_url:(str) Slack notification URL
    :return:(None)
    """
    log = setup_logging()

    log.info(f"Satellite: {sensor}")
    log.info(f"Notification URL: {notification_url}")

    message = dedent(f"Data Latency Checker - Latency Exceed on {sensor}!\n")

    log.info(message)
    if notification_url is not None:
        send_slack_notification(notification_url, "Data Latency Checker", message)


def latency_checker(
    satellite: str, latency: int = 3, notification_slack_url: str = None
) -> int:
    """
    Function to detect and send a slack message to the given URL reporting higher than specified latency on the given sensor
    :param satellite:(str) Name of satellite (product)
    :param latency:(int) Maximum latency for satellite in days
    :param notification_slack_url:(str) Slack notification URL
    :return:(None)
    """

    if latency > 0:
        today = date.today()
        date_n_days_ago = today - timedelta(days=latency)

        dc = datacube.Datacube(app="latency_checker")
        pl = dc.list_products()

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

        if satellite in pl.name:
            ds = dc.find_datasets(product=satellite, **query)
            print("Datasets since ", date_n_days_ago, " : ", len(ds))

            if len(ds) <= 0:
                latency_check_slack(
                    sensor=satellite, notification_url=notification_slack_url
                )
            else:
                print("Latency on ", satellite, " valid.")
            return 0
        else:
            print("Invalid Product!")
            return -1
    else:
        print("Invalid Latency!")
        return -1


@click.argument(
    "satellite",
    type=str,
    nargs=1,
    required=True,
    default="satellite or product name",
)
@click.argument(
    "latency",
    type=int,
    nargs=1,
    required=True,
    default=3,
)
@slack_url
@click.option("--version", is_flag=True, default=False)
@click.command("latency-check")
def cli(
    satellite: str = None,
    latency: int = 3,
    slack_url: str = None,
    version: bool = False,
):
    """
    Post a high latency warning message on Slack given a latency on a product or satellite
    """

    if version:
        click.echo(__version__)
    res = latency_checker(
        satellite=satellite, latency=latency, notification_slack_url=slack_url
    )
