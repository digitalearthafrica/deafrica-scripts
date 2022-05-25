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


def latency_check(notification_slack_url: str = None) -> None:
    """
    Function to detect and send a slack message reporting higher than specified latency on the below sensors
    Latency for Landsat 9: 10 days
    Latency for Landsat 8: 20 days
    Latency for Sentinel 2: 3 days
    Latency for Sentinel 1: 3 days
    :param notification_slack_url:(str) Slack notification URL
    :return:(None)
    """

    today = date.today()
    date_03_days_ago = today - timedelta(days=3)
    date_10_days_ago = today - timedelta(days=10)
    date_20_days_ago = today - timedelta(days=20)

    dc = datacube.Datacube(app="latency_checker")

    central_lat = 0
    central_lon = 0
    buffer = 90
    lats = (central_lat - buffer, central_lat + buffer)
    lons = (central_lon - buffer, central_lon + buffer)

    query = {
        "x": lons,
        "y": lats,
        "time": (date_10_days_ago, today),
        "group_by": "solar_day",
    }
    ds_ls9_sr = dc.find_datasets(product="ls9_sr", **query)
    print("ls9_sr since ", date_10_days_ago, " : ", len(ds_ls9_sr))

    query = {
        "x": lons,
        "y": lats,
        "time": (date_20_days_ago, today),
        "group_by": "solar_day",
    }
    ds_ls8_sr = dc.find_datasets(product="ls8_sr", **query)
    print("ls8_sr since ", date_20_days_ago, " : ", len(ds_ls8_sr))

    query = {
        "x": lons,
        "y": lats,
        "time": (date_03_days_ago, today),
        "group_by": "solar_day",
    }
    ds_s2_l2a = dc.find_datasets(product="s2_l2a", **query)
    print("s2_l2a since ", date_03_days_ago, " : ", len(ds_s2_l2a))

    ds_s1_rtc = dc.find_datasets(product="s1_rtc", **query)
    print("s1_rtc since ", date_03_days_ago, " : ", len(ds_s1_rtc))

    if len(ds_ls9_sr) <= 0:
        latency_check_slack(sensor="Landsat 9", notification_url=notification_slack_url)
    if len(ds_ls8_sr) <= 0:
        latency_check_slack(sensor="Landsat 8", notification_url=notification_slack_url)
    if len(ds_s2_l2a) <= 0:
        latency_check_slack(
            sensor="Sentinel 2", notification_url=notification_slack_url
        )
    if len(ds_s1_rtc) <= 0:
        latency_check_slack(
            sensor="Sentinel 1", notification_url=notification_slack_url
        )


@slack_url
@click.option("--version", is_flag=True, default=False)
@click.command("latency-check")
def cli(
    slack_url: str = None,
    version: bool = False,
):
    """
    Post a high latency warning message on Slack
    """

    if version:
        click.echo(__version__)
    latency_check(notification_slack_url=slack_url)
