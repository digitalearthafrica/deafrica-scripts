import json
import logging
import sys
from textwrap import dedent
from typing import Dict, Optional
import rasterio
from rasterio.session import AWSSession
import requests
import ntpath
import os

import click
from odc.aws import s3_fetch, s3_client
from odc.aws.queue import get_queue, publish_messages
from stac_sentinel import sentinel_s2_l2a

from deafrica import __version__
from deafrica.utils import (
    find_latest_report,
    read_report_missing_scenes,
    split_list_equally,
    send_slack_notification,
    setup_logging,
    slack_url,
)

SOURCE_REGION = "us-west-2"
S3_BUCKET_PATH = "s3://deafrica-sentinel-2/status-report/"

import warnings

# supress a FutureWarning from pyproj
warnings.simplefilter(action="ignore", category=FutureWarning)


def get_cog_shape_transform(cog_url: str):
    """get the shape and transform of a cog

    Args:
        cog_url (str): url to the AWS hosted COG

    Returns:
        tuple: tuple (shape, transform)
    """

    # Open the COG file from URL
    with rasterio.Env(AWSSession()) as env:
        with rasterio.open(cog_url) as src:
            # Get the shape of the raster
            shape = src.shape
            # Get the projection of the raster
            transform = src.transform

    return shape, transform


def get_common_message_attributes(stac_doc: Dict, product_name: str) -> Dict:
    """
    :param stac_doc: STAC dict
    :return: common message attributes dict
    """
    msg_attributes = {
        "product": {
            "Type": "String",
            "Value": product_name,
        }
    }

    date_time = stac_doc.get("properties").get("datetime")
    if date_time:
        msg_attributes["datetime"] = {
            "Type": "String",
            "Value": date_time,
        }

    cloud_cover = stac_doc.get("properties").get("eo:cloud_cover")
    if cloud_cover:
        msg_attributes["cloudcover"] = {
            "Type": "Number",
            "Value": str(cloud_cover),
        }

    maturity = stac_doc.get("properties").get("dea:dataset_maturity")
    if maturity:
        msg_attributes["maturity"] = {
            "Type": "String",
            "Value": maturity,
        }

    bbox = stac_doc.get("bbox")
    if bbox and len(bbox) > 3:
        msg_attributes["bbox.ll_lon"] = {
            "Type": "Number",
            "Value": str(bbox[0]),
        }
        msg_attributes["bbox.ll_lat"] = {
            "Type": "Number",
            "Value": str(bbox[1]),
        }
        msg_attributes["bbox.ur_lon"] = {
            "Type": "Number",
            "Value": str(bbox[2]),
        }
        msg_attributes["bbox.ur_lat"] = {
            "Type": "Number",
            "Value": str(bbox[3]),
        }

    return msg_attributes


def prepare_s2_l2a_stac(src_stac_doc: Dict):
    """Prepares the appopriate STAC data to send with the sqs message.
    The original json/stac_doc must be modified with the appropriate
    fields. The metadata changes across time which makes this a bit
    messy for capturing known edge cases.

    Args:
        src_stac_doc: Original STAC doc/json from gap report

    Returns:
        stac_metadata: formatted stacmetadata for the sns message
    """

    # change the properties to align with original sqs message
    # read in the tileinfo.json file and create a STAC document
    # using the sentinel_s2_l2a package.
    # This will form the basis of our SNS message body as it is
    # most closely aligned with the original message
    if "tileinfo_metadata" in list(src_stac_doc["assets"].keys()):
        tileinfo_url = src_stac_doc["assets"]["tileinfo_metadata"]["href"]
    else:
        tileinfo_url = src_stac_doc["assets"]["info"]["href"]

    tileinfo = requests.get(tileinfo_url, stream=True)
    tileinfo = json.loads(tileinfo.text)

    # update the base url to generate STAC metadata for source
    base_url = f"https://roda.sentinel-hub.com/sentinel-s2-l2a/{tileinfo['path']}"
    tileinfo_metadata = sentinel_s2_l2a(tileinfo, base_url=base_url)

    # add the links from the tileinfo
    relevant_links = ["self", "canonical", "derived_from"]
    links = [x for x in src_stac_doc["links"] if x["rel"] in relevant_links]

    # update the link path to the origin
    SENTINEL_2_COGS_URL = "https://sentinel-cogs.s3.us-west-2.amazonaws.com"
    for link in links:
        link["href"] = link["href"].replace("s3://sentinel-cogs", SENTINEL_2_COGS_URL)
        if link["rel"] == "derived_from":
            # add the title and data type
            link["title"] = "Source STAC Item"
            link["type"] = "application/json"

    # add links
    tileinfo_metadata["links"] = links

    # get the proj:shape, href, and proj:transform from tileinfo
    asset_dict = {}  # store in a dict for mapping based on file, e.g. B02.tif
    for asset in list(src_stac_doc["assets"].keys()):
        _, asset_file = ntpath.split(src_stac_doc["assets"][asset]["href"])
        if "proj:shape" in src_stac_doc["assets"][asset].keys():
            asset_dict[asset_file] = {
                "proj:shape": src_stac_doc["assets"][asset]["proj:shape"],
                "proj:transform": src_stac_doc["assets"][asset]["proj:transform"],
                "href": src_stac_doc["assets"][asset]["href"],
                "type": src_stac_doc["assets"][asset]["type"],
            }
            # projection format [320, 0, 399960, 0, -320, 3500040] ->
            # [320, 0, 399960, 0, -320, 3500040, 0, 0, 1]
            if len(asset_dict[asset_file]["proj:transform"]) == 6:
                asset_dict[asset_file]["proj:transform"] += [0, 0, 1]

    # add this data to our STAC doccument
    for asset in list(tileinfo_metadata["assets"].keys()):
        _, asset_file = ntpath.split(tileinfo_metadata["assets"][asset]["href"])
        asset_file = asset_file.replace("jp2", "tif")
        # set the proj:shape and proj:transform for each asset
        if asset_file in asset_dict:
            tileinfo_metadata["assets"][asset]["proj:shape"] = asset_dict[asset_file][
                "proj:shape"
            ]
            tileinfo_metadata["assets"][asset]["proj:transform"] = asset_dict[
                asset_file
            ]["proj:transform"]
            tileinfo_metadata["assets"][asset]["href"] = asset_dict[asset_file]["href"]
            tileinfo_metadata["assets"][asset]["type"] = asset_dict[asset_file]["type"]

    # fix the PVI / Overview Asset
    asset_base_url, _ = ntpath.split(tileinfo_metadata["assets"]["B01"]["href"])
    asset_type = tileinfo_metadata["assets"]["B01"]["type"]
    tileinfo_metadata["assets"]["overview"]["href"] = os.path.join(
        asset_base_url, "L2A_PVI.tif"
    )
    tileinfo_metadata["assets"]["overview"]["type"] = asset_type

    # fix the shape/transform for the extra bands
    # for whatever reason, these are incorrect in the src_stac_file...
    # code reaches out to cogs for shape and transform
    for asset in ["overview", "WVP", "AOT"]:
        shape, transform = get_cog_shape_transform(
            tileinfo_metadata["assets"][asset]["href"]
        )
        transform = list(transform) + [0, 0, 1] if len(transform) == 6 else transform
        tileinfo_metadata["assets"][asset]["proj:shape"] = shape
        tileinfo_metadata["assets"][asset]["proj:transform"] = list(transform)

    # remove un-needed assets
    tileinfo_metadata["assets"].pop("visual_20m")
    tileinfo_metadata["assets"].pop("visual_60m")

    # add links for stac_extensions
    stac_ext_links = []
    for ext in tileinfo_metadata["stac_extensions"]:
        for ext_link in src_stac_doc["stac_extensions"]:
            if ext in ext_link:
                stac_ext_links.append(ext_link)
    tileinfo_metadata["stac_extensions"] = stac_ext_links

    # add some extra properties
    # replace cc, (0 in tileinfo). Note this value is slighlty different to the value
    # that comes with the original SNS message, unsure why
    # eo:cloud_cover should be only difference to original STAC SNS
    tileinfo_metadata["properties"]["eo:cloud_cover"] = src_stac_doc["properties"][
        "eo:cloud_cover"
    ]
    tileinfo_metadata["properties"]["sentinel:valid_cloud_cover"] = True
    if "s2:processing_baseline" in list(src_stac_doc["properties"].keys()):
        tileinfo_metadata["properties"]["sentinel:processing_baseline"] = src_stac_doc[
            "properties"
        ]["s2:processing_baseline"]
    else:
        tileinfo_metadata["properties"]["sentinel:processing_baseline"] = src_stac_doc[
            "properties"
        ]["sentinel:processing_baseline"]
    if "earthsearch:boa_offset_applied" in list(src_stac_doc["properties"].keys()):
        tileinfo_metadata["properties"]["sentinel:boa_offset_applied"] = src_stac_doc[
            "properties"
        ]["earthsearch:boa_offset_applied"]
    else:
        tileinfo_metadata["properties"]["sentinel:boa_offset_applied"] = src_stac_doc[
            "properties"
        ]["sentinel:boa_offset_applied"]

    # # update collection to cogs
    tileinfo_metadata["collection"] = "sentinel-s2-l2a-cogs"

    # set the final stac metadata doc
    stac_metadata = tileinfo_metadata

    return stac_metadata


def prepare_message(
    scene_paths: list, product_name: str, log: Optional[logging.Logger] = None
):
    """
    Prepare a single message for each stac file
    """

    s3 = s3_client(region_name=SOURCE_REGION)

    message_id = 0
    for s3_path in scene_paths:
        try:
            contents = s3_fetch(url=s3_path, s3=s3)
            contents_dict = json.loads(contents)

            if product_name == "s2_l2a":
                stac_metadata = prepare_s2_l2a_stac(contents_dict)
                attributes = get_common_message_attributes(stac_metadata, product_name)

            if product_name == "s2_l2a_c1":
                # TODO something different will need to be done here
                attributes = get_common_message_attributes(contents_dict, product_name)
                stac_metadata = contents_dict

            message = {
                "Id": str(message_id),
                "MessageBody": json.dumps(
                    {
                        "Message": json.dumps(stac_metadata),
                        "MessageAttributes": attributes,
                    }
                ),
            }
            message_id += 1
            yield message
        except Exception as exc:
            if log:
                log.error(f"Error generating message for : {s3_path}")
                log.error(f"{exc}")


def send_messages(
    idx: int,
    queue_name: str,
    max_workers: int = 2,
    product_name: str = "s2_l2a",
    limit: int = None,
    slack_url: str = None,
    dryrun: bool = False,
) -> None:
    """
    Publish a list of missing scenes to an specific queue and by the end of that it's able to notify slack the result

    :param limit: (int) optional limit of messages to be read from the report
    :param max_workers: (int) total number of pods used for the task. This number is used to split the number of scenes
    equally among the PODS
    :param idx: (int) sequential index which will be used to define the range of scenes that the POD will work with
    :param queue_name: (str) queue to be sens to
    :param slack_url: (str) Optional slack URL in case of you want to send a slack notification
    """
    log = setup_logging()

    if dryrun:
        log.info("dryrun, messages not sent")

    latest_report = find_latest_report(
        report_folder_path=S3_BUCKET_PATH,
        not_contains="orphaned",
        contains="gap_report",
    )

    log.info("working")
    log.info(f"Latest report: {latest_report}")

    if "update" in latest_report:
        log.info("FORCED UPDATE FLAGGED!")

    log.info(f"Limited: {int(limit) if limit else 'No limit'}")
    log.info(f"Number of workers: {max_workers}")

    files = read_report_missing_scenes(report_path=latest_report, limit=limit)

    log.info(f"Number of scenes found {len(files)}")
    log.info(f"Example scenes: {files[0:10]}")

    # Split scenes equally among the workers
    split_list_scenes = split_list_equally(
        list_to_split=files, num_inter_lists=int(max_workers)
    )

    # In case of the index being bigger than the number of positions in the array, the extra POD isn' necessary
    if len(split_list_scenes) <= idx:
        log.warning(f"Worker {idx} Skipped!")
        sys.exit(0)

    log.info(f"Executing worker {idx}")

    messages = prepare_message(
        scene_paths=split_list_scenes[idx], product_name=product_name, log=log
    )

    queue = get_queue(queue_name=queue_name)

    batch = []
    failed = 0
    sent = 0
    error_list = []
    for message in messages:
        try:
            batch.append(message)
            if len(batch) == 10:
                if not dryrun:
                    publish_messages(queue=queue, messages=batch)
                batch = []
                sent += 10
        except Exception as exc:
            failed += 1
            error_list.append(exc)
            batch = []

    if len(batch) > 0:
        if not dryrun:
            publish_messages(queue=queue, messages=batch)
        sent += len(batch)

    environment = "DEV" if "dev" in queue_name else "PDS"
    error_flag = ":red_circle:" if failed > 0 else ""

    message = dedent(
        f"{error_flag}*Sentinel 2 GAP Filler - {environment}*\n"
        f"Attempted messages prepared: {len(files)}\n"
        f"Failed messages prepared: {len(files) - sent}\n"
        f"Sent Messages: {sent}\n"
        f"Failed Messages: {failed}\n"
    )
    if (slack_url is not None) and (not dryrun):
        send_slack_notification(slack_url, "S2 Gap Filler", message)

    log.info(message)

    if failed > 0:
        sys.exit(1)


@click.command("s2-gap-filler")
@click.argument("idx", type=int, nargs=1, required=True)
@click.argument("max_workers", type=int, nargs=1, default=2)
@click.argument(
    "sync_queue_name", type=str, nargs=1, default="deafrica-pds-sentinel-2-sync-scene"
)
@click.argument("product_name", type=str, nargs=1, default="s2_l2a")
@click.option(
    "--limit",
    "-l",
    help="Limit the number of messages to transfer.",
    default=None,
)
@slack_url
@click.option("--version", is_flag=True, default=False)
@click.option("--dryrun", is_flag=True, default=False)
def cli(
    idx: int,
    max_workers: int = 2,
    sync_queue_name: str = "deafrica-pds-sentinel-2-sync-scene",
    product_name: str = "s2_l2a",
    limit: int = None,
    slack_url: str = None,
    version: bool = False,
    dryrun: bool = False,
):
    """
    Publish missing scenes

    idx: (int) sequential index which will be used to define the range of scenes that the POD will work with

    max_workers: (int) total number of pods used for the task. This number is used to split the number of scenes
    equally among the PODS

    sync_queue_name: (str) Sync queue name

    limit: (str) optional limit of messages to be read from the report

    slack_url: (str) Slack notification channel hook URL
    """
    if version:
        click.echo(__version__)

    valid_product_name = ["s2_l2a", "s2_l2a_c1"]
    if product_name not in valid_product_name:
        raise ValueError(f"Product name must be on of {valid_product_name}")

    if limit is not None:
        try:
            limit = int(limit)
        except ValueError:
            raise ValueError(f"Limit {limit} is not valid")

        if limit < 1:
            raise ValueError(f"Limit {limit} lower than 1.")

    # send the right range of scenes for this worker
    send_messages(
        idx=idx,
        queue_name=sync_queue_name,
        max_workers=max_workers,
        product_name=product_name,
        limit=limit,
        slack_url=slack_url,
        dryrun=dryrun,
    )
