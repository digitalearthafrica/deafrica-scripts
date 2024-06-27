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
        tuple: (shape, transform)
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
    param
        stac_doc (dict): STAC dict
        product_name (str): product name. e.g. s2_l2a

    return:
        (dict): common message attributes dict
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
    The provided json/stac_doc must be modified with the appropriate
    fields as it no longer contains all necessary info to form the STAC
    document needed for indexing into DEAfrica. The external tileinfo.json
    is used in combination with the sentinel_s2_l2a package to create a new STAC
    metadata doc that closely aligns with the formatting provided in the original
    provider SQS message. Data such as links, tif shapes and properties
    must also be corrected.

    Args:
        src_stac_doc: Source STAC doc/json from gap report

    Returns:
        stac_metadata: formatted stac metadata for the sns message. This
        will be delivered to the sqs queue to kick off the deafrica
        indexing procedure.
    """

    # change the properties to align with the sqs message provided
    # by the upstream provider. Read in the tileinfo.json file and
    # create a new STAC document using the sentinel_s2_l2a package.
    # This will form the basis of our SNS message body
    if "tileinfo_metadata" in list(src_stac_doc["assets"].keys()):
        # get the url to the tileinfo file
        tileinfo_url = src_stac_doc["assets"]["tileinfo_metadata"]["href"]
    else:
        tileinfo_url = src_stac_doc["assets"]["info"]["href"]

    tileinfo = requests.get(tileinfo_url, stream=True)
    tileinfo = json.loads(tileinfo.text)

    # update the base url to generate STAC new metadata for the message
    base_url = f"https://roda.sentinel-hub.com/sentinel-s2-l2a/{tileinfo['path']}"
    new_stac_doc = sentinel_s2_l2a(tileinfo, base_url=base_url)

    # get and edit the relevant links from the src_stac_doc file
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

    # add links to the new stac doc
    new_stac_doc["links"] = links

    # get the proj:shape, href, and proj:transform from the source stac Doc
    # store in a dict for mapping based on file name, e.g. B02.tif
    asset_dict = {}
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

    # add this data to our new STAC document
    for asset in list(new_stac_doc["assets"].keys()):
        _, asset_file = ntpath.split(new_stac_doc["assets"][asset]["href"])
        asset_file = asset_file.replace("jp2", "tif")
        # set the proj:shape and proj:transform for each asset
        if asset_file in asset_dict:
            new_stac_doc["assets"][asset]["proj:shape"] = asset_dict[asset_file][
                "proj:shape"
            ]
            new_stac_doc["assets"][asset]["proj:transform"] = asset_dict[asset_file][
                "proj:transform"
            ]
            new_stac_doc["assets"][asset]["href"] = asset_dict[asset_file]["href"]
            new_stac_doc["assets"][asset]["type"] = asset_dict[asset_file]["type"]

    # fix the PVI / Overview Asset
    asset_base_url, _ = ntpath.split(new_stac_doc["assets"]["B01"]["href"])
    asset_type = new_stac_doc["assets"]["B01"]["type"]
    new_stac_doc["assets"]["overview"]["href"] = os.path.join(
        asset_base_url, "L2A_PVI.tif"
    )
    new_stac_doc["assets"]["overview"]["type"] = asset_type

    # TODO this can be removed if fixed by provider
    # fix the shape/transform for some extra bands
    # for whatever reason, these are incorrect in the src_stac_file...
    # code makes a requeststo actual cogs for shape and transform
    for asset in ["overview", "WVP", "AOT"]:
        shape, transform = get_cog_shape_transform(
            new_stac_doc["assets"][asset]["href"]
        )
        # reformat the transform if needed
        transform = list(transform) + [0, 0, 1] if len(transform) == 6 else transform
        new_stac_doc["assets"][asset]["proj:shape"] = shape
        new_stac_doc["assets"][asset]["proj:transform"] = list(transform)

    # remove un-needed assets
    new_stac_doc["assets"].pop("visual_20m")
    new_stac_doc["assets"].pop("visual_60m")

    # add links for stac_extensions
    stac_ext_links = []
    for ext in new_stac_doc["stac_extensions"]:
        for ext_link in src_stac_doc["stac_extensions"]:
            if ext in ext_link:
                stac_ext_links.append(ext_link)
    new_stac_doc["stac_extensions"] = stac_ext_links

    # add/edit some extra properties
    # replace cloud cover as it is fixed at 0 in tileinfo file.
    # We replace it with the value from the src stac document.
    # Note this value is different to what is provided in th original
    # SNS message from the provider. It is unsure where this difference originates.
    # eo:cloud_cover should be only difference from the provider STAC SNS
    new_stac_doc["properties"]["eo:cloud_cover"] = src_stac_doc["properties"][
        "eo:cloud_cover"
    ]
    new_stac_doc["properties"]["sentinel:valid_cloud_cover"] = True
    if "s2:processing_baseline" in list(src_stac_doc["properties"].keys()):
        new_stac_doc["properties"]["sentinel:processing_baseline"] = src_stac_doc[
            "properties"
        ]["s2:processing_baseline"]
    else:
        new_stac_doc["properties"]["sentinel:processing_baseline"] = src_stac_doc[
            "properties"
        ]["sentinel:processing_baseline"]
    if "earthsearch:boa_offset_applied" in list(src_stac_doc["properties"].keys()):
        new_stac_doc["properties"]["sentinel:boa_offset_applied"] = src_stac_doc[
            "properties"
        ]["earthsearch:boa_offset_applied"]
    else:
        new_stac_doc["properties"]["sentinel:boa_offset_applied"] = src_stac_doc[
            "properties"
        ]["sentinel:boa_offset_applied"]

    # update collection to cogs
    new_stac_doc["collection"] = "sentinel-s2-l2a-cogs"

    return new_stac_doc


def prepare_message(
    scene_paths: list, product_name: str, log: Optional[logging.Logger] = None
):
    """
    Prepare a single message for each STAC file. The upstream source STAC JSON
    document (provided from the gap report) no longer contains the right fields
    and has some incorrect values. The STAC document therefore has to be recreated
    to reflect what was received in the SQS messaged from the provider to ensure
    The indexing pipeline works.

    Two web requests are made for each message. 1) to retreive the original
    tileinfo.json metadata file that accommodates each product. 2)
    A rasterio request to AWS to read the shape and transform of COGs which have
    incorrect data in the provided STAC file. E.g. the AOT.tif shape/transform
    provided is incorrect.

    raises:
        RuntimeError if collection 1 data is passed. Logic does not yet exist.

    yields:
        message: SNS message with STAC document as payload.
    """

    s3 = s3_client(region_name=SOURCE_REGION)

    message_id = 0
    for s3_path in scene_paths:
        try:
            # read the provided STAC document
            contents = s3_fetch(url=s3_path, s3=s3)
            src_stac_doc = json.loads(contents)

            # Handle formatting shifting changes from upstream metadata and collections,
            # so they can be transformed into a STAC document along with message attributes
            # for the SNS message, to be indexed into a consistent DEAfrica product.
            if product_name == "s2_l2a":
                stac_metadata = prepare_s2_l2a_stac(src_stac_doc)
                attributes = get_common_message_attributes(stac_metadata, product_name)

            if product_name == "s2_l2a_c1":
                # TODO something different will need to be done here
                raise RuntimeError(
                    "s2_l2a_c1 (collection 1) logic is not yet supported"
                )
                # attributes = get_common_message_attributes(src_stac_doc, product_name)

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
    max_workers: int = 1,
    product_name: str = "s2_l2a",
    limit: int = None,
    slack_url: str = None,
    dryrun: bool = False,
) -> None:
    """
    Publish a list of missing scenes to an specific queue

    params:
        limit: (int) optional limit of messages to be read from the report
        max_workers: (int) total number of pods used for the task. This number is used to
            split the number of scenes equally among the PODS
        idx: (int) sequential index which will be used to define the range of scenes that the POD will work with
        queue_name: (str) queue for formatted messages to be sent to
        slack_url: (str) Optional slack URL in case of you want to send a slack notification
        dryrun: (bool) if true do not send messages. used for testing.

    returns:
        None.
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
        f"{error_flag}*Sentinel 2 GAP Filler (worker {idx}) - {environment}*\n"
        f"Total messages: {len(files)}\n"
        f"Attempted worker messages prepared: {len(split_list_scenes[idx])}\n"
        f"Failed messages prepared: {len(split_list_scenes[idx]) - sent}\n"
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
@click.argument("max_workers", type=int, nargs=1, default=1)
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
    max_workers: int = 1,
    sync_queue_name: str = "deafrica-pds-sentinel-2-sync-scene",
    product_name: str = "s2_l2a",
    limit: int = None,
    slack_url: str = None,
    version: bool = False,
    dryrun: bool = False,
):
    """
    Publish missing scenes. Messages are backfilled for missing products. Missing products will
    therefore be synced and indexed as originally intended.

    params:
        idx: (int) sequential index which will be used to define the range of scenes that the POD will work with
        max_workers: (int) total number of pods used for the task. This number is used to
            split the number of scenes equally among the PODS
        sync_queue_name: (str) Sync queue name
        product_name (str): Product name being indexed. default is s2_l2a.
        limit: (str) optional limit of messages to be read from the report
        slack_url: (str) Slack notification channel hook URL
        version: (bool) echo the scripts version
        dryrun: (bool) if true do not send messages. used for testing.

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
