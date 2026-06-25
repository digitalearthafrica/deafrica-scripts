#!/usr/bin/env python3
"""
Sentinel-1 RTC CARD4L metadata XML generator.

Scans the CF bucket for completed CDSE batch jobs and generates
CARD4L-compliant metadata XML files for each completed tile.

A tile is considered complete if:
  - userdata.json exists in the CF bucket (job is done)
  - metadata XML does not already exist in the CF bucket

Options:
    --cf-bucket        - CloudFerro bucket where batch job outputs are stored
    --dry-run          - Report what XML files would be generated without creating them
"""

import json
import os

import boto3
import click
from botocore.exceptions import ClientError

from card4l_metadata.tile_metadata_producer import TileMetadataProducer
from card4l_metadata.model.userdata import Userdata, InputTile
from card4l_metadata.model.dtos import (
    BatchProcessTaskDto,
    GridTile,
    TilingGridDescriptorDto,
)
from card4l_metadata.model.tile_data import TileId
from card4l_metadata.s3 import Card4lS3
from card4l_metadata.model import tile_data as tile_data_mod
from card4l_metadata.s1.manifest import S1ManifestParser
from card4l_metadata import xml_producer

from deafrica.data.cdse_pipelines.s1 import fetch_scenes_from_sh, get_africa_grid
from deafrica.data.cdse_pipelines.auth import get_session
from deafrica.logs import setup_logging

log = setup_logging()

TILING_GRID_NAME = "WGS84 1 degree grid"
BASE_FOLDER = "s1_rtc"


def cdse_derived_from_uri(scene_id: str) -> str:
    clean_id = scene_id.replace(".SAFE", "").removesuffix("_COG")
    parts = clean_id.split("_")
    start_ts = parts[4]
    year, month, day = start_ts[0:4], start_ts[4:6], start_ts[6:8]
    return f"s3://eodata/Sentinel-1/SAR/IW_GRDH_1S-COG/{year}/{month}/{day}/{clean_id}_COG.SAFE"


def s3_key_exists(s3_client, bucket: str, key: str) -> bool:
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "403"):
            return False
        raise


def list_job_folders(cf_s3, cf_bucket: str) -> list[dict]:
    """
    List all job folders in the CF bucket.
    Returns a list of dicts with keys: year, month, day, datatake, job_id, tile.
    Expected structure: s1_rtc/{year}/{month}/{day}/{datatake}/{job_id}/{tile}/
    """
    paginator = cf_s3.get_paginator("list_objects_v2")
    folders = []
    seen = set()

    pages = paginator.paginate(
        Bucket=cf_bucket, Prefix=f"{BASE_FOLDER}/", Delimiter="/"
    )
    year_prefixes = [
        p["Prefix"] for page in pages for p in page.get("CommonPrefixes", [])
    ]

    for year_prefix in year_prefixes:
        for month_page in cf_s3.get_paginator("list_objects_v2").paginate(
            Bucket=cf_bucket, Prefix=year_prefix, Delimiter="/"
        ):
            for month_prefix in month_page.get("CommonPrefixes", []):
                for day_page in cf_s3.get_paginator("list_objects_v2").paginate(
                    Bucket=cf_bucket, Prefix=month_prefix["Prefix"], Delimiter="/"
                ):
                    for day_prefix in day_page.get("CommonPrefixes", []):
                        for datatake_page in cf_s3.get_paginator(
                            "list_objects_v2"
                        ).paginate(
                            Bucket=cf_bucket, Prefix=day_prefix["Prefix"], Delimiter="/"
                        ):
                            for datatake_prefix in datatake_page.get(
                                "CommonPrefixes", []
                            ):
                                for job_page in cf_s3.get_paginator(
                                    "list_objects_v2"
                                ).paginate(
                                    Bucket=cf_bucket,
                                    Prefix=datatake_prefix["Prefix"],
                                    Delimiter="/",
                                ):
                                    for job_prefix in job_page.get(
                                        "CommonPrefixes", []
                                    ):
                                        for tile_page in cf_s3.get_paginator(
                                            "list_objects_v2"
                                        ).paginate(
                                            Bucket=cf_bucket,
                                            Prefix=job_prefix["Prefix"],
                                            Delimiter="/",
                                        ):
                                            for tile_prefix in tile_page.get(
                                                "CommonPrefixes", []
                                            ):
                                                parts = (
                                                    tile_prefix["Prefix"]
                                                    .rstrip("/")
                                                    .split("/")
                                                )
                                                if len(parts) < 7:
                                                    continue
                                                key = tuple(parts[1:7])
                                                if key in seen:
                                                    continue
                                                seen.add(key)
                                                folders.append(
                                                    {
                                                        "year": int(parts[1]),
                                                        "month": int(parts[2]),
                                                        "day": int(parts[3]),
                                                        "datatake": parts[4],
                                                        "job_id": parts[5],
                                                        "tile": parts[6],
                                                    }
                                                )
    return folders


def xml_key(
    year: int, month: int, day: int, datatake: str, job_id: str, tile: str
) -> str:
    return f"{BASE_FOLDER}/{year}/{month:02d}/{day:02d}/{datatake}/{job_id}/{tile}/metadata.xml"


def userdata_key(
    year: int, month: int, day: int, datatake: str, job_id: str, tile: str
) -> str:
    return f"{BASE_FOLDER}/{year}/{month:02d}/{day:02d}/{datatake}/{job_id}/{tile}/userdata.json"


def request_key(year: int, month: int, day: int, datatake: str, job_id: str) -> str:
    return f"{BASE_FOLDER}/{year}/{month:02d}/{day:02d}/{datatake}/{job_id}/request-{job_id}.json"


def patch_manifest_parser():
    """Patch S1ManifestParser to strip _COG.SAFE from raw_product_id."""
    original_init = S1ManifestParser.__init__

    def patched_init(self, aws_product_url: str, s3):
        original_init(self, aws_product_url, s3)
        self.raw_product_id = self.raw_product_id.replace("_COG.SAFE", "").replace(
            ".SAFE", ""
        )

    S1ManifestParser.__init__ = patched_init


def patch_xml_filename():
    """
    Patch xml_producer._filename to return the full DEAfrica filename convention
    regardless of the delivery URL template used for reading TIFFs.
    """

    def patched_filename(tile_data, output_id):
        t = tile_data.id
        return (
            f"s1_rtc_{t.datatake_id}_{t.tile_name}_"
            f"{t.datatake_year}_{t.datatake_month:02d}_{t.datatake_day:02d}"
            f"_{output_id}.tif"
        )

    xml_producer._filename = patched_filename


def generate_xml(
    year: int,
    month: int,
    day: int,
    datatake: str,
    job_id: str,
    tile: str,
    cf_s3,
    cf_bucket: str,
    session,
    africa_grid,
    cdse_s3_client,
):
    date = f"{year}-{month:02d}-{day:02d}"

    # fetch scenes from SH catalog
    scenes = fetch_scenes_from_sh(session, tile, date, datatake, africa_grid)
    if not scenes:
        log.warning(f"  no scenes found for {tile} {date} {datatake} - skipping")
        return False

    # fetch userdata.json for service version
    obj = cf_s3.get_object(
        Bucket=cf_bucket,
        Key=userdata_key(year, month, day, datatake, job_id, tile),
    )
    userdata_json = json.loads(obj["Body"].read())
    service_version = userdata_json["serviceVersion"]

    userdata = Userdata(
        tiles=[
            InputTile(
                data_path=cdse_derived_from_uri(scene["id"]),
                data_geometry=scene["geometry"],
            )
            for scene in scenes
        ],
        service_version=service_version,
    )

    # fetch request.json
    obj = cf_s3.get_object(
        Bucket=cf_bucket,
        Key=request_key(year, month, day, datatake, job_id),
    )
    request_data = json.loads(obj["Body"].read())

    # overwrite delivery URL to match actual bare TIFF paths in CF bucket
    # (filenames in XML are handled separately by the patched _filename function)
    request_data["request"]["output"]["delivery"]["s3"][
        "url"
    ] = f"s3://{cf_bucket}/{BASE_FOLDER}/{year}/{month:02d}/{day:02d}/{datatake}/{job_id}/<tileName>/<outputId>.<format>"
    batch_task = BatchProcessTaskDto.from_dict(request_data)

    grid_tile = GridTile(name=tile)
    grid_descriptor = TilingGridDescriptorDto(name=TILING_GRID_NAME, unit="DEGREE")

    # use CF S3 client for output (TIFFs live in CF bucket)
    output_s3 = Card4lS3(s3_client=cf_s3)
    s1_s3 = Card4lS3(s3_client=cdse_s3_client)

    # patch TileId.from_s3_key
    tile_data_mod.TileId.from_s3_key = classmethod(
        lambda cls, key: TileId(
            tile_name=tile,
            datatake_year=year,
            datatake_month=month,
            datatake_day=day,
            datatake_id=datatake,
        )
    )

    producer = TileMetadataProducer(
        None, batch_task, grid_tile, grid_descriptor, output_s3, s1_s3
    )
    producer._read_userdata = lambda: userdata
    producer.generate_metadata()

    return True


@click.command("s1-generate-xml-metadata")
@click.option(
    "--cf-bucket",
    default="cdse_batch_test_bucket",
    show_default=True,
    help="CloudFerro bucket where batch job outputs are stored.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Report what XML files would be generated without creating them.",
)
def cli(cf_bucket, dry_run):
    """
    Generate CARD4L metadata XML files for completed CDSE batch jobs.

    Scans the CF bucket for completed jobs (those with a userdata.json)
    and generates metadata XML for any tile that doesn't already have one.

    Example commands:

        # dry run - report what would be generated
        s1-generate-xml-metadata --dry-run

        # generate XML for all completed jobs
        s1-generate-xml-metadata

        # use a specific CF bucket
        s1-generate-xml-metadata --cf-bucket cdse_batch_prod_bucket
    """
    cf_s3 = boto3.client(
        "s3",
        endpoint_url="https://s3.waw3-1.cloudferro.com",
        aws_access_key_id=os.environ["S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["S3_SECRET_ACCESS_KEY"],
    )

    cdse_s3_client = boto3.client(
        "s3",
        endpoint_url="https://eodata.dataspace.copernicus.eu",
        aws_access_key_id=os.environ["CDSE_S3_ACCESS_KEY"],
        aws_secret_access_key=os.environ["CDSE_S3_SECRET_KEY"],
    )

    patch_manifest_parser()
    patch_xml_filename()

    session = get_session()
    africa_grid = get_africa_grid()

    log.info(f"Scanning {cf_bucket} for completed jobs ...")
    folders = list_job_folders(cf_s3, cf_bucket)
    log.info(f"Found {len(folders)} tile/job combinations")

    would_generate = []
    already_exists = []
    not_complete = []

    for f in folders:
        year, month, day = f["year"], f["month"], f["day"]
        datatake, job_id, tile = f["datatake"], f["job_id"], f["tile"]

        label = f"{tile} {year}-{month:02d}-{day:02d} {datatake} (job {job_id})"

        # check if XML already exists in CF bucket
        key = xml_key(year, month, day, datatake, job_id, tile)
        if s3_key_exists(cf_s3, cf_bucket, key):
            log.info(f"  already exists: {label}")
            already_exists.append(label)
            continue

        # check if userdata.json exists (job complete)
        ud_key = userdata_key(year, month, day, datatake, job_id, tile)
        if not s3_key_exists(cf_s3, cf_bucket, ud_key):
            log.info(f"  not complete yet: {label}")
            not_complete.append(label)
            continue

        would_generate.append(label)

        if dry_run:
            log.info(f"  [dry-run] would generate XML: {label}")
        else:
            log.info(f"  generating XML: {label}")
            try:
                generate_xml(
                    year,
                    month,
                    day,
                    datatake,
                    job_id,
                    tile,
                    cf_s3,
                    cf_bucket,
                    session,
                    africa_grid,
                    cdse_s3_client,
                )
                log.info(f"  done: {label}")
            except Exception as e:
                log.error(f"  failed for {label}: {e}")

    log.info(f"\nSummary:")
    log.info(f"  already have XML: {len(already_exists)}")
    log.info(f"  not complete yet: {len(not_complete)}")
    log.info(f"  {'would generate' if dry_run else 'generated'}: {len(would_generate)}")


if __name__ == "__main__":
    cli()
