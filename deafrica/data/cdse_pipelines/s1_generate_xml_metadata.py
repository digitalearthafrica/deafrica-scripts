#!/usr/bin/env python3
"""
Sentinel-1 RTC CARD4L metadata XML generator.

Scans the CloudFerro (CF) bucket for completed CDSE batch jobs and generates a
CARD4L-compliant metadata XML file for each tile.

A metadata XML file is generated if:
  - a metadata XML does not already exist for that key AND
  - userdata.json exists in the CF bucket (i.e. that the Batch processing job has finished)


Options:
    --cf-bucket        - CloudFerro bucket where batch job outputs are stored
    --start-date       - Only process jobs on or after this date (YYYY-MM-DD, optional)
    --end-date         - Only process jobs on or before this date (YYYY-MM-DD, optional)
    --n-workers        - Number of parallel S3 HEAD request workers (default 32)
    --dry-run          - Report what XML files would be generated without creating them
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime

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

TILING_GRID_NAME = "WGS84 1 degree grid"  # for output in metadata.xml
BASE_FOLDER = "s1_rtc"


def cdse_derived_from_uri(scene_id: str) -> str:
    """Convert a scene ID to the derived S3 path in the CDSE bucket."""
    clean_id = scene_id.replace(".SAFE", "").removesuffix("_COG")
    parts = clean_id.split("_")
    start_ts = parts[4]  # start timestamp
    year, month, day = start_ts[0:4], start_ts[4:6], start_ts[6:8]
    return f"s3://eodata/Sentinel-1/SAR/IW_GRDH_1S-COG/{year}/{month}/{day}/{clean_id}_COG.SAFE"


def s3_key_exists(s3_client, bucket: str, key: str) -> bool:
    """Check if an S3 key exists in the given bucket."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "403"):
            return False
        raise


def _parse_int(s: str) -> int | None:
    """Parse a string as int, returning None if it's not a valid integer."""
    try:
        return int(s)
    except ValueError:
        return None


def _is_valid_tile(s: str) -> bool:
    """
    Validate that a string looks like a tile name e.g. N00E024, S10W030.
    Must be exactly 7 chars: [NS][0-9]{2}[EW][0-9]{3}
    """
    if len(s) != 7:
        return False
    return (
        s[0] in ("N", "S")
        and s[1:3].isdigit()
        and s[3] in ("E", "W")
        and s[4:7].isdigit()
    )


def _is_valid_job_id(s: str) -> bool:
    """Validate that a string looks like a UUID job ID."""
    parts = s.split("-")
    return len(parts) == 5 and all(p.isalnum() for p in parts)


def _is_valid_datatake(s: str) -> bool:
    """Validate that a string looks like a hex datatake ID e.g. 07AD51."""
    try:
        int(s, 16)
        return len(s) == 6
    except ValueError:
        return False


def list_job_folders(
    cf_s3,
    cf_bucket: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[dict]:
    """
    List all job folders in the CF bucket matching the expected structure:
    s1_rtc/{year}/{month}/{day}/{datatake}/{job_id}/{tile}/

    Skips any prefixes that don't match expected patterns and optionally filters by date range.
    """
    paginator = cf_s3.get_paginator("list_objects_v2")
    seen = set()
    folders = []

    for page in paginator.paginate(Bucket=cf_bucket, Prefix=f"{BASE_FOLDER}/"):
        for obj in page.get("Contents", []):
            parts = obj["Key"].split("/")
            # expected: s1_rtc/{year}/{month}/{day}/{datatake}/{job_id}/{tile}/{file}
            if len(parts) != 8:
                continue

            _, year_s, month_s, day_s, datatake, job_id, tile, _ = parts

            year = _parse_int(year_s)
            month = _parse_int(month_s)
            day = _parse_int(day_s)

            if year is None or month is None or day is None:
                continue
            if not (1 <= month <= 12):
                continue
            if not _is_valid_datatake(datatake):
                continue
            if not _is_valid_job_id(job_id):
                continue
            if not _is_valid_tile(tile):
                continue

            try:
                folder_date = date(year, month, day)
            except (ValueError, TypeError):
                continue

            if start_date and folder_date < start_date:
                continue
            if end_date and folder_date > end_date:
                continue

            key = (year, month, day, datatake, job_id, tile)
            if key in seen:
                continue
            seen.add(key)
            folders.append(
                {
                    "year": year,
                    "month": month,
                    "day": day,
                    "datatake": datatake,
                    "job_id": job_id,
                    "tile": tile,
                }
            )

    return folders


def check_folder_status(cf_s3, cf_bucket, f) -> tuple[dict, str]:
    """
    Check whether a folder has XML already or is ready to generate.
    Returns (folder, status) where status is 'exists', 'ready', or 'not_complete'.
    """
    year, month, day = f["year"], f["month"], f["day"]
    datatake, job_id, tile = f["datatake"], f["job_id"], f["tile"]

    if s3_key_exists(
        cf_s3, cf_bucket, xml_key(year, month, day, datatake, job_id, tile)
    ):
        return f, "exists"
    if s3_key_exists(
        cf_s3, cf_bucket, userdata_key(year, month, day, datatake, job_id, tile)
    ):
        return f, "ready"
    return f, "not_complete"


def xml_key(
    year: int, month: int, day: int, datatake: str, job_id: str, tile: str
) -> str:
    """Generate the S3 key for the metadata XML file for a given tile/job."""
    return f"{BASE_FOLDER}/{year}/{month:02d}/{day:02d}/{datatake}/{job_id}/{tile}/metadata.xml"


def userdata_key(
    year: int, month: int, day: int, datatake: str, job_id: str, tile: str
) -> str:
    """Generate the S3 key for the userdata JSON file for a given tile/job."""
    return f"{BASE_FOLDER}/{year}/{month:02d}/{day:02d}/{datatake}/{job_id}/{tile}/userdata.json"


def request_key(year: int, month: int, day: int, datatake: str, job_id: str) -> str:
    """Generate the S3 key for the request JSON file for a given tile/job."""
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
    """Generate the CARD4L metadata XML for a given tile/job and upload it to S3."""
    date_str = f"{year}-{month:02d}-{day:02d}"

    # Step 1: Fetch the list of scenes from the SH catalog for this tile/date/datatake
    scenes = fetch_scenes_from_sh(session, tile, date_str, datatake, africa_grid)
    if not scenes:
        log.warning(f"  no scenes found for {tile} {date_str} {datatake} - skipping")
        return False

    # Step 2: Read userdata.json from CF bucket to get serviceVersion and other info
    obj = cf_s3.get_object(
        Bucket=cf_bucket,
        Key=userdata_key(year, month, day, datatake, job_id, tile),
    )
    userdata_json = json.loads(obj["Body"].read())
    service_version = userdata_json["serviceVersion"]

    # Step 3: Create Userdata object with InputTile entries for each scene
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

    # Step 4: Read the request JSON from CF bucket to get the data transfer object (DTO) with batch request JSON
    obj = cf_s3.get_object(
        Bucket=cf_bucket,
        Key=request_key(year, month, day, datatake, job_id),
    )
    request_data = json.loads(obj["Body"].read())

    request_data["request"]["output"]["delivery"]["s3"][
        "url"
    ] = f"s3://{cf_bucket}/{BASE_FOLDER}/{year}/{month:02d}/{day:02d}/{datatake}/{job_id}/{tile}/<outputId>.<format>"
    batch_task = BatchProcessTaskDto.from_dict(request_data)

    grid_tile = GridTile(name=tile)
    grid_descriptor = TilingGridDescriptorDto(name=TILING_GRID_NAME, unit="DEGREE")

    # Step 5: Create S3 clients for output and input
    output_s3 = Card4lS3(s3_client=cf_s3)
    s1_s3 = Card4lS3(s3_client=cdse_s3_client)

    tile_data_mod.TileId.from_s3_key = classmethod(
        lambda *_: TileId(
            tile_name=tile,
            datatake_year=year,
            datatake_month=month,
            datatake_day=day,
            datatake_id=datatake,
        )
    )

    # Step 6: Create TileMetadataProducer
    producer = TileMetadataProducer(
        None, batch_task, grid_tile, grid_descriptor, output_s3, s1_s3
    )
    producer._read_userdata = lambda: userdata

    # Step 7: Generate the metadata XML and upload to S3
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
    "--start-date",
    default=None,
    help="Only process jobs on or after this date (YYYY-MM-DD). Optional.",
)
@click.option(
    "--end-date",
    default=None,
    help="Only process jobs on or before this date (YYYY-MM-DD). Optional.",
)
@click.option(
    "--n-workers",
    default=32,
    show_default=True,
    help="Number of parallel S3 HEAD request workers.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Report what XML files would be generated without creating them.",
)
def cli(cf_bucket, start_date, end_date, n_workers, dry_run):
    """
    Generate CARD4L metadata XML files for completed CDSE batch jobs.

    Scans the CF bucket for completed jobs (those with a userdata.json)
    and generates metadata XML for any tile that doesn't already have one.

    Example commands:

        # dry run - report what would be generated
        s1_generate_xml_metadata --dry-run

        # generate XML for all completed jobs
        s1_generate_xml_metadata

        # limit to a date range
        s1_generate_xml_metadata --start-date 2026-06-01 --end-date 2026-06-30

        # use a specific CF bucket
        s1_generate_xml_metadata --cf-bucket cdse_batch_prod_bucket
    """
    start = datetime.strptime(start_date, "%Y-%m-%d").date() if start_date else None
    end = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None

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
    folders = list_job_folders(cf_s3, cf_bucket, start_date=start, end_date=end)
    log.info(f"Found {len(folders)} tile/job combinations")

    would_generate = []
    already_exists = []
    not_complete = []

    # parallel status checks
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {
            pool.submit(check_folder_status, cf_s3, cf_bucket, f): f for f in folders
        }
        for future in as_completed(futures):
            f, status = future.result()
            year, month, day = f["year"], f["month"], f["day"]
            datatake, job_id, tile = f["datatake"], f["job_id"], f["tile"]
            label = f"{tile} {year}-{month:02d}-{day:02d} {datatake} (job {job_id})"

            if status == "exists":
                log.info(f"XML already exists: {label}")
                already_exists.append(label)
            elif status == "not_complete":
                log.info(f"userdata.json does not exist yet: {label}")
                not_complete.append(label)
            else:
                would_generate.append((f, label))

    # generate XML sequentially (SH catalog + S3 writes)
    for f, label in would_generate:
        year, month, day = f["year"], f["month"], f["day"]
        datatake, job_id, tile = f["datatake"], f["job_id"], f["tile"]

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
    log.info(f"already have XML: {len(already_exists)}")
    log.info(f"processing not complete yet: {len(not_complete)}")
    log.info(
        f"{'would generate XML' if dry_run else 'generated XML'}: {len(would_generate)}"
    )


if __name__ == "__main__":
    cli()
