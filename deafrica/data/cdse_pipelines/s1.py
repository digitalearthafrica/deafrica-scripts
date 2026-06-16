#!/usr/bin/env python3
"""
Sentinel-1 RTC gap check + CDSE batch backfill submission.

1. Runs the s1_rtc gap check reporting missing datasets in the DE Africa bucket 
   for a given date range (default: last 7 days) against the SentinelHub (SH) catalogue.
2. For each missing dataset, builds a CDSE batch processing payload and
   submits and starts the job against the CDSE BatchProcessing V2 API.

Options:
    --start-date, --end-date   - YYYY-MM-DD (defaults to last 7 days)
    --n-workers                - parallel S3 HEAD requests (default 32)
    --dry-run                  - if set, do the gap check but don't submit jobs
    --output-bucket            - S3 bucket batch outputs are written to
    --output                   - optional path to write the JSON report to
"""

import json
import sys
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import click
import geopandas as gpd
import pandas as pd
import requests
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from shapely.geometry import mapping

from auth import get_session
from payloads import build_batch_payload
from satellites import SATELLITES

from deafrica.logs import setup_logging

# Configuration
SH_CATALOG_URL = "https://sh.dataspace.copernicus.eu/api/v1/catalog/1.0.0/search"
SH_BATCH_URL = "https://sh.dataspace.copernicus.eu/api/v2/batch/process"
S1_COLLECTION = "sentinel-1-grd"

TILING_GRID = "https://s3.eu-central-1.amazonaws.com/sh-batch-grids/tiling-grid-3.zip"
AFRICA_EXTENT_URL = (
    "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/africa-extent.json"
)

S1_BUCKET_NAME = "deafrica-sentinel-1"
BASE_FOLDER_NAME = "s1_rtc"
REGION_NAME = "af-south-1"

TILING_ID = 3
RESOLUTION = 0.0002
DEFAULT_OUTPUT_BUCKET = "cdse_batch_test_bucket"

log = setup_logging()

def get_africa_grid() -> gpd.GeoDataFrame:
    "Loads the global tiling grid and clips it to the Africa extent."
    log.info("Loading tiling grid and Africa extent ...")
    grid = gpd.read_file(TILING_GRID)
    africa_extent_json = requests.get(AFRICA_EXTENT_URL, timeout=60).json()
    africa_extent = gpd.GeoDataFrame.from_features(
        africa_extent_json["features"], crs="EPSG:4326"
    )
    return gpd.overlay(grid, africa_extent, how="intersection")

def _keep_iw_scene(filename: str) -> bool:
    """
    Determines whether a Sentinel-1 filename corresponds to an IW GRD 1SDV product, 
    which are the only ones relevant for our Radiometric Terrain Correction (RTC) processing.
    """
    scene_id = filename.replace(".SAFE", "").removesuffix("_COG")
    parts = scene_id.split("_")
    return (
        len(parts) >= 9
        and parts[1] == "IW"
        and parts[2] == "GRDH"
        and parts[3] == "1SDV"
    )

def _frame_from_features(features: list[dict]) -> gpd.GeoDataFrame:
    "Converts a list of GeoJSON-like features into a GeoDataFrame."
    if not features:
        return gpd.GeoDataFrame(columns=["filename", "geometry"], crs="EPSG:4326")
    return gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")


def search_sh_catalog(session, bbox: list[float], date: str) -> gpd.GeoDataFrame:
    "Searches the SentinelHub catalogue API for Sentinel-1 IW GRD scenes intersecting the given bbox and date, returning a GeoDataFrame of results."
    body = {
        "collections": [S1_COLLECTION],
        "bbox": [round(v, 4) for v in bbox],
        "datetime": f"{date}T00:00:00Z/{date}T23:59:59Z",
        "limit": 100,
        "fields": {"include": ["id", "geometry"], "exclude": []},
    }

    features = []
    while True:
        resp = session.post(SH_CATALOG_URL, json=body, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        for f in data.get("features", []):
            if not _keep_iw_scene(f["id"]):
                continue
            features.append(
                {
                    "type": "Feature",
                    "geometry": f["geometry"],
                    "properties": {"filename": f["id"]},
                }
            )
        nxt = data.get("context", {}).get("next")
        if nxt is None:
            break
        body["next"] = nxt
    return _frame_from_features(features)


def search_scenes(bbox: list[float], start_date: str, end_date: str) -> gpd.GeoDataFrame:
    "Searches the SentinelHub catalogue for Sentinel-1 IW GRD scenes intersecting the given bbox and date range, returning a GeoDataFrame of results."
    session = get_session()

    frames = []
    for day in pd.date_range(start_date, end_date, freq="D"):
        d = day.strftime("%Y-%m-%d")
        log.info(f"Searching SH catalogue for {d} ...")
        try:
            day_scenes = search_sh_catalog(session, bbox, d)
            log.info(f"  {len(day_scenes)} IW GRD scenes")
            if not day_scenes.empty:
                frames.append(day_scenes)
        except Exception as e:
            log.error(f"  search failed for {d}: {e}")

    if not frames:
        return _frame_from_features([])
    return gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs="EPSG:4326")

def create_dataset_names(grided_results: gpd.GeoDataFrame) -> list[str]:
    "Converts the grided search results into a list of expected s1_rtc dataset URIs in the DE Africa bucket."
    datasets = set()
    for _, row in grided_results.iterrows():
        parts = row["filename"].split("_")
        date = parts[4][0:8]
        datatake = parts[7]
        grid_name = row["NAME"]
        datasets.add(
            f"{BASE_FOLDER_NAME}/{grid_name}/{date[0:4]}/{date[4:6]}/{date[6:8]}/{datatake}"
        )
    return sorted(datasets)

def expected_metadata_key(dataset: str) -> str:
    "Given a dataset URI like s3://bucket/s1_rtc/TILE/YYYY/MM/DD/DATATAKE, returns the expected S3 key for the metadata JSON file that should exist alongside the tif files."
    p = dataset.split("/")
    filename = f"{p[0]}_{p[5]}_{p[1]}_{p[2]}_{p[3]}_{p[4]}_metadata.json"
    return f"{dataset}/{filename}"

def check_metadata_exists(s3, dataset: str) -> tuple[str, bool]:
    "Checks whether the expected metadata JSON file exists in S3 for the given dataset URI, returning a tuple of (dataset, exists)."
    key = expected_metadata_key(dataset)
    try:
        s3.head_object(Bucket=S1_BUCKET_NAME, Key=key)
        return dataset, True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "403"):
            return dataset, False
        raise

def find_missing(datasets: list[str], n_workers: int = 32) -> list[str]:
    "Checks which of the expected datasets are missing from the S3 bucket by verifying the existence of their metadata JSON files, using a thread pool for concurrency."
    s3 = boto3.client(
        "s3", region_name=REGION_NAME, config=Config(signature_version=UNSIGNED)
    )
    missing_datasets = []
    log.info(f"Checking {len(datasets)} expected datasets against s3://{S1_BUCKET_NAME} ...")
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {pool.submit(check_metadata_exists, s3, d): d for d in datasets}
        for i, future in enumerate(as_completed(futures), 1):
            dataset, exists = future.result()
            if not exists:
                missing_datasets.append(f"s3://{S1_BUCKET_NAME}/{dataset}")
            if i % 500 == 0:
                log.info(f"  checked {i}/{len(datasets)}")
    return sorted(missing_datasets)

def submit_backfill_jobs(missing: list[str], grid: gpd.GeoDataFrame, output_bucket: str) -> list[dict]:
    """For each missing s1_rtc dataset URI, build + submit + start a CDSE batch job."""
    session = get_session()
    results = []

    for uri in missing:
        # s3://deafrica-sentinel-1/s1_rtc/<TILE>/<YYYY>/<MM>/<DD>/<DATATAKE>
        parts = uri.replace(f"s3://{S1_BUCKET_NAME}/", "").split("/")
        tile, year, month, day, datatake = parts[1], parts[2], parts[3], parts[4], parts[5]

        cell = grid[grid["NAME"] == tile]
        if cell.empty:
            log.error(f"  tile {tile} not found in grid - skipping {uri}")
            results.append({"uri": uri, "status": "error", "error": "tile not found in grid"})
            continue

        geometry = mapping(cell.to_crs("EPSG:4326").union_all().buffer(-1e-5))

        time_from = f"{year}-{month}-{day}T00:00:00Z"
        time_to = f"{year}-{month}-{int(day) + 1:02d}T00:00:00Z"

        payload = build_batch_payload(
            sat_config=SATELLITES["s1"],
            geometry=geometry,
            time_from=time_from,
            time_to=time_to,
            tiling_id=TILING_ID,
            resolution=RESOLUTION,
        )

        payload["output"]["delivery"]["s3"]["url"] = (
            f"s3://{output_bucket}/s1_rtc/{year}/{month}/{day}/{datatake}"
        )

        try:
            resp = session.post(SH_BATCH_URL, json=payload)
            resp.raise_for_status()
            job = resp.json()
            job_id = job["id"]

            session.post(f"{SH_BATCH_URL}/{job_id}/start")

            log.info(f"Created + started job for {tile} {datatake}: {job_id}")
            results.append({"uri": uri, "status": "started", "job_id": job_id, "tile": tile, "datatake": datatake})
        except Exception as e:
            log.error(f"  job submission failed for {uri}: {e}")
            results.append({"uri": uri, "status": "error", "error": str(e)})

    return results


@click.command("s1-gap-check")
@click.option(
    "--start-date",
    "-s",
    default=None,
    help="Start date YYYY-MM-DD (inclusive). Defaults to 7 days before end date.",
)
@click.option(
    "--end-date",
    "-e",
    default=None,
    help="End date YYYY-MM-DD (inclusive). Defaults to yesterday.",
)
@click.option(
    "--output-bucket",
    "-b",
    default=DEFAULT_OUTPUT_BUCKET,
    show_default=True,
    help="S3 bucket batch job outputs are written to.",
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
    help="Run the gap check but do not submit any backfill jobs.",
)
@click.option(
    "--limit",
    default=None,
    type=int,
    help="Only submit the first N missing datasets. Useful for testing.",
)
@click.option(
    "--output",
    "-o",
    default=None,
    help="Optional path to write the JSON report to.",
)
def cli(start_date, end_date, output_bucket, n_workers, dry_run, limit, output):
    """
    Check for missing s1_rtc datasets in the DE Africa bucket and submit CDSE
    batch backfill jobs for any gaps found.

    Example command:

    s1-gap-check --start-date 2026-06-03 --end-date 2026-06-10 --dry-run

    s1-gap-check --start-date 2026-06-03 --end-date 2026-06-10 -o gaps.json
    """
    now = datetime.now(timezone.utc)

    # End = yesterday (date-only, inclusive)
    end_date = end_date or (now - timedelta(days=1)).date().isoformat()

    # Start = 7 days before end
    start_date = start_date or (now - timedelta(days=8)).date().isoformat()

    log.info(f"Gap check: {start_date} -> {end_date}, dry_run={dry_run}")

    africa_grid = get_africa_grid()
    bbox = list(africa_grid.total_bounds)

    scenes = search_scenes(bbox, start_date, end_date)
    if scenes.empty:
        log.info("No scenes returned for this period - nothing to check.")
        sys.exit(0)

    log.info(f"Total scenes: {len(scenes)}")

    # Clip scenes to grid and convert to expected dataset names, then check which are missing from S3
    grided = gpd.overlay(scenes, africa_grid, how="intersection")
    grided = grided[grided.geometry.to_crs("EPSG:3857").area > 0]

    datasets = create_dataset_names(grided)
    log.info(f"Expecting {len(datasets)} datasets for {start_date} -> {end_date}")

    missing_datasets = find_missing(datasets, n_workers=n_workers)
    log.info(f"Missing: {len(missing_datasets)}")

    report = {
        "start_date": start_date,
        "end_date": end_date,
        "expected": len(datasets),
        "missing_count": len(missing_datasets),
        "missing_datasets": missing_datasets,
    }

    if missing_datasets and not dry_run:
        to_submit = missing_datasets[:limit] if limit else missing_datasets
        if limit:
            log.info(f"Limiting submission to {len(to_submit)} of {len(missing_datasets)} missing datasets")
        report["jobs"] = submit_backfill_jobs(to_submit, africa_grid, output_bucket)

    print(json.dumps(report, indent=2))

    if output:
        with open(output, "w") as f:
            json.dump(report, f, indent=2)
        log.info(f"Report written to {output}")


if __name__ == "__main__":
    cli()
