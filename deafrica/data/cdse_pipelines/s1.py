#!/usr/bin/env python3
"""
Sentinel-1 RTC gap check + CDSE batch backfill submission.

1. Runs the s1_rtc gap check reporting missing datasets in the DE Africa bucket 
   for a given date range (default: last 7 days) against the SentinelHub (SH) catalogue.
2. For each missing dataset, builds a CDSE batch processing payload and
   submits and starts the job against the CDSE BatchProcessing V2 API.
3. Generates a STAC item (using pystac, validated against the spec) for each
   missing dataset, with derived_from links to all source S1 scenes covering
   that tile/date/datatake.

Options:
    --start-date, --end-date   - YYYY-MM-DD (defaults to last 7 days)
    --n-workers                - parallel S3 HEAD requests (default 32)
    --dry-run                  - if set, do the gap check but don't submit jobs
    --stac-test                - generate a single STAC item JSON for the first
                                 missing dataset and exit (implies --dry-run)
    --output-bucket            - S3 bucket batch outputs are written to
    --s3-bucket-name           - S3 bucket to check for existing s1_rtc datasets
    --output                   - optional path to write the JSON report to
"""

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import click
import geopandas as gpd
import pandas as pd
import pystac
import requests
from botocore import UNSIGNED
from botocore.config import Config
from botocore.exceptions import ClientError
from pystac.extensions.sar import (
    SarExtension,
    FrequencyBand,
    Polarization,
    ObservationDirection,
)
from pystac.extensions.sat import SatExtension, OrbitState
from pystac.extensions.projection import ProjectionExtension
from shapely.geometry import mapping

from .auth import get_session
from .payloads import build_batch_payload
from .satellites import SATELLITES

from deafrica.logs import setup_logging

# Configuration
SH_CATALOG_URL = "https://sh.dataspace.copernicus.eu/api/v1/catalog/1.0.0/search"
SH_BATCH_URL = "https://sh.dataspace.copernicus.eu/api/v2/batch/process"
S1_COLLECTION = "sentinel-1-grd"

TILING_GRID = "https://s3.eu-central-1.amazonaws.com/sh-batch-grids/tiling-grid-3.zip"
AFRICA_EXTENT_URL = "https://raw.githubusercontent.com/digitalearthafrica/deafrica-extent/master/africa-extent.json"

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


def search_scenes(
    bbox: list[float], start_date: str, end_date: str
) -> gpd.GeoDataFrame:
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


def check_metadata_exists(s3, dataset: str, bucket_name: str) -> tuple[str, bool]:
    "Checks whether the expected metadata JSON file exists in S3 for the given dataset URI, returning a tuple of (dataset, exists)."
    key = expected_metadata_key(dataset)
    try:
        s3.head_object(Bucket=bucket_name, Key=key)
        return dataset, True
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "403"):
            return dataset, False
        raise


def find_missing(
    datasets: list[str], bucket_name: str, n_workers: int = 32
) -> list[str]:
    "Checks which of the expected datasets are missing from the S3 bucket by verifying the existence of their metadata JSON files, using a thread pool for concurrency."
    s3 = boto3.client(
        "s3", region_name=REGION_NAME, config=Config(signature_version=UNSIGNED)
    )
    missing_datasets = []
    log.info(
        f"Checking {len(datasets)} expected datasets against s3://{bucket_name} ..."
    )
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {
            pool.submit(check_metadata_exists, s3, d, bucket_name): d for d in datasets
        }
        for i, future in enumerate(as_completed(futures), 1):
            dataset, exists = future.result()
            if not exists:
                missing_datasets.append(f"s3://{bucket_name}/{dataset}")
            if i % 500 == 0:
                log.info(f"  checked {i}/{len(datasets)}")
    return sorted(missing_datasets)


def fetch_scenes_from_sh(
    session, tile: str, date: str, datatake: str, grid: gpd.GeoDataFrame
) -> list[dict]:
    """
    Query SH catalog (full properties, no fields filter) for ALL scenes
    matching tile/date/datatake. A tile can be covered by more than one
    scene from the same datatake (adjacent passes), so this returns a list,
    not just the first match.
    """
    cell = grid[grid["NAME"] == tile]
    if cell.empty:
        return []

    bbox = [round(v, 4) for v in cell.to_crs("EPSG:4326").total_bounds]

    body = {
        "collections": [S1_COLLECTION],
        "bbox": bbox,
        "datetime": f"{date}T00:00:00Z/{date}T23:59:59Z",
        "limit": 100,
        # No fields filter — get all properties
    }

    resp = session.post(SH_CATALOG_URL, json=body, timeout=60)
    resp.raise_for_status()

    matches = []
    for item in resp.json().get("features", []):
        clean = item["id"].replace(".SAFE", "").removesuffix("_COG")
        parts = clean.split("_")
        if len(parts) >= 8 and parts[7] == datatake:
            matches.append(item)
    return matches


def derived_from_uri(scene_id: str) -> str:
    """
    Construct the sentinel-s1-l1c source product path from a scene ID.

    Target format (matching DEAfrica convention):
        s3://sentinel-s1-l1c/GRD/{year}/{month}/{day}/IW/DV/{scene_id}

    Note: month and day are NOT zero-padded in this bucket's convention,
    and the scene ID used is the original L1C scene ID (no .SAFE, no _COG
    suffix - those are CDSE/CloudFerro-specific additions).
    """
    clean_id = scene_id.replace(".SAFE", "").removesuffix("_COG")
    parts = clean_id.split("_")
    # parts[4] is the start timestamp, e.g. "20260512T160924"
    start_ts = parts[4]
    year = start_ts[0:4]
    month = str(int(start_ts[4:6]))  # no zero-padding
    day = str(int(start_ts[6:8]))  # no zero-padding
    return f"s3://sentinel-s1-l1c/GRD/{year}/{month}/{day}/IW/DV/{clean_id}"


def _parse_datetime(dt_str: str) -> datetime:
    "Parses an ISO8601 datetime string, handling the 'Z' suffix for UTC if present."
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00"))


def build_stac_item(
    session,
    tile: str,
    date: str,
    datatake: str,
    sat_config: dict,
    bands: list[str],
    grid: gpd.GeoDataFrame,
    s3_bucket_name: str = S1_BUCKET_NAME,
) -> pystac.Item | None:
    """
    Build a validated pystac.Item for a CDSE batch output tile.

    A tile/date/datatake combination may correspond to multiple source
    scenes (e.g. tile spans two adjacent passes) - all are added as
    separate derived_from links. Scene properties (orbit, platform, etc.)
    are taken from the first match.
    """
    scenes = fetch_scenes_from_sh(session, tile, date, datatake, grid)
    if not scenes:
        log.warning(f"No matching scene found for {tile}/{date}/{datatake}")
        return None

    primary = scenes[0]
    props = primary["properties"]

    cell = grid[grid["NAME"] == tile]
    if cell.empty:
        log.warning(f"Tile {tile} not found in grid")
        return None

    geom = cell.to_crs("EPSG:4326").geometry.iloc[0]
    bbox = list(cell.to_crs("EPSG:4326").total_bounds)

    item_id = f"{tile}_{date.replace('-', '_')}_{datatake}"
    item_datetime = (
        _parse_datetime(props["datetime"])
        if props.get("datetime")
        else datetime.now(timezone.utc)
    )

    item = pystac.Item(
        id=item_id,
        geometry=geom.__geo_interface__,
        bbox=bbox,
        datetime=item_datetime,
        properties={
            "odc:region_code": tile,
            "odc:product": sat_config.get("odc_product", "s1_rtc"),
            "odc:processing_datetime": datetime.now(timezone.utc).isoformat(),
        },
    )

    # SAR extension
    polarizations = [
        Polarization(p)
        for p in props.get("sar:polarizations", [])
        if p in Polarization.__members__
    ]
    if (
        props.get("sar:instrument_mode")
        and props.get("sar:frequency_band")
        and polarizations
    ):
        sar_ext = SarExtension.ext(item, add_if_missing=True)
        obs_dir = props.get("sar:observation_direction")
        sar_ext.apply(
            instrument_mode=props["sar:instrument_mode"],
            frequency_band=FrequencyBand(props["sar:frequency_band"]),
            polarizations=polarizations,
            product_type="RTC",
            center_frequency=props.get("sar:center_frequency"),
            observation_direction=(
                ObservationDirection(obs_dir)
                if obs_dir in ("left", "right")
                else ObservationDirection.RIGHT
            ),
        )

    # SAT extension
    orbit_state = props.get("sat:orbit_state")
    relative_orbit = props.get("sat:relative_orbit")
    if relative_orbit is not None and relative_orbit < 0:
        relative_orbit = None  # SH catalog sometimes returns -1 as a null sentinel

    if orbit_state or relative_orbit is not None:
        sat_ext = SatExtension.ext(item, add_if_missing=True)
        sat_ext.apply(
            orbit_state=(
                OrbitState(orbit_state)
                if orbit_state in ("ascending", "descending", "geostationary")
                else None
            ),
            relative_orbit=relative_orbit,
            absolute_orbit=props.get("sat:absolute_orbit"),
        )

    # Projection extension
    proj_ext = ProjectionExtension.ext(item, add_if_missing=True)
    proj_ext.epsg = 4326

    # Extra core properties
    item.properties["constellation"] = "sentinel-1"
    item.properties["instruments"] = ["c-sar"]
    if props.get("platform"):
        item.properties["platform"] = props["platform"]
    item.properties["start_datetime"] = props.get(
        "start_datetime", props.get("datetime")
    )
    item.properties["end_datetime"] = props.get("end_datetime", props.get("datetime"))

    # Assets (batch output tifs)
    # Per-asset proj:shape/proj:transform - derived from the tile's bbox and
    # the resolution used in the batch payload (RESOLUTION, in degrees).
    # Shape is constant across all RTC outputs for a tile since they're all
    # processed at the same resolution over the same extent.
    proj_shape = [
        round((bbox[3] - bbox[1]) / RESOLUTION),  # height (lat extent / res)
        round((bbox[2] - bbox[0]) / RESOLUTION),  # width  (lon extent / res)
    ]
    proj_transform = [RESOLUTION, 0.0, bbox[0], 0.0, -RESOLUTION, bbox[3]]

    year, month, day = date.split("-")
    base_uri = f"s3://{s3_bucket_name}/{BASE_FOLDER_NAME}/{tile}/{year}/{month}/{day}/{datatake}"
    filename_prefix = f"s1_rtc_{datatake}_{tile}_{year}_{month}_{day}"

    # Polarization bands (VV, VH, etc.) - vary by sat_config
    for band in bands:
        item.add_asset(
            band.lower(),
            pystac.Asset(
                href=f"{base_uri}/{band}.tif",
                title=f"{filename_prefix}_{band}",
                media_type=pystac.MediaType.COG,
                description=f"polarization {band}",
                roles=["data"],
                extra_fields={
                    "proj:shape": proj_shape,
                    "proj:transform": proj_transform,
                    "sar:polarizations": [band],
                },
            ),
        )

    # Always-present RTC outputs - same for every Sentinel-1 RTC product
    # regardless of polarization config, so these are not driven by `bands`.
    rtc_fixed_assets = {
        "area": {
            "filename": "AREA.tif",
            "title": f"{filename_prefix}_AREA",
            "description": "normalized scattering area",
            "media_type": pystac.MediaType.COG,
            "roles": ["data"],
            "has_proj": True,
        },
        "angle": {
            "filename": "ANGLE.tif",
            "title": f"{filename_prefix}_ANGLE",
            "description": "local incidence angle",
            "media_type": pystac.MediaType.COG,
            "roles": ["data"],
            "has_proj": True,
        },
        "mask": {
            "filename": "MASK.tif",
            "title": f"{filename_prefix}_MASK",
            "description": "data mask",
            "media_type": pystac.MediaType.COG,
            "roles": ["data"],
            "has_proj": True,
        },
    }

    for key, cfg in rtc_fixed_assets.items():
        extra_fields = {}
        if cfg["has_proj"]:
            extra_fields["proj:shape"] = proj_shape
            extra_fields["proj:transform"] = proj_transform

        item.add_asset(
            key,
            pystac.Asset(
                href=f"{base_uri}/{cfg['filename']}",
                title=cfg["title"],
                media_type=cfg["media_type"],
                description=cfg["description"],
                roles=cfg["roles"],
                extra_fields=extra_fields,
            ),
        )

    # self link: where this STAC item file itself will live, alongside the tifs
    item.add_link(
        pystac.Link(
            rel="self",
            target=f"{base_uri}/{filename_prefix}_metadata.json",
            media_type="application/json",
        )
    )

    # derived_from links (one per source scene - can be more than one)
    for scene in scenes:
        item.add_link(
            pystac.Link(
                rel="derived_from",
                target=derived_from_uri(scene["id"]),
            )
        )

    # Validate against core STAC spec + applied extension schemas
    item.validate()

    return item


def save_stac_item(item: pystac.Item, out_dir: str = "stac_items") -> str:
    "Write a pystac.Item to a local JSON file. Returns the path written."
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, f"{item.id}.json")
    item.save_object(dest_href=out_path)
    return out_path


def upload_stac_item(local_path: str, item: pystac.Item, output_bucket: str) -> str:
    """
    Upload a locally saved STAC item JSON to S3 alongside the tif assets.
    Uses the item's self link to determine the S3 key.
    S3 credentials are read from env vars S3_ACCESS_KEY and S3_SECRET_ACCESS_KEY —
    the same credentials used in the batch payload delivery.
    Returns the S3 URI the item was uploaded to.
    """
    self_link = item.get_single_link("self")
    if self_link is None:
        raise ValueError(
            f"Item {item.id} has no self link — cannot determine S3 upload path"
        )

    s3_uri = self_link.target
    key = "/".join(s3_uri.split("/")[3:])

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "https://s3.waw3-1.cloudferro.com"),
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("S3_SECRET_ACCESS_KEY"),
    )
    s3.upload_file(
        local_path,
        output_bucket,
        key,
        ExtraArgs={"ContentType": "application/json"},
    )
    log.info(f"  uploaded STAC item to s3://{output_bucket}/{key}")
    return f"s3://{output_bucket}/{key}"


def submit_backfill_jobs(
    missing: list[str],
    grid: gpd.GeoDataFrame,
    output_bucket: str,
    bucket_name: str,
    generate_stac: bool = True,
) -> list[dict]:
    """For each missing s1_rtc dataset URI, build + submit + start a CDSE batch job."""

    session = get_session()
    results = []

    for uri in missing:
        parts = uri.replace(f"s3://{bucket_name}/", "").split("/")
        tile, year, month, day, datatake = (
            parts[1],
            parts[2],
            parts[3],
            parts[4],
            parts[5],
        )
        date = f"{year}-{month}-{day}"

        cell = grid[grid["NAME"] == tile]
        if cell.empty:
            log.error(f"  tile {tile} not found in grid - skipping {uri}")
            results.append(
                {"uri": uri, "status": "error", "error": "tile not found in grid"}
            )
            continue

        geometry = mapping(cell.to_crs("EPSG:4326").union_all().buffer(-1e-5))

        payload = build_batch_payload(
            sat_config=SATELLITES["s1"],
            geometry=geometry,
            time_from=f"{date}T00:00:00Z",
            time_to=f"{year}-{month}-{int(day) + 1:02d}T00:00:00Z",
            tiling_id=TILING_ID,
            resolution=RESOLUTION,
        )

        payload["output"]["delivery"]["s3"][
            "url"
        ] = f"s3://{output_bucket}/s1_rtc/{year}/{month}/{day}/{datatake}"

        try:
            resp = session.post(SH_BATCH_URL, json=payload)
            resp.raise_for_status()
            job_id = resp.json()["id"]
            session.post(f"{SH_BATCH_URL}/{job_id}/start")
            log.info(f"Created + started job for {tile} {datatake}: {job_id}")

            result = {
                "uri": uri,
                "status": "started",
                "job_id": job_id,
                "tile": tile,
                "datatake": datatake,
            }

            if generate_stac:
                item = build_stac_item(
                    session,
                    tile,
                    date,
                    datatake,
                    SATELLITES["s1"],
                    ["VV", "VH"],
                    grid,
                    s3_bucket_name=bucket_name,
                )
                if item:
                    out_path = save_stac_item(item)
                    log.info(f"  wrote STAC item locally: {out_path}")
                    try:
                        s3_uri = upload_stac_item(out_path, item, output_bucket)
                        result["stac_item"] = s3_uri
                    except Exception as e:
                        log.error(f"  failed to upload STAC item to S3: {e}")
                        result["stac_item"] = out_path  # fall back to local path

            results.append(result)

        except Exception as e:
            log.error(f"  job submission failed for {uri}: {e}")
            results.append({"uri": uri, "status": "error", "error": str(e)})

    return results


@click.command("s1-cdse-pipeline")
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
    "--s3-bucket-name",
    default=S1_BUCKET_NAME,
    show_default=True,
    help="S3 bucket to check for existing s1_rtc datasets.",
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
    "--stac-test",
    is_flag=True,
    default=False,
    help="Generate a single STAC item JSON for the first missing dataset and exit.",
)
@click.option(
    "--limit",
    default=None,
    type=int,
    help="Only submit the first N missing datasets. Useful for testing.",
)
@click.option(
    "--output", "-o", default=None, help="Optional path to write the JSON report to."
)
def cli(
    start_date,
    end_date,
    output_bucket,
    s3_bucket_name,
    n_workers,
    dry_run,
    stac_test,
    limit,
    output,
):
    """
    Check for missing s1_rtc datasets in the DE Africa bucket and submit CDSE
    batch backfill jobs for any gaps found.

    Example commands:

        # dry run — gap check only, no jobs submitted
        s1-cdse-pipeline --start-date 2026-06-03 --end-date 2026-06-10 --dry-run

        # test STAC generation for first missing dataset only (no jobs submitted)
        s1-cdse-pipeline --start-date 2026-06-03 --end-date 2026-06-10 --stac-test --dry-run

        # limit to 5 backfill jobs (with STAC generation) for testing
        s1-cdse-pipeline --start-date 2026-06-03 --end-date 2026-06-10 --limit 5

        # full run
        s1-cdse-pipeline --start-date 2026-06-03 --end-date 2026-06-10
    """
    now = datetime.now(timezone.utc)

    # End = yesterday (date-only, inclusive)
    end_date = end_date or (now - timedelta(days=1)).date().isoformat()

    # Start = 7 days before end
    start_date = start_date or (now - timedelta(days=8)).date().isoformat()

    log.info(
        f"Gap check: {start_date} -> {end_date}, dry_run={dry_run}, stac_test={stac_test}"
    )

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

    missing_datasets = find_missing(
        datasets, bucket_name=s3_bucket_name, n_workers=n_workers
    )
    log.info(f"Missing: {len(missing_datasets)}")

    report = {
        "start_date": start_date,
        "end_date": end_date,
        "expected": len(datasets),
        "missing_count": len(missing_datasets),
        "missing_datasets": missing_datasets,
    }

    # --stac-test: generate one STAC item for the first missing dataset and exit.
    # No jobs are submitted regardless of --dry-run.
    if stac_test and missing_datasets:
        uri = missing_datasets[0]
        parts = uri.replace(f"s3://{s3_bucket_name}/", "").split("/")
        tile, year, month, day, datatake = (
            parts[1],
            parts[2],
            parts[3],
            parts[4],
            parts[5],
        )
        date = f"{year}-{month}-{day}"

        log.info(f"Generating test STAC item for {tile} {date} {datatake} ...")
        session = get_session()
        item = build_stac_item(
            session,
            tile,
            date,
            datatake,
            output_bucket,
            SATELLITES["s1"],
            ["VV", "VH"],
            africa_grid,
            s3_bucket_name=s3_bucket_name,
        )
        if item:
            out_path = save_stac_item(item)
            log.info(f"Wrote test STAC item locally: {out_path}")
            try:
                s3_uri = upload_stac_item(out_path, item, output_bucket)
                log.info(f"Uploaded test STAC item to: {s3_uri}")
            except Exception as e:
                log.error(f"Failed to upload STAC item to S3: {e}")
            print(json.dumps(item.to_dict(), indent=2))
        else:
            log.warning("No STAC item generated - no matching scene found.")
        sys.exit(0)

    if missing_datasets and not dry_run:
        to_submit = missing_datasets[:limit] if limit else missing_datasets
        if limit:
            log.info(
                f"Limiting submission to {len(to_submit)} of {len(missing_datasets)} missing datasets"
            )
        report["jobs"] = submit_backfill_jobs(
            to_submit, africa_grid, output_bucket, bucket_name=s3_bucket_name
        )

    print(json.dumps(report, indent=2))

    if output:
        with open(output, "w") as f:
            json.dump(report, f, indent=2)
        log.info(f"Report written to {output}")


if __name__ == "__main__":
    cli()
