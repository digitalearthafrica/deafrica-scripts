#!/usr/bin/env python3
"""
Sentinel-1 RTC gap check + CDSE batch backfill submission.

The daily run scans the Sentinel Hub catalogue (default: seven inclusive days
ending yesterday) and classifies every expected tile + date + datatake:

    on_pds         published metadata JSON is already in the DE Africa bucket
    pending_sync   CloudFerro already has the output, or Batch is DONE
    in_flight      a Batch job is still CREATED, ANALYSING, or PROCESSING
    restart        ANALYSIS_DONE has been waiting longer than 6 hours
    retry          FAILED or PARTIAL, with fewer than 3 attempts
    exhausted      FAILED or PARTIAL after 3 attempts
    stopped        STOPPED or CANCELED; not retried
    true_gap       nowhere else

Only true gaps and bounded retries create a Batch job. A stuck ANALYSIS_DONE
job is started again instead of being recreated. A daily and monthly PU budget
refuses work that would cross the cap.

Options:
    --start-date, --end-date   YYYY-MM-DD. Defaults: yesterday, back 7 days.
    --backfill                 required when the window is longer than 14 days
    --n-workers                parallel S3 HEAD requests (default 32)
    --dry-run                  classify only; do not start Batch jobs
    --limit                    submit at most N actionable datasets
    --pu-per-tile              estimated PU for one tile (default 265)
    --daily-pu-budget          default 100000; this run will not cross it
    --monthly-pu-budget        default 2000000 of S1 PU since the 1st
    --no-pu-budget             disable both caps (intentional backfill)
    --output-bucket            CloudFerro bucket Batch writes to
    --s3-bucket-name           PDS bucket checked for published metadata
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
from shapely.geometry import mapping, shape
from shapely.ops import unary_union

from .auth import get_session
from .payloads import build_batch_payload
from .satellites import SATELLITES
from .utils import poll_status

from deafrica.logs import setup_logging

# Configuration
SH_CATALOG_URL = "https://sh.dataspace.copernicus.eu/api/v1/catalog/1.0.0/search"
SH_BATCH_URL = "https://sh.dataspace.copernicus.eu/api/v2/batch/process"
S1_COLLECTION = "sentinel-1-grd"

TILING_GRID = "https://s3.eu-central-1.amazonaws.com/sh-batch-grids/tiling-grid-3.zip"
AFRICA_EXTENT_URL = "https://cdn.jsdelivr.net/gh/digitalearthafrica/deafrica-extent@master/africa-extent.json"

S1_BUCKET_NAME = "deafrica-sentinel-1"
BASE_FOLDER_NAME = "s1_rtc"
REGION_NAME = "af-south-1"

RESOLUTION = SATELLITES["s1"]["input"]["resolution"]
DEFAULT_OUTPUT_BUCKET = "cdse_batch_test_bucket"
CONTROL_PREFIX = "s1_rtc_control"
DEFAULT_PU_PER_TILE = 265
DEFAULT_DAILY_PU_BUDGET = 100_000
DEFAULT_MONTHLY_PU_BUDGET = 2_000_000
MAX_SUBMIT_ATTEMPTS = 3
ANALYSIS_DONE_STALE = timedelta(hours=6)
MAX_SCAN_DAYS_WITHOUT_BACKFILL = 14
DEFAULT_S1_ANALYSE_TIMEOUT = 30 * 60
IN_FLIGHT_STATUSES = {"CREATED", "ANALYSING", "PROCESSING"}
RETRY_STATUSES = {"FAILED", "PARTIAL"}
STOPPED_STATUSES = {"STOPPED", "CANCELED"}
UNCOSTED_STATUSES = IN_FLIGHT_STATUSES | {"ANALYSIS_DONE"}
ACTIONABLE_STATES = {"restart", "true_gap", "retry"}
STATE_NAMES = (
    "on_pds",
    "pending_sync",
    "in_flight",
    "restart",
    "retry",
    "exhausted",
    "stopped",
    "true_gap",
)

log = setup_logging()


def next_day_iso(date: str) -> str:
    "Return the next calendar day for a YYYY-MM-DD date string."
    return (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )


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


def _clip_scenes_to_tile(scenes: list[dict], tile_geom) -> object:
    """
    Returns the intersection of all scene footprints with the tile geometry.
    For a single scene this is a clean polygon. For multiple scenes the footprints
    should tessellate perfectly, but floating point gaps in the SH catalog geometries
    can produce a MultiPolygon — in that case we fall back to the convex hull to
    guarantee a single Polygon.
    """
    clipped = tile_geom.intersection(
        unary_union([shape(s["geometry"]) for s in scenes])
    )
    if clipped.geom_type == "MultiPolygon":
        clipped = clipped.convex_hull
    return clipped


def search_sh_catalog(session, bbox: list[float], date: str) -> list[dict]:
    """Search the Sentinel Hub catalogue for Sentinel-1 IW GRD scenes.

    Returns the original features, including properties, so later STAC and XML
    steps can reuse this response instead of posting once per tile.
    """
    body = {
        "collections": [S1_COLLECTION],
        "bbox": [round(v, 4) for v in bbox],
        "datetime": f"{date}T00:00:00Z/{date}T23:59:59Z",
        "limit": 100,
    }

    features = []
    while True:
        resp = session.post(SH_CATALOG_URL, json=body, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        for feature in data.get("features", []):
            if _keep_iw_scene(feature["id"]):
                features.append(feature)
        nxt = data.get("context", {}).get("next")
        if nxt is None:
            break
        body["next"] = nxt
    return features


def features_to_frame(features: list[dict]) -> gpd.GeoDataFrame:
    "Build the filename/geometry frame used to name expected datasets."
    rows = [
        {
            "type": "Feature",
            "geometry": feature["geometry"],
            "properties": {"filename": feature["id"]},
        }
        for feature in features
        if feature.get("geometry")
    ]
    return _frame_from_features(rows)


def search_scenes(
    bbox: list[float], start_date: str, end_date: str, session=None
) -> tuple[gpd.GeoDataFrame, dict[str, list[dict]], list[str]]:
    """Search each day in the window.

    Returns the scene frame, features keyed by the search date, and dates whose
    search raised. A failed day is absent from the feature map and is not
    treated as an empty catalogue.
    """
    session = session or get_session()
    features_by_date: dict[str, list[dict]] = {}
    failures: list[str] = []
    frames = []
    for day in pd.date_range(start_date, end_date, freq="D"):
        date = day.strftime("%Y-%m-%d")
        log.info(f"Searching SH catalogue for {date} ...")
        try:
            day_features = search_sh_catalog(session, bbox, date)
        except Exception as exc:
            log.error(f"  search failed for {date}: {exc}")
            failures.append(date)
            continue
        features_by_date[date] = day_features
        log.info(f"  {len(day_features)} IW GRD scenes")
        if day_features:
            frames.append(features_to_frame(day_features))

    if not frames:
        return _frame_from_features([]), features_by_date, failures
    scenes = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs="EPSG:4326")
    return scenes, features_by_date, failures


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
        "s3",
        region_name=REGION_NAME,
        config=Config(
            signature_version=UNSIGNED,
            max_pool_connections=n_workers,
        ),
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


def pds_present_datasets(
    datasets: list[str], bucket_name: str, n_workers: int = 32
) -> set[str]:
    "Return the dataset ids whose PDS metadata JSON is already published."
    s3 = boto3.client(
        "s3",
        region_name=REGION_NAME,
        config=Config(
            signature_version=UNSIGNED,
            max_pool_connections=n_workers,
        ),
    )
    present = set()
    log.info(
        f"Checking {len(datasets)} expected datasets against s3://{bucket_name} ..."
    )
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {
            pool.submit(check_metadata_exists, s3, dataset, bucket_name): dataset
            for dataset in datasets
        }
        for future in as_completed(futures):
            dataset, exists = future.result()
            if exists:
                present.add(dataset)
    return present


def scene_datatake(scene_id: str) -> str | None:
    parts = scene_id.replace(".SAFE", "").removesuffix("_COG").split("_")
    if len(parts) < 8:
        return None
    return parts[7]


def scan_day_count(start_date: str, end_date: str) -> int:
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    return (end - start).days + 1


def parse_dataset_id(dataset: str) -> dict:
    """Split s1_rtc/{tile}/{yyyy}/{mm}/{dd}/{datatake} into its parts.

    Accepts an s3://bucket/ prefix and ignores anything after the datatake.
    """
    path = dataset
    if path.startswith("s3://"):
        path = path.split("/", 3)[-1]
    parts = path.strip("/").split("/")
    if len(parts) < 6:
        raise ValueError(f"Not an s1_rtc dataset id: {dataset}")
    product, tile, year, month, day, datatake = parts[:6]
    return {
        "dataset": "/".join(parts[:6]),
        "tile": tile,
        "year": year,
        "month": month,
        "day": day,
        "datatake": datatake,
        "date": f"{year}-{month}-{day}",
        "product": product,
    }


def cloudferro_s3_client():
    "S3 client for the CloudFerro staging bucket, using the Batch delivery keys."
    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "https://s3.waw3-1.cloudferro.com"),
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("S3_SECRET_ACCESS_KEY"),
    )


def catalog_cache_key(date: str) -> str:
    return f"{CONTROL_PREFIX}/catalog/{date}.json"


def dataset_control_key(dataset: str) -> str:
    parsed = parse_dataset_id(dataset)
    return (
        f"{CONTROL_PREFIX}/datasets/{parsed['tile']}/"
        f"{parsed['year']}/{parsed['month']}/{parsed['day']}/"
        f"{parsed['datatake']}.json"
    )


def job_control_key(job_id: str) -> str:
    return f"{CONTROL_PREFIX}/jobs/{job_id}.json"


def _s3_missing(err: ClientError) -> bool:
    return err.response.get("Error", {}).get("Code") in {
        "404",
        "NoSuchKey",
        "NotFound",
    }


def get_json(s3, bucket: str, key: str) -> dict | None:
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as err:
        if _s3_missing(err):
            return None
        raise
    return json.loads(obj["Body"].read())


def put_json(s3, bucket: str, key: str, payload: dict) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )


def write_catalog_caches(
    s3, bucket: str, features_by_date: dict[str, list[dict]]
) -> None:
    "Persist one catalogue file per successfully searched day."
    for date, features in sorted(features_by_date.items()):
        put_json(
            s3,
            bucket,
            catalog_cache_key(date),
            {"date": date, "features": features},
        )
        log.info(f"Wrote catalogue cache for {date} ({len(features)} scenes)")


def scenes_for_tile(features: list[dict], tile_geom) -> list[dict]:
    "Keep catalogue features whose footprint intersects the tile."
    if tile_geom is None:
        return []
    matched = []
    for feature in features:
        geometry = feature.get("geometry")
        if geometry and shape(geometry).intersects(tile_geom):
            matched.append(feature)
    return matched


def tile_geometry(grid: gpd.GeoDataFrame, tile: str):
    cell = grid[grid["NAME"] == tile]
    if cell.empty:
        return None
    return cell.to_crs("EPSG:4326").geometry.iloc[0]


def resolve_tile_scenes(
    features_by_date: dict[str, list[dict]] | None,
    date: str,
    datatake: str,
    tile_geom,
    fetch,
) -> list[dict]:
    """Use the day's catalogue response when that date was searched.

    `fetch` runs only when the date is absent, which is a failed search or a
    later workflow whose cache object was never written.
    """
    if features_by_date is not None and date in features_by_date:
        features = [
            feature
            for feature in features_by_date[date]
            if scene_datatake(feature.get("id", "")) == datatake
        ]
        return scenes_for_tile(features, tile_geom)
    return fetch()


def scenes_for_xml(
    cf_s3,
    bucket: str,
    session,
    tile: str,
    date: str,
    datatake: str,
    grid: gpd.GeoDataFrame,
    fetch=None,
) -> list[dict]:
    """Scenes for CARD4L XML. Prefer the submit-time catalogue cache."""
    fetch = fetch or fetch_scenes_from_sh
    cached = get_json(cf_s3, bucket, catalog_cache_key(date))
    tile_geom = tile_geometry(grid, tile)
    if cached is not None:
        return resolve_tile_scenes(
            {date: cached.get("features") or []},
            date,
            datatake,
            tile_geom,
            fetch=lambda: fetch(session, tile, date, datatake, grid),
        )
    return fetch(session, tile, date, datatake, grid)


def dataset_id_from_cloudferro_key(key: str) -> str | None:
    """Map s1_rtc/yyyy/mm/dd/datatake/job_id/tile/file to the dataset id."""
    parts = key.strip("/").split("/")
    if len(parts) < 7:
        return None
    product, year, month, day, datatake, _job_id, tile = parts[:7]
    if product != BASE_FOLDER_NAME:
        return None
    if not (len(year) == 4 and year.isdigit()):
        return None
    if not (len(month) == 2 and month.isdigit() and len(day) == 2 and day.isdigit()):
        return None
    if len(tile) != 7:
        return None
    return f"{product}/{tile}/{year}/{month}/{day}/{datatake}"


def cloudferro_dataset_ids(s3, bucket: str, dates: list[str]) -> set[str]:
    "Dataset ids that already have any object under the CloudFerro tile prefix."
    found = set()
    paginator = s3.get_paginator("list_objects_v2")
    for date in dates:
        year, month, day = date.split("-")
        prefix = f"{BASE_FOLDER_NAME}/{year}/{month}/{day}/"
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                dataset = dataset_id_from_cloudferro_key(obj["Key"])
                if dataset:
                    found.add(dataset)
    return found


def _record_time(record: dict) -> datetime | None:
    # status_since is when the job entered its current status. updated_at is
    # rewritten on every poll, so it cannot be used to spot a stuck analysis.
    for field in ("status_since", "submitted_at"):
        raw = record.get(field)
        if not raw:
            continue
        try:
            parsed = _parse_datetime(raw)
        except (TypeError, ValueError):
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    return None


def classify_dataset(
    *,
    on_pds: bool,
    on_cloudferro: bool,
    record: dict | None,
    now: datetime,
) -> str:
    """Decide what to do with one expected dataset. See the module docstring."""
    if on_pds:
        return "on_pds"
    if on_cloudferro:
        return "pending_sync"
    if not record:
        return "true_gap"

    status = record.get("status")
    if status == "DONE":
        return "pending_sync"
    if status in STOPPED_STATUSES:
        return "stopped"
    if status == "ANALYSIS_DONE":
        stamp = _record_time(record)
        if stamp is None or now - stamp >= ANALYSIS_DONE_STALE:
            return "restart"
        return "in_flight"
    if status in IN_FLIGHT_STATUSES:
        return "in_flight"

    attempts = int(record.get("attempts") or 0)
    if status in RETRY_STATUSES:
        if attempts >= MAX_SUBMIT_ATTEMPTS:
            return "exhausted"
        return "retry"
    return "in_flight"


def count_states(rows: list[dict]) -> dict[str, int]:
    counts = {name: 0 for name in STATE_NAMES}
    for row in rows:
        state = row["state"]
        counts[state] = counts.get(state, 0) + 1
    return counts


def queue_sort_key(row: dict, preferred_date: str) -> tuple:
    """Restarts, then preferred-day gaps, then older gaps, then retries.

    Newer dates come first inside a group so a tight budget spends on the
    newest holes.
    """
    state = row["state"]
    if state == "restart":
        group = 0
    elif state == "true_gap" and row["date"] == preferred_date:
        group = 1
    elif state == "true_gap":
        group = 2
    else:
        group = 3
    ordinal = datetime.strptime(row["date"], "%Y-%m-%d").toordinal()
    return (group, -ordinal, row["dataset"])


def ordered_actionable(rows: list[dict], preferred_date: str) -> list[dict]:
    actionable = [row for row in rows if row["state"] in ACTIONABLE_STATES]
    return sorted(actionable, key=lambda row: queue_sort_key(row, preferred_date))


def apply_pu_budget(
    items: list[dict],
    *,
    pu_per_tile: int,
    daily_budget: int | None,
    monthly_budget: int | None,
    month_to_date: float,
    enabled: bool,
) -> tuple[list[dict], list[dict]]:
    """Keep the prefix of `items` that fits both caps.

    The first item that would cross either cap, and everything after it, is
    deferred. Costs are uniform, so this is the same queue the sort produced.
    """
    if not enabled:
        return list(items), []

    accepted = []
    spent = 0
    for index, item in enumerate(items):
        next_spend = spent + pu_per_tile
        over_daily = daily_budget is not None and next_spend > daily_budget
        over_month = (
            monthly_budget is not None and month_to_date + next_spend > monthly_budget
        )
        if over_daily or over_month:
            return accepted, items[index:]
        accepted.append(item)
        spent = next_spend
    return accepted, []


def _job_created(job: dict) -> datetime | None:
    raw = job.get("created")
    if not raw:
        return None
    try:
        parsed = _parse_datetime(raw)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def s1_month_to_date_pu(
    jobs: list[dict],
    extra_records: list[dict],
    month_start: datetime,
    pu_per_tile: int,
) -> float:
    """S1 PU since month_start, plus an estimate for jobs that have no cost yet."""
    total = 0.0
    counted = set()
    for job in jobs:
        created = _job_created(job)
        if created is None or created < month_start:
            continue
        # CDSE returns description on request, next to processRequest/input/output.
        description = (job.get("request") or {}).get("description") or ""
        if "s1_rtc" not in description:
            continue
        job_id = job.get("id")
        if job_id:
            counted.add(job_id)
        cost = float(job.get("costPU") or 0)
        if cost:
            total += cost
        elif job.get("status") in UNCOSTED_STATUSES:
            total += pu_per_tile

    for record in extra_records:
        job_id = record.get("job_id")
        if job_id in counted:
            continue
        if record.get("status") not in UNCOSTED_STATUSES:
            continue
        total += float(record.get("estimated_pu") or pu_per_tile)
        if job_id:
            counted.add(job_id)
    return total


def list_batch_jobs_since(session, since: datetime, max_pages: int = 500) -> list[dict]:
    """List Batch jobs created on or after `since`, newest first.

    Stops once a page contains a job older than `since`. The list endpoint
    pages with `links.next`, or with `viewtoken` when no next URL is given.
    """
    jobs = []
    params: dict | None = {"count": 100, "sort": "created:desc"}
    url = SH_BATCH_URL
    seen = set()
    for _ in range(max_pages):
        resp = session.get(url, params=params, timeout=120)
        resp.raise_for_status()
        payload = resp.json()
        page = payload.get("data") if isinstance(payload, dict) else payload
        if not page:
            break
        reached_older = False
        for job in page:
            created = _job_created(job)
            if created is not None and created < since:
                reached_older = True
                continue
            jobs.append(job)
        if reached_older:
            break
        links = payload.get("links") or {} if isinstance(payload, dict) else {}
        next_url = links.get("next")
        token = links.get("nextToken")
        if next_url:
            if next_url in seen:
                break
            seen.add(next_url)
            url = next_url
            params = None
            continue
        if not token or token in seen:
            break
        seen.add(token)
        params = {"count": 100, "sort": "created:desc", "viewtoken": token}
    return jobs


def get_batch_job(session, job_id: str) -> dict:
    resp = session.get(f"{SH_BATCH_URL}/{job_id}", timeout=60)
    resp.raise_for_status()
    return resp.json()


def load_control_record(s3, bucket: str, dataset: str) -> dict | None:
    return get_json(s3, bucket, dataset_control_key(dataset))


def refresh_control_records(
    session,
    s3,
    bucket: str,
    datasets: list[str],
    on_pds: set[str],
    on_cloudferro: set[str],
) -> dict[str, dict]:
    """Refresh Batch status for datasets that are not already on PDS or CloudFerro."""
    records = {}
    now = datetime.now(timezone.utc).isoformat()
    for dataset in datasets:
        if dataset in on_pds or dataset in on_cloudferro:
            continue
        record = load_control_record(s3, bucket, dataset)
        if not record:
            continue
        job_id = record.get("job_id")
        if not job_id:
            records[dataset] = record
            continue
        try:
            info = get_batch_job(session, job_id)
        except Exception as exc:
            log.error(f"  status refresh failed for {dataset} job {job_id}: {exc}")
            records[dataset] = record
            continue
        updated = dict(record)
        new_status = info.get("status") or record.get("status")
        if new_status != record.get("status") or not record.get("status_since"):
            updated["status_since"] = (
                info.get("lastUpdated")
                or info.get("userActionUpdated")
                or record.get("submitted_at")
                or now
            )
        if new_status:
            updated["status"] = new_status
        if info.get("costPU") is not None:
            updated["cost_pu"] = info["costPU"]
        if info.get("error"):
            updated["error"] = info["error"]
        updated["updated_at"] = now
        try:
            put_json(s3, bucket, dataset_control_key(dataset), updated)
        except Exception as exc:
            log.error(f"  could not store refreshed status for {dataset}: {exc}")
        records[dataset] = updated
    return records


def classify_datasets(
    datasets: list[str],
    *,
    on_pds: set[str],
    on_cloudferro: set[str],
    records: dict[str, dict],
    now: datetime,
) -> list[dict]:
    rows = []
    for dataset in datasets:
        parsed = parse_dataset_id(dataset)
        record = records.get(dataset)
        rows.append(
            {
                "dataset": dataset,
                "date": parsed["date"],
                "tile": parsed["tile"],
                "datatake": parsed["datatake"],
                "record": record,
                "state": classify_dataset(
                    on_pds=dataset in on_pds,
                    on_cloudferro=dataset in on_cloudferro,
                    record=record,
                    now=now,
                ),
            }
        )
    return rows


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
    tile: str,
    date: str,
    datatake: str,
    sat_config: dict,
    bands: list[str],
    grid: gpd.GeoDataFrame,
    scenes: list[dict],
    geom,
    s3_bucket_name: str = S1_BUCKET_NAME,
) -> pystac.Item | None:
    """
    Build a validated pystac.Item for a CDSE batch output tile.

    scenes and geom are pre-computed by the caller (fetch_scenes_from_sh +
    _clip_scenes_to_tile) so this function does not hit the SH catalog itself.
    Scene properties (orbit, platform, etc.) are taken from the first scene.
    All scenes are added as separate derived_from links.
    """
    if not scenes:
        log.warning(f"No scenes provided for {tile}/{date}/{datatake}")
        return None

    primary = scenes[0]
    props = primary["properties"]

    cell = grid[grid["NAME"] == tile]
    if cell.empty:
        log.warning(f"Tile {tile} not found in grid")
        return None

    # tile bounds drive proj:shape and proj:transform (full tile extent)
    # geom.bounds drives the STAC item bbox (actual scene coverage)
    tile_bbox = list(cell.to_crs("EPSG:4326").total_bounds)
    item_bbox = list(geom.bounds)

    item_id = f"{tile}_{date.replace('-', '_')}_{datatake}"
    item_datetime = (
        _parse_datetime(props["datetime"])
        if props.get("datetime")
        else datetime.now(timezone.utc)
    )

    item = pystac.Item(
        id=item_id,
        geometry=geom.__geo_interface__,
        bbox=item_bbox,
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

    # Assets (Batch output tifs)
    # proj:shape and proj:transform are derived from the full tile bbox and
    # resolution
    proj_shape = [
        round((tile_bbox[3] - tile_bbox[1]) / RESOLUTION),  # height
        round((tile_bbox[2] - tile_bbox[0]) / RESOLUTION),  # width
    ]
    proj_transform = [RESOLUTION, 0.0, tile_bbox[0], 0.0, -RESOLUTION, tile_bbox[3]]

    year, month, day = date.split("-")
    base_uri = f"s3://{s3_bucket_name}/{BASE_FOLDER_NAME}/{tile}/{year}/{month}/{day}/{datatake}"
    filename_prefix = f"s1_rtc_{datatake}_{tile}_{year}_{month}_{day}"

    # Polarization bands (VV, VH, etc.) - vary by sat_config
    for band in bands:
        item.add_asset(
            band.lower(),
            pystac.Asset(
                href=f"{base_uri}/{filename_prefix}_{band}.tif",
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
            "filename": f"{filename_prefix}_AREA.tif",
            "title": f"{filename_prefix}_AREA",
            "description": "normalized scattering area",
            "media_type": pystac.MediaType.COG,
            "roles": ["data"],
            "has_proj": True,
        },
        "angle": {
            "filename": f"{filename_prefix}_ANGLE.tif",
            "title": f"{filename_prefix}_ANGLE",
            "description": "local incidence angle",
            "media_type": pystac.MediaType.COG,
            "roles": ["data"],
            "has_proj": True,
        },
        "mask": {
            "filename": f"{filename_prefix}_MASK.tif",
            "title": f"{filename_prefix}_MASK",
            "description": "data mask",
            "media_type": pystac.MediaType.COG,
            "roles": ["data"],
            "has_proj": True,
        },
        "metadata": {
            "filename": f"{filename_prefix}_metadata.xml",
            "title": f"{filename_prefix}_metadata",
            "description": "CARD4L (CEOS Analysis Ready Data for Land) processing metadata",
            "media_type": "application/xml",
            "roles": ["metadata", "card4l"],
            "has_proj": False,
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


def upload_stac_item(local_path: str, s3_key: str, output_bucket: str) -> str:
    """
    Upload a locally saved STAC item JSON to S3 alongside the tif assets.
    S3 credentials are read from env vars S3_ACCESS_KEY and S3_SECRET_ACCESS_KEY —
    the same credentials used in the batch payload delivery.
    Returns the S3 URI the item was uploaded to.
    """

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "https://s3.waw3-1.cloudferro.com"),
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("S3_SECRET_ACCESS_KEY"),
    )
    s3.upload_file(
        local_path,
        output_bucket,
        s3_key,
        ExtraArgs={"ContentType": "application/json"},
    )
    log.info(f"  uploaded STAC item to s3://{output_bucket}/{s3_key}")
    return f"s3://{output_bucket}/{s3_key}"


ANALYSIS_TERMINAL = {
    "ANALYSIS_DONE",
    "PROCESSING",
    "DONE",
    "FAILED",
    "CANCELED",
    "STOPPED",
}


def _update_dataset_record(s3, bucket: str, dataset: str, **fields) -> dict:
    record = load_control_record(s3, bucket, dataset) or {"dataset": dataset}
    previous_status = record.get("status")
    record.update(fields)
    now = datetime.now(timezone.utc).isoformat()
    record["updated_at"] = now
    if fields.get("status") and fields["status"] != previous_status:
        record["status_since"] = now
    put_json(s3, bucket, dataset_control_key(dataset), record)
    return record


def _save_new_job_record(
    s3,
    bucket: str,
    row: dict,
    job_id: str,
    description: str,
    pu_per_tile: int,
) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    attempts = int((row.get("record") or {}).get("attempts") or 0) + 1
    record = {
        "dataset": row["dataset"],
        "tile": row["tile"],
        "date": row["date"],
        "datatake": row["datatake"],
        "job_id": job_id,
        "status": "CREATED",
        "attempts": attempts,
        "submitted_at": now,
        "updated_at": now,
        "status_since": now,
        "estimated_pu": pu_per_tile,
        "description": description,
    }
    put_json(s3, bucket, dataset_control_key(row["dataset"]), record)
    put_json(
        s3,
        bucket,
        job_control_key(job_id),
        {
            "job_id": job_id,
            "datasets": [row["dataset"]],
            "description": description,
            "submitted_at": now,
        },
    )
    return record


def _attach_stac(
    result: dict,
    *,
    tile: str,
    date: str,
    datatake: str,
    year: str,
    month: str,
    day: str,
    grid: gpd.GeoDataFrame,
    scenes: list[dict],
    geom,
    pds_bucket: str,
    output_bucket: str,
    job_id: str,
) -> dict:
    """Upload the STAC item. A failure keeps the job id and marks the result as an error."""
    try:
        item = build_stac_item(
            tile,
            date,
            datatake,
            SATELLITES["s1"],
            ["VV", "VH"],
            grid,
            scenes=scenes,
            geom=geom,
            s3_bucket_name=pds_bucket,
        )
        if item is None:
            raise RuntimeError("STAC item was not built")
        out_path = save_stac_item(item)
        log.info(f"  wrote STAC item locally: {out_path}")
        filename_prefix = f"s1_rtc_{datatake}_{tile}_{year}_{month}_{day}"
        s3_key = (
            f"s1_rtc/{year}/{month}/{day}/{datatake}/{job_id}/"
            f"{tile}/{filename_prefix}_metadata.json"
        )
        result["stac_item"] = upload_stac_item(out_path, s3_key, output_bucket)
    except Exception as exc:
        log.error(f"  failed to upload STAC item for {result['dataset']}: {exc}")
        result["status"] = "error"
        result["error"] = f"stac upload failed: {exc}"
    return result


def _restart_analysed_job(session, s3, bucket: str, row: dict) -> dict:
    job_id = row["record"]["job_id"]
    dataset = row["dataset"]
    result = {
        "dataset": dataset,
        "tile": row["tile"],
        "datatake": row["datatake"],
        "job_id": job_id,
        "status": "restarted",
        "submitted": False,
    }
    try:
        resp = session.post(f"{SH_BATCH_URL}/{job_id}/start", timeout=120)
        if not resp.ok:
            log.error(f"Restart failed ({resp.status_code}): {resp.text[:2000]}")
            resp.raise_for_status()
    except Exception as exc:
        log.error(f"  restart failed for {dataset}: {exc}")
        result["status"] = "error"
        result["error"] = str(exc)
        return result
    _update_dataset_record(s3, bucket, dataset, status="PROCESSING")
    result["submitted"] = True
    log.info(f"Restarted analysed job for {row['tile']} {row['datatake']}: {job_id}")
    return result


def _submit_new_job(
    session,
    s3,
    bucket: str,
    row: dict,
    grid: gpd.GeoDataFrame,
    features_by_date: dict[str, list[dict]],
    output_bucket: str,
    pds_bucket: str,
    pu_per_tile: int,
    analyse_timeout: int,
    generate_stac: bool,
) -> dict:
    parsed = parse_dataset_id(row["dataset"])
    tile = parsed["tile"]
    date = parsed["date"]
    year, month, day, datatake = (
        parsed["year"],
        parsed["month"],
        parsed["day"],
        parsed["datatake"],
    )
    result = {
        "dataset": row["dataset"],
        "tile": tile,
        "datatake": datatake,
        "status": "error",
        "submitted": False,
    }

    tile_geom = tile_geometry(grid, tile)
    if tile_geom is None:
        result["error"] = "tile not found in grid"
        log.error(f"  tile {tile} not found in grid - skipping {row['dataset']}")
        return result

    scenes = resolve_tile_scenes(
        features_by_date,
        date,
        datatake,
        tile_geom,
        fetch=lambda: fetch_scenes_from_sh(session, tile, date, datatake, grid),
    )
    if not scenes:
        result["error"] = "no scenes found in SH catalog"
        log.error(f"  no scenes found for {tile} {date} {datatake}")
        return result

    clipped_geom = _clip_scenes_to_tile(scenes, tile_geom)
    payload = build_batch_payload(
        sat_config=SATELLITES["s1"],
        geometry=mapping(clipped_geom),
        time_from=f"{date}T00:00:00Z",
        time_to=f"{next_day_iso(date)}T00:00:00Z",
    )
    payload["description"] = f"s1_rtc {date} {datatake} {tile}"
    payload["output"]["delivery"]["s3"][
        "url"
    ] = f"s3://{output_bucket}/s1_rtc/{year}/{month}/{day}/{datatake}"

    try:
        resp = session.post(SH_BATCH_URL, json=payload, timeout=120)
        if not resp.ok:
            log.error(f"Create failed ({resp.status_code}): {resp.text[:2000]}")
            resp.raise_for_status()
        job_id = resp.json()["id"]
    except Exception as exc:
        log.error(f"  job creation failed for {row['dataset']}: {exc}")
        result["error"] = str(exc)
        return result

    result["job_id"] = job_id
    try:
        _save_new_job_record(
            s3, bucket, row, job_id, payload["description"], pu_per_tile
        )
    except Exception as exc:
        log.error(f"  control record write failed for {job_id}: {exc}")
        result["error"] = f"control record write failed: {exc}"
        return result

    try:
        analyse = session.post(f"{SH_BATCH_URL}/{job_id}/analyse", timeout=120)
        if not analyse.ok:
            log.error(f"Analyse failed ({analyse.status_code}): {analyse.text[:2000]}")
            analyse.raise_for_status()
        info = poll_status(
            session,
            job_id,
            terminal=ANALYSIS_TERMINAL,
            timeout=analyse_timeout,
        )
    except TimeoutError:
        try:
            info = get_batch_job(session, job_id)
        except Exception:
            info = {"status": "ANALYSING"}
        _update_dataset_record(
            s3, bucket, row["dataset"], status=info.get("status") or "ANALYSING"
        )
        result["status"] = "in_flight"
        result["submitted"] = True
        result["error"] = "analysis timed out"
        log.error(f"  analysis timed out for {job_id}")
        return result
    except Exception as exc:
        log.error(f"  analysis failed for {job_id}: {exc}")
        _update_dataset_record(
            s3, bucket, row["dataset"], status="FAILED", error=str(exc)
        )
        result["error"] = str(exc)
        return result

    status = info.get("status")
    _update_dataset_record(
        s3,
        bucket,
        row["dataset"],
        status=status,
        cost_pu=info.get("costPU"),
        error=info.get("error"),
    )
    if status in {"FAILED", "CANCELED", "STOPPED"}:
        result["error"] = f"analysis ended in {status}"
        log.error(f"  {result['error']} for {job_id}")
        return result

    if status == "ANALYSIS_DONE":
        start = session.post(f"{SH_BATCH_URL}/{job_id}/start", timeout=120)
        if not start.ok:
            log.error(f"Start failed ({start.status_code}): {start.text[:2000]}")
            result["error"] = f"start failed: {start.text[:500]}"
            return result
        _update_dataset_record(s3, bucket, row["dataset"], status="PROCESSING")
    elif status == "DONE" and not info.get("costPU"):
        log.info(f"  {job_id} finished during analysis with costPU=0")

    result["status"] = "started"
    result["submitted"] = True
    log.info(f"Created + started job for {tile} {datatake}: {job_id}")
    if generate_stac:
        result = _attach_stac(
            result,
            tile=tile,
            date=date,
            datatake=datatake,
            year=year,
            month=month,
            day=day,
            grid=grid,
            scenes=scenes,
            geom=clipped_geom,
            pds_bucket=pds_bucket,
            output_bucket=output_bucket,
            job_id=job_id,
        )
    return result


def submit_actionable_jobs(
    rows: list[dict],
    grid: gpd.GeoDataFrame,
    features_by_date: dict[str, list[dict]],
    output_bucket: str,
    pds_bucket: str,
    *,
    session,
    s3,
    pu_per_tile: int,
    analyse_timeout: int,
    generate_stac: bool = True,
) -> list[dict]:
    """Start restarts and create jobs for true gaps and retries."""
    results = []
    for row in rows:
        if row["state"] == "restart":
            results.append(_restart_analysed_job(session, s3, output_bucket, row))
            continue
        results.append(
            _submit_new_job(
                session,
                s3,
                output_bucket,
                row,
                grid,
                features_by_date,
                output_bucket,
                pds_bucket,
                pu_per_tile,
                analyse_timeout,
                generate_stac,
            )
        )
    return results


def _window_dates(start_date: str, end_date: str) -> list[str]:
    return [
        day.strftime("%Y-%m-%d")
        for day in pd.date_range(start_date, end_date, freq="D")
    ]


def _write_report(report: dict, output: str | None) -> None:
    print(json.dumps(report, indent=2))
    if output:
        with open(output, "w") as handle:
            json.dump(report, handle, indent=2)
        log.info(f"Report written to {output}")


def _log_classify_summary(report: dict) -> None:
    log.info(
        "S1 classify expected=%s on_pds=%s pending_sync=%s in_flight=%s "
        "restart=%s retry=%s exhausted=%s stopped=%s true_gap=%s "
        "submitted=%s deferred_budget=%s estimated_pu=%s "
        "month_to_date_pu=%s search_failures=%s",
        report["expected"],
        report["on_pds"],
        report["pending_sync"],
        report["in_flight"],
        report["restart"],
        report["retry"],
        report["exhausted"],
        report["stopped"],
        report["true_gap"],
        report["submitted"],
        report["deferred_budget"],
        report["estimated_pu"],
        report["month_to_date_pu"],
        len(report["search_failures"]),
    )


@click.command("s1-cdse-pipeline")
@click.option(
    "--start-date",
    "-s",
    default=None,
    help=(
        "Start date YYYY-MM-DD (inclusive). Defaults to eight days before "
        "today, a seven-day scan ending yesterday."
    ),
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
    help="Classify the window but do not start any Batch jobs.",
)
@click.option(
    "--stac-test",
    is_flag=True,
    default=False,
    help="Write one STAC item for the first true gap and exit without submitting.",
)
@click.option(
    "--limit",
    default=None,
    type=int,
    help="Submit at most N actionable datasets after classification.",
)
@click.option(
    "--pu-per-tile",
    default=DEFAULT_PU_PER_TILE,
    show_default=True,
    type=int,
    help="Estimated processing units for one Sentinel-1 RTC tile.",
)
@click.option(
    "--daily-pu-budget",
    default=DEFAULT_DAILY_PU_BUDGET,
    show_default=True,
    type=int,
    help="Do not start work that would spend more than this many PU in one run.",
)
@click.option(
    "--monthly-pu-budget",
    default=DEFAULT_MONTHLY_PU_BUDGET,
    show_default=True,
    type=int,
    help="Do not start work that would push S1 month-to-date PU above this.",
)
@click.option(
    "--no-pu-budget",
    is_flag=True,
    default=False,
    help="Disable the daily and monthly PU caps. For a watched backfill only.",
)
@click.option(
    "--backfill",
    is_flag=True,
    default=False,
    help=(
        "Allow a scan longer than "
        f"{MAX_SCAN_DAYS_WITHOUT_BACKFILL} days. Required for historical runs."
    ),
)
@click.option(
    "--analyse-timeout",
    default=DEFAULT_S1_ANALYSE_TIMEOUT,
    show_default=True,
    type=int,
    help="Seconds to wait for Batch analysis of one job before moving on.",
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
    pu_per_tile,
    daily_pu_budget,
    monthly_pu_budget,
    no_pu_budget,
    backfill,
    analyse_timeout,
    output,
):
    """
    Classify s1_rtc datasets and submit CDSE batch jobs only for true gaps.

    A tile that is already on PDS, still on CloudFerro, or owned by a live
    Batch job is not submitted again. FAILED jobs are retried at most three
    times. The default window is seven inclusive days ending yesterday.

    Example commands:

        # classify only
        s1-cdse-pipeline --start-date 2026-06-03 --end-date 2026-06-10 --dry-run

        # historical window
        s1-cdse-pipeline --start-date 2026-05-01 --end-date 2026-06-10 --backfill --dry-run
    """
    now = datetime.now(timezone.utc)
    end_date = end_date or (now - timedelta(days=1)).date().isoformat()
    start_date = start_date or (now - timedelta(days=8)).date().isoformat()
    window_days = scan_day_count(start_date, end_date)
    if window_days > MAX_SCAN_DAYS_WITHOUT_BACKFILL and not backfill:
        raise click.UsageError(
            f"Scan window is {window_days} days. Pass --backfill to run a "
            f"window longer than {MAX_SCAN_DAYS_WITHOUT_BACKFILL} days."
        )

    log.info(
        f"Gap check: {start_date} -> {end_date}, dry_run={dry_run}, "
        f"stac_test={stac_test}, backfill={backfill}"
    )

    session = get_session()
    africa_grid = get_africa_grid()
    bbox = list(africa_grid.total_bounds)
    scenes, features_by_date, search_failures = search_scenes(
        bbox, start_date, end_date, session=session
    )

    try:
        cf_s3 = cloudferro_s3_client()
    except Exception as exc:
        cf_s3 = None
        log.error(f"CloudFerro client was not created: {exc}")
    else:
        try:
            write_catalog_caches(cf_s3, output_bucket, features_by_date)
        except Exception as exc:
            log.error(f"Catalogue cache was not written: {exc}")

    if scenes.empty:
        log.info("No scenes returned for this period - nothing to check.")
        report = {
            "start_date": start_date,
            "end_date": end_date,
            "expected": 0,
            "search_failures": search_failures,
            "submitted": 0,
            "deferred_budget": 0,
            "estimated_pu": 0,
            "month_to_date_pu": None,
        }
        for name in STATE_NAMES:
            report[name] = 0
        _log_classify_summary(report)
        _write_report(report, output)
        sys.exit(0)

    log.info(f"Total scenes: {len(scenes)}")
    grided = gpd.overlay(scenes, africa_grid, how="intersection")
    grided = grided[grided.geometry.to_crs("EPSG:3857").area > 0]
    datasets = create_dataset_names(grided)
    log.info(f"Expecting {len(datasets)} datasets for {start_date} -> {end_date}")

    on_pds = pds_present_datasets(
        datasets, bucket_name=s3_bucket_name, n_workers=n_workers
    )
    if cf_s3 is None:
        log.error("CloudFerro is unavailable; refusing to submit without it.")
        sys.exit(1)
    try:
        on_cloudferro = cloudferro_dataset_ids(
            cf_s3, output_bucket, _window_dates(start_date, end_date)
        )
    except Exception as exc:
        log.error(f"CloudFerro listing failed; refusing to submit: {exc}")
        sys.exit(1)
    records = refresh_control_records(
        session, cf_s3, output_bucket, datasets, on_pds, on_cloudferro
    )
    rows = classify_datasets(
        datasets,
        on_pds=on_pds,
        on_cloudferro=on_cloudferro,
        records=records,
        now=now,
    )
    counts = count_states(rows)
    actionable = ordered_actionable(rows, preferred_date=end_date)
    if limit is not None:
        actionable = actionable[:limit]

    budget_error = None
    month_to_date = 0.0
    if no_pu_budget:
        month_to_date_report = None
    else:
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        try:
            jobs = list_batch_jobs_since(session, month_start)
            month_to_date = s1_month_to_date_pu(
                jobs,
                list(records.values()),
                month_start,
                pu_per_tile,
            )
            month_to_date_report = month_to_date
        except Exception as exc:
            budget_error = str(exc)
            month_to_date_report = None
            log.error(f"Could not read month-to-date PU: {exc}")

    if budget_error and not dry_run and not stac_test:
        accepted, deferred = [], actionable
    else:
        accepted, deferred = apply_pu_budget(
            actionable,
            pu_per_tile=pu_per_tile,
            daily_budget=None if no_pu_budget else daily_pu_budget,
            monthly_budget=None if no_pu_budget else monthly_pu_budget,
            month_to_date=month_to_date,
            enabled=not no_pu_budget and budget_error is None,
        )

    report = {
        "start_date": start_date,
        "end_date": end_date,
        "expected": len(datasets),
        "submitted": 0,
        "deferred_budget": len(deferred),
        "deferred_datasets": [row["dataset"] for row in deferred],
        "exhausted_datasets": [
            row["dataset"] for row in rows if row["state"] == "exhausted"
        ],
        "estimated_pu": len(accepted) * pu_per_tile,
        "month_to_date_pu": month_to_date_report,
        "search_failures": search_failures,
        "budget_error": budget_error,
        "jobs": [],
    }
    report.update(counts)

    if stac_test:
        gap = next((row for row in rows if row["state"] == "true_gap"), None)
        if gap is None:
            log.info("No true gap to build a test STAC item for.")
        else:
            _write_test_stac(
                gap, features_by_date, africa_grid, session, s3_bucket_name
            )
        _log_classify_summary(report)
        _write_report(report, output)
        sys.exit(0)

    if accepted and not dry_run:
        report["jobs"] = submit_actionable_jobs(
            accepted,
            africa_grid,
            features_by_date,
            output_bucket,
            s3_bucket_name,
            session=session,
            s3=cf_s3,
            pu_per_tile=pu_per_tile,
            analyse_timeout=analyse_timeout,
        )
        report["submitted"] = sum(1 for job in report["jobs"] if job.get("submitted"))

    _log_classify_summary(report)
    _write_report(report, output)


def _write_test_stac(row, features_by_date, grid, session, pds_bucket: str) -> None:
    parsed = parse_dataset_id(row["dataset"])
    tile = parsed["tile"]
    date = parsed["date"]
    datatake = parsed["datatake"]
    log.info(f"Generating test STAC item for {tile} {date} {datatake} ...")
    tile_geom = tile_geometry(grid, tile)
    scenes = resolve_tile_scenes(
        features_by_date,
        date,
        datatake,
        tile_geom,
        fetch=lambda: fetch_scenes_from_sh(session, tile, date, datatake, grid),
    )
    if not scenes or tile_geom is None:
        log.warning("No scenes found in SH catalog — cannot generate STAC item.")
        return
    clipped_geom = _clip_scenes_to_tile(scenes, tile_geom)
    item = build_stac_item(
        tile,
        date,
        datatake,
        SATELLITES["s1"],
        ["VV", "VH"],
        grid,
        scenes=scenes,
        geom=clipped_geom,
        s3_bucket_name=pds_bucket,
    )
    if item is None:
        log.warning("No STAC item generated.")
        return
    out_path = save_stac_item(item)
    log.info(f"Wrote test STAC item locally: {out_path}")
    print(json.dumps(item.to_dict(), indent=2))


if __name__ == "__main__":
    cli()
