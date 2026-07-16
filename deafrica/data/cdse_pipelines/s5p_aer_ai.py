#!/usr/bin/env python3
"""
Submits a CDSE Batch Processing V2 job for Sentinel-5P TROPOMI L2 Aerosol
Index (s5p_tropomi_l2_aer_ai).
If run in stac mode, generates a STAC item for every processed tile and uploads it alongside the tifs.

Three stages, each its own subcommand (so each can run as its own Argo step):

    s5p-aer-ai-batch-test submit -d 2026-02-09   # create + analyse + start; prints
                                                 # {"job_id": ..., "timeliness": ...}
    s5p-aer-ai-batch-test wait --job-id <id>     # poll until terminal; exit 0 iff DONE
    s5p-aer-ai-batch-test stac -d 2026-02-09     # build + upload STAC items
"""

import concurrent.futures
import json
import os
import re
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone

import boto3
import botocore.exceptions
import click
import geopandas as gpd
import pandas as pd
import pyogrio
import pystac
from pyproj import Transformer
from pystac.extensions.projection import ProjectionExtension

from .auth import get_session
from .payloads import build_batch_payload, redact_payload
from .satellites import SATELLITES
from .s1 import SH_CATALOG_URL, _parse_datetime, save_stac_item, upload_stac_item

from deafrica.logs import setup_logging

SH_BATCH_URL = "https://sh.dataspace.copernicus.eu/api/v2/batch/process"

# CloudFerro staging bucket the batch job delivers into
DEFAULT_OUTPUT_BUCKET = "cdse_batch_test_bucket"

S5P_L2_COLLECTION = "sentinel-5p-l2"
# Native product type as it appears in S5P product ids and EODATA paths
NATIVE_PRODUCT_TYPE = "L2__AER_AI"
# DE Africa product type used in item ids, folder names, and product:type
PRODUCT_TYPE = "TROPO_L2_AI"
ODC_PRODUCT = "s5p_tropomi_l2_aer_ai"

# Output bands written by evalscripts/s5p_aer_ai.js and their nodata values
S5P_AER_AI_ASSET_NODATA = {
    "AER_AI_340_380": -9999,
    "AER_AI_354_388": -9999,
    "dataMask": 255,
}

# S5P processing mode (4-char field of the product id) -> archive folder
# timeliness token. NRTI (near-real-time) -> NR; OFFL (offline) and RPRO
# (reprocessed) are both non-time-critical -> NT.
S5P_TIMELINESS_TOKEN = {"NRTI": "NR", "OFFL": "NT", "RPRO": "NT"}

# product:timeliness (ISO 8601 duration) per timeliness category:
# NR (near-real-time) <= 3h, NT (non-time-critical) <= 1 month.
TIMELINESS_DURATION = {"NR": "PT3H", "NT": "P1M"}

# Dedup preference when the same acquisition exists in several processing
# modes: reprocessed beats offline beats near-real-time.
S5P_MODE_RANK = {"NRTI": 0, "OFFL": 1, "RPRO": 2}

# Key order of "properties" in metadata.json
PROPERTY_ORDER = [
    "title",
    "description",
    "platform",
    "product:type",
    "instruments",
    "odc:region_code",
    "odc:product",
    "license",
    "license_url",
    "proj:code",
    "product:timeliness",
    "product:timeliness_category",
    "start_datetime",
    "end_datetime",
    "odc:processing_datetime",
]

# stac_extensions order
EXTENSION_ORDER = ("/product/", "/projection/")

# Final home of the products - the bucket all STAC hrefs point at by default (--href-bucket)
DEAFRICA_S3_BUCKET = "deafrica-sentinel-5p-aer-ai"


def deafrica_base_uri(
    date: str, tile: str, timeliness: str, bucket: str = DEAFRICA_S3_BUCKET
) -> str:
    """
    The DE Africa prefix for a dataset:

        s3://{bucket}/Sentinel-5p/TROPOMI/TROPO_L2_AI/{YYYY}/{MM}/
            S5p_TROPO_L2_AI_{YYYYMMDD}_{timeliness}_{tile}
    """
    year, month, day = date.split("-")
    dataset = f"S5p_{PRODUCT_TYPE}_{year}{month}{day}_{timeliness}_{tile}"
    return f"s3://{bucket}/Sentinel-5p/TROPOMI/{PRODUCT_TYPE}/{year}/{month}/{dataset}"


LICENSE = "CC-BY-4.0"
LICENSE_URL = "https://creativecommons.org/licenses/by/4.0/deed.en"

PRODUCT_EXT_SCHEMA = "https://stac-extensions.github.io/product/v0.1.0/schema.json"

# Kenya test tile. Overrides geopackage input, acting as an AOI filter: only intersecting features are processed.
TEST_BBOX = [37.2, 1.806, 38.103, 2.715]

log = setup_logging()


def _fetch_grid_from_cloudferro(grid_href: str) -> str:
    """
    Download a geopackage from CloudFerro object storage to a local
    temp file and return its path.
    """
    bucket, key = grid_href.removeprefix("s3://").split("/", 1)
    local_path = os.path.join(
        tempfile.gettempdir(), f"s5p_aer_ai_grid_{os.path.basename(key)}"
    )
    if os.path.exists(local_path):
        log.info(f"Using cached tiling grid: {local_path}")
        return local_path

    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "https://s3.waw3-1.cloudferro.com"),
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("S3_SECRET_ACCESS_KEY"),
    )
    s3.download_file(bucket, key, local_path)
    log.info(f"Downloaded tiling grid to {local_path}")
    return local_path


def load_tiling_grid(sat_config: dict) -> gpd.GeoDataFrame:
    """
    Load the utm_1km tiling geopackage into a single EPSG:4326 GeoDataFrame.
    The gpkg follows the BatchV2 GeoPackage-input spec.
    """
    grid_href = sat_config.get("grid") or sat_config["input"]
    if isinstance(grid_href, dict):
        grid_href = grid_href.get("features", {}).get("s3", {}).get("url", grid_href)
    log.info(f"Loading utm_1km tiling grid from {grid_href} ...")
    if grid_href.startswith("s3://"):
        grid_href = _fetch_grid_from_cloudferro(grid_href)

    layers = [layer[0] for layer in pyogrio.list_layers(grid_href)]
    frames = []
    for layer in layers:
        gdf = gpd.read_file(grid_href, layer=layer)

        for alt in ("identifier", "NAME", "Name", "tileName", "tile_name"):
            if alt in gdf.columns and "name" not in gdf.columns:
                gdf = gdf.rename(columns={alt: "name"})
                break

        # Native CRS of this layer - authoritative EPSG for its tiles
        epsg = gdf.crs.to_epsg() if gdf.crs else None
        if epsg is None:
            m = re.match(r"feature_(\d+)", layer)
            epsg = int(m.group(1)) if m else None
        gdf["epsg"] = epsg

        # Exact tile bounds in the native CRS
        bounds = gdf.geometry.bounds
        gdf["native_minx"] = bounds["minx"]
        gdf["native_miny"] = bounds["miny"]
        gdf["native_maxx"] = bounds["maxx"]
        gdf["native_maxy"] = bounds["maxy"]

        frames.append(gdf.to_crs("EPSG:4326"))

    grid = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs="EPSG:4326")
    if "name" not in grid.columns:
        raise ValueError(f"Tiling grid has no tile-name column: {list(grid.columns)}")
    log.info(f"Loaded {len(grid)} tiles from {len(layers)} layers")
    return grid


def epsg_from_tile_name(tile: str) -> int:
    """
    Derive the tile's native UTM EPSG from its MGRS-style name prefix,
    e.g. "33QZA_0_0" -> zone 33, latitude band Q (northern) -> EPSG:32633.
    Bands C-M are southern hemisphere (327xx), N-X northern (326xx).
    """
    mgrs = tile.split("_")[0]
    zone = int(mgrs[:2])
    band = mgrs[2].upper()
    if not ("C" <= band <= "X") or band in ("I", "O"):
        raise ValueError(f"Cannot derive UTM zone from tile name {tile!r}")
    return (32600 if band >= "N" else 32700) + zone


def tiles_for_bbox(
    grid: gpd.GeoDataFrame, bbox: list[float] | None
) -> gpd.GeoDataFrame:
    "Return grid features intersecting bbox (EPSG:4326), or all features if bbox is None."
    if bbox is None:
        return grid
    grid_4326 = grid.to_crs("EPSG:4326")
    minx, miny, maxx, maxy = bbox
    mask = grid_4326.intersects(
        gpd.GeoSeries.from_wkt(
            [
                f"POLYGON(({minx} {miny},{maxx} {miny},{maxx} {maxy},{minx} {maxy},{minx} {miny}))"
            ]
        ).iloc[0]
    )
    return grid[mask.values]


def _processing_mode(product_id: str) -> str:
    """
    NRTI / OFFL / RPRO processing-mode token of an S5P product id
    (fixed-width field 2): S5P_RPRO_L2__AER_AI_... -> "RPRO".
    """
    return product_id[4:8]


def _timeliness(product_id: str) -> str:
    "NR / NT archive folder timeliness token for an S5P product id."
    return S5P_TIMELINESS_TOKEN.get(_processing_mode(product_id), "NT")


def _processing_ts(product_id: str) -> str:
    "Processing timestamp, last '_' field of the id (e.g. '20221027T132153')."
    return product_id.removesuffix(".nc").rsplit("_", 1)[1]


def _acquisition_key(product_id: str) -> tuple[str, str]:
    """
    Key identifying a unique acquisition regardless of processing mode /
    baseline: (platform, orbit number). Keyed on orbit rather than sensing
    start because NRTI products are ~5-minute granules while OFFL/RPRO are
    full orbits, so their start timestamps never match even though they are
    the same acquisition. S5P product ids have fixed-width fields:
    S5P_NRTI_L2__AER_AI_20260702T115240_20260702T115740_45174_...
        ->  ("S5P", "45174").
    """
    return product_id[:3], product_id[52:57]


def fetch_s5p_aer_ai_scenes(session, bbox: list[float], date: str) -> list[dict]:
    """
    Query the SH catalog for all L2__AER_AI scenes
    intersecting bbox on the given date. NRTI/OFFL/RPRO duplicates of the same
    acquisition are deduped preferring RPRO, then OFFL, matching
    the batch's default "mostRecent" mosaicking behaviour where the most
    recently processed version is preferred.
    """
    body = {
        "collections": [S5P_L2_COLLECTION],
        "bbox": [round(v, 4) for v in bbox],
        "datetime": f"{date}T00:00:00Z/{date}T23:59:59Z",
        "limit": 100,
        # No fields filter - we want full properties (datetimes)
    }

    features = []
    while True:
        resp = session.post(SH_CATALOG_URL, json=body, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        features.extend(
            f for f in data.get("features", []) if NATIVE_PRODUCT_TYPE in f["id"]
        )
        nxt = data.get("context", {}).get("next")
        if nxt is None:
            break
        body["next"] = nxt

    # Dedupe NRTI/OFFL/RPRO versions of the same acquisition: prefer
    # RPRO > OFFL > NRTI, then the newer processing timestamp
    best: dict[tuple, dict] = {}
    for f in features:
        key = _acquisition_key(f["id"])
        if key not in best:
            best[key] = f
            continue
        cur, new = best[key]["id"], f["id"]
        cur_rank = (S5P_MODE_RANK.get(_processing_mode(cur), -1), _processing_ts(cur))
        new_rank = (S5P_MODE_RANK.get(_processing_mode(new), -1), _processing_ts(new))
        if new_rank > cur_rank:
            best[key] = f
    return sorted(best.values(), key=lambda f: f["id"])


def resolve_job_timeliness(session, date: str, bbox: list[float]) -> str:
    """
    Resolve the single timeliness token used in the job's delivery folder
    names. Batch V2's delivery URL is one template for the whole job, so all tile
    folders share one label. We query the day's deduped scenes over the AOI:
    if their timeliness is uniform, use it; if mixed (e.g. NRTI not yet
    superseded by OFFL everywhere), prefer NT - matching both the dedup
    preference and the batch's mostRecent mosaicking - and warn.
    """
    scenes = fetch_s5p_aer_ai_scenes(session, bbox, date)
    cats = {_timeliness(s["id"]) for s in scenes}
    if not cats:
        log.warning(
            f"No scenes found for {date} over AOI; defaulting folder timeliness to NT"
        )
        return "NT"
    if len(cats) > 1:
        chosen = "NT" if "NT" in cats else sorted(cats)[0]
        log.warning(
            f"Mixed timeliness {sorted(cats)} for {date}; using {chosen} in "
            f"delivery folder names"
        )
        return chosen
    return cats.pop()


def _r15(v: float) -> float:
    """
    Round to 15 significant figures. Used to serialise all
    coordinate / bbox / proj:transform floats at %.15g precision.
    """
    return float(f"{v:.15g}")


def derived_from_uri(product_id: str) -> str:
    """
    creo://eodata source path for an S5P L2 product, matching the current
    archive convention (lowercase eodata):
        creo://eodata/Sentinel-5P/TROPOMI/L2__AER_AI/YYYY/MM/DD/{id}.nc
    """
    pid = product_id if product_id.endswith(".nc") else f"{product_id}.nc"
    native_type = pid[9:19]  # "L2__AER_AI"
    start_ts = pid[20:35]  # "20180702T104927"
    year, month, day = start_ts[0:4], start_ts[4:6], start_ts[6:8]
    return f"creo://eodata/Sentinel-5P/TROPOMI/{native_type}/{year}/{month}/{day}/{pid}"


def build_s5p_aer_ai_stac_item(
    tile: str,
    date: str,
    grid: gpd.GeoDataFrame,
    scenes: list[dict],
    base_uri: str,
) -> pystac.Item | None:
    """
    Build a validated pystac.Item for one s5p_tropomi_l2_aer_ai tile/date
    dataset.

    Follows the existing archive convention STAC 1.1.0, projection v2.0.0 (proj:code + per-asset shape/transform in
    the tile's UTM CRS), product (product:type + timeliness), derived_from ->
    creo://eodata
    source products.
    """
    if not scenes:
        log.warning(f"No scenes provided for {tile}/{date}")
        return None

    cell = grid[grid["name"] == tile]
    if cell.empty:
        log.warning(f"Tile {tile} not found in grid")
        return None

    row = cell.iloc[0]

    # Native CRS: from the gpkg layer the tile came from; fall back to
    # deriving it from an MGRS-style name prefix.
    if "epsg" in cell.columns and pd.notna(row.get("epsg")):
        epsg = int(row["epsg"])
    else:
        epsg = epsg_from_tile_name(tile)

    # Tile bounds in the native CRS: prefer the exact values captured from
    # the gpkg layer before reprojection; fall back to a 4326->UTM round trip.
    native_cols = ("native_minx", "native_miny", "native_maxx", "native_maxy")
    if any(c not in cell.columns or pd.isna(row.get(c)) for c in native_cols):
        raise ValueError(
            f"Tile {tile}: native-CRS bounds missing from grid - was the "
            f"grid loaded via load_tiling_grid()?"
        )
    minx = float(row["native_minx"])
    miny = float(row["native_miny"])
    maxx = float(row["native_maxx"])
    maxy = float(row["native_maxy"])

    # Item geometry/bbox: full tile footprint in EPSG:4326
    tfm = Transformer.from_crs(f"EPSG:{epsg}", "EPSG:4326", always_xy=True)
    corners_native = [(minx, maxy), (maxx, maxy), (maxx, miny), (minx, miny)]
    ring = [
        [_r15(lon), _r15(lat)]
        for lon, lat in (tfm.transform(x, y) for x, y in corners_native)
    ]
    ring.append(list(ring[0]))
    item_geometry = {"type": "Polygon", "coordinates": [ring]}
    lons = [p[0] for p in ring]
    lats = [p[1] for p in ring]
    item_bbox = [min(lons), min(lats), max(lons), max(lats)]

    # Pixel grid: the BatchV2 gpkg spec requires width+height and/or
    # resolution, with width/height taking precedence when both are present -
    # mirror that so the STAC matches what the batch actually rendered.
    width = row.get("width") if "width" in cell.columns else None
    height = row.get("height") if "height" in cell.columns else None
    resolution = row.get("resolution") if "resolution" in cell.columns else None

    if pd.notna(width) and pd.notna(height):
        width, height = int(width), int(height)
    elif pd.notna(resolution):
        resolution = float(resolution)
        width = round((maxx - minx) / resolution)
        height = round((maxy - miny) / resolution)
    else:
        raise ValueError(
            f"Tile {tile}: geopackage feature has neither width/height nor "
            f"resolution - cannot determine the pixel grid"
        )

    px_x = (maxx - minx) / width
    px_y = (maxy - miny) / height
    proj_shape = [height, width]
    # 9-element (3x3 affine) form at %.15g
    proj_transform = [_r15(px_x), 0, _r15(minx), 0, _r15(-px_y), _r15(maxy), 0, 0, 1]

    year, month, day = date.split("-")
    item_id = f"Sentinel-5p_TROPOMI_{PRODUCT_TYPE}_{year}_{month}_{day}_{tile}"

    scene_datetimes = [
        _parse_datetime(s["properties"]["datetime"])
        for s in scenes
        if s["properties"].get("datetime")
    ]
    start_dt = min(scene_datetimes) if scene_datetimes else None
    end_dt = max(scene_datetimes) if scene_datetimes else None

    item = pystac.Item(
        id=item_id,
        geometry=item_geometry,
        bbox=item_bbox,
        datetime=datetime(int(year), int(month), int(day), tzinfo=timezone.utc),
        properties={
            "title": item_id,
            "description": f"Sentinel-5p product {PRODUCT_TYPE} for {tile}",
            "platform": "Sentinel-5p",
            "instruments": ["TROPOMI"],
            # stripped ("33QZA_0_0" -> "33QZA")
            "odc:region_code": tile.split("_")[0],
            "odc:product": ODC_PRODUCT,
            "license": LICENSE,
            "license_url": LICENSE_URL,
            # Processing timestamp
            "odc:processing_datetime": datetime.now(timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            ),
        },
    )

    if start_dt:
        item.properties["start_datetime"] = start_dt.isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z")
        item.properties["end_datetime"] = end_dt.isoformat(timespec="seconds").replace(
            "+00:00", "Z"
        )

    # Item-level proj:code abiding by Projection extension v2.0.0
    proj_ext = ProjectionExtension.ext(item, add_if_missing=True)
    proj_ext.code = f"EPSG:{epsg}"

    # Product extension
    if PRODUCT_EXT_SCHEMA not in item.stac_extensions:
        item.stac_extensions.append(PRODUCT_EXT_SCHEMA)
    item.properties["product:type"] = PRODUCT_TYPE

    # Timeliness of the contributing scenes; if orbits of mixed categories
    # cross the tile, prefer NT - matching the dedup preference and the
    # batch's mostRecent mosaicking.
    cats = {_timeliness(s["id"]) for s in scenes}
    timeliness_cat = "NT" if "NT" in cats else sorted(cats)[0]
    item.properties["product:timeliness"] = TIMELINESS_DURATION[timeliness_cat]
    item.properties["product:timeliness_category"] = timeliness_cat

    # Assets - one COG per evalscript output band
    for band, nodata in S5P_AER_AI_ASSET_NODATA.items():
        item.add_asset(
            band,
            pystac.Asset(
                href=f"{base_uri}/{band}.tif",
                title=band,
                media_type=pystac.MediaType.COG,
                roles=["data"],
                extra_fields={
                    "nodata": nodata,
                    "proj:shape": proj_shape,
                    "proj:transform": proj_transform,
                },
            ),
        )

    # self link: the metadata.json's final archive home, alongside the tifs
    # (the file is staged on the CF bucket until the sync, like the tifs)
    item.add_link(
        pystac.Link(
            rel="self",
            target=f"{base_uri}/metadata.json",
            media_type="application/json",
        )
    )

    # derived_from links - one per source scene
    for scene in scenes:
        item.add_link(
            pystac.Link(rel="derived_from", target=derived_from_uri(scene["id"]))
        )

    # Ensure intended ordering of keys
    ordered = {k: item.properties[k] for k in PROPERTY_ORDER if k in item.properties}
    ordered.update({k: v for k, v in item.properties.items() if k not in ordered})
    item.properties = ordered

    def _ext_rank(uri: str) -> int:
        for i, frag in enumerate(EXTENSION_ORDER):
            if frag in uri:
                return i
        return len(EXTENSION_ORDER)

    item.stac_extensions.sort(key=_ext_rank)

    # Validate against core STAC 1.1.0 + applied extension schemas
    item.validate()

    return item


def generate_stac_items(
    session,
    grid: gpd.GeoDataFrame,
    date: str,
    output_bucket: str,
    href_bucket: str,
    bbox: list[float] | None,
    job_timeliness: str,
    max_workers: int = 8,
) -> list[dict]:
    """
    Build a STAC item for every grid tile in the processed AOI
    and upload it to output_bucket alongside the delivered
    tifs. Tiles are processed in parallel (default 8 workers - all I/O bound).
    """
    s3_cf = boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT", "https://s3.waw3-1.cloudferro.com"),
        aws_access_key_id=os.environ.get("S3_ACCESS_KEY"),
        aws_secret_access_key=os.environ.get("S3_SECRET_ACCESS_KEY"),
    )

    tiles = tiles_for_bbox(grid, bbox)
    log.info(
        f"Generating STAC items for {len(tiles)} tiles (max_workers={max_workers}) ..."
    )

    def process_tile(row) -> dict:
        tile = row["name"]
        tile_bbox = [
            round(v, 4)
            for v in gpd.GeoSeries([row.geometry], crs=grid.crs)
            .to_crs("EPSG:4326")
            .total_bounds
        ]

        # Check existence first
        staging = deafrica_base_uri(date, tile, job_timeliness, output_bucket)
        s3_key = staging.split(output_bucket + "/", 1)[1] + "/metadata.json"
        try:
            s3_cf.head_object(Bucket=output_bucket, Key=s3_key)
            log.info(f"  {tile}: metadata.json already exists - skipping")
            return {
                "tile": tile,
                "status": "exists",
                "stac_item": f"s3://{output_bucket}/{s3_key}",
            }
        except botocore.exceptions.ClientError:
            pass  # doesn't exist, proceed

        try:
            scenes = fetch_s5p_aer_ai_scenes(session, tile_bbox, date)
        except Exception as e:
            log.error(f"  scene lookup failed for {tile}: {e}")
            return {"tile": tile, "status": "error", "error": str(e)}

        if not scenes:
            log.info(f"  {tile}: no {NATIVE_PRODUCT_TYPE} scenes on {date} - skipping")
            return {"tile": tile, "status": "no_scenes"}

        tile_timeliness = _timeliness(scenes[0]["id"])
        if tile_timeliness != job_timeliness:
            log.warning(
                f"  {tile}: scene timeliness {tile_timeliness} != folder "
                f"label {job_timeliness}; hrefs use the folder label"
            )

        first_band = next(iter(S5P_AER_AI_ASSET_NODATA))
        tif_key = staging.split(output_bucket + "/", 1)[1] + f"/{first_band}.tif"
        try:
            s3_cf.head_object(Bucket=output_bucket, Key=tif_key)
        except botocore.exceptions.ClientError:
            log.warning(
                f"  {tile}: scenes exist but no {first_band}.tif at "
                f"{staging} - skipping (job not delivered here?)"
            )
            return {"tile": tile, "status": "no_tifs"}

        # hrefs point at the FINAL archive home, not the CF staging path
        base_uri = deafrica_base_uri(date, tile, job_timeliness, href_bucket)

        try:
            item = build_s5p_aer_ai_stac_item(tile, date, grid, scenes, base_uri)
        except Exception as e:
            log.error(f"  STAC build failed for {tile}: {e}")
            return {"tile": tile, "status": "error", "error": str(e)}

        if item is None:
            return {"tile": tile, "status": "skipped"}

        out_path = save_stac_item(item)
        log.info(f"  wrote STAC item locally: {out_path}")

        try:
            s3_uri = upload_stac_item(out_path, s3_key, output_bucket)
            return {"tile": tile, "status": "uploaded", "stac_item": s3_uri}
        except Exception as e:
            log.error(f"  failed to upload STAC item to S3: {e}")
            return {"tile": tile, "status": "local_only", "stac_item": out_path}

    rows = [row for _, row in tiles.iterrows()]
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(process_tile, rows))

    return results


def poll_status(
    session, job_id: str, terminal: set, interval: int = 30, timeout: int = 3600
) -> dict:
    "Poll the batch job until it reaches one of the given terminal states."
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = session.get(f"{SH_BATCH_URL}/{job_id}", timeout=60)
        resp.raise_for_status()
        info = resp.json()
        status = info.get("status")
        log.info(f"  job {job_id}: {status}")
        if status in terminal:
            return info
        time.sleep(interval)
    raise TimeoutError(f"Job {job_id} did not reach {terminal} within {timeout}s")


_date_option = click.option(
    "--date",
    "-d",
    required=True,
    help="Acquisition date YYYY-MM-DD (start of the range if --end-date is given).",
)
_end_date_option = click.option(
    "--end-date",
    default=None,
    help="Optional inclusive end date YYYY-MM-DD. Each date in the range is "
    "its own job / dataset - dataset identity is tile + single date.",
)
_output_bucket_option = click.option(
    "--output-bucket",
    "-b",
    default=DEFAULT_OUTPUT_BUCKET,
    show_default=True,
    help="CloudFerro bucket batch outputs (tifs + staged metadata.json) are "
    "written to.",
)
_href_bucket_option = click.option(
    "--href-bucket",
    default=DEAFRICA_S3_BUCKET,
    show_default=True,
    help="Bucket the STAC hrefs point at (final DE Africa archive home). "
    "Only affects the URIs inside metadata.json, not where anything is "
    "physically written.",
)
_full_aoi_option = click.option(
    "--full-aoi",
    is_flag=True,
    default=False,
    help="Process all geopackage features (ignore TEST_BBOX).",
)


def _date_range(start: str, end: str | None) -> list[str]:
    "Inclusive list of YYYY-MM-DD dates from start to end (end defaults to start)."
    start_dt = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d") if end else start_dt
    if end_dt < start_dt:
        raise click.BadParameter(f"--end-date {end} is before --date {start}")
    return [
        (start_dt + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range((end_dt - start_dt).days + 1)
    ]


@click.group("s5p-aer-ai-cdse-pipeline")
def cli():
    """
    Run s5p_tropomi_l2_aer_ai batch jobs (one per date) in three stages, each
    independently runnable (and retryable) as its own Argo step:

      submit -> wait -> stac
    """


def _submit_one(
    session, sat_config, grid, date, output_bucket, aoi_bbox, dry_run
) -> dict:
    """
    Create, analyse, and start the batch job for one date. Returns
    {"date", "job_id", "timeliness", "status"}; raises on any failure so the
    caller can record it and continue with the remaining dates.
    """
    timeliness_bbox = aoi_bbox or [round(v, 4) for v in grid.total_bounds]
    job_timeliness = resolve_job_timeliness(session, date, timeliness_bbox)
    log.info(
        f"{date}: archive folder timeliness (from product filenames): {job_timeliness}"
    )

    payload = build_batch_payload(
        sat_config=sat_config,
        time_from=f"{date}T00:00:00Z",
        time_to=f"{date}T23:59:59Z",
        bbox=aoi_bbox,
    )
    payload["description"] = f"Batch Sentinel-5p AER_AI test {date}"

    # Physical delivery to CF bucket (--output-bucket)
    payload["output"]["delivery"]["s3"]["url"] = (
        deafrica_base_uri(date, "<tileName>", job_timeliness, output_bucket)
        + "/<outputId>.<format>"
    )

    if dry_run:
        print(json.dumps(redact_payload(payload), indent=2))
        return {"date": date, "timeliness": job_timeliness, "status": "dry_run"}

    resp = session.post(SH_BATCH_URL, json=payload)
    if not resp.ok:
        # surface the API's validation message - batch 400s are usually specific
        log.error(f"Create failed ({resp.status_code}): {resp.text[:2000]}")
        resp.raise_for_status()
    job_id = resp.json()["id"]
    log.info(f"Created job {job_id}")

    # Analyse before starting - validates the geopackage and job parameters
    analyse = session.post(f"{SH_BATCH_URL}/{job_id}/analyse")
    if not analyse.ok:
        log.error(f"Analyse failed ({analyse.status_code}): {analyse.text[:2000]}")
        analyse.raise_for_status()

    info = poll_status(
        session,
        job_id,
        terminal={"ANALYSIS_DONE", "FAILED", "CANCELED"},
        timeout=900,
    )
    if info.get("status") != "ANALYSIS_DONE":
        log.error(f"Analysis did not complete: {json.dumps(info, indent=2)[:2000]}")
        raise RuntimeError(f"Job {job_id} analysis ended in {info.get('status')}")

    start = session.post(f"{SH_BATCH_URL}/{job_id}/start")
    if not start.ok:
        log.error(f"Start failed ({start.status_code}): {start.text[:2000]}")
        start.raise_for_status()
    log.info(f"Started job {job_id}")

    year, month, day = date.split("-")
    log.info(
        f"Outputs will be delivered under "
        f"s3://{output_bucket}/Sentinel-5p/TROPOMI/{PRODUCT_TYPE}/{year}/{month}/ "
        f"- same keys as the final archive"
    )

    return {
        "date": date,
        "job_id": job_id,
        "timeliness": job_timeliness,
        "status": "started",
    }


@cli.command("submit")
@_date_option
@_end_date_option
@_output_bucket_option
@_full_aoi_option
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Print the payload(s) (minus S3 credentials) but do not submit.",
)
def submit(date, end_date, output_bucket, full_aoi, dry_run):
    """
    Create, analyse, and start one batch job per date.

    A failed date is recorded and the remaining dates still submit; the exit
    code is non-zero if any date failed.
    """
    sat_config = SATELLITES["s5p_aer_ai"]
    session = get_session()

    aoi_bbox = None if full_aoi else TEST_BBOX

    grid = load_tiling_grid(sat_config)

    jobs = []
    for d in _date_range(date, end_date):
        try:
            jobs.append(
                _submit_one(
                    session, sat_config, grid, d, output_bucket, aoi_bbox, dry_run
                )
            )
        except Exception as e:
            log.error(f"{d}: submit failed: {e}")
            jobs.append({"date": d, "status": "failed", "error": str(e)})

    if not dry_run:
        print(json.dumps({"jobs": jobs}))
    if any(j["status"] == "failed" for j in jobs):
        sys.exit(1)


@cli.command("wait")
@click.option(
    "--job-id",
    "job_ids",
    required=True,
    multiple=True,
    help="Batch job id printed by `submit`",
)
@click.option(
    "--interval",
    default=30,
    show_default=True,
    help="Seconds between status polls.",
)
@click.option(
    "--timeout",
    default=6 * 3600,
    show_default=True,
    help="Give up on a job (non-zero exit) if it is not terminal within this "
    "many seconds. Applied per job.",
)
def wait(job_ids, interval, timeout):
    """
    Poll each batch job until it reaches a terminal state.

    Exits 0 only if every job is DONE, so downstream steps (stac) can depend
    on it. Safe to kill and rerun with the same --job-id(s) at any time:
    polling holds no state beyond the ids, so a restarted pod resumes exactly
    where the dead one left off without touching the jobs.
    """
    session = get_session()

    results = []
    all_done = True
    for job_id in job_ids:
        try:
            info = poll_status(
                session,
                job_id,
                terminal={"DONE", "FAILED", "CANCELED", "PARTIAL"},
                interval=interval,
                timeout=timeout,
            )
        except TimeoutError as e:
            log.error(str(e))
            results.append({"id": job_id, "status": "TIMEOUT"})
            all_done = False
            continue
        results.append(info)
        if info.get("status") != "DONE":
            all_done = False

    print(json.dumps(results, indent=2))

    if not all_done:
        sys.exit(1)


@cli.command("stac")
@_date_option
@_end_date_option
@_output_bucket_option
@_href_bucket_option
@_full_aoi_option
@click.option(
    "--timeliness",
    default=None,
    type=click.Choice(sorted(set(S5P_TIMELINESS_TOKEN.values()))),
    help="Delivery folder timeliness token the job(s) were submitted with "
    "(from `submit`'s output; applied to every date in the range). If "
    "omitted, re-resolved from the catalog per date - which can disagree "
    "with the delivered folders if scenes changed (e.g. NRTI superseded by "
    "OFFL) since submission.",
)
@click.option(
    "--test",
    is_flag=True,
    default=False,
    help="Generate and print one STAC item for the first tile with scenes, "
    "then exit. Nothing is uploaded.",
)
def stac(date, end_date, output_bucket, href_bucket, full_aoi, timeliness, test):
    """
    Generate a STAC item for every processed tile of every date and upload it
    alongside the tifs on --output-bucket. Idempotent: tiles whose
    metadata.json already exists are skipped, so this is safe to retry;
    tiles with no delivered tifs are skipped with status "no_tifs".
    """
    sat_config = SATELLITES["s5p_aer_ai"]
    session = get_session()

    aoi_bbox = None if full_aoi else TEST_BBOX

    grid = load_tiling_grid(sat_config)

    dates = _date_range(date, end_date)

    # --stac-test: no job submitted - build one item against the SH catalog
    # and print it, to eyeball the metadata before paying for a batch run.
    if test:
        tiles = tiles_for_bbox(grid, aoi_bbox)
        for d in dates:
            for _, row in tiles.iterrows():
                tile = row["name"]
                tile_bbox = [
                    round(v, 4)
                    for v in gpd.GeoSeries([row.geometry], crs=grid.crs)
                    .to_crs("EPSG:4326")
                    .total_bounds
                ]
                scenes = fetch_s5p_aer_ai_scenes(session, tile_bbox, d)
                if not scenes:
                    log.info(f"{tile}: no {NATIVE_PRODUCT_TYPE} scenes on {d}")
                    continue
                tile_timeliness = timeliness or _timeliness(scenes[0]["id"])
                base_uri = deafrica_base_uri(d, tile, tile_timeliness, href_bucket)
                item = build_s5p_aer_ai_stac_item(tile, d, grid, scenes, base_uri)
                if item:
                    out_path = save_stac_item(item)
                    log.info(f"Wrote test STAC item locally: {out_path}")
                    print(json.dumps(item.to_dict(), indent=2))
                    return
        log.warning("No STAC item generated - no tiles with scenes found.")
        return

    all_results = {}
    for d in dates:
        if timeliness:
            job_timeliness = timeliness
        else:
            timeliness_bbox = aoi_bbox or [round(v, 4) for v in grid.total_bounds]
            job_timeliness = resolve_job_timeliness(session, d, timeliness_bbox)
        log.info(
            f"{d}: archive folder timeliness (from product filenames): {job_timeliness}"
        )

        all_results[d] = generate_stac_items(
            session, grid, d, output_bucket, href_bucket, aoi_bbox, job_timeliness
        )

    print(json.dumps({"stac_items": all_results}, indent=2))


if __name__ == "__main__":
    cli()
