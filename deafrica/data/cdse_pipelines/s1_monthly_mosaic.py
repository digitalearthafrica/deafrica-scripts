#!/usr/bin/env python3
"""
Copies Sentinel-1 monthly mosaic tiles from the CDSE eodata
archive into a DE Africa bucket and writes a metadata.json STAC item alongside
the tifs.

Designed to run once a month (e.g. as an Argo cron step):

    s1-monthly-mosaic-sync --date 2025-01                        # dev bucket (default)
    s1-monthly-mosaic-sync --date 2025-01 \\
        --dst-bucket deafrica-sentinel-1-monthly-mosaic  # production

A tile whose metadata.json already exists in the destination is
skipped (use --overwrite to redo it), and individual tifs already present
with the same size are not re-uploaded. It is safe to re-run after a partial
failure.
"""

import concurrent.futures
import json
import logging
import os
import re
import shutil
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from functools import lru_cache

import boto3
import botocore.exceptions
import click
import geopandas as gpd
import rasterio
from pyproj import Transformer
from shapely.geometry import Polygon
from shapely.ops import unary_union
from shapely.prepared import prep

__version__ = "1.0.0"

log = logging.getLogger("s1-monthly-mosaic-sync")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SRC_BUCKET = "eodata"
SRC_PREFIX = "Global-Mosaics/Sentinel-1/S1SAR_L3_IW_MCM"

DEFAULT_DST_BUCKET = "deafrica-sentinel-1-dev-monthly-mosaic"
PROD_DST_BUCKET = "deafrica-sentinel-1-monthly-mosaic"
DST_PREFIX = "s1_monthly_mosaic/S1SAR_L3_IW_MCM"

# DE Africa continental extent
DEFAULT_EXTENT = (
    "https://raw.githubusercontent.com/digitalearthafrica/"
    "deafrica-extent/master/africa-extent.json"
)

POLARIZATIONS = ("VV", "VH")

COG_MEDIA_TYPE = "image/tiff; application=geotiff; profile=cloud-optimized"

STAC_EXTENSIONS = [
    "https://stac-extensions.github.io/alternate-assets/v1.1.0/schema.json",
    "https://stac-extensions.github.io/storage/v1.0.0/schema.json",
    "https://stac-extensions.github.io/sar/v1.0.0/schema.json",
    "https://stac-extensions.github.io/projection/v1.2.0/schema.json",
    "https://stac-extensions.github.io/product/v0.1.0/schema.json",
]

# e.g. "Sentinel-1_IW_mosaic_2025_M01_45RUJ_0_0" -> ("45RUJ", "0", "0")
TILE_NAME_RE = re.compile(r"_(\d{2}[C-X][A-HJ-NP-Z]{2})_(\d+)_(\d+)$")

# ---------------------------------------------------------------------------
# MGRS geometry from the tile name (no network required)
# ---------------------------------------------------------------------------

_COL_SETS = ["ABCDEFGH", "JKLMNPQR", "STUVWXYZ"]
_ROW_LETTERS = "ABCDEFGHJKLMNPQRSTUV"  # 20-letter northing cycle (no I/O)
_BAND_LETTERS = "CDEFGHJKLMNPQRSTUVWX"  # 8-degree latitude bands (no I/O)


def epsg_from_tile_name(tile5: str) -> int:
    """
    Native UTM EPSG from an MGRS tile prefix, e.g. "39NUD" -> EPSG:32639.
    Bands C-M are southern hemisphere (327xx), N-X northern (326xx).
    """
    zone = int(tile5[:2])
    band = tile5[2].upper()
    if band not in _BAND_LETTERS:
        raise ValueError(f"Cannot derive UTM zone from tile name {tile5!r}")
    return (32600 if band >= "N" else 32700) + zone


@lru_cache(maxsize=None)
def _to_4326(epsg: int) -> Transformer:
    return Transformer.from_crs(f"EPSG:{epsg}", "EPSG:4326", always_xy=True)


@lru_cache(maxsize=None)
def _from_4326(epsg: int) -> Transformer:
    return Transformer.from_crs("EPSG:4326", f"EPSG:{epsg}", always_xy=True)


@lru_cache(maxsize=None)
def mgrs_square_utm(tile5: str) -> tuple[int, int, int]:
    """
    (epsg, easting_min, northing_min) of the 100 km MGRS square named by a
    tile prefix like "39NUD". Pure arithmetic on the MGRS lettering scheme:

      - column letter -> easting, cycling A-H / J-R / S-Z by zone triplet
      - row letter -> northing mod 2,000,000 m (offset by 5 in even zones)
      - latitude band letter disambiguates which 2,000 km cycle applies
    """
    zone = int(tile5[:2])
    band, col, row = tile5[2], tile5[3], tile5[4]
    epsg = epsg_from_tile_name(tile5)

    easting = 100_000 * (_COL_SETS[(zone - 1) % 3].index(col) + 1)

    row_offset = 0 if zone % 2 else 5
    northing = 100_000 * ((_ROW_LETTERS.index(row) - row_offset) % 20)

    # Resolve the 2,000 km ambiguity using the band's minimum-latitude
    # northing at the zone's central meridian. 110 km tolerance covers
    # squares that straddle a band boundary.
    lat_min = -80 + 8 * _BAND_LETTERS.index(band)
    lon_central = (zone - 1) * 6 - 180 + 3
    _, north_min = _from_4326(epsg).transform(lon_central, lat_min)
    while northing < north_min - 110_000:
        northing += 2_000_000

    return epsg, easting, northing


def tile_footprint_4326(tile5: str, pad: float = 120.0) -> Polygon | None:
    """
    Approximate EPSG:4326 footprint of the tile's 100 km square (edges
    densified so reprojection curvature is captured), padded slightly to
    cover the mosaic's buffer pixels. Used only for the Africa intersection
    test - asset grids in metadata.json come from the actual COG headers.

    Returns None for tiles that wrap the antimeridian (far edges of zones
    01/60, e.g. 01KAA near Fiji): their wrapped longitudes would otherwise
    produce a polygon smeared across the whole globe that spuriously
    intersects everything. Africa is nowhere near +-180, so these can never
    be wanted tiles.
    """
    epsg, e0, n0 = mgrs_square_utm(tile5)
    e0, n0 = e0 - pad, n0 - pad
    e1, n1 = e0 + 100_000 + 2 * pad, n0 + 100_000 + 2 * pad

    corners = [(e0, n0), (e0, n1), (e1, n1), (e1, n0)]
    steps = 4
    pts = []
    for (ax, ay), (bx, by) in zip(corners, corners[1:] + corners[:1]):
        for i in range(steps):
            pts.append((ax + (bx - ax) * i / steps, ay + (by - ay) * i / steps))
    tfm = _to_4326(epsg)
    lonlat = [tfm.transform(x, y) for x, y in pts]

    lons = [p[0] for p in lonlat]
    if max(lons) - min(lons) > 180:
        return None

    return Polygon(lonlat)


def _r15(v: float) -> float:
    "Round to 15 significant figures (coordinate / bbox serialisation)."
    return float(f"{v:.15g}")


def _num(v: float):
    "Ints where integral (proj:transform of [20, 0, 300000, ...] not 20.0)."
    f = float(v)
    return int(f) if f.is_integer() else _r15(f)


def _ts(dt: datetime) -> str:
    "Timestamp in the archive's microsecond-Z format."
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _parse_ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def cdse_client():
    access_key = os.environ.get("CDSE_S3_ACCESS_KEY")
    secret_key = os.environ.get("CDSE_S3_SECRET_ACCESS_KEY")
    if not (access_key and secret_key):
        raise click.ClickException(
            "CDSE eodata credentials not set. Export CDSE_S3_ACCESS_KEY and "
            "CDSE_S3_SECRET_ACCESS_KEY - generate them in the CDSE S3 Keys "
            "Manager (https://documentation.dataspace.copernicus.eu/APIs/S3.html). "
            "Note these are CDSE keys: CloudFerro waw3-1 staging keys and AWS "
            "keys will be rejected by eodata."
        )
    endpoint = os.environ.get(
        "CDSE_S3_ENDPOINT", "https://eodata.dataspace.copernicus.eu"
    )
    log.info(f"CDSE source client: {endpoint} (access key {access_key[:4]}...)")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=os.environ.get("CDSE_S3_REGION", "eu-central-1"),
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def dst_client():
    endpoint = os.environ.get("DST_S3_ENDPOINT")
    return boto3.client("s3", endpoint_url=endpoint) if endpoint else boto3.client("s3")


def _head(s3, bucket: str, key: str) -> dict | None:
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError:
        return None


def load_africa_extent(extent: str):
    log.info(f"Loading African extent from {extent} ...")
    gdf = gpd.read_file(extent)
    if gdf.crs and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs("EPSG:4326")
    geom = unary_union(gdf.geometry.values)
    log.info(f"Extent loaded ({len(gdf)} feature(s), bounds {gdf.total_bounds})")
    return geom


def list_month_tiles(s3_src, src_bucket: str, year: str, month: str) -> list[str]:
    """
    Tile folder names for the month, e.g.
    "Sentinel-1_IW_mosaic_2025_M01_45RUJ_0_0", from a delimiter listing of
    {SRC_PREFIX}/{YYYY}/{MM}/01/.
    """
    prefix = f"{SRC_PREFIX}/{year}/{month}/01/"
    names = []
    paginator = s3_src.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=src_bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            names.append(cp["Prefix"].rstrip("/").rsplit("/", 1)[-1])
    log.info(f"Source listing: {len(names)} tiles under s3://{src_bucket}/{prefix}")
    return sorted(names)


def african_tiles(names: list[str], africa_geom) -> list[tuple[str, str]]:
    "Filter to (name, tile5) pairs whose MGRS footprint intersects Africa."
    africa = prep(africa_geom)
    kept = []
    for name in names:
        m = TILE_NAME_RE.search(name)
        if not m:
            log.warning(f"Skipping unparseable tile name: {name}")
            continue
        tile5 = m.group(1)
        try:
            footprint = tile_footprint_4326(tile5)
            if footprint is not None and africa.intersects(footprint):
                kept.append((name, tile5))
        except Exception as e:
            log.warning(f"Skipping {name}: footprint derivation failed: {e}")
    log.info(f"{len(kept)} of {len(names)} tiles intersect the African extent")
    return kept


def fetch_userdata(s3_src, src_bucket: str, src_base: str) -> dict:
    """
    The CDSE userdata.json delivered with each mosaic tile (OriginDate,
    ContentDate, processing centre). Missing/unreadable -> {} and the item
    falls back to derived values.
    """
    try:
        obj = s3_src.get_object(Bucket=src_bucket, Key=f"{src_base}/userdata.json")
        return json.loads(obj["Body"].read())
    except Exception as e:
        log.warning(f"  userdata.json unavailable at {src_base}: {e}")
        return {}


def build_metadata(
    name: str,
    tile5: str,
    year: str,
    month: str,
    grids: dict[str, dict],
    userdata: dict,
    dst_bucket: str,
    src_bucket: str,
) -> dict:
    """
    STAC item dict replicating the existing archive convention (STAC 1.0.0,
    proj:epsg + per-asset shape/transform, sar/product fields,
    derived_from -> the eodata source folder). `grids` maps polarization ->
    {"transform": [a,b,c,d,e,f], "shape": [h, w]} read from the actual COGs.
    """
    epsg = epsg_from_tile_name(tile5)
    dst_base = f"s3://{dst_bucket}/{DST_PREFIX}/{year}/{month}/01/{name}"
    src_base = f"s3://{src_bucket}/{SRC_PREFIX}/{year}/{month}/01/{name}"

    # Footprint from the VV grid: BL, TL, TR, BR ring order as in the archive
    g = grids["VV"]
    a, _, x0, _, e, y1 = g["transform"]
    h, w = g["shape"]
    x1 = x0 + a * w
    y0 = y1 + e * h  # e is negative
    tfm = _to_4326(epsg)
    ring = [
        [_r15(lon), _r15(lat)]
        for lon, lat in (
            tfm.transform(x, y) for x, y in [(x0, y0), (x0, y1), (x1, y1), (x1, y0)]
        )
    ]
    ring.append(list(ring[0]))
    lons = [p[0] for p in ring]
    lats = [p[1] for p in ring]
    bbox = [min(lons), min(lats), max(lons), max(lats)]

    # Timestamps: the month's content window for datetime/start/end
    content = userdata.get("ContentDate") or {}
    start = (
        _parse_ts(content["Start"])
        if content.get("Start")
        else datetime(int(year), int(month), 1, tzinfo=timezone.utc)
    )
    if content.get("End"):
        end = _parse_ts(content["End"])
    else:
        nxt_y, nxt_m = (
            (int(year) + 1, 1) if month == "12" else (int(year), int(month) + 1)
        )
        end = datetime(nxt_y, nxt_m, 1, tzinfo=timezone.utc) - timedelta(seconds=1)

    facility = next(
        (
            attr["Value"]
            for attr in userdata.get("Attributes", [])
            if attr.get("Name") == "processingCenter"
        ),
        "Sinergise Solutions",
    )
    platform = next(
        (
            attr["Value"]
            for attr in userdata.get("Attributes", [])
            if attr.get("Name") == "platformShortName"
        ),
        "SENTINEL-1",
    )
    product_type = userdata.get("ProductType", "S1SAR_L3_IW_MCM")

    assets = {}
    for pol in POLARIZATIONS:
        pg = grids[pol]
        assets[pol] = {
            "href": f"{dst_base}/{pol}.tif",
            "title": pol,
            "type": COG_MEDIA_TYPE,
            "description": f"polarization {pol}",
            "roles": ["data"],
            "sar:polarizations": [pol],
            "proj:transform": [float(v) for v in pg["transform"]],
            "proj:shape": [int(v) for v in pg["shape"]],
        }

    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "stac_extensions": STAC_EXTENSIONS,
        "id": name,
        "geometry": {"type": "Polygon", "coordinates": [ring]},
        "properties": {
            "processingCenter": facility,
            "platformShortName": platform,
            "datetime": _ts(start),
            "end_datetime": _ts(end),
            "start_datetime": _ts(start),
            "productType": product_type,
            "proj:epsg": epsg,
            "odc:product": "s1_monthly_mosaic_cdse",
            "odc:region_code": tile5,
            "sar:instrument_mode": "IW",
            "sar:frequency_band": "C",
            "sar:polarizations": list(POLARIZATIONS),
            "sar:product_type": "GRD",
            "product:type": "GRD",
        },
        "bbox": bbox,
        "links": [
            {
                "href": f"{dst_base}/metadata.json",
                "rel": "self",
                "type": "application/json",
            },
            {"href": src_base, "rel": "derived_from"},
        ],
        "assets": assets,
    }


def process_tile(
    s3_src,
    s3_dst,
    name: str,
    tile5: str,
    year: str,
    month: str,
    src_bucket: str,
    dst_bucket: str,
    overwrite: bool,
) -> dict:
    src_base = f"{SRC_PREFIX}/{year}/{month}/01/{name}"
    dst_base = f"{DST_PREFIX}/{year}/{month}/01/{name}"

    if not overwrite and _head(s3_dst, dst_bucket, f"{dst_base}/metadata.json"):
        log.info(f"  {name}: metadata.json already exists - skipping")
        return {"tile": name, "status": "exists"}

    tmpdir = tempfile.mkdtemp(prefix=f"s1sync_{tile5}_")
    try:
        grids = {}
        uploaded = []
        for pol in POLARIZATIONS:
            src_key = f"{src_base}/{pol}.tif"
            dst_key = f"{dst_base}/{pol}.tif"

            src_head = _head(s3_src, src_bucket, src_key)
            if src_head is None:
                log.warning(f"  {name}: missing {pol}.tif at source - skipping tile")
                return {"tile": name, "status": "missing_source", "missing": pol}

            local = os.path.join(tmpdir, f"{pol}.tif")
            s3_src.download_file(src_bucket, src_key, local)

            with rasterio.open(local) as ds:
                t = ds.transform
                grids[pol] = {
                    "transform": [t.a, t.b, t.c, t.d, t.e, t.f],
                    "shape": [ds.height, ds.width],
                }

            dst_head = _head(s3_dst, dst_bucket, dst_key)
            if (
                not overwrite
                and dst_head
                and dst_head["ContentLength"] == src_head["ContentLength"]
            ):
                log.info(
                    f"  {name}: {pol}.tif already in destination - not re-uploading"
                )
            else:
                s3_dst.upload_file(
                    local,
                    dst_bucket,
                    dst_key,
                    ExtraArgs={"ContentType": COG_MEDIA_TYPE},
                )
                uploaded.append(pol)

        userdata = fetch_userdata(s3_src, src_bucket, src_base)
        item = build_metadata(
            name, tile5, year, month, grids, userdata, dst_bucket, src_bucket
        )
        s3_dst.put_object(
            Bucket=dst_bucket,
            Key=f"{dst_base}/metadata.json",
            Body=json.dumps(item, indent=2).encode(),
            ContentType="application/json",
        )
        log.info(
            f"  {name}: done (uploaded: {', '.join(uploaded) or 'nothing new'} "
            f"+ metadata.json)"
        )
        return {
            "tile": name,
            "status": "synced",
            "uploaded": uploaded,
            "metadata": f"s3://{dst_bucket}/{dst_base}/metadata.json",
        }
    except Exception as e:
        log.error(f"  {name}: failed: {e}")
        return {"tile": name, "status": "error", "error": str(e)}
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@click.command("s1-monthly-mosaic-sync")
@click.option(
    "--date",
    "-d",
    required=True,
    help="Mosaic month, YYYY-MM (a YYYY-MM-DD is accepted; the day is ignored).",
)
@click.option(
    "--dst-bucket",
    "-b",
    default=DEFAULT_DST_BUCKET,
    show_default=True,
    help=f"Destination bucket. Use {PROD_DST_BUCKET} for the operational run.",
)
@click.option(
    "--src-bucket",
    default=SRC_BUCKET,
    show_default=True,
    help="CDSE source bucket.",
)
@click.option(
    "--extent",
    default=DEFAULT_EXTENT,
    show_default=True,
    help="African extent (path or URL readable by geopandas).",
)
@click.option(
    "--tiles",
    default=None,
    help="Comma-separated MGRS tile prefixes (e.g. 39NUD,33QZA) to restrict "
    "to, for testing. Applied after the Africa filter.",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Process at most this many tiles (testing).",
)
@click.option(
    "--max-workers",
    default=4,
    show_default=True,
    help="Parallel tile workers. Each downloads/uploads two COGs, so keep "
    "modest unless bandwidth allows.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Re-copy tifs and regenerate metadata.json even if they exist.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="List the tiles that would be synced, copy nothing.",
)
def cli(
    date, dst_bucket, src_bucket, extent, tiles, limit, max_workers, overwrite, dry_run
):
    """
    Copy one month of Sentinel-1 IW monthly mosaics (VV/VH COGs) for
    Africa-intersecting tiles from CDSE eodata into a DE Africa bucket,
    writing a metadata.json STAC item per tile.

    Idempotent; safe to re-run for the same month after partial failures.
    Exits non-zero if any tile fails.
    """
    m = re.fullmatch(r"(\d{4})-(\d{2})(?:-\d{2})?", date)
    if not m:
        raise click.BadParameter(f"--date must be YYYY-MM, got {date!r}")
    year, month = m.group(1), m.group(2)

    africa = load_africa_extent(extent)

    s3_src = cdse_client()
    names = list_month_tiles(s3_src, src_bucket, year, month)
    if not names:
        log.error(
            f"No tiles found for {year}-{month} - the mosaic may not be "
            f"published yet (CDSE publishes these with a delay of several months)."
        )
        sys.exit(1)

    selected = african_tiles(names, africa)

    if tiles:
        wanted = {t.strip().upper() for t in tiles.split(",")}
        selected = [(n, t5) for n, t5 in selected if t5 in wanted]
        log.info(f"--tiles filter: {len(selected)} tiles remain")
    if limit:
        selected = selected[:limit]
        log.info(f"--limit: processing first {len(selected)} tiles")

    if dry_run:
        print(
            json.dumps(
                {
                    "month": f"{year}-{month}",
                    "dst_bucket": dst_bucket,
                    "tile_count": len(selected),
                    "tiles": [n for n, _ in selected],
                },
                indent=2,
            )
        )
        return

    s3_dst = dst_client()
    log.info(
        f"Syncing {len(selected)} tiles for {year}-{month} -> "
        f"s3://{dst_bucket}/{DST_PREFIX}/{year}/{month}/01/ "
        f"(max_workers={max_workers})"
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        results = list(
            ex.map(
                lambda nt: process_tile(
                    s3_src,
                    s3_dst,
                    nt[0],
                    nt[1],
                    year,
                    month,
                    src_bucket,
                    dst_bucket,
                    overwrite,
                ),
                selected,
            )
        )

    counts = {}
    for r in results:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    print(
        json.dumps(
            {"month": f"{year}-{month}", "summary": counts, "results": results},
            indent=2,
        )
    )

    if counts.get("error") or counts.get("missing_source"):
        sys.exit(1)


if __name__ == "__main__":
    cli()
