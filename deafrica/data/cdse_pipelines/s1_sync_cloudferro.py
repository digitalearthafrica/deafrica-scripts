#!/usr/bin/env python3
"""
Sync CDSE batch outputs from CloudFerro S3 to AWS S3.

This is the Argo-friendly CloudFerro sync command. It can sync a single CDSE
output prefix, or discover completed prefixes under a broader CloudFerro prefix
and sync them with bounded parallelism.

Sentinel-1 uses a product-specific key transformation. Direct-copy products
such as Sentinel-3 and Sentinel-5P preserve the CloudFerro key layout.

The sync is gated on product-specific completion files, for example:
  - Sentinel-1: metadata.xml and userdata.json
  - Sentinel-3 LFR: metadata.json and userdata.json

Example:
    sentinel-sync-cloudferro \
        --product s1_rtc \
        --source-bucket cdse_batch_test_bucket \
        --source-prefix s1_rtc/2026/06/23/0834AD/JOB_ID/N00E005/ \
        --stac-item \
            s1_rtc/2026/06/23/0834AD/JOB_ID/N00E005/s1_rtc_0834AD_N00E005_2026_06_23_metadata.json \
        --destination-bucket deafrica-sentinel-1 \
        --dry-run

    sentinel-sync-cloudferro \
        --product s3_olci_l2_lfr_cdse \
        --source-bucket cdse_batch_test_bucket \
        --source-prefix s3_lfr_test/2026/06/23/N00E005/ \
        --destination-bucket <destination-bucket> \
        --dry-run

    sentinel-sync-cloudferro \
        --product s3_olci_l2_lfr_cdse \
        --source-bucket cdse_batch_test_bucket \
        --all-prefixes \
        --discovery-prefix s3_lfr_test/ \
        --max-workers 4 \
        --destination-bucket <destination-bucket>
"""

import json
import os
import urllib.parse
import urllib.request
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from fnmatch import fnmatch

import boto3
import click
from botocore.exceptions import ClientError

from deafrica.logs import setup_logging

DEFAULT_SOURCE_BUCKET = "cdse_batch_test_bucket"
DEFAULT_DESTINATION_BUCKET = "deafrica-sentinel-1"
DEFAULT_CLOUDFERRO_ENDPOINT_URL = "https://s3.waw3-1.cloudferro.com"
DEFAULT_CLOUDFERRO_REGION = "RegionOne"
DEFAULT_DRY_RUN = False
DEFAULT_MIN_OBJECT_AGE_MINUTES = 60
DEFAULT_MAX_WORKERS = 4
DEFAULT_INVALID_OBJECT_EXAMPLE_LIMIT = 10
DEFAULT_CDSE_BATCH_PROCESS_URL = (
    "https://sh.dataspace.copernicus.eu/api/v2/batch/process"
)
DEFAULT_CDSE_TOKEN_URL = (
    "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/"
    "protocol/openid-connect/token"
)
REQUIRED_COMPLETION_FILES = {"metadata.xml", "userdata.json"}
EXCLUDED_COPY_FILENAMES = {
    "metadata.json": "excluded_source_metadata_json",
    "userdata.json": "excluded_userdata_json",
}
SYNC_COUNTER_FIELDS = (
    "found",
    "copyable",
    "skipped_excluded",
    "skipped_unexpected",
    "would_copy",
    "copied",
    "skipped_matching",
    "skipped_mismatched",
    "failed",
)

log = setup_logging()


@dataclass(frozen=True)
class ProductSyncSpec:
    name: str
    discovery_prefix: str
    default_destination_bucket: str | None
    required_files: frozenset[str]
    key_prefix_from_key: Callable[[str], str | None]
    key_transform: Callable[[str], str]
    skip_reason: Callable[[str], str | None]
    stac_item_from_objects: Callable[[list[dict]], str | None]
    requires_stac_item_arg: bool = False


@dataclass(frozen=True)
class SyncConfig:
    source_bucket: str
    source_prefix: str
    stac_item: str
    destination_bucket: str
    dry_run: bool
    min_object_age_minutes: int
    job_id: str | None
    cloudferro_endpoint_url: str
    cloudferro_region: str
    cdse_batch_process_url: str
    cdse_token_url: str


def product_spec_for_name(name: str) -> ProductSyncSpec:
    try:
        return PRODUCT_SPECS[name]
    except KeyError as err:
        raise click.ClickException(f"Unsupported product: {name}") from err


def as_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).lower() in {"1", "true", "yes", "y"}


def _required_env(*names: str) -> str:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    raise ValueError(f"One of these environment variables is required: {names}")


def cloudferro_client(config: SyncConfig):
    """Create an S3 client for CloudFerro's S3-compatible object storage."""
    return boto3.client(
        "s3",
        endpoint_url=config.cloudferro_endpoint_url,
        aws_access_key_id=_required_env("CLOUDFERRO_ACCESS_KEY_ID", "S3_ACCESS_KEY"),
        aws_secret_access_key=_required_env(
            "CLOUDFERRO_SECRET_ACCESS_KEY", "S3_SECRET_ACCESS_KEY"
        ),
        region_name=config.cloudferro_region,
    )


def aws_client():
    """Create the destination AWS S3 client using the normal AWS credential chain."""
    return boto3.client("s3")


def sync_prefix(config: SyncConfig, product: ProductSyncSpec = None) -> dict:
    product = product or DEFAULT_PRODUCT_SPEC
    log.info(
        "Syncing %s CloudFerro prefix s3://%s/%s to s3://%s",
        product.name,
        config.source_bucket,
        config.source_prefix,
        config.destination_bucket,
    )

    summary = {
        "product": product.name,
        "source_bucket": config.source_bucket,
        "source_prefix": config.source_prefix,
        "stac_item": config.stac_item,
        "destination_bucket": config.destination_bucket,
        "dry_run": config.dry_run,
        "min_object_age_minutes": config.min_object_age_minutes,
        "job_id": config.job_id,
        "job_status": None,
        "job_ready": config.job_id is None,
        "skip_reason": None,
        "required_files_present": False,
        "missing_required_files": [],
        "ready_to_sync": False,
        "too_recent_files": [],
        "found": 0,
        "copyable": 0,
        "skipped_excluded": 0,
        "skipped_unexpected": 0,
        "skipped_source_objects": [],
        "would_copy": 0,
        "copied": 0,
        "skipped_matching": 0,
        "skipped_mismatched": 0,
        "failed": 0,
        "failures": [],
    }

    if config.job_id:
        job_status = get_batch_job_status(config)
        summary["job_status"] = job_status
        summary["job_ready"] = job_status == "DONE"

        if job_status != "DONE":
            log.warning(
                "Skipping sync because CDSE Batch job %s status is %s",
                config.job_id,
                job_status,
            )
            summary["skip_reason"] = "batch_job_not_done"
            return summary

    source_s3 = cloudferro_client(config)
    destination_s3 = aws_client()

    source_objects = build_source_objects(source_s3, config)
    summary["found"] = len(source_objects)

    required_files = validate_required_files(source_objects, product)
    summary.update(required_files)
    if not required_files["required_files_present"]:
        log.warning(
            "Source prefix is missing required completion files: %s",
            required_files["missing_required_files"],
        )
        summary["skip_reason"] = "missing_required_files"
        return summary

    copyable_objects, skipped_source_objects = classify_source_objects(
        source_objects, product
    )
    summary["copyable"] = len(copyable_objects)
    summary["skipped_source_objects"] = skipped_source_objects
    summary["skipped_excluded"] = sum(
        1 for obj in skipped_source_objects if obj["reason"].startswith("excluded_")
    )
    summary["skipped_unexpected"] = sum(
        1 for obj in skipped_source_objects if obj["reason"].startswith("unexpected_")
    )

    readiness = validate_source_prefix_ready(
        source_objects, config.min_object_age_minutes
    )
    summary.update(readiness)
    if not readiness["ready_to_sync"]:
        log.warning("Source prefix is not ready to sync: %s", readiness)
        return summary

    for source_object in copyable_objects:
        source_bucket = source_object["Bucket"]
        source_key = source_object["Key"]
        source_size = source_object["Size"]
        destination_key = None

        try:
            destination_key = source_object.get("DestinationKey") or transform_key(
                source_key, product
            )

            status = destination_status(
                destination_s3, config.destination_bucket, destination_key, source_size
            )

            if status == "matching":
                log.info(
                    "Skipping matching object s3://%s/%s",
                    config.destination_bucket,
                    destination_key,
                )
                summary["skipped_matching"] += 1
                continue

            if status == "mismatched":
                log.warning(
                    "Skipping existing mismatched object s3://%s/%s to avoid overwrite",
                    config.destination_bucket,
                    destination_key,
                )
                summary["skipped_mismatched"] += 1
                continue

            log.info(
                "Copying s3://%s/%s to s3://%s/%s",
                source_bucket,
                source_key,
                config.destination_bucket,
                destination_key,
            )

            if config.dry_run:
                summary["would_copy"] += 1
                continue

            copy_object(
                source_s3=source_s3,
                destination_s3=destination_s3,
                source_bucket=source_bucket,
                source_key=source_key,
                source_size=source_size,
                destination_bucket=config.destination_bucket,
                destination_key=destination_key,
            )
            summary["copied"] += 1
        except Exception as err:
            log.exception("Failed to process s3://%s/%s", source_bucket, source_key)
            summary["failed"] += 1
            summary["failures"].append(
                {
                    "source_key": source_key,
                    "destination_key": destination_key,
                    "error": str(err),
                }
            )

    log.info("Sync summary: %s", summary)
    return summary


def sync_all_prefixes(
    config: SyncConfig,
    discovery_prefix: str = None,
    max_workers: int = DEFAULT_MAX_WORKERS,
    max_prefixes: int | None = None,
    product: ProductSyncSpec = None,
) -> dict:
    """Discover completed tile prefixes and sync them with bounded parallelism."""
    product = product or DEFAULT_PRODUCT_SPEC
    discovery_prefix = normalize_s3_prefix(
        discovery_prefix or product.discovery_prefix
    )
    max_workers = max(1, max_workers)
    if max_prefixes is not None:
        max_prefixes = max(1, max_prefixes)

    log.info(
        "Discovering completed %s CloudFerro prefixes under: s3://%s/%s",
        product.name,
        config.source_bucket,
        discovery_prefix,
    )

    source_s3 = cloudferro_client(config)
    discovery = discover_completed_prefixes(
        source_s3,
        bucket=config.source_bucket,
        discovery_prefix=discovery_prefix,
        product=product,
    )
    discovered_completed_prefixes = discovery["completed_prefixes"]
    completed_prefixes = discovered_completed_prefixes
    if max_prefixes is not None:
        completed_prefixes = discovered_completed_prefixes[:max_prefixes]
        if len(discovered_completed_prefixes) > len(completed_prefixes):
            log.info(
                "Limiting all-prefix sync to first %s of %s completed prefixes",
                len(completed_prefixes),
                len(discovered_completed_prefixes),
            )

    summary = {
        "mode": "all_prefixes",
        "product": product.name,
        "source_bucket": config.source_bucket,
        "destination_bucket": config.destination_bucket,
        "dry_run": config.dry_run,
        "min_object_age_minutes": config.min_object_age_minutes,
        "discovery_prefix": discovery_prefix,
        "max_workers": max_workers,
        "max_prefixes": max_prefixes,
        "discovered_prefixes": len(discovered_completed_prefixes)
        + len(discovery["skipped_prefixes"]),
        "completed_prefixes": len(discovered_completed_prefixes),
        "selected_prefixes": len(completed_prefixes),
        "limited_prefixes": (
            len(discovered_completed_prefixes) - len(completed_prefixes)
        ),
        "skipped_prefixes": discovery["skipped_prefixes"],
        "skipped_invalid_object_count": discovery["skipped_invalid_object_count"],
        "skipped_invalid_object_examples": discovery["skipped_invalid_object_examples"],
        "processed_prefixes": 0,
        "prefixes_with_failures": 0,
        "prefixes_skipped_during_sync": 0,
        "prefix_exceptions": [],
        "prefix_summaries": [],
    }
    for field in SYNC_COUNTER_FIELDS:
        summary[field] = 0

    if not completed_prefixes:
        log.warning(
            "No completed prefixes found under s3://%s/%s",
            config.source_bucket,
            discovery_prefix,
        )
        return summary

    log.info(
        "Syncing %s completed prefixes with up to %s worker(s)",
        len(completed_prefixes),
        max_workers,
    )

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_prefix = {
            executor.submit(
                sync_prefix,
                replace(
                    config,
                    source_prefix=prefix["source_prefix"],
                    stac_item=prefix["stac_item"],
                    job_id=None,
                ),
                product,
            ): prefix
            for prefix in completed_prefixes
        }

        for future in as_completed(future_to_prefix):
            prefix = future_to_prefix[future]
            try:
                prefix_summary = future.result()
            except Exception as err:
                log.exception(
                    "Failed to sync discovered prefix %s", prefix["source_prefix"]
                )
                summary["prefix_exceptions"].append(
                    {
                        "source_prefix": prefix["source_prefix"],
                        "stac_item": prefix["stac_item"],
                        "error": str(err),
                    }
                )
                continue

            summary["processed_prefixes"] += 1
            summary["prefix_summaries"].append(prefix_summary)
            add_sync_totals(summary, prefix_summary)

            if prefix_summary.get("failed", 0):
                summary["prefixes_with_failures"] += 1
            if prefix_summary.get("skip_reason") or not prefix_summary.get(
                "ready_to_sync", False
            ):
                summary["prefixes_skipped_during_sync"] += 1

    summary["prefix_summaries"].sort(key=lambda item: item["source_prefix"])
    summary["prefix_exceptions"].sort(key=lambda item: item["source_prefix"])
    summary["prefixes_with_failures"] += len(summary["prefix_exceptions"])

    log.info("All-prefix sync summary: %s", summary)
    return summary


def add_sync_totals(overall_summary: dict, prefix_summary: dict) -> None:
    for field in SYNC_COUNTER_FIELDS:
        overall_summary[field] += prefix_summary.get(field, 0) or 0


def discover_completed_prefixes(
    client,
    bucket: str,
    discovery_prefix: str,
    product: ProductSyncSpec = None,
) -> dict:
    """Find completed product prefixes under a broader source prefix."""
    product = product or DEFAULT_PRODUCT_SPEC
    objects_by_prefix: dict[str, list[dict]] = {}
    skipped_invalid_object_count = 0
    skipped_invalid_object_examples = []

    for source_object in list_source_objects(client, bucket, discovery_prefix):
        source_key = source_object["Key"]
        source_prefix = tile_prefix_from_key(source_key, product)
        if not source_prefix:
            skipped_invalid_object_count += 1
            if (
                len(skipped_invalid_object_examples)
                < DEFAULT_INVALID_OBJECT_EXAMPLE_LIMIT
            ):
                skipped_invalid_object_examples.append(
                    {
                        "source_key": source_key,
                        "reason": "unexpected_key_layout",
                    }
                )
            continue

        objects_by_prefix.setdefault(source_prefix, []).append(source_object)

    completed_prefixes = []
    skipped_prefixes = []

    for source_prefix, source_objects in sorted(objects_by_prefix.items()):
        required_files = validate_required_files(source_objects, product)
        if not required_files["required_files_present"]:
            skipped_prefixes.append(
                {
                    "source_prefix": source_prefix,
                    "reason": "missing_required_files",
                    "missing_required_files": required_files["missing_required_files"],
                    "found": len(source_objects),
                }
            )
            continue

        stac_item = find_stac_item(source_objects, product)
        if not stac_item:
            skipped_prefixes.append(
                {
                    "source_prefix": source_prefix,
                    "reason": "missing_stac_item",
                    "missing_required_files": [],
                    "found": len(source_objects),
                }
            )
            continue

        completed_prefixes.append(
            {
                "source_prefix": source_prefix,
                "stac_item": stac_item,
                "found": len(source_objects),
            }
        )

    return {
        "completed_prefixes": completed_prefixes,
        "skipped_prefixes": skipped_prefixes,
        "skipped_invalid_object_count": skipped_invalid_object_count,
        "skipped_invalid_object_examples": skipped_invalid_object_examples,
    }


def tile_prefix_from_key(
    source_key: str, product: ProductSyncSpec = None
) -> str | None:
    product = product or DEFAULT_PRODUCT_SPEC
    return product.key_prefix_from_key(source_key)


def s1_tile_prefix_from_key(source_key: str) -> str | None:
    try:
        product, year, month, day, datatake_id, job_id, tile_id, filename_parts = (
            parse_source_key(source_key)
        )
    except ValueError:
        return None

    if not filename_parts:
        return None

    return "/".join([product, year, month, day, datatake_id, job_id, tile_id]) + "/"


def direct_product_prefix_from_key_factory(
    expected_prefix: str,
    allow_nested_files: bool = False,
) -> Callable[[str], str | None]:
    expected_parts = expected_prefix.strip("/").split("/")

    def key_prefix_from_key(source_key: str) -> str | None:
        parts = source_key.strip("/").split("/")
        if parts[: len(expected_parts)] != expected_parts:
            return None

        remaining = parts[len(expected_parts) :]
        if len(remaining) < 5:
            return None

        year, month, day, grouping_id, *filename_parts = remaining
        if not filename_parts:
            return None
        if not allow_nested_files and len(filename_parts) != 1:
            return None
        if not (year.isdigit() and len(year) == 4):
            return None
        if not (month.isdigit() and len(month) == 2):
            return None
        if not (day.isdigit() and len(day) == 2):
            return None

        return "/".join([*expected_parts, year, month, day, grouping_id]) + "/"

    return key_prefix_from_key


def find_stac_item(
    source_objects: list[dict], product: ProductSyncSpec = None
) -> str | None:
    product = product or DEFAULT_PRODUCT_SPEC
    return product.stac_item_from_objects(source_objects)


def find_s1_stac_item(source_objects: list[dict]) -> str | None:
    """Return the generated STAC metadata JSON key for a discovered tile prefix."""
    for source_object in sorted(source_objects, key=lambda item: item["Key"]):
        source_key = source_object["Key"]
        filename = object_filename(source_key)
        if filename.endswith("_metadata.json") and s1_skip_reason(source_key) is None:
            return source_key

    return None


def find_metadata_json_stac_item(source_objects: list[dict]) -> str | None:
    """Return the product STAC metadata.json key for a discovered tile prefix."""
    for source_object in sorted(source_objects, key=lambda item: item["Key"]):
        source_key = source_object["Key"]
        if object_filename(source_key) == "metadata.json":
            return source_key

    return None


def normalize_s3_prefix(prefix: str) -> str:
    prefix = prefix.strip().lstrip("/")
    if prefix and not prefix.endswith("/"):
        return f"{prefix}/"
    return prefix


def get_batch_job_status(config: SyncConfig) -> str:
    """Return the CDSE Batch Processing API status for a job id."""
    token = get_cdse_access_token(config.cdse_token_url)
    url = f"{config.cdse_batch_process_url.rstrip('/')}/{config.job_id}"
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        },
    )

    with urllib.request.urlopen(request, timeout=30) as response:
        job = json.load(response)

    status = job.get("status")
    if not status:
        raise ValueError(f"CDSE Batch job response did not include status: {job}")

    return status


def get_cdse_access_token(token_url: str) -> str:
    """Fetch a CDSE OAuth access token, or reuse a supplied temporary token."""
    access_token = os.environ.get("CDSE_ACCESS_TOKEN")
    if access_token:
        return access_token

    client_id = _required_env("CDSE_CLIENT_ID", "SH_CLIENT_ID")
    client_secret = _required_env("CDSE_CLIENT_SECRET", "SH_CLIENT_SECRET")
    data = urllib.parse.urlencode(
        {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        token_url,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )

    with urllib.request.urlopen(request, timeout=30) as response:
        token = json.load(response)

    access_token = token.get("access_token")
    if not access_token:
        raise ValueError("CDSE token response did not include access_token")

    return access_token


def build_source_objects(client, config: SyncConfig) -> list[dict]:
    """List tile outputs and include the STAC item needed for indexing."""
    source_objects = list(
        list_source_objects(client, config.source_bucket, config.source_prefix)
    )
    if not config.stac_item:
        return source_objects

    stac_bucket, stac_key = parse_s3_uri(config.stac_item, config.source_bucket)
    seen = {
        (source_object["Bucket"], source_object["Key"])
        for source_object in source_objects
    }
    if (stac_bucket, stac_key) in seen:
        return source_objects

    source_objects.append(
        describe_source_object(
            client=client,
            bucket=stac_bucket,
            key=stac_key,
            destination_key=stac_key,
        )
    )
    return source_objects


def parse_s3_uri(value: str, default_bucket: str) -> tuple[str, str]:
    if value.startswith("s3://"):
        parsed = urllib.parse.urlparse(value)
        return parsed.netloc, parsed.path.lstrip("/")

    return default_bucket, value.strip("/")


def describe_source_object(
    client, bucket: str, key: str, destination_key: str | None = None
) -> dict:
    source = client.head_object(Bucket=bucket, Key=key)
    metadata = {
        "Bucket": bucket,
        "Key": key,
        "Size": source["ContentLength"],
        "LastModified": source["LastModified"],
    }
    if destination_key:
        metadata["DestinationKey"] = destination_key
    return metadata


def list_source_objects(client, bucket: str, prefix: str):
    """Yield source object metadata needed for copy and destination checks."""
    paginator = client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/"):
                yield {
                    "Bucket": bucket,
                    "Key": key,
                    "Size": obj["Size"],
                    "LastModified": obj["LastModified"],
                }


def validate_source_prefix_ready(
    source_objects: list[dict], min_object_age_minutes: int
) -> dict:
    """Check the prefix is old enough to avoid copying active writes."""
    min_age = timedelta(minutes=min_object_age_minutes)
    now = datetime.now(timezone.utc)
    too_recent_files = []

    for source_object in source_objects:
        filename = object_filename(source_object["Key"])
        last_modified = source_object["LastModified"]
        if last_modified.tzinfo is None:
            last_modified = last_modified.replace(tzinfo=timezone.utc)

        if now - last_modified < min_age:
            too_recent_files.append(
                {
                    "file": filename,
                    "last_modified": last_modified.isoformat(),
                }
            )

    return {
        "ready_to_sync": bool(source_objects) and not too_recent_files,
        "too_recent_files": too_recent_files,
    }


def validate_required_files(
    source_objects: list[dict], product: ProductSyncSpec = None
) -> dict:
    """Check CDSE output and CARD4L metadata are complete before syncing."""
    product = product or DEFAULT_PRODUCT_SPEC
    filenames = {
        object_filename(source_object["Key"]) for source_object in source_objects
    }
    missing = sorted(product.required_files - filenames)

    return {
        "required_files_present": not missing,
        "missing_required_files": missing,
    }


def classify_source_objects(
    source_objects: list[dict],
    product: ProductSyncSpec = None,
) -> tuple[list[dict], list[dict]]:
    """Split CloudFerro objects into deliverables and source-side/internal files."""
    product = product or DEFAULT_PRODUCT_SPEC
    copyable = []
    skipped = []

    for source_object in source_objects:
        reason = skip_reason(source_object["Key"], product)
        if reason:
            skipped.append({"source_key": source_object["Key"], "reason": reason})
        else:
            copyable.append(source_object)

    return copyable, skipped


def skip_reason(source_key: str, product: ProductSyncSpec = None) -> str | None:
    product = product or DEFAULT_PRODUCT_SPEC
    return product.skip_reason(source_key)


def s1_skip_reason(source_key: str) -> str | None:
    filename = object_filename(source_key)
    excluded_reason = EXCLUDED_COPY_FILENAMES.get(filename)
    if excluded_reason:
        return excluded_reason

    try:
        filename_prefix = dataset_filename_prefix(source_key)
    except ValueError:
        return "unexpected_key_layout"

    if filename.endswith(".tif"):
        return None
    if filename == "metadata.xml":
        return None
    if filename in {
        f"{filename_prefix}metadata.json",
        f"{filename_prefix}metadata.xml",
    }:
        return None

    return "unexpected_file"


def excluded_pattern_reason(pattern: str) -> str:
    known_reasons = {
        "metadata.json": "excluded_source_metadata_json",
        "request-*.json": "excluded_request_json",
        "userdata.json": "excluded_userdata_json",
    }
    if pattern in known_reasons:
        return known_reasons[pattern]

    normalized = pattern.replace("*", "wildcard").replace(".", "_").replace("-", "_")
    return f"excluded_{normalized}"


def direct_product_skip_reason_factory(
    expected_prefix: str,
    allowed_filename_patterns: tuple[str, ...],
    excluded_filename_patterns: tuple[str, ...] = (
        "userdata.json",
        "request-*.json",
    ),
    allow_nested_files: bool = False,
) -> Callable[[str], str | None]:
    key_prefix_from_key = direct_product_prefix_from_key_factory(
        expected_prefix,
        allow_nested_files=allow_nested_files,
    )

    def skip_reason(source_key: str) -> str | None:
        filename = object_filename(source_key)
        for pattern in excluded_filename_patterns:
            if fnmatch(filename, pattern):
                return excluded_pattern_reason(pattern)

        if not key_prefix_from_key(source_key):
            return "unexpected_key_layout"

        for pattern in allowed_filename_patterns:
            if fnmatch(filename, pattern):
                return None

        return "unexpected_file"

    return skip_reason


def object_filename(key: str) -> str:
    return key.strip("/").rsplit("/", 1)[-1]


def dataset_filename_prefix(source_key: str) -> str:
    product, year, month, day, datatake_id, _job_id, tile_id, _filename_parts = (
        parse_source_key(source_key)
    )
    return f"{product}_{datatake_id}_{tile_id}_{year}_{month}_{day}_"


def parse_source_key(
    source_key: str,
) -> tuple[str, str, str, str, str, str, str, list[str]]:
    parts = source_key.strip("/").split("/")
    if len(parts) < 8:
        raise ValueError(
            "Expected key layout "
            "product/year/month/day/datatake_id/job_id/tile_id/file, "
            f"got {source_key}"
        )

    product, year, month, day, datatake_id, job_id, tile_id, *filename_parts = parts
    if product != "s1_rtc":
        raise ValueError(f"Unexpected product in key: {source_key}")
    if not (year.isdigit() and len(year) == 4):
        raise ValueError(f"Invalid year in key: {source_key}")
    if not (month.isdigit() and len(month) == 2):
        raise ValueError(f"Invalid month in key: {source_key}")
    if not (day.isdigit() and len(day) == 2):
        raise ValueError(f"Invalid day in key: {source_key}")

    return product, year, month, day, datatake_id, job_id, tile_id, filename_parts


def transform_key(source_key: str, product: ProductSyncSpec = None) -> str:
    product = product or DEFAULT_PRODUCT_SPEC
    return product.key_transform(source_key)


def s1_transform_key(source_key: str) -> str:
    """Move the tile id before the date, drop the CDSE job id, and rename the file.

    Source:
        s1_rtc/year/month/day/datatake_id/job_id/tile_id/file

    Destination:
        s1_rtc/tile_id/year/month/day/datatake_id/product_datatake_tile_date_file
    """
    product, year, month, day, datatake_id, _job_id, tile_id, filename_parts = (
        parse_source_key(source_key)
    )

    filename_parts[-1] = destination_filename(
        product, datatake_id, tile_id, year, month, day, filename_parts[-1]
    )
    return "/".join([product, tile_id, year, month, day, datatake_id, *filename_parts])


def identity_transform_key(source_key: str) -> str:
    return source_key


def destination_filename(
    product: str,
    datatake_id: str,
    tile_id: str,
    year: str,
    month: str,
    day: str,
    filename: str,
) -> str:
    prefix = f"{product}_{datatake_id}_{tile_id}_{year}_{month}_{day}_"
    if filename.startswith(prefix):
        return filename

    return f"{prefix}{filename}"


def destination_status(client, bucket: str, key: str, source_size: int) -> str:
    """Return missing, matching, or mismatched without allowing overwrites."""
    try:
        destination = client.head_object(Bucket=bucket, Key=key)
        destination_size = destination["ContentLength"]
        if destination_size == source_size:
            return "matching"
        log.warning(
            "Destination object exists but size differs for s3://%s/%s: "
            "source=%s, destination=%s. Not overwriting.",
            bucket,
            key,
            source_size,
            destination_size,
        )
        return "mismatched"
    except ClientError as err:
        if err.response.get("Error", {}).get("Code") in {
            "404",
            "NoSuchKey",
            "NotFound",
        }:
            return "missing"
        raise


def copy_object(
    source_s3,
    destination_s3,
    source_bucket: str,
    source_key: str,
    source_size: int,
    destination_bucket: str,
    destination_key: str,
) -> None:
    source_object = source_s3.get_object(Bucket=source_bucket, Key=source_key)
    body = source_object["Body"]

    extra_args = {}
    if source_object.get("ContentType"):
        extra_args["ContentType"] = source_object["ContentType"]

    try:
        destination_s3.upload_fileobj(
            body,
            destination_bucket,
            destination_key,
            ExtraArgs=extra_args,
        )
    finally:
        body.close()

    verify_copy(source_size, destination_s3, destination_bucket, destination_key)


def verify_copy(
    source_size: int, destination_s3, destination_bucket: str, destination_key: str
) -> None:
    """Confirm that the uploaded object size matches the source listing."""
    destination = destination_s3.head_object(
        Bucket=destination_bucket,
        Key=destination_key,
    )

    destination_size = destination["ContentLength"]
    if destination_size != source_size:
        raise ValueError(
            f"Size mismatch for {destination_key}: "
            f"source={source_size}, destination={destination_size}"
        )


S1_RTC_PRODUCT = ProductSyncSpec(
    name="s1_rtc",
    discovery_prefix="s1_rtc/",
    default_destination_bucket=DEFAULT_DESTINATION_BUCKET,
    required_files=frozenset(REQUIRED_COMPLETION_FILES),
    key_prefix_from_key=s1_tile_prefix_from_key,
    key_transform=s1_transform_key,
    skip_reason=s1_skip_reason,
    stac_item_from_objects=find_s1_stac_item,
    requires_stac_item_arg=True,
)

S3_OLCI_L2_LFR_CDSE_SOURCE_PREFIX = "s3_lfr_test"
# Keep allow_nested_files=False for S3 LFR intentionally.
# The nested job-id layout should be ignored:
#   s3_lfr_test/YYYY/MM/DD/JOB_ID/TILE/file
#
# The accepted direct layout is:
#   s3_lfr_test/YYYY/MM/DD/TILE/file
S3_OLCI_L2_LFR_CDSE_PRODUCT = ProductSyncSpec(
    name="s3_olci_l2_lfr_cdse",
    discovery_prefix=f"{S3_OLCI_L2_LFR_CDSE_SOURCE_PREFIX}/",
    default_destination_bucket=None,
    required_files=frozenset({"metadata.json", "userdata.json"}),
    key_prefix_from_key=direct_product_prefix_from_key_factory(
        S3_OLCI_L2_LFR_CDSE_SOURCE_PREFIX,
        allow_nested_files=False,
    ),
    key_transform=identity_transform_key,
    skip_reason=direct_product_skip_reason_factory(
        expected_prefix=S3_OLCI_L2_LFR_CDSE_SOURCE_PREFIX,
        allowed_filename_patterns=("*.tif", "metadata.json"),
        allow_nested_files=False,
    ),
    stac_item_from_objects=find_metadata_json_stac_item,
)

PRODUCT_SPECS = {
    S1_RTC_PRODUCT.name: S1_RTC_PRODUCT,
    S3_OLCI_L2_LFR_CDSE_PRODUCT.name: S3_OLCI_L2_LFR_CDSE_PRODUCT,
}
DEFAULT_PRODUCT_SPEC = S1_RTC_PRODUCT


@click.command("sentinel-sync-cloudferro")
@click.option(
    "--product",
    default=DEFAULT_PRODUCT_SPEC.name,
    show_default=True,
    envvar="PRODUCT",
    type=click.Choice(sorted(PRODUCT_SPECS)),
    help="Product rules to use for discovery, validation, and key transforms.",
)
@click.option(
    "--source-bucket",
    default=DEFAULT_SOURCE_BUCKET,
    show_default=True,
    envvar="SOURCE_BUCKET",
    help="CloudFerro source bucket containing CDSE batch outputs.",
)
@click.option(
    "--source-prefix",
    default=None,
    envvar="SOURCE_PREFIX",
    help="CloudFerro prefix for one CDSE tile output. Required unless --all-prefixes is set.",
)
@click.option(
    "--stac-item",
    default=None,
    envvar="STAC_ITEM",
    help="STAC metadata JSON key or s3:// URI to include in the sync. Required unless --all-prefixes is set.",
)
@click.option(
    "--all-prefixes",
    is_flag=True,
    default=False,
    envvar="ALL_PREFIXES",
    help="Discover and sync all completed tile prefixes under --discovery-prefix.",
)
@click.option(
    "--discovery-prefix",
    default=None,
    envvar="DISCOVERY_PREFIX",
    help=(
        "CloudFerro prefix to scan when --all-prefixes is set. "
        "Defaults to the selected product prefix."
    ),
)
@click.option(
    "--max-workers",
    default=DEFAULT_MAX_WORKERS,
    show_default=True,
    envvar="MAX_WORKERS",
    type=click.IntRange(1),
    help="Maximum number of tile prefixes to sync concurrently in --all-prefixes mode.",
)
@click.option(
    "--max-prefixes",
    default=None,
    envvar="MAX_PREFIXES",
    type=click.IntRange(1),
    help="Maximum number of discovered prefixes to process in --all-prefixes mode.",
)
@click.option(
    "--destination-bucket",
    default=None,
    envvar="DESTINATION_BUCKET",
    help=(
        "AWS S3 destination bucket. Defaults to the selected product bucket "
        "when configured."
    ),
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=DEFAULT_DRY_RUN,
    show_default=True,
    envvar="DRY_RUN",
    help="Report planned copies without uploading objects.",
)
@click.option(
    "--min-object-age-minutes",
    default=DEFAULT_MIN_OBJECT_AGE_MINUTES,
    show_default=True,
    envvar="MIN_OBJECT_AGE_MINUTES",
    type=int,
    help="Minimum object age before syncing, to avoid active writes.",
)
@click.option(
    "--job-id",
    default=None,
    envvar="JOB_ID",
    help="Optional CDSE Batch Processing job id to require DONE status.",
)
@click.option(
    "--cloudferro-endpoint-url",
    default=DEFAULT_CLOUDFERRO_ENDPOINT_URL,
    show_default=True,
    envvar="CLOUDFERRO_ENDPOINT_URL",
    help="CloudFerro S3-compatible endpoint URL.",
)
@click.option(
    "--cloudferro-region",
    default=DEFAULT_CLOUDFERRO_REGION,
    show_default=True,
    envvar="CLOUDFERRO_REGION",
    help="CloudFerro S3-compatible region name.",
)
@click.option(
    "--cdse-batch-process-url",
    default=DEFAULT_CDSE_BATCH_PROCESS_URL,
    show_default=True,
    envvar="CDSE_BATCH_PROCESS_URL",
    help="CDSE Batch Processing API URL.",
)
@click.option(
    "--cdse-token-url",
    default=DEFAULT_CDSE_TOKEN_URL,
    show_default=True,
    envvar="CDSE_TOKEN_URL",
    help="CDSE OAuth token URL.",
)
@click.option(
    "--output",
    "-o",
    default=None,
    help="Optional local path to write the JSON sync summary.",
)
def cli(
    product,
    source_bucket,
    source_prefix,
    stac_item,
    all_prefixes,
    discovery_prefix,
    max_workers,
    max_prefixes,
    destination_bucket,
    dry_run,
    min_object_age_minutes,
    job_id,
    cloudferro_endpoint_url,
    cloudferro_region,
    cdse_batch_process_url,
    cdse_token_url,
    output,
):
    """Sync CDSE tile outputs from CloudFerro to AWS S3."""
    product_spec = product_spec_for_name(product)
    discovery_prefix = discovery_prefix or product_spec.discovery_prefix
    destination_bucket = destination_bucket or product_spec.default_destination_bucket
    if not destination_bucket:
        raise click.UsageError(
            f"--destination-bucket is required for product {product_spec.name}"
        )

    if all_prefixes:
        if job_id:
            log.warning("Ignoring --job-id in --all-prefixes mode")
            job_id = None
    else:
        if not source_prefix:
            raise click.UsageError(
                "--source-prefix is required unless --all-prefixes is set"
            )
        if product_spec.requires_stac_item_arg and not stac_item:
            raise click.UsageError(
                f"--stac-item is required for product {product_spec.name} "
                "unless --all-prefixes is set"
            )

    config = SyncConfig(
        source_bucket=source_bucket,
        source_prefix=source_prefix.lstrip("/") if source_prefix else "",
        stac_item=stac_item or "",
        destination_bucket=destination_bucket,
        dry_run=as_bool(dry_run),
        min_object_age_minutes=min_object_age_minutes,
        job_id=job_id,
        cloudferro_endpoint_url=cloudferro_endpoint_url,
        cloudferro_region=cloudferro_region,
        cdse_batch_process_url=cdse_batch_process_url,
        cdse_token_url=cdse_token_url,
    )
    try:
        if all_prefixes:
            summary = sync_all_prefixes(
                config,
                discovery_prefix=discovery_prefix,
                max_workers=max_workers,
                max_prefixes=max_prefixes,
                product=product_spec,
            )
        else:
            summary = sync_prefix(config, product=product_spec)
    except ClientError as err:
        raise click.ClickException(format_client_error(err)) from err

    print(json.dumps(summary, indent=2, default=str))
    if output:
        with open(output, "w") as f:
            json.dump(summary, f, indent=2, default=str)
        log.info("Summary written to %s", output)


def format_client_error(err: ClientError) -> str:
    error = err.response.get("Error", {})
    operation = err.operation_name
    code = error.get("Code", "Unknown")
    message = error.get("Message") or "No message returned by the S3 service."
    return f"{operation} failed with {code}: {message}"


if __name__ == "__main__":
    cli()
