from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

SOURCE_BUCKET = "deafrica-sentinel-1-staging-frankfurt"
DESTINATION_BUCKET = "deafrica-sentinel-1"
SOURCE_REGION = "eu-central-1"
DESTINATION_REGION = "af-south-1"

BASE_PREFIX = "s1_rtc/"
REPORT_TYPE = "s1_frankfurt_gap_report"
REPORT_SCHEMA_VERSION = 1

REPORT_STATUSES = {
    "complete_in_dest",
    "dest_metadata_present_assets_missing",
    "missing_everything_in_dest",
    "metadata_missing_assets_present",
    "metadata_missing_some_assets_missing",
    "source_incomplete",
    "check_error",
}

SAFE_FILL_STATUSES = {
    "missing_everything_in_dest",
    "metadata_missing_assets_present",
    "metadata_missing_some_assets_missing",
}

REQUIRED_SUFFIXES = (
    "_VV.tif",
    "_VH.tif",
    "_ANGLE.tif",
    "_AREA.tif",
    "_MASK.tif",
    "_metadata.json",
    "_metadata.xml",
    "_userdata.json",
)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class DatasetPlan:
    date: str
    tile: str
    datatake: str
    metadata_key: str
    status: str
    dest_metadata_exists: bool
    source_complete: bool
    source_object_count: int
    dest_existing_count: int
    dest_missing_count: int
    missing_required_source: list[str]
    missing_dest_assets: list[str]
    mismatched_dest_assets: list[str]
    existing_dest_assets: list[str]
    source_prefix: str
    check_error: str | None = None


def s3_client(region_name: str):
    return boto3.client("s3", region_name=region_name)


def parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def each_day(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def s3_url(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


def split_s3_url(url: str) -> tuple[str, str]:
    parsed = urlparse(url)
    if parsed.scheme != "s3":
        raise ValueError(f"Expected s3:// URL, got {url}")
    return parsed.netloc, parsed.path.lstrip("/")


def list_common_prefixes(client, bucket: str, prefix: str, delimiter: str = "/"):
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter=delimiter):
        for item in page.get("CommonPrefixes", []):
            yield item["Prefix"]


def list_object_keys(client, bucket: str, prefix: str) -> list[str]:
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(item["Key"] for item in page.get("Contents", []))
    return keys


def list_object_sizes(client, bucket: str, prefix: str) -> dict[str, int]:
    paginator = client.get_paginator("list_objects_v2")
    sizes: dict[str, int] = {}
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        sizes.update({item["Key"]: item["Size"] for item in page.get("Contents", [])})
    return sizes


def head_object(client, bucket: str, key: str) -> dict | None:
    try:
        return client.head_object(Bucket=bucket, Key=key)
    except ClientError as err:
        code = err.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise


def object_exists(client, bucket: str, key: str) -> bool:
    return head_object(client, bucket, key) is not None


def object_sizes_match(source_head: dict | None, destination_head: dict | None) -> bool:
    if not source_head or not destination_head:
        return False
    return source_head["ContentLength"] == destination_head["ContentLength"]


def object_size_matches(source_head: dict | None, destination_size: int | None) -> bool:
    if not source_head or destination_size is None:
        return False
    return source_head["ContentLength"] == destination_size


def read_json_object(client, bucket: str, key: str) -> dict:
    obj = client.get_object(Bucket=bucket, Key=key)
    body = obj["Body"]
    try:
        return json.loads(body.read().decode("utf-8"))
    finally:
        body.close()


def put_json_object(client, bucket: str, key: str, data: dict) -> None:
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def put_text_object(
    client, bucket: str, key: str, text: str, content_type: str
) -> None:
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=text.encode("utf-8"),
        ContentType=content_type,
    )


def s3_event_message(bucket: str, key: str) -> dict:
    return {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": bucket},
                    "object": {"key": key},
                }
            }
        ]
    }


def parse_dataset_key(metadata_key: str) -> tuple[str, str, str]:
    parts = metadata_key.split("/")
    if len(parts) < 7:
        raise ValueError(f"Unexpected Sentinel-1 RTC key layout: {metadata_key}")
    product, tile, year, month, day, datatake = parts[:6]
    if product != "s1_rtc":
        raise ValueError(f"Unexpected product in key: {metadata_key}")
    return tile, f"{year}-{month}-{day}", datatake


def key_from_href(href: str, metadata_key: str) -> str:
    metadata_prefix = metadata_key.rsplit("/", 1)[0] + "/"
    if href.startswith("s3://"):
        parsed = urlparse(href)
        key = parsed.path.lstrip("/")
    else:
        key = href

    if key.startswith(BASE_PREFIX):
        return key
    return metadata_prefix + key.rsplit("/", 1)[-1]


def expected_asset_keys_from_stac(stac: dict, metadata_key: str) -> set[str]:
    keys: set[str] = set()
    for asset in stac.get("assets", {}).values():
        href = asset.get("href")
        if href:
            keys.add(key_from_href(href, metadata_key))
    return keys


def expected_keys_from_stac(stac: dict, metadata_key: str) -> set[str]:
    metadata_base = metadata_key.removesuffix("_metadata.json")
    expected_keys = {metadata_key}
    expected_keys.update(expected_asset_keys_from_stac(stac, metadata_key))
    expected_keys.update(f"{metadata_base}{suffix}" for suffix in REQUIRED_SUFFIXES)
    return expected_keys


def discover_metadata_keys(
    source_s3,
    source_bucket: str,
    start: date,
    end: date,
    tile: str | None = None,
):
    tile_prefixes = (
        [f"{BASE_PREFIX}{tile.strip('/')}/"]
        if tile
        else list(list_common_prefixes(source_s3, source_bucket, BASE_PREFIX))
    )
    for tile_prefix in tile_prefixes:
        for day in each_day(start, end):
            date_prefix = f"{tile_prefix}{day:%Y/%m/%d}/"
            for datatake_prefix in list_common_prefixes(
                source_s3, source_bucket, date_prefix
            ):
                for key in list_object_keys(source_s3, source_bucket, datatake_prefix):
                    if key.endswith("_metadata.json"):
                        yield key


def build_plan_for_metadata(
    source_s3,
    destination_s3,
    source_bucket: str,
    destination_bucket: str,
    metadata_key: str,
) -> DatasetPlan:
    tile, day, datatake = parse_dataset_key(metadata_key)
    source_prefix = metadata_key.rsplit("/", 1)[0] + "/"
    source_keys = set(list_object_keys(source_s3, source_bucket, source_prefix))

    stac = read_json_object(source_s3, source_bucket, metadata_key)
    expected_keys = expected_keys_from_stac(stac, metadata_key)
    return build_plan_for_expected_keys(
        source_s3=source_s3,
        destination_s3=destination_s3,
        source_bucket=source_bucket,
        destination_bucket=destination_bucket,
        metadata_key=metadata_key,
        expected_keys=expected_keys,
        source_prefix=source_prefix,
        source_object_count=len(source_keys & expected_keys),
    )


def build_plan_for_metadata_without_listing(
    source_s3,
    destination_s3,
    source_bucket: str,
    destination_bucket: str,
    metadata_key: str,
) -> DatasetPlan:
    tile, day, datatake = parse_dataset_key(metadata_key)
    source_prefix = metadata_key.rsplit("/", 1)[0] + "/"

    stac = read_json_object(source_s3, source_bucket, metadata_key)
    expected_keys = expected_keys_from_stac(stac, metadata_key)
    return build_plan_for_expected_keys(
        source_s3=source_s3,
        destination_s3=destination_s3,
        source_bucket=source_bucket,
        destination_bucket=destination_bucket,
        metadata_key=metadata_key,
        expected_keys=expected_keys,
        source_prefix=source_prefix,
    )


def build_plan_for_expected_keys(
    source_s3,
    destination_s3,
    source_bucket: str,
    destination_bucket: str,
    metadata_key: str,
    expected_keys: set[str],
    source_prefix: str,
    source_object_count: int | None = None,
) -> DatasetPlan:
    tile, day, datatake = parse_dataset_key(metadata_key)
    source_heads = {
        key: head_object(source_s3, source_bucket, key) for key in sorted(expected_keys)
    }
    source_keys = {key for key, source_head in source_heads.items() if source_head}
    missing_required_source = sorted(expected_keys - source_keys)
    source_complete = not missing_required_source

    destination_sizes = list_object_sizes(
        destination_s3, destination_bucket, source_prefix
    )
    destination_metadata_size = destination_sizes.get(metadata_key)
    dest_metadata_exists = destination_metadata_size is not None
    dest_metadata_matches = object_size_matches(
        source_heads.get(metadata_key),
        destination_metadata_size,
    )

    asset_keys = sorted(key for key in source_keys if key != metadata_key)
    existing_dest_assets: list[str] = []
    missing_dest_assets: list[str] = []
    mismatched_dest_assets: list[str] = []
    for key in asset_keys:
        destination_size = destination_sizes.get(key)
        if destination_size is None:
            missing_dest_assets.append(key)
        elif object_size_matches(source_heads[key], destination_size):
            existing_dest_assets.append(key)
        else:
            mismatched_dest_assets.append(key)
    if dest_metadata_exists and not dest_metadata_matches:
        mismatched_dest_assets.append(metadata_key)

    assets_needing_copy = missing_dest_assets + mismatched_dest_assets
    if not source_complete:
        status = "source_incomplete"
    elif dest_metadata_exists and dest_metadata_matches and not assets_needing_copy:
        status = "complete_in_dest"
    elif dest_metadata_exists and assets_needing_copy:
        status = "dest_metadata_present_assets_missing"
    elif not existing_dest_assets and len(assets_needing_copy) == len(asset_keys):
        status = "missing_everything_in_dest"
    elif existing_dest_assets and not assets_needing_copy:
        status = "metadata_missing_assets_present"
    elif existing_dest_assets and assets_needing_copy:
        status = "metadata_missing_some_assets_missing"
    else:
        status = "source_incomplete"
        missing_required_source.append("No non-metadata source assets found")

    source_object_count = (
        len(source_keys) if source_object_count is None else source_object_count
    )
    existing_dest_keys = set(existing_dest_assets) | set(mismatched_dest_assets)
    if dest_metadata_exists:
        existing_dest_keys.add(metadata_key)
    return DatasetPlan(
        date=day,
        tile=tile,
        datatake=datatake,
        metadata_key=metadata_key,
        status=status,
        dest_metadata_exists=dest_metadata_exists,
        source_complete=source_complete,
        source_object_count=source_object_count,
        dest_existing_count=len(existing_dest_keys),
        dest_missing_count=len(assets_needing_copy) + int(not dest_metadata_exists),
        missing_required_source=missing_required_source,
        missing_dest_assets=missing_dest_assets,
        mismatched_dest_assets=mismatched_dest_assets,
        existing_dest_assets=existing_dest_assets,
        source_prefix=source_prefix,
    )


def plan_to_dict(plan: DatasetPlan) -> dict:
    return asdict(plan)


def status_summary(datasets: list[dict]) -> dict[str, int]:
    summary: dict[str, int] = {status: 0 for status in sorted(REPORT_STATUSES)}
    for dataset in datasets:
        status = dataset["status"]
        summary[status] = summary.get(status, 0) + 1
    return {status: count for status, count in sorted(summary.items()) if count}


def report_key(start_date: str, end_date: str) -> str:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return (
        f"status-report/{today}_s1_frankfurt_gap_report_"
        f"{start_date}_to_{end_date}.json"
    )


def csv_key_from_report_key(key: str) -> str:
    return key.removesuffix(".json") + ".csv"


def build_report(
    start_date: str,
    end_date: str,
    source_bucket: str,
    destination_bucket: str,
    datasets: list[dict],
    complete: bool = True,
    error: str | None = None,
    scope: dict | None = None,
) -> dict:
    check_error_count = sum(1 for item in datasets if item["status"] == "check_error")
    return {
        "report_type": REPORT_TYPE,
        "schema_version": REPORT_SCHEMA_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "start_date": start_date,
        "end_date": end_date,
        "scope": scope or {},
        "source_bucket": source_bucket,
        "destination_bucket": destination_bucket,
        "complete": complete,
        "error": error,
        "check_error_count": check_error_count,
        "summary": status_summary(datasets),
        "total_datasets_checked": len(datasets),
        "statuses": sorted(REPORT_STATUSES),
        "fillable_statuses": sorted(SAFE_FILL_STATUSES),
        "datasets": datasets,
        "complete_in_dest": [
            item["metadata_key"]
            for item in datasets
            if item["status"] == "complete_in_dest"
        ],
        "missing_datasets": [
            item["metadata_key"]
            for item in datasets
            if item["status"] == "missing_everything_in_dest"
        ],
        "metadata_missing_assets_present": [
            item["metadata_key"]
            for item in datasets
            if item["status"] == "metadata_missing_assets_present"
        ],
        "metadata_missing_some_assets_missing": [
            {
                "metadata_key": item["metadata_key"],
                "missing_dest_assets": item["missing_dest_assets"],
                "mismatched_dest_assets": item["mismatched_dest_assets"],
            }
            for item in datasets
            if item["status"] == "metadata_missing_some_assets_missing"
        ],
        "dest_metadata_present_assets_missing": [
            {
                "metadata_key": item["metadata_key"],
                "missing_dest_assets": item["missing_dest_assets"],
                "mismatched_dest_assets": item["mismatched_dest_assets"],
            }
            for item in datasets
            if item["status"] == "dest_metadata_present_assets_missing"
        ],
        "source_incomplete": [
            {
                "metadata_key": item["metadata_key"],
                "missing_required_source": item["missing_required_source"],
            }
            for item in datasets
            if item["status"] == "source_incomplete"
        ],
    }


def csv_from_datasets(datasets: list[dict]) -> str:
    header = [
        "date",
        "tile",
        "datatake",
        "metadata_key",
        "status",
        "dest_metadata_exists",
        "source_complete",
        "source_object_count",
        "dest_existing_count",
        "dest_missing_count",
        "missing_required_source",
        "missing_dest_assets",
        "mismatched_dest_assets",
        "existing_dest_assets",
        "source_prefix",
        "check_error",
    ]
    rows = [",".join(header)]
    for item in datasets:
        values = []
        for column in header:
            value = item.get(column, "")
            if isinstance(value, list):
                value = ";".join(value)
            text = str(value).replace('"', '""')
            values.append(f'"{text}"')
        rows.append(",".join(values))
    return "\n".join(rows) + "\n"


def describe_exception(exc: Exception) -> str:
    if isinstance(exc, ClientError):
        error = exc.response.get("Error", {})
        code = error.get("Code", "Unknown")
        message = error.get("Message", str(exc))
        operation = exc.operation_name or "UnknownOperation"
        return f"ClientError:{operation}:{code}:{message}"
    return f"{type(exc).__name__}: {exc}"


def read_report(report_path: str, region_name: str = DESTINATION_REGION) -> dict:
    if report_path.startswith("s3://"):
        bucket, key = split_s3_url(report_path)
        return read_json_object(s3_client(region_name), bucket, key)

    with open(report_path, "r", encoding="utf-8") as stream:
        return json.load(stream)


def copy_one_object(
    source_s3,
    destination_s3,
    source_bucket: str,
    destination_bucket: str,
    key: str,
) -> None:
    source_head = head_object(source_s3, source_bucket, key)
    if not source_head:
        raise RuntimeError(f"Source object disappeared before copy: {key}")

    obj = source_s3.get_object(Bucket=source_bucket, Key=key)
    body = obj["Body"]
    extra_args = {}
    if obj.get("ContentType"):
        extra_args["ContentType"] = obj["ContentType"]

    try:
        destination_s3.upload_fileobj(
            body,
            destination_bucket,
            key,
            ExtraArgs=extra_args,
        )
    finally:
        body.close()

    destination_head = head_object(destination_s3, destination_bucket, key)
    if not destination_head:
        raise RuntimeError(f"Copy verification failed for {key}: destination missing")
    if not object_sizes_match(source_head, destination_head):
        source_size = source_head["ContentLength"]
        destination_size = destination_head["ContentLength"]
        raise RuntimeError(
            f"Copy verification failed for {key}: "
            f"source={source_size}, destination={destination_size}"
        )


def fill_dataset(
    source_s3,
    destination_s3,
    source_bucket: str,
    destination_bucket: str,
    metadata_key: str,
    dryrun: bool,
) -> tuple[str, int]:
    plan = build_plan_for_metadata_without_listing(
        source_s3=source_s3,
        destination_s3=destination_s3,
        source_bucket=source_bucket,
        destination_bucket=destination_bucket,
        metadata_key=metadata_key,
    )

    if plan.dest_metadata_exists:
        return f"skipped_status_{plan.status}", 0
    if plan.status not in SAFE_FILL_STATUSES:
        return f"skipped_status_{plan.status}", 0

    copied = 0
    for key in sorted(set(plan.missing_dest_assets + plan.mismatched_dest_assets)):
        source_head = head_object(source_s3, source_bucket, key)
        destination_head = head_object(destination_s3, destination_bucket, key)
        if object_sizes_match(source_head, destination_head):
            continue
        if not dryrun:
            copy_one_object(
                source_s3,
                destination_s3,
                source_bucket,
                destination_bucket,
                key,
            )
        copied += 1

    if object_exists(destination_s3, destination_bucket, metadata_key):
        return "skipped_metadata_appeared_during_copy", copied

    if not dryrun:
        copy_one_object(
            source_s3,
            destination_s3,
            source_bucket,
            destination_bucket,
            metadata_key,
        )
    copied += 1
    return (
        "dryrun_would_copy_metadata_last" if dryrun else "copied_metadata_last"
    ), copied
