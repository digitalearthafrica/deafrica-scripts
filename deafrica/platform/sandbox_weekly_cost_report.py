import csv
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3
import click
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

DEFAULT_KUBECOST_URL = "http://kubecost-cost-analyzer.kubecost.svc.cluster.local:9090"
DEFAULT_REPORT_PATH = "/tmp/sandbox_weekly_cost_report.csv"
GOOGLE_DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.file"
CSV_MIME_TYPE = "text/csv"


def report_date():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def report_environment(environment):
    environment = (environment or "").strip().lower()
    if not environment:
        return ""

    if environment not in {"dev", "prod"}:
        raise ValueError("REPORT_ENVIRONMENT must be one of: dev, prod")

    return environment


def report_upload_target(target):
    target = (target or "both").strip().lower()
    aliases = {"gd": "google-drive"}
    target = aliases.get(target, target)

    if target not in {"s3", "google-drive", "both"}:
        raise ValueError("REPORT_UPLOAD_TARGET must be one of: s3, google-drive, both")

    return target


def kubecost_get(kubecost_url, path, params):
    query = urllib.parse.urlencode(params)
    url = f"{kubecost_url.rstrip('/')}/model/{path}?{query}"
    request = urllib.request.Request(url, headers={"Accept": "application/json"})

    print(f"Querying {url}")
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.loads(response.read().decode("utf-8"))


def accumulated_data(response):
    data = response.get("data", [])
    if not data:
        return {}
    if isinstance(data, list):
        return data[0] or {}
    return data


def cost(value):
    return float(value or 0.0)


def node_asset_key(asset_key, asset):
    properties = asset.get("properties") or {}
    names = {
        asset_key,
        asset.get("key"),
        asset.get("name"),
        asset.get("providerID"),
        properties.get("providerID"),
    }
    return {name for name in names if name}


def index_node_assets(assets):
    node_assets = []
    for asset_key, asset in assets.items():
        if str(asset.get("type", "")).lower() != "node":
            continue

        asset["_asset_key"] = asset_key
        node_assets.append(asset)

    exact_index = {}
    for asset in node_assets:
        for key in node_asset_key(asset["_asset_key"], asset):
            exact_index[key] = asset

    return node_assets, exact_index


def find_node_asset(node_name, node_assets, exact_index):
    if not node_name:
        return None
    if node_name in exact_index:
        return exact_index[node_name]

    for asset in node_assets:
        haystack = " ".join(
            str(value or "") for value in node_asset_key(asset["_asset_key"], asset)
        )
        if node_name in haystack:
            return asset

    return None


def node_instance_type(asset):
    if not asset:
        return ""

    labels = asset.get("labels", {}) or {}
    properties = asset.get("properties", {}) or {}

    return (
        asset.get("nodeType")
        or properties.get("nodeType")
        or properties.get("instanceType")
        or properties.get("instance_type")
        or labels.get("node_kubernetes_io_instance_type")
        or labels.get("label_node_kubernetes_io_instance_type")
        or ""
    )


def provider_id(asset):
    if not asset:
        return ""
    properties = asset.get("properties") or {}
    return asset.get("providerID") or properties.get("providerID", "")


def write_report(
    report_path, allocation, node_assets, node_index, namespace, pod_prefix
):
    rows = []

    for allocation_key, pod_data in sorted(allocation.items()):
        if allocation_key.startswith("__"):
            continue

        properties = pod_data.get("properties", {}) or {}
        pod_namespace = properties.get("namespace", "")
        if pod_namespace != namespace:
            continue

        pod_name = properties.get("pod") or pod_data.get("name") or allocation_key
        if pod_name.startswith("__"):
            continue
        if pod_prefix and not pod_name.startswith(pod_prefix):
            continue

        node_name = properties.get("node", "")
        node_asset = find_node_asset(node_name, node_assets, node_index)

        rows.append(
            {
                "pod_name": pod_name,
                "namespace": pod_namespace,
                "node_name": node_name,
                "pod_allocation_start": pod_data.get("start", ""),
                "pod_allocation_end": pod_data.get("end", ""),
                "pod_runtime_hours": round(cost(pod_data.get("minutes")) / 60, 2),
                "node_start": (node_asset or {}).get("start", ""),
                "node_end": (node_asset or {}).get("end", ""),
                "total_node_runtime_hours": round(
                    cost((node_asset or {}).get("minutes")) / 60, 2
                ),
                "node_instance_type": node_instance_type(node_asset),
                "node_provider_id": provider_id(node_asset),
                "total_instance_cost": round(
                    cost((node_asset or {}).get("totalCost")), 4
                ),
                "pod_total_cost": round(cost(pod_data.get("totalCost")), 4),
            }
        )

    fieldnames = [
        "pod_name",
        "namespace",
        "node_name",
        "pod_allocation_start",
        "pod_allocation_end",
        "pod_runtime_hours",
        "node_start",
        "node_end",
        "total_node_runtime_hours",
        "node_instance_type",
        "node_provider_id",
        "total_instance_cost",
        "pod_total_cost",
    ]

    with open(report_path, "w", newline="") as report:
        writer = csv.DictWriter(report, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} {namespace} pod cost rows to {report_path}")
    return len(rows)


def generate_report(kubecost_url, kubecost_window, report_path, namespace, pod_prefix):
    allocation = accumulated_data(
        kubecost_get(
            kubecost_url,
            "allocation",
            {
                "window": kubecost_window,
                "aggregate": "namespace,pod,node",
                "accumulate": "true",
                "idle": "false",
                "filterNamespaces": namespace,
            },
        )
    )

    assets = accumulated_data(
        kubecost_get(
            kubecost_url,
            "assets",
            {
                "window": kubecost_window,
                "accumulate": "true",
            },
        )
    )

    node_assets, node_index = index_node_assets(assets)
    return write_report(
        report_path, allocation, node_assets, node_index, namespace, pod_prefix
    )


def upload_report_to_s3(report_path, report_name, bucket, prefix, region):
    if not bucket:
        raise ValueError("REPORT_S3_BUCKET is required for S3 upload")

    s3_prefix = prefix.strip("/") if prefix else ""
    s3_key = f"{s3_prefix}/{report_name}" if s3_prefix else report_name
    s3_url = f"s3://{bucket}/{s3_key}"

    s3_client = boto3.client("s3", region_name=region or None)
    s3_client.upload_file(report_path, bucket, s3_key)

    print(f"Uploaded report to {s3_url}")
    return s3_url


def upload_report_to_google_drive(
    report_path, report_name, google_drive_folder_id, google_credentials_file
):
    if not google_drive_folder_id:
        raise ValueError("GOOGLE_DRIVE_FOLDER_ID is required for Google Drive upload")

    if not google_credentials_file:
        raise ValueError(
            "GOOGLE_APPLICATION_CREDENTIALS is required for Google Drive upload"
        )

    credentials_path = Path(google_credentials_file)
    if not credentials_path.exists():
        raise FileNotFoundError(
            f"Google credentials file does not exist: {credentials_path}"
        )

    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=[GOOGLE_DRIVE_SCOPE],
    )
    service = build("drive", "v3", credentials=credentials, cache_discovery=False)

    file_metadata = {
        "name": report_name,
        "parents": [google_drive_folder_id],
    }
    media = MediaFileUpload(report_path, mimetype=CSV_MIME_TYPE, resumable=False)

    uploaded_file = (
        service.files()
        .create(
            body=file_metadata,
            media_body=media,
            fields="id, webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )

    print(f"Uploaded report to Google Drive file id {uploaded_file.get('id')}")
    print(f"Google Drive report link: {uploaded_file.get('webViewLink')}")
    return uploaded_file


def main(
    kubecost_url=DEFAULT_KUBECOST_URL,
    kubecost_window="lastweek",
    report_path=DEFAULT_REPORT_PATH,
    namespace="sandbox",
    pod_prefix="jupyter-",
    environment="",
    upload_target="both",
    s3_bucket="deafrica-cost-usage-reports",
    s3_prefix="sandbox-weekly-report",
    aws_region="af-south-1",
    google_drive_folder_id=None,
    google_credentials_file=None,
):
    environment = report_environment(environment)
    environment_suffix = f"-{environment}" if environment else ""
    report_name = f"sandbox-weekly-cost-report{environment_suffix}-{report_date()}.csv"
    Path(report_path).parent.mkdir(parents=True, exist_ok=True)
    generate_report(kubecost_url, kubecost_window, report_path, namespace, pod_prefix)

    target = report_upload_target(upload_target)
    print(f"Report upload target: {target}")

    if target in {"s3", "both"}:
        upload_report_to_s3(report_path, report_name, s3_bucket, s3_prefix, aws_region)
    else:
        print("Skipping S3 upload")

    if target in {"google-drive", "both"}:
        upload_report_to_google_drive(
            report_path,
            report_name,
            google_drive_folder_id,
            google_credentials_file,
        )
    else:
        print("Skipping Google Drive upload")


@click.command("sandbox-weekly-cost-report")
@click.option("--kubecost-url", envvar="KUBECOST_URL", default=DEFAULT_KUBECOST_URL)
@click.option(
    "--window",
    "kubecost_window",
    envvar="KUBECOST_WINDOW",
    default="lastweek",
)
@click.option("--report-path", envvar="REPORT_PATH", default=DEFAULT_REPORT_PATH)
@click.option("--namespace", envvar="REPORT_NAMESPACE", default="sandbox")
@click.option("--pod-prefix", envvar="REPORT_POD_PREFIX", default="jupyter-")
@click.option("--environment", envvar="REPORT_ENVIRONMENT", default="")
@click.option("--upload-target", envvar="REPORT_UPLOAD_TARGET", default="both")
@click.option(
    "--s3-bucket",
    envvar="REPORT_S3_BUCKET",
    default="deafrica-cost-usage-reports",
)
@click.option("--s3-prefix", envvar="REPORT_PREFIX", default="sandbox-weekly-report")
@click.option("--aws-region", envvar="AWS_DEFAULT_REGION", default="af-south-1")
@click.option("--google-drive-folder-id", envvar="GOOGLE_DRIVE_FOLDER_ID")
@click.option("--google-credentials-file", envvar="GOOGLE_APPLICATION_CREDENTIALS")
def cli(
    kubecost_url,
    kubecost_window,
    report_path,
    namespace,
    pod_prefix,
    environment,
    upload_target,
    s3_bucket,
    s3_prefix,
    aws_region,
    google_drive_folder_id,
    google_credentials_file,
):
    main(
        kubecost_url=kubecost_url,
        kubecost_window=kubecost_window,
        report_path=report_path,
        namespace=namespace,
        pod_prefix=pod_prefix,
        environment=environment,
        upload_target=upload_target,
        s3_bucket=s3_bucket,
        s3_prefix=s3_prefix,
        aws_region=aws_region,
        google_drive_folder_id=google_drive_folder_id,
        google_credentials_file=google_credentials_file,
    )


if __name__ == "__main__":
    cli()
