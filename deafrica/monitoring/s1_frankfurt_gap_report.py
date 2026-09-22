import json
import logging
import sys
from pathlib import Path

import click

from deafrica import __version__
from deafrica.click_options import slack_url
from deafrica.logs import setup_logging
from deafrica.monitoring._s1_frankfurt_gap import (
    DESTINATION_BUCKET,
    DESTINATION_REGION,
    SOURCE_BUCKET,
    SOURCE_REGION,
    build_plan_for_metadata,
    build_plan_for_metadata_without_listing,
    build_report,
    describe_exception,
    discover_metadata_keys,
    parse_date,
    plan_to_dict,
    put_json_object,
    report_key,
    s3_client,
    s3_url,
)
from deafrica.utils import send_slack_notification

log = logging.getLogger(__name__)


@click.command("s1-frankfurt-gap-report", no_args_is_help=True)
@click.version_option(version=__version__)
@click.argument("bucket-name", type=str, nargs=1, required=True)
@click.option("--start-date", type=str, required=True, help="Start date, YYYY-MM-DD.")
@click.option("--end-date", type=str, required=True, help="End date, YYYY-MM-DD.")
@click.option("--source-bucket", default=SOURCE_BUCKET, show_default=True)
@click.option("--destination-bucket", default=DESTINATION_BUCKET, show_default=True)
@click.option("--source-region", default=SOURCE_REGION, show_default=True)
@click.option("--destination-region", default=DESTINATION_REGION, show_default=True)
@click.option("--max-datasets", type=int, default=None)
@click.option("--tile", type=str, default=None, help="Limit discovery to one tile.")
@click.option(
    "--metadata-key",
    type=str,
    default=None,
    help="Check one known source metadata key without listing the source bucket.",
)
@click.option("--output-key", type=str, default=None)
@click.option(
    "--local-output-json",
    type=click.Path(dir_okay=False, path_type=Path),
    default=None,
    help="Write the JSON report to a local file instead of S3.",
)
@slack_url
def cli(
    bucket_name: str,
    start_date: str,
    end_date: str,
    source_bucket: str,
    destination_bucket: str,
    source_region: str,
    destination_region: str,
    max_datasets: int | None,
    tile: str | None,
    metadata_key: str | None,
    output_key: str | None,
    local_output_json: Path | None,
    slack_url: str | None,
) -> None:
    log = setup_logging()

    start = parse_date(start_date)
    end = parse_date(end_date)
    if end < start:
        raise ValueError("--end-date must be on or after --start-date")
    if max_datasets is not None and max_datasets < 1:
        raise ValueError("--max-datasets must be at least 1")

    source_s3 = s3_client(source_region)
    destination_s3 = s3_client(destination_region)

    datasets: list[dict] = []
    complete = True
    error = None
    check_error_count = 0

    try:
        single_metadata_key = metadata_key is not None
        metadata_keys = (
            [metadata_key]
            if single_metadata_key
            else discover_metadata_keys(source_s3, source_bucket, start, end, tile)
        )
        for index, current_metadata_key in enumerate(metadata_keys, start=1):
            if max_datasets and index > max_datasets:
                complete = False
                error = f"max_datasets limit reached: {max_datasets}"
                log.warning("Frankfurt gap report truncated: %s", error)
                break

            try:
                if single_metadata_key:
                    plan = build_plan_for_metadata_without_listing(
                        source_s3=source_s3,
                        destination_s3=destination_s3,
                        source_bucket=source_bucket,
                        destination_bucket=destination_bucket,
                        metadata_key=current_metadata_key,
                    )
                else:
                    plan = build_plan_for_metadata(
                        source_s3=source_s3,
                        destination_s3=destination_s3,
                        source_bucket=source_bucket,
                        destination_bucket=destination_bucket,
                        metadata_key=current_metadata_key,
                    )
            except Exception as exc:
                details = describe_exception(exc)
                if "ExpiredToken" in details:
                    raise
                check_error_count += 1
                log.exception("Check failed for %s", current_metadata_key)
                datasets.append(
                    {
                        "date": "",
                        "tile": "",
                        "datatake": "",
                        "metadata_key": current_metadata_key,
                        "status": "check_error",
                        "dest_metadata_exists": False,
                        "source_complete": False,
                        "source_object_count": 0,
                        "dest_existing_count": 0,
                        "dest_missing_count": 0,
                        "missing_required_source": [],
                        "missing_dest_assets": [],
                        "mismatched_dest_assets": [],
                        "existing_dest_assets": [],
                        "source_prefix": current_metadata_key.rsplit("/", 1)[0] + "/",
                        "check_error": details,
                    }
                )
                continue

            dataset = plan_to_dict(plan)
            datasets.append(dataset)
            log.info(
                "%s missing=%s %s",
                dataset["status"],
                dataset["dest_missing_count"],
                current_metadata_key,
            )
    except Exception as exc:
        complete = False
        error = describe_exception(exc)
        log.exception("Frankfurt gap report ended before completion")

    if check_error_count:
        complete = False
        check_error_message = f"dataset check errors: {check_error_count}"
        error = f"{error}; {check_error_message}" if error else check_error_message

    report = build_report(
        start_date=start_date,
        end_date=end_date,
        source_bucket=source_bucket,
        destination_bucket=destination_bucket,
        datasets=datasets,
        complete=complete,
        error=error,
        scope={
            "tile": tile,
            "metadata_key": metadata_key,
            "single_metadata_key": metadata_key is not None,
        },
    )

    if local_output_json:
        local_output_json.parent.mkdir(parents=True, exist_ok=True)
        local_output_json.write_text(
            json.dumps(report, indent=2) + "\n",
            encoding="utf-8",
        )
        report_url = str(local_output_json)
        log.info("Frankfurt gap report written to %s", report_url)
    else:
        report_s3 = s3_client(destination_region)
        output_key = output_key or report_key(start_date, end_date)
        put_json_object(report_s3, bucket_name, output_key, report)

        report_url = s3_url(bucket_name, output_key)
        log.info("Frankfurt gap report written to %s", report_url)
    log.info("Summary: %s", report["summary"])

    message = (
        f"*SENTINEL 1 FRANKFURT GAP REPORT - PDS*\n"
        f"Source: `{source_bucket}`\n"
        f"Destination: `{destination_bucket}`\n"
        f"Window: `{start_date}` to `{end_date}`\n"
        f"Complete: `{complete}`\n"
        f"Datasets checked: `{len(datasets)}`\n"
        f"Summary: `{report['summary']}`\n"
        f"Report: `{report_url}`\n"
    )
    if error:
        message += f"Error: `{error}`\n"

    if slack_url:
        send_slack_notification(slack_url, "S1 Frankfurt Gap Report", message)
    else:
        log.info(message)

    if not complete:
        sys.exit(1)
