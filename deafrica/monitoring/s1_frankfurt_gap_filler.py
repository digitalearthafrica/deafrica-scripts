import json
import logging
import sys

import boto3
import click

from deafrica import __version__
from deafrica.click_options import limit, slack_url
from deafrica.logs import setup_logging
from deafrica.monitoring._s1_frankfurt_gap import (
    DESTINATION_BUCKET,
    DESTINATION_REGION,
    REPORT_SCHEMA_VERSION,
    REPORT_TYPE,
    SAFE_FILL_STATUSES,
    SOURCE_BUCKET,
    SOURCE_REGION,
    describe_exception,
    fill_dataset,
    read_report,
    s3_client,
    s3_event_message,
)
from deafrica.utils import send_slack_notification, split_list_equally

log = logging.getLogger(__name__)


@click.command("s1-frankfurt-gap-filler", no_args_is_help=True)
@click.argument("worker-idx", type=int, nargs=1, required=True)
@click.argument("max-workers", type=int, nargs=1, required=True)
@click.argument("report-path", type=str, nargs=1, required=True)
@click.option(
    "--sns-topic-arn",
    type=str,
    default=None,
    help="Publish an S3 event message for copied metadata to trigger indexing.",
)
@click.option("--source-bucket", default=SOURCE_BUCKET, show_default=True)
@click.option("--destination-bucket", default=DESTINATION_BUCKET, show_default=True)
@click.option("--source-region", default=SOURCE_REGION, show_default=True)
@click.option("--destination-region", default=DESTINATION_REGION, show_default=True)
@click.option(
    "--allow-incomplete-report",
    is_flag=True,
    default=False,
    help="Allow filling from a report whose complete flag is false.",
)
@click.option(
    "--publish-existing-metadata",
    is_flag=True,
    default=False,
    help="Publish indexing messages for report candidates already complete in PDS.",
)
@click.option("--dryrun", is_flag=True, default=False)
@click.option("--version", is_flag=True, default=False)
@limit
@slack_url
def cli(
    worker_idx: int,
    max_workers: int,
    report_path: str,
    sns_topic_arn: str | None,
    source_bucket: str,
    destination_bucket: str,
    source_region: str,
    destination_region: str,
    allow_incomplete_report: bool,
    publish_existing_metadata: bool,
    dryrun: bool,
    version: bool,
    limit: int | None,
    slack_url: str | None,
) -> None:
    log = setup_logging()

    if version:
        click.echo(__version__)
        sys.exit(0)

    if max_workers < 1:
        raise ValueError("max-workers must be at least 1")
    if worker_idx < 0:
        raise ValueError("worker-idx must be 0 or greater")

    if limit is not None:
        limit = int(limit)
        if limit < 1:
            raise ValueError(f"Limit {limit} lower than 1.")
    if publish_existing_metadata and not sns_topic_arn:
        raise ValueError("--publish-existing-metadata requires --sns-topic-arn")

    report = read_report(report_path, region_name=destination_region)
    if report.get("report_type") != REPORT_TYPE:
        raise RuntimeError(
            f"Refusing to fill from report type {report.get('report_type')}. "
            f"Expected {REPORT_TYPE}."
        )
    if report.get("schema_version") != REPORT_SCHEMA_VERSION:
        raise RuntimeError(
            f"Refusing to fill from report schema {report.get('schema_version')}. "
            f"Expected {REPORT_SCHEMA_VERSION}."
        )

    if report.get("complete") is not True and not allow_incomplete_report:
        raise RuntimeError(
            "Refusing to fill from an incomplete report. "
            "Re-run the report, or pass --allow-incomplete-report intentionally."
        )

    report_source = report.get("source_bucket")
    report_destination = report.get("destination_bucket")
    if report_source and report_source != source_bucket:
        raise RuntimeError(
            f"Report source bucket {report_source} does not match {source_bucket}"
        )
    if report_destination and report_destination != destination_bucket:
        raise RuntimeError(
            "Report destination bucket "
            f"{report_destination} does not match {destination_bucket}"
        )

    report_fillable_statuses = set(report.get("fillable_statuses") or [])
    fillable_statuses = SAFE_FILL_STATUSES & report_fillable_statuses
    candidates = [
        item["metadata_key"]
        for item in report.get("datasets", [])
        if item.get("status") in fillable_statuses
    ]
    if limit:
        candidates = candidates[:limit]

    if not candidates:
        log.info("No fillable Frankfurt gap datasets found in report")
        return

    chunks = split_list_equally(candidates, max_workers)
    if len(chunks) <= worker_idx:
        log.warning("Worker %s skipped; no work assigned", worker_idx)
        return

    tasks = chunks[worker_idx]
    log.info("Report: %s", report_path)
    log.info("Dry run: %s", dryrun)
    log.info("Fillable statuses: %s", sorted(fillable_statuses))
    log.info("Total fill candidates: %s", len(candidates))
    log.info("Worker %s processing %s datasets", worker_idx, len(tasks))

    source_s3 = s3_client(source_region)
    destination_s3 = s3_client(destination_region)
    sns_client = (
        boto3.client("sns", region_name=destination_region)
        if sns_topic_arn and not dryrun
        else None
    )
    results: dict[str, int] = {}
    copy_failures: list[str] = []
    indexing_failures: list[str] = []
    copied_objects = 0
    datasets_copied = 0
    datasets_skipped = 0
    indexing_messages = 0
    indexing_messages_planned = 0

    for metadata_key in tasks:
        try:
            status, count = fill_dataset(
                source_s3=source_s3,
                destination_s3=destination_s3,
                source_bucket=source_bucket,
                destination_bucket=destination_bucket,
                metadata_key=metadata_key,
                dryrun=dryrun,
            )
        except Exception as exc:
            details = describe_exception(exc)
            copy_failures.append(f"{metadata_key}: {details}")
            log.exception("Failed to fill %s", metadata_key)
            continue

        results[status] = results.get(status, 0) + 1
        copied_objects += count
        if status in {"copied_metadata_last", "dryrun_would_copy_metadata_last"}:
            datasets_copied += 1
        elif status.startswith("skipped_"):
            datasets_skipped += 1
        log.info("%s objects=%s %s", status, count, metadata_key)

        should_publish = sns_topic_arn and (
            status == "copied_metadata_last"
            or (
                publish_existing_metadata
                and status == "skipped_status_complete_in_dest"
            )
        )
        if should_publish:
            if dryrun:
                indexing_messages_planned += 1
                continue
            try:
                publish_indexing_message(
                    sns_client=sns_client,
                    sns_topic_arn=sns_topic_arn,
                    destination_bucket=destination_bucket,
                    metadata_key=metadata_key,
                )
                indexing_messages += 1
            except Exception as exc:
                details = describe_exception(exc)
                indexing_failures.append(f"{metadata_key}: {details}")
                log.exception("Failed to publish indexing message for %s", metadata_key)

    message = (
        f"*SENTINEL 1 FRANKFURT GAP FILLER - PDS*\n"
        f"Report: `{report_path}`\n"
        f"Worker: `{worker_idx}/{max_workers}`\n"
        f"Dry run: `{dryrun}`\n"
        f"Datasets attempted: `{len(tasks)}`\n"
        f"Datasets copied/planned: `{datasets_copied}`\n"
        f"Datasets skipped: `{datasets_skipped}`\n"
        f"Objects copied/planned: `{copied_objects}`\n"
        f"Indexing messages published: `{indexing_messages}`\n"
        f"Indexing messages planned: `{indexing_messages_planned}`\n"
        f"Results: `{results}`\n"
        f"Copy failures: `{len(copy_failures)}`\n"
        f"Indexing failures: `{len(indexing_failures)}`\n"
    )
    if copy_failures:
        message += f"Example copy failures: `{copy_failures[:5]}`\n"
    if indexing_failures:
        message += f"Example indexing failures: `{indexing_failures[:5]}`\n"

    if slack_url:
        send_slack_notification(slack_url, "S1 Frankfurt Gap Filler", message)
    else:
        log.info(message)

    if copy_failures or indexing_failures:
        raise RuntimeError("Some Frankfurt gap filler tasks failed")


def publish_indexing_message(
    sns_client,
    sns_topic_arn: str,
    destination_bucket: str,
    metadata_key: str,
) -> None:
    message = s3_event_message(destination_bucket, metadata_key)
    sns_client.publish(TopicArn=sns_topic_arn, Message=json.dumps(message))
