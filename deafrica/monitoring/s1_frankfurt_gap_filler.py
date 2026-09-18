import logging
import sys

import click

from deafrica import __version__
from deafrica.click_options import limit, slack_url
from deafrica.logs import setup_logging
from deafrica.monitoring._s1_frankfurt_gap import (
    DESTINATION_BUCKET,
    DESTINATION_REGION,
    REPORT_TYPE,
    SAFE_FILL_STATUSES,
    SOURCE_BUCKET,
    SOURCE_REGION,
    describe_exception,
    fill_dataset,
    read_report,
    s3_client,
)
from deafrica.utils import send_slack_notification, split_list_equally

log = logging.getLogger(__name__)


@click.command("s1-frankfurt-gap-filler", no_args_is_help=True)
@click.argument("worker-idx", type=int, nargs=1, required=True)
@click.argument("max-workers", type=int, nargs=1, required=True)
@click.argument("report-path", type=str, nargs=1, required=True)
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
@click.option("--dryrun", is_flag=True, default=False)
@click.option("--version", is_flag=True, default=False)
@limit
@slack_url
def cli(
    worker_idx: int,
    max_workers: int,
    report_path: str,
    source_bucket: str,
    destination_bucket: str,
    source_region: str,
    destination_region: str,
    allow_incomplete_report: bool,
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

    report = read_report(report_path, region_name=destination_region)
    if report.get("report_type") != REPORT_TYPE:
        raise RuntimeError(
            f"Refusing to fill from report type {report.get('report_type')}. "
            f"Expected {REPORT_TYPE}."
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

    candidates = [
        item["metadata_key"]
        for item in report.get("datasets", [])
        if item.get("status") in SAFE_FILL_STATUSES
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
    log.info("Fillable statuses: %s", sorted(SAFE_FILL_STATUSES))
    log.info("Total fill candidates: %s", len(candidates))
    log.info("Worker %s processing %s datasets", worker_idx, len(tasks))

    source_s3 = s3_client(source_region)
    destination_s3 = s3_client(destination_region)
    results: dict[str, int] = {}
    failures: list[str] = []
    copied_objects = 0

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
            failures.append(f"{metadata_key}: {details}")
            log.exception("Failed to fill %s", metadata_key)
            continue

        results[status] = results.get(status, 0) + 1
        copied_objects += count
        log.info("%s objects=%s %s", status, count, metadata_key)

    message = (
        f"*SENTINEL 1 FRANKFURT GAP FILLER - PDS*\n"
        f"Report: `{report_path}`\n"
        f"Worker: `{worker_idx}/{max_workers}`\n"
        f"Dry run: `{dryrun}`\n"
        f"Datasets attempted: `{len(tasks)}`\n"
        f"Objects copied/planned: `{copied_objects}`\n"
        f"Results: `{results}`\n"
        f"Failures: `{len(failures)}`\n"
    )
    if failures:
        message += f"Example failures: `{failures[:5]}`\n"

    if slack_url:
        send_slack_notification(slack_url, "S1 Frankfurt Gap Filler", message)
    else:
        log.info(message)

    if failures:
        raise RuntimeError("Some Frankfurt gap filler tasks failed")
