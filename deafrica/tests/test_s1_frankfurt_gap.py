import json
from io import BytesIO

import boto3
from botocore.exceptions import ClientError
from click.testing import CliRunner
from moto import mock_s3

from deafrica import __version__
from deafrica.monitoring._s1_frankfurt_gap import (
    REPORT_SCHEMA_VERSION,
    REPORT_TYPE,
    REQUIRED_SUFFIXES,
    build_plan_for_metadata_without_listing,
    fill_dataset,
    s3_event_message,
)
from deafrica.monitoring import (
    s1_frankfurt_gap_filler,
    s1_frankfurt_gap_report,
)

SOURCE_BUCKET = "source-bucket"
DESTINATION_BUCKET = "destination-bucket"
REPORT_BUCKET = "report-bucket"
REGION = "us-east-1"
PREFIX = "s1_rtc/N00E005/2026/06/23/0834AD/"
BASE = f"{PREFIX}s1_rtc_0834AD_N00E005_2026_06_23"
METADATA_KEY = f"{BASE}_metadata.json"


def test_report_version_does_not_require_arguments():
    result = CliRunner().invoke(s1_frankfurt_gap_report.cli, ["--version"])

    assert result.exit_code == 0
    assert __version__ in result.output


def test_filler_version_does_not_require_arguments():
    result = CliRunner().invoke(s1_frankfurt_gap_filler.cli, ["--version"])

    assert result.exit_code == 0
    assert __version__ in result.output


class FakeS3Client:
    def __init__(self, bodies=None):
        self.bodies = bodies or {}
        self.uploads = []

    def head_object(self, Bucket, Key):
        if Key not in self.bodies:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": len(self.bodies[Key])}

    def get_object(self, Bucket, Key):
        if Key not in self.bodies:
            raise ClientError({"Error": {"Code": "404"}}, "GetObject")
        return {
            "Body": BytesIO(self.bodies[Key]),
            "ContentType": (
                "application/json" if Key.endswith(".json") else "image/tiff"
            ),
        }

    def upload_fileobj(self, body, Bucket, Key, ExtraArgs=None):
        self.bodies[Key] = body.read()
        self.uploads.append(Key)

    def get_paginator(self, operation_name):
        return FakePaginator(self.bodies)


class FakePaginator:
    def __init__(self, bodies):
        self.bodies = bodies

    def paginate(self, Bucket, Prefix):
        return [
            {
                "Contents": [
                    {"Key": key, "Size": len(body)}
                    for key, body in self.bodies.items()
                    if key.startswith(Prefix)
                ]
            }
        ]


class FakeSNSClient:
    def __init__(self):
        self.published = []

    def publish(self, TopicArn, Message):
        self.published.append({"TopicArn": TopicArn, "Message": Message})
        return {"ResponseMetadata": {"RequestId": "request-id"}}


class AssertingCompleteDestinationSNSClient(FakeSNSClient):
    def __init__(self, source, destination):
        super().__init__()
        self.source = source
        self.destination = destination

    def publish(self, TopicArn, Message):
        for key, body in self.source.bodies.items():
            assert self.destination.bodies[key] == body
        return super().publish(TopicArn, Message)


class FailingSNSClient(FakeSNSClient):
    def publish(self, TopicArn, Message):
        self.published.append({"TopicArn": TopicArn, "Message": Message})
        raise ClientError({"Error": {"Code": "InternalError"}}, "Publish")


class FailingUploadS3Client(FakeS3Client):
    def __init__(self, bodies=None, fail_key=None):
        super().__init__(bodies)
        self.fail_key = fail_key

    def upload_fileobj(self, body, Bucket, Key, ExtraArgs=None):
        if Key == self.fail_key:
            raise ClientError({"Error": {"Code": "InternalError"}}, "Upload")
        return super().upload_fileobj(body, Bucket, Key, ExtraArgs=ExtraArgs)


def asset_key(suffix):
    return f"{BASE}{suffix}"


def stac_body():
    assets = {
        suffix: {"href": asset_key(suffix)}
        for suffix in REQUIRED_SUFFIXES
        if suffix != "_metadata.json"
    }
    return json.dumps({"assets": assets}).encode("utf-8")


def dataset_bodies(omit=()):
    omit = set(omit)
    bodies = {}
    if METADATA_KEY not in omit:
        bodies[METADATA_KEY] = stac_body()
    for suffix in REQUIRED_SUFFIXES:
        key = asset_key(suffix)
        if key == METADATA_KEY or key in omit:
            continue
        bodies[key] = f"source-body-{suffix}".encode("utf-8")
    return bodies


def put_dataset(client, bucket, suffix, omit=()):
    prefix = f"s1_rtc/N00E005/2026/06/2{suffix}/0834A{suffix}/"
    base = f"{prefix}s1_rtc_0834A{suffix}_N00E005_2026_06_2{suffix}"
    metadata_key = f"{base}_metadata.json"
    assets = {
        required_suffix: {"href": f"{base}{required_suffix}"}
        for required_suffix in REQUIRED_SUFFIXES
        if required_suffix != "_metadata.json"
    }
    omit = set(omit)
    if metadata_key not in omit:
        client.put_object(
            Bucket=bucket, Key=metadata_key, Body=json.dumps({"assets": assets})
        )
    for required_suffix in REQUIRED_SUFFIXES:
        key = f"{base}{required_suffix}"
        if key == metadata_key or key in omit:
            continue
        client.put_object(Bucket=bucket, Key=key, Body=f"body-{required_suffix}")
    return metadata_key


def create_bucket(client, bucket):
    client.create_bucket(Bucket=bucket)


def test_dest_metadata_with_missing_asset_is_not_complete():
    source = FakeS3Client(dataset_bodies())
    destination_bodies = dataset_bodies()
    destination_bodies.pop(asset_key("_VV.tif"))
    destination = FakeS3Client(destination_bodies)

    plan = build_plan_for_metadata_without_listing(
        source, destination, SOURCE_BUCKET, DESTINATION_BUCKET, METADATA_KEY
    )

    assert plan.status == "dest_metadata_present_assets_missing"
    assert asset_key("_VV.tif") in plan.missing_dest_assets


def test_missing_source_required_object_is_source_incomplete():
    missing_key = asset_key("_VH.tif")
    source = FakeS3Client(dataset_bodies(omit={missing_key}))
    destination = FakeS3Client({})

    plan = build_plan_for_metadata_without_listing(
        source, destination, SOURCE_BUCKET, DESTINATION_BUCKET, METADATA_KEY
    )

    assert plan.status == "source_incomplete"
    assert missing_key in plan.missing_required_source


def test_destination_size_mismatch_is_not_treated_as_present():
    source = FakeS3Client(dataset_bodies())
    destination_bodies = dataset_bodies(omit={METADATA_KEY})
    destination_bodies[asset_key("_VV.tif")] = b"short"
    destination = FakeS3Client(destination_bodies)

    plan = build_plan_for_metadata_without_listing(
        source, destination, SOURCE_BUCKET, DESTINATION_BUCKET, METADATA_KEY
    )

    assert plan.status == "metadata_missing_some_assets_missing"
    assert asset_key("_VV.tif") in plan.mismatched_dest_assets
    assert asset_key("_VV.tif") not in plan.existing_dest_assets


def test_destination_prefix_listing_finds_missing_and_mismatched_assets():
    source = FakeS3Client(dataset_bodies())
    destination_bodies = dataset_bodies()
    destination_bodies.pop(asset_key("_VH.tif"))
    destination_bodies[asset_key("_VV.tif")] = b"short"
    destination = FakeS3Client(destination_bodies)

    plan = build_plan_for_metadata_without_listing(
        source, destination, SOURCE_BUCKET, DESTINATION_BUCKET, METADATA_KEY
    )

    assert plan.status == "dest_metadata_present_assets_missing"
    assert asset_key("_VH.tif") in plan.missing_dest_assets
    assert asset_key("_VV.tif") in plan.mismatched_dest_assets


def test_mismatched_metadata_is_counted_once_as_existing():
    source = FakeS3Client(dataset_bodies())
    destination_bodies = dataset_bodies()
    destination_bodies[METADATA_KEY] = b"short"
    destination = FakeS3Client(destination_bodies)

    plan = build_plan_for_metadata_without_listing(
        source, destination, SOURCE_BUCKET, DESTINATION_BUCKET, METADATA_KEY
    )

    assert plan.status == "dest_metadata_present_assets_missing"
    assert METADATA_KEY in plan.mismatched_dest_assets
    assert plan.dest_existing_count == len(REQUIRED_SUFFIXES)


def test_fill_repairs_mismatch_and_copies_metadata_last():
    source_bodies = dataset_bodies()
    destination_bodies = {
        key: value
        for key, value in source_bodies.items()
        if key not in {METADATA_KEY, asset_key("_VV.tif")}
    }
    destination_bodies[asset_key("_VV.tif")] = b"short"
    source = FakeS3Client(source_bodies)
    destination = FakeS3Client(destination_bodies)

    status, copied = fill_dataset(
        source, destination, SOURCE_BUCKET, DESTINATION_BUCKET, METADATA_KEY, False
    )

    assert status == "copied_metadata_last"
    assert copied == 2
    assert destination.uploads[-1] == METADATA_KEY
    assert (
        destination.bodies[asset_key("_VV.tif")] == source_bodies[asset_key("_VV.tif")]
    )


def test_fill_dryrun_copies_nothing():
    source = FakeS3Client(dataset_bodies())
    destination = FakeS3Client({})

    status, copied = fill_dataset(
        source, destination, SOURCE_BUCKET, DESTINATION_BUCKET, METADATA_KEY, True
    )

    assert status == "dryrun_would_copy_metadata_last"
    assert copied == len(REQUIRED_SUFFIXES)
    assert destination.uploads == []
    assert destination.bodies == {}


@mock_s3
def test_max_datasets_marks_report_incomplete(tmp_path):
    client = boto3.client("s3", region_name=REGION)
    for bucket in [SOURCE_BUCKET, DESTINATION_BUCKET, REPORT_BUCKET]:
        create_bucket(client, bucket)
    put_dataset(client, SOURCE_BUCKET, "3")
    put_dataset(client, SOURCE_BUCKET, "4")

    output = tmp_path / "report.json"
    result = CliRunner().invoke(
        s1_frankfurt_gap_report.cli,
        [
            REPORT_BUCKET,
            "--start-date",
            "2026-06-23",
            "--end-date",
            "2026-06-24",
            "--source-bucket",
            SOURCE_BUCKET,
            "--destination-bucket",
            DESTINATION_BUCKET,
            "--source-region",
            REGION,
            "--destination-region",
            REGION,
            "--max-datasets",
            "1",
            "--local-output-json",
            str(output),
        ],
    )

    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit)
    assert result.exception.code == 1
    report = json.loads(output.read_text())
    assert report["complete"] is False
    assert report["error"] == "max_datasets limit reached: 1"
    assert report["total_datasets_checked"] == 1


def test_report_rejects_nonpositive_max_datasets(tmp_path):
    result = CliRunner().invoke(
        s1_frankfurt_gap_report.cli,
        [
            REPORT_BUCKET,
            "--start-date",
            "2026-06-23",
            "--end-date",
            "2026-06-23",
            "--max-datasets",
            "0",
            "--local-output-json",
            str(tmp_path / "report.json"),
        ],
    )

    assert result.exit_code != 0
    assert "--max-datasets must be at least 1" in str(result.exception)


@mock_s3
def test_report_tile_filter_limits_discovery(tmp_path):
    client = boto3.client("s3", region_name=REGION)
    for bucket in [SOURCE_BUCKET, DESTINATION_BUCKET, REPORT_BUCKET]:
        create_bucket(client, bucket)
    included = put_dataset(client, SOURCE_BUCKET, "3")
    other_prefix = "s1_rtc/S99E099/2026/06/23/0834AZ/"
    other_base = f"{other_prefix}s1_rtc_0834AZ_S99E099_2026_06_23"
    other_metadata = f"{other_base}_metadata.json"
    other_assets = {
        suffix: {"href": f"{other_base}{suffix}"}
        for suffix in REQUIRED_SUFFIXES
        if suffix != "_metadata.json"
    }
    client.put_object(
        Bucket=SOURCE_BUCKET,
        Key=other_metadata,
        Body=json.dumps({"assets": other_assets}),
    )
    for suffix in REQUIRED_SUFFIXES:
        key = f"{other_base}{suffix}"
        if key != other_metadata:
            client.put_object(Bucket=SOURCE_BUCKET, Key=key, Body=f"body-{suffix}")

    output = tmp_path / "report.json"
    result = CliRunner().invoke(
        s1_frankfurt_gap_report.cli,
        [
            REPORT_BUCKET,
            "--start-date",
            "2026-06-23",
            "--end-date",
            "2026-06-23",
            "--tile",
            "N00E005",
            "--source-bucket",
            SOURCE_BUCKET,
            "--destination-bucket",
            DESTINATION_BUCKET,
            "--source-region",
            REGION,
            "--destination-region",
            REGION,
            "--local-output-json",
            str(output),
        ],
    )

    assert result.exit_code == 0
    report = json.loads(output.read_text())
    assert [item["metadata_key"] for item in report["datasets"]] == [included]
    assert output.exists()
    assert not output.with_suffix(".csv").exists()
    assert report["scope"] == {
        "tile": "N00E005",
        "metadata_key": None,
        "single_metadata_key": False,
    }


@mock_s3
def test_check_error_marks_report_incomplete(tmp_path):
    client = boto3.client("s3", region_name=REGION)
    for bucket in [SOURCE_BUCKET, DESTINATION_BUCKET, REPORT_BUCKET]:
        create_bucket(client, bucket)

    output = tmp_path / "report.json"
    result = CliRunner().invoke(
        s1_frankfurt_gap_report.cli,
        [
            REPORT_BUCKET,
            "--start-date",
            "2026-06-23",
            "--end-date",
            "2026-06-23",
            "--metadata-key",
            METADATA_KEY,
            "--source-bucket",
            SOURCE_BUCKET,
            "--destination-bucket",
            DESTINATION_BUCKET,
            "--source-region",
            REGION,
            "--destination-region",
            REGION,
            "--local-output-json",
            str(output),
        ],
    )

    assert result.exit_code == 1
    assert isinstance(result.exception, SystemExit)
    assert result.exception.code == 1
    report = json.loads(output.read_text())
    assert report["complete"] is False
    assert report["check_error_count"] == 1
    assert report["error"] == "dataset check errors: 1"
    assert report["scope"] == {
        "tile": None,
        "metadata_key": METADATA_KEY,
        "single_metadata_key": True,
    }


def test_filler_refuses_unknown_schema(tmp_path):
    report = {
        "report_type": REPORT_TYPE,
        "schema_version": REPORT_SCHEMA_VERSION + 1,
        "complete": True,
        "source_bucket": SOURCE_BUCKET,
        "destination_bucket": DESTINATION_BUCKET,
        "fillable_statuses": ["missing_everything_in_dest"],
        "datasets": [],
    }
    report_path = tmp_path / "report.json"
    report_path.write_text(json.dumps(report))

    result = CliRunner().invoke(
        s1_frankfurt_gap_filler.cli,
        ["0", "1", str(report_path), "--dryrun"],
    )

    assert result.exit_code != 0
    assert "Refusing to fill from report schema" in str(result.exception)


def test_filler_refuses_incomplete_report(tmp_path):
    report = {
        "report_type": REPORT_TYPE,
        "schema_version": REPORT_SCHEMA_VERSION,
        "complete": False,
        "source_bucket": SOURCE_BUCKET,
        "destination_bucket": DESTINATION_BUCKET,
        "fillable_statuses": ["missing_everything_in_dest"],
        "datasets": [],
    }
    report_path = tmp_path / "report.json"
    report_path.write_text(json.dumps(report))

    result = CliRunner().invoke(
        s1_frankfurt_gap_filler.cli,
        ["0", "1", str(report_path), "--dryrun"],
    )

    assert result.exit_code != 0
    assert "Refusing to fill from an incomplete report" in str(result.exception)


def test_filler_rejects_worker_index_outside_worker_range(tmp_path):
    result = CliRunner().invoke(
        s1_frankfurt_gap_filler.cli,
        ["1", "1", str(tmp_path / "report.json"), "--dryrun"],
    )

    assert result.exit_code != 0
    assert "worker-idx must be less than max-workers" in str(result.exception)


def test_filler_allows_valid_worker_with_no_assigned_work(tmp_path, monkeypatch):
    report = {
        "report_type": REPORT_TYPE,
        "schema_version": REPORT_SCHEMA_VERSION,
        "complete": True,
        "source_bucket": SOURCE_BUCKET,
        "destination_bucket": DESTINATION_BUCKET,
        "fillable_statuses": ["missing_everything_in_dest"],
        "datasets": [
            {
                "metadata_key": METADATA_KEY,
                "status": "missing_everything_in_dest",
            }
        ],
    }
    report_path = tmp_path / "report.json"
    report_path.write_text(json.dumps(report))
    monkeypatch.setattr(
        s1_frankfurt_gap_filler,
        "s3_client",
        lambda region_name: (_ for _ in ()).throw(
            AssertionError("Worker with no assigned work should not create S3 clients")
        ),
    )

    result = CliRunner().invoke(
        s1_frankfurt_gap_filler.cli,
        [
            "1",
            "5",
            str(report_path),
            "--source-bucket",
            SOURCE_BUCKET,
            "--destination-bucket",
            DESTINATION_BUCKET,
            "--dryrun",
        ],
    )

    assert result.exit_code == 0


def test_s3_event_message_matches_s1_gap_filler_shape():
    assert s3_event_message(DESTINATION_BUCKET, METADATA_KEY) == {
        "Records": [
            {
                "s3": {
                    "bucket": {"name": DESTINATION_BUCKET},
                    "object": {"key": METADATA_KEY},
                }
            }
        ]
    }


def test_filler_publishes_indexing_message_after_copy(tmp_path, monkeypatch):
    source = FakeS3Client(dataset_bodies())
    destination = FakeS3Client({})
    sns = AssertingCompleteDestinationSNSClient(source, destination)
    report = {
        "report_type": REPORT_TYPE,
        "schema_version": REPORT_SCHEMA_VERSION,
        "complete": True,
        "source_bucket": SOURCE_BUCKET,
        "destination_bucket": DESTINATION_BUCKET,
        "fillable_statuses": ["missing_everything_in_dest"],
        "datasets": [
            {
                "metadata_key": METADATA_KEY,
                "status": "missing_everything_in_dest",
            }
        ],
    }
    report_path = tmp_path / "report.json"
    report_path.write_text(json.dumps(report))

    monkeypatch.setattr(
        s1_frankfurt_gap_filler,
        "s3_client",
        lambda region_name: source if region_name == "source-region" else destination,
    )
    monkeypatch.setattr(
        s1_frankfurt_gap_filler.boto3,
        "client",
        lambda service_name, region_name=None: sns,
    )

    result = CliRunner().invoke(
        s1_frankfurt_gap_filler.cli,
        [
            "0",
            "1",
            str(report_path),
            "--sns-topic-arn",
            "arn:aws:sns:af-south-1:123:index",
            "--source-bucket",
            SOURCE_BUCKET,
            "--destination-bucket",
            DESTINATION_BUCKET,
            "--source-region",
            "source-region",
            "--destination-region",
            "destination-region",
        ],
    )

    assert result.exit_code == 0
    assert len(sns.published) == 1
    assert sns.published[0]["TopicArn"] == "arn:aws:sns:af-south-1:123:index"
    assert json.loads(sns.published[0]["Message"]) == s3_event_message(
        DESTINATION_BUCKET, METADATA_KEY
    )


def test_filler_reports_indexing_failure_after_successful_copy(tmp_path, monkeypatch):
    source = FakeS3Client(dataset_bodies())
    destination = FakeS3Client({})
    sns = FailingSNSClient()
    slack_messages = []
    report = {
        "report_type": REPORT_TYPE,
        "schema_version": REPORT_SCHEMA_VERSION,
        "complete": True,
        "source_bucket": SOURCE_BUCKET,
        "destination_bucket": DESTINATION_BUCKET,
        "fillable_statuses": ["missing_everything_in_dest"],
        "datasets": [
            {
                "metadata_key": METADATA_KEY,
                "status": "missing_everything_in_dest",
            }
        ],
    }
    report_path = tmp_path / "report.json"
    report_path.write_text(json.dumps(report))

    monkeypatch.setattr(
        s1_frankfurt_gap_filler,
        "s3_client",
        lambda region_name: source if region_name == "source-region" else destination,
    )
    monkeypatch.setattr(
        s1_frankfurt_gap_filler.boto3,
        "client",
        lambda service_name, region_name=None: sns,
    )
    monkeypatch.setattr(
        s1_frankfurt_gap_filler,
        "send_slack_notification",
        lambda url, title, message: slack_messages.append(message),
    )

    result = CliRunner().invoke(
        s1_frankfurt_gap_filler.cli,
        [
            "0",
            "1",
            str(report_path),
            "--sns-topic-arn",
            "arn:aws:sns:af-south-1:123:index",
            "--source-bucket",
            SOURCE_BUCKET,
            "--destination-bucket",
            DESTINATION_BUCKET,
            "--source-region",
            "source-region",
            "--destination-region",
            "destination-region",
            "--slack_url",
            "http://example.com/slack",
        ],
    )

    assert result.exit_code != 0
    assert isinstance(result.exception, RuntimeError)
    assert str(result.exception) == "Some Frankfurt gap filler tasks failed"
    assert destination.bodies[METADATA_KEY] == source.bodies[METADATA_KEY]
    assert destination.uploads[-1] == METADATA_KEY
    assert len(sns.published) == 1
    assert "Copy failures: `0`" in slack_messages[0]
    assert "Indexing failures: `1`" in slack_messages[0]


def test_filler_does_not_publish_or_copy_metadata_after_asset_failure(
    tmp_path, monkeypatch
):
    source = FakeS3Client(dataset_bodies())
    destination = FailingUploadS3Client({}, fail_key=asset_key("_VV.tif"))
    sns = FakeSNSClient()
    slack_messages = []
    report = {
        "report_type": REPORT_TYPE,
        "schema_version": REPORT_SCHEMA_VERSION,
        "complete": True,
        "source_bucket": SOURCE_BUCKET,
        "destination_bucket": DESTINATION_BUCKET,
        "fillable_statuses": ["missing_everything_in_dest"],
        "datasets": [
            {
                "metadata_key": METADATA_KEY,
                "status": "missing_everything_in_dest",
            }
        ],
    }
    report_path = tmp_path / "report.json"
    report_path.write_text(json.dumps(report))

    monkeypatch.setattr(
        s1_frankfurt_gap_filler,
        "s3_client",
        lambda region_name: source if region_name == "source-region" else destination,
    )
    monkeypatch.setattr(
        s1_frankfurt_gap_filler.boto3,
        "client",
        lambda service_name, region_name=None: sns,
    )
    monkeypatch.setattr(
        s1_frankfurt_gap_filler,
        "send_slack_notification",
        lambda url, title, message: slack_messages.append(message),
    )

    result = CliRunner().invoke(
        s1_frankfurt_gap_filler.cli,
        [
            "0",
            "1",
            str(report_path),
            "--sns-topic-arn",
            "arn:aws:sns:af-south-1:123:index",
            "--source-bucket",
            SOURCE_BUCKET,
            "--destination-bucket",
            DESTINATION_BUCKET,
            "--source-region",
            "source-region",
            "--destination-region",
            "destination-region",
            "--slack_url",
            "http://example.com/slack",
        ],
    )

    assert result.exit_code != 0
    assert isinstance(result.exception, RuntimeError)
    assert str(result.exception) == "Some Frankfurt gap filler tasks failed"
    assert METADATA_KEY not in destination.bodies
    assert METADATA_KEY not in destination.uploads
    assert sns.published == []
    assert "Copy failures: `1`" in slack_messages[0]
    assert "Indexing failures: `0`" in slack_messages[0]


def test_filler_publishes_existing_complete_retry(tmp_path, monkeypatch):
    source = FakeS3Client(dataset_bodies())
    destination = FakeS3Client(dataset_bodies())
    sns = FakeSNSClient()
    report = {
        "report_type": REPORT_TYPE,
        "schema_version": REPORT_SCHEMA_VERSION,
        "complete": True,
        "source_bucket": SOURCE_BUCKET,
        "destination_bucket": DESTINATION_BUCKET,
        "fillable_statuses": ["missing_everything_in_dest"],
        "datasets": [
            {
                "metadata_key": METADATA_KEY,
                "status": "missing_everything_in_dest",
            }
        ],
    }
    report_path = tmp_path / "report.json"
    report_path.write_text(json.dumps(report))

    monkeypatch.setattr(
        s1_frankfurt_gap_filler,
        "s3_client",
        lambda region_name: source if region_name == "source-region" else destination,
    )
    monkeypatch.setattr(
        s1_frankfurt_gap_filler.boto3,
        "client",
        lambda service_name, region_name=None: sns,
    )

    result = CliRunner().invoke(
        s1_frankfurt_gap_filler.cli,
        [
            "0",
            "1",
            str(report_path),
            "--sns-topic-arn",
            "arn:aws:sns:af-south-1:123:index",
            "--source-bucket",
            SOURCE_BUCKET,
            "--destination-bucket",
            DESTINATION_BUCKET,
            "--source-region",
            "source-region",
            "--destination-region",
            "destination-region",
        ],
    )

    assert result.exit_code == 0
    assert destination.uploads == []
    assert len(sns.published) == 1
    assert json.loads(sns.published[0]["Message"]) == s3_event_message(
        DESTINATION_BUCKET, METADATA_KEY
    )


def test_filler_can_publish_existing_complete_metadata_from_report(
    tmp_path, monkeypatch
):
    source = FakeS3Client(dataset_bodies())
    destination = FakeS3Client(dataset_bodies())
    sns = FakeSNSClient()
    report = {
        "report_type": REPORT_TYPE,
        "schema_version": REPORT_SCHEMA_VERSION,
        "complete": True,
        "source_bucket": SOURCE_BUCKET,
        "destination_bucket": DESTINATION_BUCKET,
        "fillable_statuses": ["missing_everything_in_dest"],
        "datasets": [
            {
                "metadata_key": METADATA_KEY,
                "status": "complete_in_dest",
            }
        ],
    }
    report_path = tmp_path / "report.json"
    report_path.write_text(json.dumps(report))

    monkeypatch.setattr(
        s1_frankfurt_gap_filler,
        "s3_client",
        lambda region_name: source if region_name == "source-region" else destination,
    )
    monkeypatch.setattr(
        s1_frankfurt_gap_filler.boto3,
        "client",
        lambda service_name, region_name=None: sns,
    )

    result = CliRunner().invoke(
        s1_frankfurt_gap_filler.cli,
        [
            "0",
            "1",
            str(report_path),
            "--sns-topic-arn",
            "arn:aws:sns:af-south-1:123:index",
            "--publish-existing-metadata",
            "--source-bucket",
            SOURCE_BUCKET,
            "--destination-bucket",
            DESTINATION_BUCKET,
            "--source-region",
            "source-region",
            "--destination-region",
            "destination-region",
        ],
    )

    assert result.exit_code == 0
    assert destination.uploads == []
    assert len(sns.published) == 1
    assert json.loads(sns.published[0]["Message"]) == s3_event_message(
        DESTINATION_BUCKET, METADATA_KEY
    )


def test_filler_dryrun_publishes_nothing(tmp_path, monkeypatch):
    source = FakeS3Client(dataset_bodies())
    destination = FakeS3Client({})
    slack_messages = []
    report = {
        "report_type": REPORT_TYPE,
        "schema_version": REPORT_SCHEMA_VERSION,
        "complete": True,
        "source_bucket": SOURCE_BUCKET,
        "destination_bucket": DESTINATION_BUCKET,
        "fillable_statuses": ["missing_everything_in_dest"],
        "datasets": [
            {
                "metadata_key": METADATA_KEY,
                "status": "missing_everything_in_dest",
            }
        ],
    }
    report_path = tmp_path / "report.json"
    report_path.write_text(json.dumps(report))

    monkeypatch.setattr(
        s1_frankfurt_gap_filler,
        "s3_client",
        lambda region_name: source if region_name == "source-region" else destination,
    )
    monkeypatch.setattr(
        s1_frankfurt_gap_filler.boto3,
        "client",
        lambda service_name, region_name=None: (_ for _ in ()).throw(
            AssertionError("SNS client should not be created during dryrun")
        ),
    )
    monkeypatch.setattr(
        s1_frankfurt_gap_filler,
        "send_slack_notification",
        lambda url, title, message: slack_messages.append(message),
    )

    result = CliRunner().invoke(
        s1_frankfurt_gap_filler.cli,
        [
            "0",
            "1",
            str(report_path),
            "--sns-topic-arn",
            "arn:aws:sns:af-south-1:123:index",
            "--source-bucket",
            SOURCE_BUCKET,
            "--destination-bucket",
            DESTINATION_BUCKET,
            "--source-region",
            "source-region",
            "--destination-region",
            "destination-region",
            "--dryrun",
            "--slack_url",
            "http://example.com/slack",
        ],
    )

    assert result.exit_code == 0
    assert destination.uploads == []
    assert destination.bodies == {}
    assert "Indexing messages published: `0`" in slack_messages[0]
    assert "Indexing messages planned: `1`" in slack_messages[0]
