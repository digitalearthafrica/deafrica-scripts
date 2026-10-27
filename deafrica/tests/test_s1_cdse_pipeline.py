import json
from datetime import datetime, timedelta, timezone
from io import BytesIO

import geopandas as gpd
from botocore.exceptions import ClientError
from click.testing import CliRunner
from shapely.geometry import box

from deafrica.data.cdse_pipelines import s1
from deafrica.data.cdse_pipelines.sentinel_sync_cloudferro import s1_transform_key

NOW = datetime(2026, 6, 24, 3, 0, tzinfo=timezone.utc)
TILE_GEOM = box(0, 0, 1, 1)
SCENE_ID = "S1A_IW_GRDH_1SDV_20260623T160924_20260623T160949_062394_0834AD_AAAA"
SCENE = {
    "id": SCENE_ID,
    "geometry": TILE_GEOM.__geo_interface__,
    "properties": {"datetime": "2026-06-23T16:09:24Z"},
}


def test_next_day_iso_handles_month_boundary():
    assert s1.next_day_iso("2026-08-31") == "2026-09-01"


def test_expected_metadata_key_matches_sync_transform():
    dataset = "s1_rtc/N00E005/2026/06/23/0834AD"
    source = (
        "s1_rtc/2026/06/23/0834AD/ed81b8dd-7673-4152-9b1b-0dcde7bccead/"
        "N00E005/s1_rtc_0834AD_N00E005_2026_06_23_metadata.json"
    )

    assert s1.expected_metadata_key(dataset) == s1_transform_key(source)


def test_cloudferro_key_maps_back_to_dataset_id():
    key = "s1_rtc/2026/06/23/0834AD/job-id/N00E005/VV.tif"

    assert s1.dataset_id_from_cloudferro_key(key) == "s1_rtc/N00E005/2026/06/23/0834AD"


def test_classify_dataset_states():
    assert (
        s1.classify_dataset(
            on_pds=True,
            on_cloudferro=True,
            record={"status": "PROCESSING"},
            now=NOW,
        )
        == "on_pds"
    )
    assert (
        s1.classify_dataset(on_pds=False, on_cloudferro=True, record=None, now=NOW)
        == "pending_sync"
    )
    assert (
        s1.classify_dataset(
            on_pds=False,
            on_cloudferro=False,
            record={"status": "DONE"},
            now=NOW,
        )
        == "pending_sync"
    )
    assert (
        s1.classify_dataset(
            on_pds=False,
            on_cloudferro=False,
            record={"status": "PROCESSING", "job_id": "job-1"},
            now=NOW,
        )
        == "in_flight"
    )
    fresh = (NOW - timedelta(hours=1)).isoformat()
    assert (
        s1.classify_dataset(
            on_pds=False,
            on_cloudferro=False,
            record={"status": "ANALYSIS_DONE", "status_since": fresh},
            now=NOW,
        )
        == "in_flight"
    )
    stale = (NOW - timedelta(hours=7)).isoformat()
    assert (
        s1.classify_dataset(
            on_pds=False,
            on_cloudferro=False,
            record={"status": "ANALYSIS_DONE", "status_since": stale},
            now=NOW,
        )
        == "restart"
    )
    assert (
        s1.classify_dataset(
            on_pds=False,
            on_cloudferro=False,
            record={"status": "FAILED", "attempts": 2},
            now=NOW,
        )
        == "retry"
    )
    assert (
        s1.classify_dataset(
            on_pds=False,
            on_cloudferro=False,
            record={"status": "PARTIAL", "attempts": 3},
            now=NOW,
        )
        == "exhausted"
    )
    assert (
        s1.classify_dataset(
            on_pds=False,
            on_cloudferro=False,
            record={"status": "STOPPED", "attempts": 1},
            now=NOW,
        )
        == "stopped"
    )
    assert (
        s1.classify_dataset(on_pds=False, on_cloudferro=False, record=None, now=NOW)
        == "true_gap"
    )


def test_submission_order_prefers_restarts_then_yesterday_then_older_gaps():
    rows = [
        {"state": "retry", "date": "2026-06-23", "dataset": "retry"},
        {"state": "true_gap", "date": "2026-06-22", "dataset": "day-before"},
        {"state": "true_gap", "date": "2026-06-23", "dataset": "yesterday"},
        {"state": "restart", "date": "2026-06-20", "dataset": "restart"},
        {"state": "true_gap", "date": "2026-06-21", "dataset": "older"},
        {"state": "on_pds", "date": "2026-06-23", "dataset": "done"},
    ]

    ordered = [row["dataset"] for row in s1.ordered_actionable(rows, "2026-06-23")]

    assert ordered == ["restart", "yesterday", "day-before", "older", "retry"]


def test_budget_defers_the_tile_that_would_cross_the_daily_cap():
    items = [{"dataset": name} for name in ("a", "b", "c")]

    accepted, deferred = s1.apply_pu_budget(
        items,
        pu_per_tile=265,
        daily_budget=500,
        monthly_budget=10_000,
        month_to_date=0,
        enabled=True,
    )

    assert [item["dataset"] for item in accepted] == ["a"]
    assert [item["dataset"] for item in deferred] == ["b", "c"]


def test_budget_defers_everything_when_month_to_date_is_too_high():
    items = [{"dataset": "a"}, {"dataset": "b"}]

    accepted, deferred = s1.apply_pu_budget(
        items,
        pu_per_tile=265,
        daily_budget=100_000,
        monthly_budget=1_000,
        month_to_date=800,
        enabled=True,
    )

    assert accepted == []
    assert [item["dataset"] for item in deferred] == ["a", "b"]


def _batch_job(job_id, description, created, cost, status):
    """Batch V2 list shape: description lives on request, not the task."""
    return {
        "id": job_id,
        "request": {
            "processRequest": {},
            "input": {},
            "output": {},
            "description": description,
        },
        "created": created,
        "costPU": cost,
        "status": status,
    }


def test_month_to_date_pu_counts_s1_only_and_estimates_uncosted_jobs():
    jobs = [
        _batch_job(
            "done",
            "s1_rtc 2026-06-02 0834AD N00E005",
            "2026-06-02T00:00:00Z",
            265,
            "DONE",
        ),
        _batch_job(
            "s3",
            "Batch sentinel-3-olci-l2 job (s3_lfr)",
            "2026-06-02T00:00:00Z",
            9000,
            "DONE",
        ),
        _batch_job(
            "running",
            "s1_rtc 2026-06-03 0834AD N00E006",
            "2026-06-03T00:00:00Z",
            0,
            "PROCESSING",
        ),
        _batch_job(
            "legacy",
            "Batch sentinel-1-grd job (s1_rtc)",
            "2026-06-04T00:00:00Z",
            265,
            "DONE",
        ),
        _batch_job(
            "last-month",
            "s1_rtc 2026-05-31 0834AD N00E005",
            "2026-05-31T00:00:00Z",
            265,
            "DONE",
        ),
        {
            "id": "flat-description-ignored",
            "description": "s1_rtc 2026-06-04 0834AD N00E007",
            "created": "2026-06-04T00:00:00Z",
            "costPU": 265,
            "status": "DONE",
        },
    ]
    records = [
        {"job_id": "running", "status": "PROCESSING", "estimated_pu": 265},
        {"job_id": "not-listed", "status": "ANALYSING", "estimated_pu": 265},
    ]

    total = s1.s1_month_to_date_pu(
        jobs, records, datetime(2026, 6, 1, tzinfo=timezone.utc), 265
    )

    assert total == 265 + 265 + 265 + 265


def test_resolve_tile_scenes_does_not_call_catalog_when_the_day_was_searched():
    called = []

    scenes = s1.resolve_tile_scenes(
        {"2026-06-23": [SCENE]},
        "2026-06-23",
        "0834AD",
        TILE_GEOM,
        fetch=lambda: called.append("fetch") or [{"id": "other"}],
    )

    assert called == []
    assert [scene["id"] for scene in scenes] == [SCENE_ID]


def test_resolve_tile_scenes_fetches_when_the_day_was_not_searched():
    called = []

    scenes = s1.resolve_tile_scenes(
        {},
        "2026-06-23",
        "0834AD",
        TILE_GEOM,
        fetch=lambda: called.append("fetch") or [{"id": "fetched"}],
    )

    assert called == ["fetch"]
    assert scenes == [{"id": "fetched"}]


class _CacheS3:
    def __init__(self, objects):
        self.objects = objects

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "GetObject")
        return {"Body": BytesIO(self.objects[Key])}


def test_scenes_for_xml_uses_catalog_cache_without_a_catalog_post():
    grid = gpd.GeoDataFrame(
        {"NAME": ["N00E000"]}, geometry=[TILE_GEOM], crs="EPSG:4326"
    )
    body = json.dumps({"date": "2026-06-23", "features": [SCENE]}).encode()
    client = _CacheS3({s1.catalog_cache_key("2026-06-23"): body})
    called = []

    scenes = s1.scenes_for_xml(
        client,
        "bucket",
        session=None,
        tile="N00E000",
        date="2026-06-23",
        datatake="0834AD",
        grid=grid,
        fetch=lambda *args, **kwargs: called.append("fetch"),
    )

    assert called == []
    assert [scene["id"] for scene in scenes] == [SCENE_ID]


def test_scenes_for_xml_falls_back_to_catalog_on_cache_miss():
    grid = gpd.GeoDataFrame(
        {"NAME": ["N00E000"]}, geometry=[TILE_GEOM], crs="EPSG:4326"
    )
    called = []

    scenes = s1.scenes_for_xml(
        _CacheS3({}),
        "bucket",
        session=None,
        tile="N00E000",
        date="2026-06-23",
        datatake="0834AD",
        grid=grid,
        fetch=lambda *args, **kwargs: called.append("fetch") or [SCENE],
    )

    assert called == ["fetch"]
    assert scenes == [SCENE]


def test_stac_upload_failure_keeps_the_job_id(monkeypatch):
    def explode(*_args, **_kwargs):
        raise RuntimeError("denied")

    monkeypatch.setattr(s1, "build_stac_item", explode)
    result = s1._attach_stac(
        {
            "dataset": "s1_rtc/N00E005/2026/06/23/0834AD",
            "status": "started",
            "submitted": True,
            "job_id": "job-1",
        },
        tile="N00E005",
        date="2026-06-23",
        datatake="0834AD",
        year="2026",
        month="06",
        day="23",
        grid=None,
        scenes=[],
        geom=None,
        pds_bucket="deafrica-sentinel-1",
        output_bucket="cf",
        job_id="job-1",
    )

    assert result["job_id"] == "job-1"
    assert result["submitted"] is True
    assert result["status"] == "error"
    assert result["error"].startswith("stac upload failed")


def test_scan_longer_than_14_days_requires_backfill():
    assert s1.scan_day_count("2026-06-01", "2026-06-14") == 14
    assert s1.scan_day_count("2026-06-01", "2026-06-15") == 15


def test_backfill_flag_is_required_for_a_long_window():
    result = CliRunner().invoke(
        s1.cli,
        ["--start-date", "2026-05-01", "--end-date", "2026-06-01"],
    )

    assert result.exit_code != 0
    assert "--backfill" in result.output
