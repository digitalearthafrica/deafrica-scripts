from datetime import datetime, timedelta, timezone

import deafrica.data.cdse_pipelines.s1_sync_cloudferro as sync_module
from deafrica.data.cdse_pipelines.s1_sync_cloudferro import (
    S3_OLCI_L2_LFR_CDSE_PRODUCT,
    S3_OLCI_L2_WFR_CDSE_PRODUCT,
    SyncConfig,
    classify_source_objects,
    discover_completed_prefixes,
    destination_filename,
    skip_reason,
    sync_all_prefixes,
    transform_key,
    validate_required_files,
    validate_source_prefix_ready,
)


class FakePaginator:
    def __init__(self, objects):
        self.objects = objects

    def paginate(self, Bucket, Prefix):
        return [
            {
                "Contents": [
                    source_object
                    for source_object in self.objects
                    if source_object["Key"].startswith(Prefix)
                ]
            }
        ]


class FakeS3Client:
    def __init__(self, objects):
        self.objects = objects

    def get_paginator(self, _operation_name):
        return FakePaginator(self.objects)


def source_object(key):
    return {
        "Key": key,
        "Size": 1,
        "LastModified": datetime(2026, 6, 23, tzinfo=timezone.utc),
    }


def test_validate_required_files_detects_complete_output():
    source_objects = [
        {"Key": "s1_rtc/2026/06/23/0834AD/job/N00E005/userdata.json"},
        {"Key": "s1_rtc/2026/06/23/0834AD/job/N00E005/metadata.xml"},
        {"Key": "s1_rtc/2026/06/23/0834AD/job/N00E005/VV.tif"},
    ]

    assert validate_required_files(source_objects) == {
        "required_files_present": True,
        "missing_required_files": [],
    }


def test_validate_required_files_reports_missing_metadata_xml():
    source_objects = [
        {"Key": "s1_rtc/2026/06/23/0834AD/job/N00E005/userdata.json"},
        {"Key": "s1_rtc/2026/06/23/0834AD/job/N00E005/VV.tif"},
    ]

    assert validate_required_files(source_objects) == {
        "required_files_present": False,
        "missing_required_files": ["metadata.xml"],
    }


def test_validate_source_prefix_ready_checks_object_age():
    old_object = {
        "Key": "s1_rtc/2026/06/23/0834AD/job/N00E005/userdata.json",
        "LastModified": datetime.now(timezone.utc) - timedelta(hours=2),
    }
    recent_object = {
        "Key": "s1_rtc/2026/06/23/0834AD/job/N00E005/metadata.xml",
        "LastModified": datetime.now(timezone.utc),
    }

    assert validate_source_prefix_ready([old_object], 60) == {
        "ready_to_sync": True,
        "too_recent_files": [],
    }

    result = validate_source_prefix_ready([old_object, recent_object], 60)
    assert result["ready_to_sync"] is False
    assert result["too_recent_files"][0]["file"] == "metadata.xml"


def test_transform_key_moves_tile_before_date_and_renames_file():
    assert transform_key(
        "s1_rtc/2026/06/23/0834AD/"
        "ed81b8dd-7673-4152-9b1b-0dcde7bccead/N00E005/VV.tif"
    ) == ("s1_rtc/N00E005/2026/06/23/0834AD/" "s1_rtc_0834AD_N00E005_2026_06_23_VV.tif")


def test_transform_key_renames_metadata_xml():
    assert transform_key(
        "s1_rtc/2026/06/23/0834AD/"
        "ed81b8dd-7673-4152-9b1b-0dcde7bccead/N00E005/metadata.xml"
    ) == (
        "s1_rtc/N00E005/2026/06/23/0834AD/"
        "s1_rtc_0834AD_N00E005_2026_06_23_metadata.xml"
    )


def test_skip_reason_excludes_source_side_json_files():
    prefix = "s1_rtc/2026/06/23/0834AD/" "ed81b8dd-7673-4152-9b1b-0dcde7bccead/N00E005"

    assert skip_reason(f"{prefix}/userdata.json") == "excluded_userdata_json"
    assert skip_reason(f"{prefix}/metadata.json") == "excluded_source_metadata_json"
    assert skip_reason(f"{prefix}/VV.tif") is None
    assert skip_reason(f"{prefix}/metadata.xml") is None
    assert (
        skip_reason(f"{prefix}/s1_rtc_0834AD_N00E005_2026_06_23_metadata.json") is None
    )
    assert skip_reason(f"{prefix}/debug.log") == "unexpected_file"


def test_classify_source_objects_splits_copyable_and_skipped_files():
    prefix = "s1_rtc/2026/06/23/0834AD/" "ed81b8dd-7673-4152-9b1b-0dcde7bccead/N00E005"
    source_objects = [
        {"Key": f"{prefix}/VV.tif"},
        {"Key": f"{prefix}/metadata.xml"},
        {"Key": f"{prefix}/s1_rtc_0834AD_N00E005_2026_06_23_metadata.json"},
        {"Key": f"{prefix}/metadata.json"},
        {"Key": f"{prefix}/userdata.json"},
    ]

    copyable, skipped = classify_source_objects(source_objects)

    assert [obj["Key"].rsplit("/", 1)[-1] for obj in copyable] == [
        "VV.tif",
        "metadata.xml",
        "s1_rtc_0834AD_N00E005_2026_06_23_metadata.json",
    ]
    assert skipped == [
        {
            "source_key": f"{prefix}/metadata.json",
            "reason": "excluded_source_metadata_json",
        },
        {"source_key": f"{prefix}/userdata.json", "reason": "excluded_userdata_json"},
    ]


def test_destination_filename_is_idempotent():
    filename = "s1_rtc_0834AD_N00E005_2026_06_23_VV.tif"

    assert (
        destination_filename(
            "s1_rtc", "0834AD", "N00E005", "2026", "06", "23", filename
        )
        == filename
    )


def test_discover_completed_prefixes_skips_incomplete_outputs():
    complete_prefix = (
        "s1_rtc/2026/06/23/0834AD/" "ed81b8dd-7673-4152-9b1b-0dcde7bccead/N00E005/"
    )
    incomplete_prefix = (
        "s1_rtc/2026/06/24/0834AE/" "ed81b8dd-7673-4152-9b1b-0dcde7bcceae/N00E006/"
    )
    stac_item = f"{complete_prefix}s1_rtc_0834AD_N00E005_2026_06_23_metadata.json"
    client = FakeS3Client(
        [
            source_object(f"{complete_prefix}userdata.json"),
            source_object(f"{complete_prefix}metadata.xml"),
            source_object(f"{complete_prefix}VV.tif"),
            source_object(stac_item),
            source_object(f"{incomplete_prefix}userdata.json"),
            source_object(f"{incomplete_prefix}VV.tif"),
        ]
    )

    discovery = discover_completed_prefixes(client, "bucket", "s1_rtc/")

    assert discovery["completed_prefixes"] == [
        {
            "source_prefix": complete_prefix,
            "stac_item": stac_item,
            "found": 4,
        }
    ]
    assert discovery["skipped_prefixes"] == [
        {
            "source_prefix": incomplete_prefix,
            "reason": "missing_required_files",
            "missing_required_files": ["metadata.xml"],
            "found": 2,
        }
    ]
    assert discovery["skipped_invalid_object_count"] == 0
    assert discovery["skipped_invalid_object_examples"] == []


def test_discover_completed_prefixes_requires_generated_stac_item():
    prefix = "s1_rtc/2026/06/23/0834AD/" "ed81b8dd-7673-4152-9b1b-0dcde7bccead/N00E005/"
    client = FakeS3Client(
        [
            source_object(f"{prefix}userdata.json"),
            source_object(f"{prefix}metadata.xml"),
            source_object(f"{prefix}metadata.json"),
            source_object(f"{prefix}VV.tif"),
        ]
    )

    discovery = discover_completed_prefixes(client, "bucket", "s1_rtc/")

    assert discovery["completed_prefixes"] == []
    assert discovery["skipped_prefixes"] == [
        {
            "source_prefix": prefix,
            "reason": "missing_stac_item",
            "missing_required_files": [],
            "found": 4,
        }
    ]


def test_discover_completed_prefixes_counts_invalid_objects_with_examples():
    invalid_objects = [
        source_object(f"s1_rtc/request-invalid-{index:02d}.json") for index in range(12)
    ]
    client = FakeS3Client(invalid_objects)

    discovery = discover_completed_prefixes(client, "bucket", "s1_rtc/")

    assert discovery["completed_prefixes"] == []
    assert discovery["skipped_prefixes"] == []
    assert discovery["skipped_invalid_object_count"] == 12
    assert discovery["skipped_invalid_object_examples"] == [
        {
            "source_key": f"s1_rtc/request-invalid-{index:02d}.json",
            "reason": "unexpected_key_layout",
        }
        for index in range(10)
    ]


def test_s3_lfr_required_files_need_userdata_and_metadata_json():
    complete = [
        {"Key": "s3_lfr_test/2026/02/09/44HME_0_0/userdata.json"},
        {"Key": "s3_lfr_test/2026/02/09/44HME_0_0/metadata.json"},
        {"Key": "s3_lfr_test/2026/02/09/44HME_0_0/OTCI.tif"},
    ]
    incomplete = [
        {"Key": "s3_lfr_test/2026/02/09/44HME_0_0/userdata.json"},
        {"Key": "s3_lfr_test/2026/02/09/44HME_0_0/OTCI.tif"},
    ]

    assert validate_required_files(complete, S3_OLCI_L2_LFR_CDSE_PRODUCT) == {
        "required_files_present": True,
        "missing_required_files": [],
    }
    assert validate_required_files(incomplete, S3_OLCI_L2_LFR_CDSE_PRODUCT) == {
        "required_files_present": False,
        "missing_required_files": ["metadata.json"],
    }


def test_s3_lfr_skip_reason_uses_userdata_as_marker_only():
    prefix = "s3_lfr_test/2026/02/09/44HME_0_0"

    assert (
        skip_reason(f"{prefix}/userdata.json", S3_OLCI_L2_LFR_CDSE_PRODUCT)
        == "excluded_userdata_json"
    )
    assert skip_reason(f"{prefix}/metadata.json", S3_OLCI_L2_LFR_CDSE_PRODUCT) is None
    assert skip_reason(f"{prefix}/OTCI.tif", S3_OLCI_L2_LFR_CDSE_PRODUCT) is None
    assert (
        skip_reason(
            "s3_lfr_test/2026/02/09/request-04eeefea-dde1.json",
            S3_OLCI_L2_LFR_CDSE_PRODUCT,
        )
        == "excluded_request_json"
    )
    assert (
        skip_reason(f"{prefix}/debug.log", S3_OLCI_L2_LFR_CDSE_PRODUCT)
        == "unexpected_file"
    )


def test_s3_lfr_classify_copies_tifs_and_metadata_but_not_userdata():
    prefix = "s3_lfr_test/2026/02/09/44HME_0_0"
    source_objects = [
        {"Key": f"{prefix}/OTCI.tif"},
        {"Key": f"{prefix}/RC681.tif"},
        {"Key": f"{prefix}/metadata.json"},
        {"Key": f"{prefix}/userdata.json"},
    ]

    copyable, skipped = classify_source_objects(
        source_objects, S3_OLCI_L2_LFR_CDSE_PRODUCT
    )

    assert [obj["Key"].rsplit("/", 1)[-1] for obj in copyable] == [
        "OTCI.tif",
        "RC681.tif",
        "metadata.json",
    ]
    assert skipped == [
        {"source_key": f"{prefix}/userdata.json", "reason": "excluded_userdata_json"}
    ]


def test_s3_lfr_discovery_accepts_direct_layout_and_ignores_job_nested_layout():
    direct_prefix = "s3_lfr_test/2026/02/09/44HME_0_0/"
    job_nested_prefix = (
        "s3_lfr_test/2026/02/09/" "bb61e2d6-8615-4001-9115-85616ba290b7/37NBB_0_0/"
    )
    request_key = "s3_lfr_test/2026/02/09/request-04eeefea-dde1.json"
    client = FakeS3Client(
        [
            source_object(f"{direct_prefix}userdata.json"),
            source_object(f"{direct_prefix}metadata.json"),
            source_object(f"{direct_prefix}OTCI.tif"),
            source_object(f"{job_nested_prefix}userdata.json"),
            source_object(f"{job_nested_prefix}metadata.json"),
            source_object(f"{job_nested_prefix}OTCI.tif"),
            source_object(request_key),
        ]
    )

    discovery = discover_completed_prefixes(
        client,
        "bucket",
        "s3_lfr_test/",
        S3_OLCI_L2_LFR_CDSE_PRODUCT,
    )

    assert discovery["completed_prefixes"] == [
        {
            "source_prefix": direct_prefix,
            "stac_item": f"{direct_prefix}metadata.json",
            "found": 3,
        }
    ]
    assert discovery["skipped_prefixes"] == []
    assert discovery["skipped_invalid_object_count"] == 4
    assert discovery["skipped_invalid_object_examples"] == [
        {
            "source_key": f"{job_nested_prefix}userdata.json",
            "reason": "unexpected_key_layout",
        },
        {
            "source_key": f"{job_nested_prefix}metadata.json",
            "reason": "unexpected_key_layout",
        },
        {
            "source_key": f"{job_nested_prefix}OTCI.tif",
            "reason": "unexpected_key_layout",
        },
        {
            "source_key": request_key,
            "reason": "unexpected_key_layout",
        },
    ]


def test_sync_all_prefixes_can_limit_processed_prefixes(monkeypatch):
    source_objects = []
    for tile in ("44HME_0_0", "44HMF_0_0", "44HMG_0_0"):
        prefix = f"s3_lfr_test/2026/02/09/{tile}/"
        source_objects.extend(
            [
                source_object(f"{prefix}userdata.json"),
                source_object(f"{prefix}metadata.json"),
                source_object(f"{prefix}OTCI.tif"),
            ]
        )

    processed_prefixes = []

    def fake_sync_prefix(config, product=None):
        processed_prefixes.append(config.source_prefix)
        return {
            "source_prefix": config.source_prefix,
            "ready_to_sync": True,
            "skip_reason": None,
            "failed": 0,
            "would_copy": 3,
        }

    monkeypatch.setattr(
        sync_module, "cloudferro_client", lambda _config: FakeS3Client(source_objects)
    )
    monkeypatch.setattr(sync_module, "sync_prefix", fake_sync_prefix)

    config = SyncConfig(
        source_bucket="bucket",
        source_prefix="",
        stac_item="",
        destination_bucket="destination-bucket",
        dry_run=True,
        min_object_age_minutes=0,
        job_id=None,
        cloudferro_endpoint_url="https://cloudferro.example",
        cloudferro_region="RegionOne",
        cdse_batch_process_url="https://batch.example",
        cdse_token_url="https://token.example",
    )

    summary = sync_all_prefixes(
        config,
        discovery_prefix="s3_lfr_test/",
        max_workers=1,
        max_prefixes=2,
        product=S3_OLCI_L2_LFR_CDSE_PRODUCT,
    )

    assert summary["completed_prefixes"] == 3
    assert summary["selected_prefixes"] == 2
    assert summary["limited_prefixes"] == 1
    assert summary["processed_prefixes"] == 2
    assert summary["would_copy"] == 6
    assert processed_prefixes == [
        "s3_lfr_test/2026/02/09/44HME_0_0/",
        "s3_lfr_test/2026/02/09/44HMF_0_0/",
    ]


def test_s3_lfr_transform_is_identity():
    key = "s3_lfr_test/2026/02/09/44HME_0_0/metadata.json"

    assert transform_key(key, S3_OLCI_L2_LFR_CDSE_PRODUCT) == key


def test_s3_wfr_direct_copy_product_rules():
    prefix = "s3_wfr_test/2026/02/09/37NBB_0_0/"
    source_objects = [
        source_object(f"{prefix}userdata.json"),
        source_object(f"{prefix}metadata.json"),
        source_object(f"{prefix}CHL_NN.tif"),
        source_object(f"{prefix}MASK.tif"),
    ]

    assert validate_required_files(source_objects, S3_OLCI_L2_WFR_CDSE_PRODUCT) == {
        "required_files_present": True,
        "missing_required_files": [],
    }

    copyable, skipped = classify_source_objects(
        source_objects, S3_OLCI_L2_WFR_CDSE_PRODUCT
    )
    assert [obj["Key"].rsplit("/", 1)[-1] for obj in copyable] == [
        "metadata.json",
        "CHL_NN.tif",
        "MASK.tif",
    ]
    assert skipped == [
        {"source_key": f"{prefix}userdata.json", "reason": "excluded_userdata_json"}
    ]

    discovery = discover_completed_prefixes(
        FakeS3Client(source_objects),
        "bucket",
        "s3_wfr_test/",
        S3_OLCI_L2_WFR_CDSE_PRODUCT,
    )
    assert discovery["completed_prefixes"] == [
        {
            "source_prefix": prefix,
            "stac_item": f"{prefix}metadata.json",
            "found": 4,
        }
    ]

    assert transform_key(f"{prefix}metadata.json", S3_OLCI_L2_WFR_CDSE_PRODUCT) == (
        f"{prefix}metadata.json"
    )
