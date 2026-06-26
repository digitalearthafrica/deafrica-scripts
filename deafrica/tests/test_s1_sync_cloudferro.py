from datetime import datetime, timedelta, timezone

from deafrica.data.cdse_pipelines.s1_sync_cloudferro import (
    classify_source_objects,
    destination_filename,
    skip_reason,
    transform_key,
    validate_required_files,
    validate_source_prefix_ready,
)


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
