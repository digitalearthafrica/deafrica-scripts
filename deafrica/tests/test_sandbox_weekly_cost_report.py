import csv
from unittest.mock import Mock

import pytest

from deafrica.platform import sandbox_weekly_cost_report as report
from deafrica.platform.sandbox_weekly_cost_report import write_report


def test_write_report_includes_allocation_average_and_storage_fields(tmp_path):
    report_path = tmp_path / "sandbox_weekly_cost_report.csv"
    allocation = {
        "sandbox/jupyter-user/ip-10-0-0-1": {
            "properties": {
                "namespace": "sandbox",
                "pod": "jupyter-user",
                "node": "ip-10-0-0-1",
            },
            "start": "2026-06-01T00:00:00Z",
            "end": "2026-06-01T02:00:00Z",
            "minutes": 120,
            "totalCost": 4.0,
            "cpuCoreRequestAverage": 1.2,
            "cpuCoreUsageAverage": 0.05,
            "cpuCost": 1.25,
            "ramByteRequestAverage": 2 * 1024**3,
            "ramByteUsageAverage": 512 * 1024**2,
            "ramCost": 2.5,
            "pvBytes": 10 * 1024**3,
            "pvByteHours": 20 * 1024**3,
            "pvCost": 0.25,
            "pvs": {
                "claim-user": {},
                "shared-data": {},
            },
        }
    }
    node_assets = [
        {
            "_asset_key": "ip-10-0-0-1",
            "type": "node",
            "name": "ip-10-0-0-1",
            "minutes": 120,
            "totalCost": 8.0,
            "properties": {
                "providerID": "aws:///af-south-1c/i-1234567890",
                "instanceType": "m6i.xlarge",
            },
        }
    ]
    node_index = {"ip-10-0-0-1": node_assets[0]}

    row_count = write_report(
        report_path, allocation, node_assets, node_index, "sandbox", "jupyter-"
    )

    with open(report_path, newline="") as report:
        rows = list(csv.DictReader(report))

    assert row_count == 1
    assert rows[0]["pod_avg_cpu_requested_cores"] == "1.2"
    assert rows[0]["pod_avg_cpu_used_cores"] == "0.05"
    assert rows[0]["pod_cpu_cost"] == "1.25"
    assert rows[0]["pod_avg_memory_requested_gib"] == "2.0"
    assert rows[0]["pod_avg_memory_used_gib"] == "0.5"
    assert rows[0]["pod_memory_cost"] == "2.5"
    assert rows[0]["pod_pvc_storage_avg_gib"] == "10.0"
    assert rows[0]["pod_pvc_storage_gib_hours"] == "20.0"
    assert rows[0]["pod_pvc_storage_cost"] == "0.25"
    assert rows[0]["pod_pvc_claims"] == "claim-user;shared-data"


@pytest.mark.parametrize("day", ["2026-12-28", "2027-01-04"])
@pytest.mark.parametrize(
    "prefix", ["sandbox-weekly-report", "/sandbox-weekly-report/", ""]
)
def test_main_archives_costs_by_year_in_both_destinations(
    monkeypatch, tmp_path, day, prefix
):
    monkeypatch.setattr(report, "report_date", Mock(return_value=day))
    monkeypatch.setattr(report, "generate_report", Mock())
    upload_s3 = Mock()
    upload_drive = Mock()
    monkeypatch.setattr(report, "upload_report_to_s3", upload_s3)
    monkeypatch.setattr(report, "upload_report_to_google_drive", upload_drive)
    report_path = str(tmp_path / "costs.csv")

    report.main(
        report_path=report_path,
        environment="prod",
        s3_prefix=prefix,
        google_drive_folder_id="costs-folder",
        google_credentials_file="credentials.json",
    )

    name = f"sandbox-weekly-cost-report-prod-{day}.csv"
    expected_prefix = "/".join(part for part in [prefix.strip("/"), day[:4]] if part)
    upload_s3.assert_called_once_with(
        report_path, name, "deafrica-cost-usage-reports", expected_prefix, "af-south-1"
    )
    upload_drive.assert_called_once_with(
        report_path, name, "costs-folder", "credentials.json", report_day=day
    )


def test_s3_only_target_does_not_require_google_credentials(monkeypatch, tmp_path):
    monkeypatch.setattr(report, "generate_report", Mock())
    monkeypatch.setattr(report, "report_date", Mock(return_value="2026-11-09"))
    upload_s3 = Mock()
    upload_drive = Mock()
    monkeypatch.setattr(report, "upload_report_to_s3", upload_s3)
    monkeypatch.setattr(report, "upload_report_to_google_drive", upload_drive)

    report.main(report_path=str(tmp_path / "costs.csv"), upload_target="s3")

    assert upload_s3.call_args.args[3] == "sandbox-weekly-report/2026"
    upload_drive.assert_not_called()


def test_generation_failure_does_not_publish_cost_report(monkeypatch, tmp_path):
    monkeypatch.setattr(
        report, "generate_report", Mock(side_effect=RuntimeError("No data"))
    )
    upload_s3 = Mock()
    upload_drive = Mock()
    monkeypatch.setattr(report, "upload_report_to_s3", upload_s3)
    monkeypatch.setattr(report, "upload_report_to_google_drive", upload_drive)

    with pytest.raises(RuntimeError, match="No data"):
        report.main(report_path=str(tmp_path / "costs.csv"))

    upload_s3.assert_not_called()
    upload_drive.assert_not_called()
