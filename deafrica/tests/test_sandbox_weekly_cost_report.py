import csv

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
