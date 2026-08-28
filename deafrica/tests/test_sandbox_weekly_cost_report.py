import csv
import json

from deafrica.platform.sandbox_weekly_cost_report import (
    index_persistent_volume_claims,
    write_report,
)


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
                "cluster=cluster-one:name=pvc-user": {
                    "byteHours": 16 * 1024**3,
                    "cost": 0.2,
                    "providerID": "vol-user",
                },
                "cluster=cluster-one:name=pvc-shared": {
                    "byteHours": 4 * 1024**3,
                    "cost": 0.05,
                    "providerID": "vol-shared",
                },
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
    claims_by_volume = {
        "pvc-user": {
            "namespace": "sandbox",
            "name": "claim-user",
            "storage_class": "gp3",
            "capacity": "10Gi",
        },
        "pvc-shared": {
            "namespace": "sandbox",
            "name": "shared-data",
            "storage_class": "efs-sc",
            "capacity": "100Gi",
        },
    }

    row_count = write_report(
        report_path,
        allocation,
        node_assets,
        node_index,
        "sandbox",
        "jupyter-",
        claims_by_volume,
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
    assert rows[0]["pod_pvc_claims"] == "sandbox/shared-data;sandbox/claim-user"
    assert rows[0]["pod_pv_names"] == "pvc-shared;pvc-user"

    pvc_allocations = json.loads(rows[0]["pod_pvc_allocations_json"])
    assert pvc_allocations == [
        {
            "claim": "sandbox/shared-data",
            "pv": "pvc-shared",
            "cluster": "cluster-one",
            "storage_class": "efs-sc",
            "capacity": "100Gi",
            "avg_storage_gib": 2.0,
            "storage_gib_hours": 4.0,
            "cost": 0.05,
            "provider_id": "vol-shared",
        },
        {
            "claim": "sandbox/claim-user",
            "pv": "pvc-user",
            "cluster": "cluster-one",
            "storage_class": "gp3",
            "capacity": "10Gi",
            "avg_storage_gib": 8.0,
            "storage_gib_hours": 16.0,
            "cost": 0.2,
            "provider_id": "vol-user",
        },
    ]


def test_index_persistent_volume_claims_uses_bound_volume_name():
    response = {
        "items": [
            {
                "metadata": {"namespace": "sandbox", "name": "claim-user"},
                "spec": {
                    "volumeName": "pvc-user",
                    "storageClassName": "gp3",
                    "resources": {"requests": {"storage": "10Gi"}},
                },
                "status": {"capacity": {"storage": "12Gi"}},
            },
            {
                "metadata": {"namespace": "sandbox", "name": "pending-claim"},
                "spec": {},
            },
        ]
    }

    assert index_persistent_volume_claims(response) == {
        "pvc-user": {
            "namespace": "sandbox",
            "name": "claim-user",
            "storage_class": "gp3",
            "capacity": "12Gi",
        }
    }
