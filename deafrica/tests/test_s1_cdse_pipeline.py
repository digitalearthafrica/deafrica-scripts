from deafrica.data.cdse_pipelines import s1


def test_next_day_iso_handles_month_boundary():
    assert s1.next_day_iso("2026-08-31") == "2026-09-01"
