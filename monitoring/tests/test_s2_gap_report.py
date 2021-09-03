from pathlib import Path

from moto import mock_s3
from urlpath import URL


@mock_s3
def test_get_and_filter_cogs_keys(
        monkeypatch, update_report_file: Path, fake_stac_file: Path
):
    print("Furure implementation")
    pass


@mock_s3
def test_get_and_filter_deafrica_keys(
        monkeypatch, update_report_file: Path, fake_stac_file: Path
):
    print("Furure implementation")
    pass


@mock_s3
def test_generate_buckets_diff(
        monkeypatch, update_report_file: Path, fake_stac_file: Path
):
    print("Furure implementation")
    pass


@mock_s3
def test_generate_buckets_diff_cli(
        monkeypatch, update_report_file: Path, fake_stac_file: Path, s3_report_file: URL
):
    print("Furure implementation")
    pass
