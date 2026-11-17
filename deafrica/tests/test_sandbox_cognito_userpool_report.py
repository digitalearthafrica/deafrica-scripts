import re
from unittest.mock import Mock

import pandas as pd
import pytest
from click.testing import CliRunner
from google.oauth2 import service_account
from googleapiclient import discovery

from deafrica import utils
from deafrica.platform import sandbox_cognito_userpool_report as report


def test_users_to_dataframe_handles_missing_attributes():
    users = [
        {
            "Username": "alice",
            "Enabled": True,
            "UserStatus": "CONFIRMED",
            "UserCreateDate": "2024-01-01T00:00:00+00:00",
            "UserLastModifiedDate": "2024-01-02T00:00:00+00:00",
            "Attributes": [
                {"Name": "email", "Value": "alice@example.com"},
                {"Name": "phone_number", "Value": "+254712345678"},
            ],
        }
    ]

    df = report.users_to_dataframe(users)
    row = df.iloc[0]

    assert list(df.columns) == report.REPORT_COLUMNS
    assert row["email"] == "alice@example.com"
    assert row["phone_number_country"] == "Kenya"
    assert row["custom:timeframe"] == ""


def test_phone_number_country_handles_invalid_values():
    assert report.phone_number_country("") == ""
    assert report.phone_number_country("not a phone number") == ""
    assert report.phone_number_country("+254000000000") == ""


def test_phone_number_country_returns_full_country_names():
    assert report.phone_number_country("+254712345678") == "Kenya"
    assert report.phone_number_country("+61412345678") == "Australia"
    assert report.phone_number_country("+79161234567") == "Russia"
    assert report.phone_number_country("+447911123456") == "Guernsey"


def test_phone_number_country_ignores_non_geographic_numbers():
    assert report.phone_number_country("+80012345678") == ""


class FakePaginator:
    def __init__(self, operation_name):
        self.operation_name = operation_name

    def paginate(self, **kwargs):
        if self.operation_name == "list_groups":
            yield {"Groups": [{"GroupName": "admins"}, {"GroupName": "users"}]}
            return

        group_name = kwargs["GroupName"]
        if group_name == "admins":
            yield {"Users": [{"Username": "alice"}]}
        elif group_name == "users":
            yield {"Users": [{"Username": "alice"}, {"Username": "bob"}]}


class FakeCognitoClient:
    def get_paginator(self, operation_name):
        return FakePaginator(operation_name)


def test_add_user_groups_adds_group_columns_in_memory():
    df = pd.DataFrame({"Username": ["alice", "bob"]})

    result = report.add_user_groups(df, FakeCognitoClient(), "test-pool-id")

    assert result.loc[0, "admins"] == "admins"
    assert result.loc[1, "admins"] == ""
    assert result.loc[0, "users"] == "users"
    assert result.loc[1, "users"] == "users"


@pytest.mark.parametrize("environment", ["prod", "dev", ""])
@pytest.mark.parametrize(
    "frequency, publish_latest",
    [("weekly", False), ("weekly", True), ("monthly", False), ("monthly", True)],
)
def test_main_publishes_matching_dated_and_latest_reports(
    monkeypatch, tmp_path, environment, publish_latest, frequency
):
    monkeypatch.chdir(tmp_path)
    for name in ["aws_region_cognito", "aws_region_ses", "user_pool_id"]:
        monkeypatch.setenv(name, "test")
    report_date = Mock(side_effect=["2026-12-31", "2027-01-01"])
    monkeypatch.setattr(report, "report_date", report_date)
    monkeypatch.setattr(report.boto3, "client", Mock())
    monkeypatch.setattr(
        report,
        "fetch_users_from_cognito",
        Mock(return_value=[{"Username": "alice"}]),
    )
    monkeypatch.setattr(report, "add_user_groups", lambda df, *_: df)
    upload = Mock()
    monkeypatch.setattr(report, "upload_report_to_google_drive", upload)

    report.main(
        "",
        "users-folder",
        "credentials.json",
        environment,
        publish_latest=publish_latest,
        frequency=frequency,
    )

    suffix = f"_{environment}" if environment else ""
    if frequency == "monthly":
        report_name = f"Users{suffix}_monthly_2026-12.xlsx"
        expected_latest = f"Users{suffix}_monthly_latest.xlsx"
    else:
        report_name = f"Users{suffix}_2026-12-31.xlsx"
        expected_latest = f"Users{suffix}_latest.xlsx"
    assert pd.read_excel(tmp_path / report_name).loc[0, "Username"] == "alice"
    upload.assert_called_once_with(
        report_name,
        report_name,
        "users-folder",
        "credentials.json",
        report_day="2026-12-31",
        latest_name=expected_latest if publish_latest else None,
    )
    report_date.assert_called_once()


def test_cli_accepts_empty_email_from_workflow(monkeypatch):
    main = Mock()
    monkeypatch.setattr(report, "main", main)

    result = CliRunner().invoke(report.cli, ["--email", "", "--environment", "prod"])

    assert result.exit_code == 0, result.output
    assert main.call_args.args[0] == ""
    assert main.call_args.kwargs["publish_latest"] is False


@pytest.mark.parametrize("value, expected", [("true", True), ("false", False)])
def test_cli_reads_publish_latest_setting(monkeypatch, value, expected):
    main = Mock()
    monkeypatch.setattr(report, "main", main)
    monkeypatch.setenv("REPORT_PUBLISH_LATEST", value)

    result = CliRunner().invoke(report.cli, [])

    assert result.exit_code == 0, result.output
    assert main.call_args.kwargs["publish_latest"] is expected


def test_cli_can_explicitly_disable_latest(monkeypatch):
    main = Mock()
    monkeypatch.setattr(report, "main", main)
    monkeypatch.setenv("REPORT_PUBLISH_LATEST", "true")

    result = CliRunner().invoke(report.cli, ["--no-publish-latest"])

    assert result.exit_code == 0, result.output
    assert main.call_args.kwargs["publish_latest"] is False


def test_cli_reads_monthly_frequency(monkeypatch):
    main = Mock()
    monkeypatch.setattr(report, "main", main)
    monkeypatch.setenv("REPORT_FREQUENCY", "monthly")
    monkeypatch.setenv("REPORT_PUBLISH_LATEST", "true")

    result = CliRunner().invoke(report.cli, [])

    assert result.exit_code == 0, result.output
    assert main.call_args.kwargs["frequency"] == "monthly"
    assert main.call_args.kwargs["publish_latest"] is True


class FakeDrive:
    """Store files and their contents across separate report runs."""

    def __init__(self):
        self.items = {}
        self.fail_upload_name = None
        self.page_size = 1

    def files(self):
        return self

    def list(self, q, pageToken=None, **kwargs):
        assert kwargs["supportsAllDrives"]
        assert kwargs["includeItemsFromAllDrives"]

        def query_value(pattern):
            value = re.search(pattern, q).group(1)
            return re.sub(r"\\(.)", r"\1", value)

        parent = query_value(r"'((?:\\.|[^'\\])*)' in parents")
        name = query_value(r"name = '((?:\\.|[^'\\])*)'")
        mime_type = query_value(r"mimeType = '((?:\\.|[^'\\])*)'")
        matches = [
            item
            for item in self.items.values()
            if parent in item["parents"]
            and item["name"] == name
            and item["mimeType"] == mime_type
            and not item.get("trashed")
        ]
        start = int(pageToken or 0)
        result = {"files": matches[start : start + self.page_size]}
        if start + self.page_size < len(matches):
            result["nextPageToken"] = str(start + self.page_size)
        return Mock(execute=lambda: result)

    def create(self, body, media_body=None, **kwargs):
        assert kwargs["supportsAllDrives"]

        def execute():
            if body["name"] == self.fail_upload_name:
                raise RuntimeError("Upload failed")
            file_id = f"file-{len(self.items) + 1}"
            item = dict(body, id=file_id, webViewLink=f"https://drive.test/{file_id}")
            if media_body:
                item["mimeType"] = media_body.mimetype()
                item["content"] = media_body.getbytes(0, media_body.size())
            self.items[file_id] = item
            return item.copy()

        return Mock(execute=execute)

    def update(self, fileId, media_body, **kwargs):
        assert kwargs["supportsAllDrives"]

        def execute():
            item = self.items[fileId]
            if item["name"] == self.fail_upload_name:
                raise RuntimeError("Upload failed")
            item["content"] = media_body.getbytes(0, media_body.size())
            return item.copy()

        return Mock(execute=execute)


@pytest.fixture
def drive_upload(monkeypatch, tmp_path):
    drive = FakeDrive()
    credentials_file = tmp_path / "credentials.json"
    credentials_file.write_text("{}")
    report_path = tmp_path / "users.xlsx"
    report_path.write_bytes(b"first report")
    monkeypatch.setattr(discovery, "build", Mock(return_value=drive))
    monkeypatch.setattr(
        service_account.Credentials, "from_service_account_file", Mock()
    )

    def upload(day="2026-11-09", parent="users", latest_name="Users_prod_latest.xlsx"):
        return utils.upload_dated_report_to_google_drive(
            report_path,
            f"Users_prod_{day}.xlsx",
            parent,
            str(credentials_file),
            day,
            report.XLSX_MIME_TYPE,
            latest_name=latest_name,
        )

    return drive, report_path, upload


def test_rerun_reuses_folder_archive_and_latest_link(drive_upload):
    drive, report_path, upload = drive_upload
    first_archive = upload()
    latest = next(
        item
        for item in drive.items.values()
        if item["name"] == "Users_prod_latest.xlsx"
    )
    latest_id = latest["id"]
    latest_link = latest["webViewLink"]
    report_path.write_bytes(b"updated report")

    second_archive = upload()

    assert len(drive.items) == 3
    assert first_archive["id"] == second_archive["id"]
    assert latest["id"] == latest_id
    assert latest["webViewLink"] == latest_link
    assert latest["parents"] == ["users"]
    assert latest["content"] == second_archive["content"] == b"updated report"
    year_folder = drive.items[second_archive["parents"][0]]
    assert year_folder["name"] == "2026"
    assert year_folder["parents"] == ["users"]


def test_new_year_creates_new_archive_and_keeps_latest_link(drive_upload):
    drive, report_path, upload = drive_upload
    old_archive = upload("2026-12-28")
    latest = next(
        item
        for item in drive.items.values()
        if item["name"] == "Users_prod_latest.xlsx"
    )
    report_path.write_bytes(b"new year report")

    new_archive = upload("2027-01-04")

    assert len(drive.items) == 5
    assert drive.items[old_archive["parents"][0]]["name"] == "2026"
    assert drive.items[new_archive["parents"][0]]["name"] == "2027"
    assert drive.items[old_archive["id"]]["content"] == b"first report"
    assert latest["content"] == new_archive["content"] == b"new year report"


@pytest.fixture
def user_report_runner(drive_upload, monkeypatch):
    drive, report_path, _ = drive_upload
    monkeypatch.chdir(report_path.parent)
    for name in ["aws_region_cognito", "aws_region_ses", "user_pool_id"]:
        monkeypatch.setenv(name, "test")
    monkeypatch.setattr(report.boto3, "client", Mock())
    monkeypatch.setattr(report, "add_user_groups", lambda df, *_: df)

    def run(day, frequency):
        monkeypatch.setattr(report, "report_date", lambda: day)
        monkeypatch.setattr(
            report,
            "fetch_users_from_cognito",
            Mock(return_value=[{"Username": f"{frequency}-user-{day}"}]),
        )
        report.main(
            "",
            "users",
            str(report_path.parent / "credentials.json"),
            "prod",
            publish_latest=True,
            frequency=frequency,
        )

    def get_file(name):
        return next(item for item in drive.items.values() if item["name"] == name)

    return drive, run, get_file


def test_monthly_and_weekly_latest_are_independent(user_report_runner):
    _, run, get_file = user_report_runner
    run("2026-11-23", "weekly")
    weekly_latest = get_file("Users_prod_latest.xlsx")
    weekly_id = weekly_latest["id"]
    weekly_content = weekly_latest["content"]

    run("2026-11-25", "monthly")

    monthly_latest = get_file("Users_prod_monthly_latest.xlsx")
    monthly_content = monthly_latest["content"]
    monthly_archive = get_file("Users_prod_monthly_2026-11.xlsx")
    assert monthly_latest["id"] != weekly_id
    assert monthly_content == monthly_archive["content"] != weekly_content
    assert weekly_latest["content"] == weekly_content

    run("2026-11-30", "weekly")

    weekly_archive = get_file("Users_prod_2026-11-30.xlsx")
    assert weekly_latest["id"] == weekly_id
    assert weekly_latest["content"] == weekly_archive["content"] != weekly_content
    assert monthly_latest["content"] == monthly_content
    assert monthly_archive["content"] == monthly_content


def test_monthly_reruns_and_rollover_keep_history_and_latest_link(user_report_runner):
    drive, run, get_file = user_report_runner
    run("2026-11-25", "monthly")
    latest = get_file("Users_prod_monthly_latest.xlsx")
    latest_id = latest["id"]
    latest_link = latest["webViewLink"]
    november = get_file("Users_prod_monthly_2026-11.xlsx")
    november_id = november["id"]
    november_content = november["content"]

    run("2026-11-26", "monthly")

    assert len(drive.items) == 3
    assert november["id"] == november_id
    assert november["content"] == latest["content"] != november_content
    november_content = november["content"]

    run("2026-12-25", "monthly")

    december = get_file("Users_prod_monthly_2026-12.xlsx")
    december_content = december["content"]
    assert len(drive.items) == 4
    assert november["content"] == november_content
    assert latest["content"] == december_content

    run("2027-01-25", "monthly")

    january = get_file("Users_prod_monthly_2027-01.xlsx")
    assert len(drive.items) == 6
    assert latest["id"] == latest_id
    assert latest["webViewLink"] == latest_link
    assert latest["parents"] == ["users"]
    assert latest["content"] == january["content"]
    assert drive.items[november["parents"][0]]["name"] == "2026"
    assert drive.items[january["parents"][0]]["name"] == "2027"
    assert november["content"] == november_content
    assert december["content"] == december_content


@pytest.mark.parametrize(
    "failed_name", ["Users_prod_monthly_2026-12.xlsx", "Users_prod_monthly_latest.xlsx"]
)
def test_failed_monthly_upload_keeps_previous_latest(user_report_runner, failed_name):
    drive, run, get_file = user_report_runner
    run("2026-11-25", "monthly")
    latest = get_file("Users_prod_monthly_latest.xlsx")
    previous_content = latest["content"]
    drive.fail_upload_name = failed_name

    with pytest.raises(RuntimeError, match="Upload failed"):
        run("2026-12-25", "monthly")

    assert latest["content"] == previous_content
    assert get_file("Users_prod_monthly_2026-11.xlsx")["content"] == previous_content


@pytest.mark.parametrize(
    "failed_name", ["Users_prod_2026-11-16.xlsx", "Users_prod_latest.xlsx"]
)
def test_failed_upload_leaves_previous_latest_available(drive_upload, failed_name):
    drive, report_path, upload = drive_upload
    upload()
    latest = next(
        item
        for item in drive.items.values()
        if item["name"] == "Users_prod_latest.xlsx"
    )
    drive.fail_upload_name = failed_name
    report_path.write_bytes(b"failed report")

    with pytest.raises(RuntimeError, match="Upload failed"):
        upload("2026-11-16")

    assert latest["content"] == b"first report"


def test_cost_upload_has_year_archive_without_latest(drive_upload):
    drive, _, upload = drive_upload

    archived = upload(parent="costs", latest_name=None)

    assert len(drive.items) == 2
    assert drive.items[archived["parents"][0]]["parents"] == ["costs"]


def test_find_item_paginates_and_stays_within_parent(drive_upload):
    drive, _, upload = drive_upload
    upload(parent="other-users")
    upload(parent="users")
    latest = next(
        item
        for item in drive.items.values()
        if item["name"] == "Users_prod_latest.xlsx" and item["parents"] == ["users"]
    )
    duplicate = dict(latest, id="zzz-duplicate")
    drive.items = {duplicate["id"]: duplicate, **drive.items}

    found = utils.find_drive_item(
        drive, "users", "Users_prod_latest.xlsx", report.XLSX_MIME_TYPE
    )

    assert found["id"] == latest["id"]


def test_query_escapes_folder_and_file_names(drive_upload):
    drive, _, upload = drive_upload
    upload(parent="users'\\folder", latest_name="Users'latest.xlsx")

    found = utils.find_drive_item(
        drive, "users'\\folder", "Users'latest.xlsx", report.XLSX_MIME_TYPE
    )

    assert found["parents"] == ["users'\\folder"]


@pytest.mark.parametrize(
    "folder, credentials, error",
    [
        ("", "credentials.json", "GOOGLE_DRIVE_FOLDER_ID"),
        ("users", "", "GOOGLE_APPLICATION_CREDENTIALS"),
    ],
)
def test_missing_configuration_fails_before_upload(folder, credentials, error):
    with pytest.raises(ValueError, match=error):
        utils.upload_dated_report_to_google_drive(
            "users.xlsx",
            "Users.xlsx",
            folder,
            credentials,
            "2026-11-09",
            report.XLSX_MIME_TYPE,
        )


@pytest.mark.parametrize("frequency", ["weekly", "monthly"])
def test_user_snapshots_use_only_existing_cognito_permissions(
    monkeypatch, tmp_path, frequency
):
    monkeypatch.chdir(tmp_path)
    for name in ["aws_region_cognito", "aws_region_ses", "user_pool_id"]:
        monkeypatch.setenv(name, "test")
    monkeypatch.setattr(report, "report_date", lambda: "2026-11-25")
    users = [
        {
            "Username": "alice",
            "UserCreateDate": "2020-01-01T00:00:00Z",
            "Attributes": [
                {"Name": "custom:last_login", "Value": "2020-01-02T00:00:00Z"}
            ],
        },
        {"Username": "bob", "UserCreateDate": "2026-11-24T00:00:00Z"},
        {"Username": "carol", "UserCreateDate": "2026-11-25T00:00:00Z"},
    ]
    responses = {
        "list_users": [{"Users": users[:1]}, {"Users": users[1:]}],
        "list_groups": [{"Groups": [{"GroupName": "admins"}]}],
        "list_users_in_group": [{"Users": [{"Username": "alice"}]}],
    }

    class ExistingCognitoReads:
        # Only the APIs supported by the original report role are available.
        def get_paginator(self, operation):
            return Mock(paginate=Mock(return_value=responses[operation]))

    client = Mock(return_value=ExistingCognitoReads())
    monkeypatch.setattr(report.boto3, "client", client)

    report.main("", frequency=frequency, environment="prod")

    filename = (
        "Users_prod_monthly_2026-11.xlsx"
        if frequency == "monthly"
        else "Users_prod_2026-11-25.xlsx"
    )
    sheets = pd.read_excel(tmp_path / filename, sheet_name=None, keep_default_na=False)
    assert list(sheets) == ["Sheet1"]
    snapshot = sheets["Sheet1"].set_index("Username")
    assert set(snapshot.index) == {"alice", "bob", "carol"}
    assert snapshot.loc["alice", "custom:last_login"] == "2020-01-02T00:00:00Z"
    assert snapshot.loc["alice", "admins"] == "admins"
    assert snapshot.loc["bob", "custom:last_login"] == ""
    assert snapshot.loc["carol", "admins"] == ""
    client.assert_called_once_with("cognito-idp", region_name="test")
