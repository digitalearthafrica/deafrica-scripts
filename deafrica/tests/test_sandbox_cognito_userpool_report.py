from unittest.mock import MagicMock

import pandas as pd
import pytest

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
    assert row["phone_number_validation_status"] == "valid"
    assert row["custom:timeframe"] == ""


def test_phone_number_details_handles_missing_and_unparseable_values():
    assert report.phone_number_details("") == ("", "missing")
    assert report.phone_number_details(None) == ("", "missing")
    assert report.phone_number_details("not a phone number") == ("", "unparseable")
    assert report.phone_number_details("+99912345") == ("", "unparseable")


def test_phone_number_country_returns_full_country_names():
    assert report.phone_number_details("+254712345678") == ("Kenya", "valid")
    assert report.phone_number_details("+61412345678") == ("Australia", "valid")
    assert report.phone_number_details("+79161234567") == ("Russia", "valid")
    assert report.phone_number_details("+447911123456") == ("Guernsey", "valid")


def test_phone_number_details_preserves_country_for_invalid_numbers():
    assert report.phone_number_details("+22178298515") == (
        "Senegal",
        "invalid length",
    )
    assert report.phone_number_details("+254000000000") == (
        "Kenya",
        "invalid number",
    )


def test_phone_number_country_ignores_non_geographic_numbers():
    assert report.phone_number_details("+80012345678") == ("", "unknown country")


def test_phone_number_country_remains_available_for_existing_callers():
    assert report.phone_number_country("+22178298515") == "Senegal"


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


def mock_google_drive(monkeypatch, tmp_path, existing_files):
    credentials_file = tmp_path / "credentials.json"
    credentials_file.write_text("{}")

    service = MagicMock()
    files = service.files.return_value
    files.list.return_value.execute.return_value = {"files": existing_files}
    upload_result = {"id": "file-id", "webViewLink": "https://drive.example/report"}
    files.create.return_value.execute.return_value = upload_result
    files.update.return_value.execute.return_value = upload_result

    monkeypatch.setattr(
        report.service_account.Credentials,
        "from_service_account_file",
        lambda *args, **kwargs: object(),
    )
    monkeypatch.setattr(report, "build", lambda *args, **kwargs: service)
    monkeypatch.setattr(report, "MediaFileUpload", MagicMock())

    return str(credentials_file), files


def upload_latest(credentials_file):
    return report.upload_report_to_google_drive(
        "Users_prod_2026-06-15.xlsx",
        "Users_prod_latest.xlsx",
        "drive-folder-id",
        credentials_file,
        overwrite_existing=True,
    )


def mock_report_generation(monkeypatch):
    monkeypatch.setattr(report, "required_env", lambda name: "test-value")
    monkeypatch.setattr(report.boto3, "client", lambda *args, **kwargs: object())
    monkeypatch.setattr(report, "fetch_users_from_cognito", lambda *args: [])
    monkeypatch.setattr(report, "add_user_groups", lambda df, *args: df)
    monkeypatch.setattr(report, "write_excel_report", lambda *args: None)


@pytest.mark.parametrize(
    ("existing_files", "method", "unused_method"),
    [
        ([], "create", "update"),
        ([{"id": "existing-file-id"}], "update", "create"),
    ],
)
def test_upload_creates_or_updates_latest_report(
    monkeypatch, tmp_path, existing_files, method, unused_method
):
    credentials_file, files = mock_google_drive(monkeypatch, tmp_path, existing_files)
    upload_latest(credentials_file)

    upload = getattr(files, method)
    upload.assert_called_once()
    getattr(files, unused_method).assert_not_called()
    if method == "create":
        assert upload.call_args.kwargs["body"]["name"] == "Users_prod_latest.xlsx"
    else:
        assert upload.call_args.kwargs["fileId"] == "existing-file-id"


@pytest.mark.parametrize(
    ("environment", "latest_report_name"),
    [
        ("dev", "Users_dev_latest.xlsx"),
        ("prod", "Users_prod_latest.xlsx"),
    ],
)
def test_main_uploads_archive_and_environment_latest_report(
    monkeypatch, environment, latest_report_name
):
    mock_report_generation(monkeypatch)
    uploads = []
    monkeypatch.setattr(
        report,
        "upload_report_to_google_drive",
        lambda *args, **kwargs: uploads.append((args, kwargs)),
    )

    report.main(
        email_address=None,
        google_drive_folder_id="drive-folder-id",
        google_credentials_file="credentials.json",
        environment=environment,
    )

    dated_report = f"Users_{environment}_{report.current_date}.xlsx"
    assert [args[0] for args, _ in uploads] == [dated_report, dated_report]
    assert [(args[1], kwargs) for args, kwargs in uploads] == [
        (dated_report, {}),
        (latest_report_name, {"overwrite_existing": True}),
    ]


def test_upload_rejects_duplicate_latest_reports(monkeypatch, tmp_path):
    credentials_file, files = mock_google_drive(
        monkeypatch,
        tmp_path,
        [{"id": "first-file-id"}, {"id": "second-file-id"}],
    )

    with pytest.raises(
        ValueError,
        match="Multiple Google Drive files named 'Users_prod_latest.xlsx'",
    ):
        upload_latest(credentials_file)

    files.create.assert_not_called()
    files.update.assert_not_called()
