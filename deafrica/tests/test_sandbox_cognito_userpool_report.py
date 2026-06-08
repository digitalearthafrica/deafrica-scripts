import pandas as pd

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
