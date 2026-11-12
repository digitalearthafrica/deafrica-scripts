import json
import os
import re
from datetime import datetime, timedelta, timezone
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import boto3
import click
import pandas as pd
import phonenumbers
from babel import Locale
from phonenumbers import NumberParseException

from deafrica.utils import upload_dated_report_to_google_drive

DEFAULT_SENDER_EMAIL = "info@digitalearthafrica.org"
XLSX_MIME_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
ENGLISH_LOCALE = Locale.parse("en")
REPORT_COLUMNS = [
    "Username",
    "email",
    "phone_number",
    "phone_number_country",
    "given_name",
    "family_name",
    "custom:organisation",
    "gender",
    "custom:age_category",
    "custom:organisation_type",
    "custom:thematic_interest",
    "custom:country",
    "custom:timeframe",
    "custom:source_of_referral",
    "email_verified",
    "phone_number_verified",
    "UserStatus",
    "Enabled",
    "UserCreateDate",
    "UserLastModifiedDate",
    "custom:last_login",
]


def report_date():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def phone_number_country(phone_number):
    phone_number = str(phone_number or "").strip()
    if not phone_number:
        return ""

    try:
        parsed_number = phonenumbers.parse(phone_number, None)
    except NumberParseException:
        return ""

    if not phonenumbers.is_valid_number(parsed_number):
        return ""

    region_code = phonenumbers.region_code_for_number(parsed_number)
    if not region_code or region_code == "001":
        return ""

    return ENGLISH_LOCALE.territories.get(region_code, "")


def report_environment(environment):
    environment = (environment or "").strip().lower()
    if not environment:
        return ""

    if environment not in {"dev", "prod"}:
        raise ValueError("REPORT_ENVIRONMENT must be one of: dev, prod")

    return environment


def required_env(name):
    value = os.getenv(name)
    if not value:
        raise ValueError(f"{name} is not set in the environment variables")
    return value


def paginate_cognito(cognito_client, operation_name, result_key, **kwargs):
    paginator = cognito_client.get_paginator(operation_name)
    for page in paginator.paginate(**kwargs):
        yield from page.get(result_key, [])


def fetch_users_from_cognito(cognito_client, user_pool_id):
    print("Fetching Cognito users...")
    return list(
        paginate_cognito(
            cognito_client,
            "list_users",
            "Users",
            UserPoolId=user_pool_id,
        )
    )


def cognito_value(value):
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def users_to_dataframe(users):
    records = []

    for user in users:
        base = {
            "Username": user.get("Username", ""),
            "UserCreateDate": cognito_value(user.get("UserCreateDate", "")),
            "UserLastModifiedDate": cognito_value(user.get("UserLastModifiedDate", "")),
            "Enabled": user.get("Enabled", ""),
            "UserStatus": user.get("UserStatus", ""),
        }

        for attr in user.get("Attributes", []):
            if attr.get("Name"):
                base[attr["Name"]] = attr.get("Value", "")

        base["phone_number_country"] = phone_number_country(base.get("phone_number"))
        records.append(base)

    df = pd.DataFrame(records)
    if "UserCreateDate" in df.columns:
        df.sort_values(by="UserCreateDate", ascending=True, inplace=True)

    return df.reindex(columns=REPORT_COLUMNS).fillna("")


def fetch_group_names(cognito_client, user_pool_id):
    return [
        group["GroupName"]
        for group in paginate_cognito(
            cognito_client,
            "list_groups",
            "Groups",
            UserPoolId=user_pool_id,
        )
        if group.get("GroupName")
    ]


def fetch_usernames_in_group(cognito_client, user_pool_id, group_name):
    users = paginate_cognito(
        cognito_client,
        "list_users_in_group",
        "Users",
        UserPoolId=user_pool_id,
        GroupName=group_name,
    )
    return {user.get("Username") for user in users if user.get("Username")}


def add_user_groups(df, cognito_client, user_pool_id):
    print("Extracting User groups")
    for group_name in fetch_group_names(cognito_client, user_pool_id):
        print(f"Extracting Users for {group_name}")
        usernames = fetch_usernames_in_group(cognito_client, user_pool_id, group_name)
        df[group_name] = df["Username"].apply(
            lambda username: group_name if username in usernames else ""
        )

    return df


def write_excel_report(report_path, df):
    df.to_excel(report_path, index=False)


def weekly_period(report_day):
    """Return the previous complete Monday-to-Monday week in UTC."""
    day = datetime.fromisoformat(report_day).replace(tzinfo=timezone.utc)
    end = day - timedelta(days=day.weekday())
    return end - timedelta(days=7), end


def utc_datetime(value):
    if not value:
        return None
    if not isinstance(value, datetime):
        try:
            value = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def login_from_log_event(event):
    """Read the existing post-authentication Lambda's login records."""
    message = event.get("message", "").strip()
    if message.startswith("{"):
        try:
            message = json.loads(message).get("message", "")
        except (ValueError, AttributeError):
            return None

    updated = re.search(r"Updated last_login for (.+) to (\S+)", message)
    if updated:
        username, value = updated.groups()
        timestamp = utc_datetime(value)
        if timestamp is None:
            raise ValueError("A recorded login has an invalid timestamp")
        return username, timestamp

    # The trigger returns the successful authentication even if the profile update fails.
    failed = re.search(r"Failed to update last_login for (.+?): ", message)
    if failed:
        return failed.group(1), datetime.fromtimestamp(
            event["timestamp"] / 1000, tz=timezone.utc
        )
    return None


def fetch_weekly_login_activity(cognito_client, logs_client, user_pool_id, start, end):
    pool = cognito_client.describe_user_pool(UserPoolId=user_pool_id)["UserPool"]
    function_arn = pool.get("LambdaConfig", {}).get("PostAuthentication", "")
    if ":function:" not in function_arn:
        raise ValueError(
            "The user pool has no post-authentication Lambda for login history"
        )
    function_name = function_arn.split(":function:", 1)[1].split(":", 1)[0]
    log_group = f"/aws/lambda/{function_name}"

    groups = logs_client.get_paginator("describe_log_groups").paginate(
        logGroupNamePrefix=log_group
    )
    group = next(
        (
            item
            for page in groups
            for item in page.get("logGroups", [])
            if item["logGroupName"] == log_group
        ),
        None,
    )
    if group is None:
        raise ValueError(
            "The login log group does not exist; cannot report weekly activity"
        )
    if group["creationTime"] > int(start.timestamp() * 1000):
        raise ValueError("The login log group does not cover the full reporting week")
    retention = group.get("retentionInDays")
    if retention and start < datetime.now(timezone.utc) - timedelta(days=retention):
        raise ValueError("Login log retention does not cover the full reporting week")

    activity = {}
    seen_ids = set()
    pages = logs_client.get_paginator("filter_log_events").paginate(
        logGroupName=log_group,
        startTime=int(start.timestamp() * 1000),
        # A sign-in just before midnight can be logged after its profile update finishes.
        endTime=int((end + timedelta(minutes=10)).timestamp() * 1000) - 1,
        filterPattern='"last_login"',
    )
    for page in pages:
        for event in page.get("events", []):
            event_id = event.get("eventId")
            if event_id and event_id in seen_ids:
                continue
            if event_id:
                seen_ids.add(event_id)
            login = login_from_log_event(event)
            if login is None:
                continue
            username, timestamp = login
            if not start <= timestamp < end:
                continue
            record = activity.setdefault(
                username, {"count": 0, "first": timestamp, "last": timestamp}
            )
            record["count"] += 1
            record["first"] = min(record["first"], timestamp)
            record["last"] = max(record["last"], timestamp)
    print(f"Found {len(activity)} users with recorded sign-ins in the reporting week")
    return activity


def write_weekly_excel_report(report_path, df, activity, start, end, report_day):
    users_by_name = {row["Username"]: row.to_dict() for _, row in df.iterrows()}
    active_rows = []
    for username, login in sorted(activity.items()):
        record = users_by_name.get(username, dict.fromkeys(df.columns, "")).copy()
        record.update(
            Username=username,
            AccountPresentAtReport=username in users_by_name,
            LoginsInWeek=login["count"],
            FirstLoginInWeek=login["first"].isoformat(),
            LastLoginInWeek=login["last"].isoformat(),
        )
        active_rows.append(record)
    active = pd.DataFrame(
        active_rows,
        columns=[
            *df.columns,
            "AccountPresentAtReport",
            "LoginsInWeek",
            "FirstLoginInWeek",
            "LastLoginInWeek",
        ],
    )
    created = df["UserCreateDate"].map(utc_datetime)
    new_users = df.loc[
        created.map(lambda value: value is not None and start <= value < end)
    ]
    summary = pd.DataFrame(
        [
            ("Reporting week start (UTC, inclusive)", start.isoformat()),
            ("Reporting week end (UTC, exclusive)", end.isoformat()),
            ("Report generated on (UTC)", report_day),
            ("Users with recorded sign-ins", len(active)),
            (
                "Recorded sign-ins during week",
                sum(row["count"] for row in activity.values()),
            ),
            ("New users registered during week", len(new_users)),
            ("Total registered users at report generation", len(df)),
            ("Accounts missing a valid creation date", int(created.isna().sum())),
            (
                "Activity definition",
                "Recorded Cognito sign-ins; ongoing sessions are not measured",
            ),
            (
                "Activity scope",
                "Shared Cognito pool: includes Sandbox, Argo and Grafana sign-ins",
            ),
            (
                "Registration coverage",
                "Accounts still present when the report was generated",
            ),
        ],
        columns=["Metric", "Value"],
    )
    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="Summary", index=False)
        active.to_excel(writer, sheet_name="Active users", index=False)
        new_users.to_excel(writer, sheet_name="New users", index=False)
        df.to_excel(writer, sheet_name="All users", index=False)
        writer.sheets["Summary"].column_dimensions["A"].width = 48
        writer.sheets["Summary"].column_dimensions["B"].width = 90
        for sheet in writer.sheets.values():
            sheet.freeze_panes = "A2"
        for name in ["Active users", "New users", "All users"]:
            writer.sheets[name].auto_filter.ref = writer.sheets[name].dimensions


def send_email_with_attachment(recipient_email, report_path, sender_email, ses_client):
    # Prepare email message
    msg = MIMEMultipart()
    msg["From"] = sender_email
    msg["To"] = recipient_email
    msg["Subject"] = "Cognito Users Report"

    # Attach the body of the email
    body = "Please find the attached Excel file containing the list of Cognito users."
    msg.attach(MIMEText(body, "plain"))

    # Attach the Excel file
    with open(report_path, "rb") as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        attachment_name = Path(report_path).name
        part.add_header(
            "Content-Disposition", f"attachment; filename={attachment_name}"
        )
        msg.attach(part)

    # Send the email via AWS SES
    try:
        ses_client.send_raw_email(
            Source=sender_email,
            Destinations=[recipient_email],
            RawMessage={"Data": msg.as_string()},
        )
        print("Email sent successfully!")
    except Exception as e:
        print(f"Error sending email via SES: {e}")


def upload_report_to_google_drive(
    report_path,
    report_name,
    google_drive_folder_id,
    google_credentials_file,
    report_day=None,
    latest_name=None,
):
    return upload_dated_report_to_google_drive(
        report_path,
        report_name,
        google_drive_folder_id,
        google_credentials_file,
        report_day or report_date(),
        XLSX_MIME_TYPE,
        latest_name=latest_name,
    )


def main(
    email_address,
    google_drive_folder_id=None,
    google_credentials_file=None,
    environment="",
    publish_latest=False,
    frequency="weekly",
):
    if frequency not in {"weekly", "monthly"}:
        raise ValueError("REPORT_FREQUENCY must be one of: weekly, monthly")
    aws_region_cognito = required_env("aws_region_cognito")
    aws_region_ses = required_env("aws_region_ses")
    user_pool_id = required_env("user_pool_id")
    sender_email = os.getenv("SENDER_EMAIL", DEFAULT_SENDER_EMAIL)
    environment = report_environment(environment)
    environment_suffix = f"_{environment}" if environment else ""
    report_day = report_date()
    report_prefix = f"Users{environment_suffix}"
    if frequency == "monthly":
        report_prefix += "_monthly"
        report_name = f"{report_prefix}_{report_day[:7]}.xlsx"
    else:
        report_name = f"{report_prefix}_{report_day}.xlsx"
    latest_name = f"{report_prefix}_latest.xlsx" if publish_latest else None
    report_path = report_name

    cognito_client = boto3.client("cognito-idp", region_name=aws_region_cognito)
    users = fetch_users_from_cognito(cognito_client, user_pool_id)
    df = users_to_dataframe(users)
    df = add_user_groups(df, cognito_client, user_pool_id)
    print(f"Writing Excel report: {report_path}...")
    if frequency == "weekly":
        start, end = weekly_period(report_day)
        logs_client = boto3.client("logs", region_name=aws_region_cognito)
        activity = fetch_weekly_login_activity(
            cognito_client, logs_client, user_pool_id, start, end
        )
        write_weekly_excel_report(report_path, df, activity, start, end, report_day)
    else:
        write_excel_report(report_path, df)

    if email_address:
        print(f"Sending email with attached Excel report...")
        ses_client = boto3.client("ses", region_name=aws_region_ses)
        send_email_with_attachment(
            email_address,
            report_path,
            sender_email,
            ses_client,
        )
    else:
        print("Skipping email because no recipient email address was provided")

    if google_drive_folder_id or google_credentials_file:
        upload_report_to_google_drive(
            report_path,
            report_name,
            google_drive_folder_id,
            google_credentials_file,
            report_day=report_day,
            latest_name=latest_name,
        )
    else:
        print("Skipping Google Drive upload")


@click.command("sandbox-users-report")
@click.option("--email", envvar="REPORT_EMAIL", help="Recipient's Email Address")
@click.option("--google-drive-folder-id", envvar="GOOGLE_DRIVE_FOLDER_ID")
@click.option("--google-credentials-file", envvar="GOOGLE_APPLICATION_CREDENTIALS")
@click.option("--environment", envvar="REPORT_ENVIRONMENT", default="")
@click.option(
    "--frequency",
    envvar="REPORT_FREQUENCY",
    type=click.Choice(["weekly", "monthly"]),
    default="weekly",
    help="Use dated weekly filenames or clearly labelled monthly snapshots.",
)
@click.option(
    "--publish-latest/--no-publish-latest",
    envvar="REPORT_PUBLISH_LATEST",
    default=False,
    help="Update the latest user file for this frequency as well as its archive.",
)
def cli(
    email,
    google_drive_folder_id,
    google_credentials_file,
    environment,
    publish_latest,
    frequency,
):
    main(
        email,
        google_drive_folder_id,
        google_credentials_file,
        environment,
        publish_latest=publish_latest,
        frequency=frequency,
    )


if __name__ == "__main__":
    cli()
