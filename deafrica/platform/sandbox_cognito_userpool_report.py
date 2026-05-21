import os
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import boto3
import click
import pandas as pd
import phonenumbers
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from phonenumbers import NumberParseException, geocoder

DEFAULT_SENDER_EMAIL = "info@digitalearthafrica.org"
GOOGLE_DRIVE_SCOPE = "https://www.googleapis.com/auth/drive.file"
XLSX_MIME_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
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

# Get the current date in YYYY-MM-DD format
current_date = datetime.now().strftime("%Y-%m-%d")


def phone_number_country(phone_number):
    phone_number = str(phone_number or "").strip()
    if not phone_number:
        return ""

    try:
        parsed_number = phonenumbers.parse(phone_number, None)
    except NumberParseException:
        return ""

    return geocoder.country_name_for_number(parsed_number, "en") or ""


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
    report_path, report_name, google_drive_folder_id, google_credentials_file
):
    if not google_drive_folder_id:
        raise ValueError("GOOGLE_DRIVE_FOLDER_ID is required for Google Drive upload")

    if not google_credentials_file:
        raise ValueError(
            "GOOGLE_APPLICATION_CREDENTIALS is required for Google Drive upload"
        )

    credentials_path = Path(google_credentials_file)
    if not credentials_path.exists():
        raise FileNotFoundError(
            f"Google credentials file does not exist: {credentials_path}"
        )

    credentials = service_account.Credentials.from_service_account_file(
        credentials_path,
        scopes=[GOOGLE_DRIVE_SCOPE],
    )
    service = build("drive", "v3", credentials=credentials, cache_discovery=False)

    file_metadata = {
        "name": report_name,
        "parents": [google_drive_folder_id],
    }
    media = MediaFileUpload(report_path, mimetype=XLSX_MIME_TYPE, resumable=False)

    uploaded_file = (
        service.files()
        .create(
            body=file_metadata,
            media_body=media,
            fields="id, webViewLink",
            supportsAllDrives=True,
        )
        .execute()
    )

    print(f"Uploaded report to Google Drive file id {uploaded_file.get('id')}")
    print(f"Google Drive report link: {uploaded_file.get('webViewLink')}")
    return uploaded_file


def main(
    email_address,
    google_drive_folder_id=None,
    google_credentials_file=None,
    environment="",
):
    aws_region_cognito = required_env("aws_region_cognito")
    aws_region_ses = required_env("aws_region_ses")
    user_pool_id = required_env("user_pool_id")
    sender_email = os.getenv("SENDER_EMAIL", DEFAULT_SENDER_EMAIL)
    environment = report_environment(environment)
    environment_suffix = f"_{environment}" if environment else ""
    report_name = f"Users{environment_suffix}_{current_date}.xlsx"
    report_path = report_name

    cognito_client = boto3.client("cognito-idp", region_name=aws_region_cognito)
    users = fetch_users_from_cognito(cognito_client, user_pool_id)
    df = users_to_dataframe(users)
    df = add_user_groups(df, cognito_client, user_pool_id)
    print(f"Writing Excel report: {report_path}...")
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
        )
    else:
        print("Skipping Google Drive upload")


@click.command("sandbox-users-report")
@click.option("--email", envvar="REPORT_EMAIL", help="Recipient's Email Address")
@click.option("--google-drive-folder-id", envvar="GOOGLE_DRIVE_FOLDER_ID")
@click.option("--google-credentials-file", envvar="GOOGLE_APPLICATION_CREDENTIALS")
@click.option("--environment", envvar="REPORT_ENVIRONMENT", default="")
def cli(email, google_drive_folder_id, google_credentials_file, environment):
    main(email, google_drive_folder_id, google_credentials_file, environment)


if __name__ == "__main__":
    cli()
