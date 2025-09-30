import csv
import json
import os
import subprocess
import pandas as pd
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import boto3
import click

# Read AWS region and User Pool ID from environment variables
AWS_REGION_COGNITO = os.getenv("aws_region_cognito")
AWS_REGION_SES = os.getenv("aws_region_ses")
USER_POOL_ID = os.getenv("user_pool_id")
# Check that all required variables are set
if not AWS_REGION_COGNITO:
    raise ValueError("aws_region_cognito is not set in the environment variables")

if not AWS_REGION_SES:
    raise ValueError("aws_region_ses is not set in the environment variables")

if not USER_POOL_ID:
    raise ValueError("user_pool_id is not set in the environment variables")

# Email Configuration
SENDER_EMAIL = "info@digitalearthafrica.org"  # SES-verified sender email

# Get the current date in YYYY-MM-DD format
current_date = datetime.now().strftime("%Y-%m-%d")

# Initialize the SES client using Boto3
ses_client = boto3.client("ses", region_name=AWS_REGION_SES)


def fetch_users_from_aws():
    # Run AWS CLI command to fetch users from AWS Cognito
    print("Fetching Cognito users...")
    aws_command = [
        "aws",
        "cognito-idp",
        "list-users",
        "--user-pool-id",
        USER_POOL_ID,
        "--region",
        AWS_REGION_COGNITO,
    ]

    # Execute AWS CLI command and capture output as JSON
    result = subprocess.run(aws_command, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"Error fetching users: {result.stderr}")
        exit(1)

    # Write the result to a JSON file
    with open("Users.json", "w") as json_file:
        json_file.write(result.stdout)


def convert_json_to_csv(json_filename, csv_filename):
    # Load the JSON data from file
    with open(json_filename) as json_file:
        data = json.load(json_file)

    # Flatten the JSON into a DataFrame
    records = []

    for user in data["Users"]:
        base = {
            "Username": user["Username"],
            "UserCreateDate": user["UserCreateDate"],
            "UserLastModifiedDate": user["UserLastModifiedDate"],
            "Enabled": user["Enabled"],
            "UserStatus": user["UserStatus"],
        }

        # Flatten Attributes
        for attr in user["Attributes"]:
            base[attr["Name"]] = attr["Value"]

        records.append(base)

    # Convert to DataFrame
    df = pd.DataFrame(records)
    df.sort_values(by="UserCreateDate", ascending=True, inplace=True)
    # Rearrange Columns
    df = df[
        [
            "Username",
            "email",
            "phone_number",
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
    ]

    # Export whole user attributes
    df.to_excel(csv_filename, index=False)


def user_groups(csv_filename):
    # Extract user groups
    print("Extracting User groups")
    aws_user_groups = [
        "aws",
        "cognito-idp",
        "list-groups",
        "--user-pool-id",
        USER_POOL_ID,
        "--region",
        AWS_REGION_COGNITO,
        "--query",
        "Groups[*].GroupName",
    ]

    result_user_command = subprocess.run(
        aws_user_groups, capture_output=True, text=True
    )
    result_user_groups = json.loads(result_user_command.stdout)

    # Extract users in each group
    user_groups_dict = {}
    for i, j in enumerate(result_user_groups):
        print(f"Extracting Users for {j}")
        aws_group_users = [
            "aws",
            "cognito-idp",
            "list-users-in-group",
            "--user-pool-id",
            USER_POOL_ID,
            "--region",
            AWS_REGION_COGNITO,
            "--query",
            "Users[*].Username",
            "--group-name",
            j,
        ]

        result_group_users_command = subprocess.run(
            aws_group_users, capture_output=True, text=True
        )
        result_group_users = json.loads(result_group_users_command.stdout)
        result_group_users_name = [j] * len(result_group_users)
        df_group = dict(zip(result_group_users, result_group_users_name))
        df_group = pd.DataFrame(list(df_group.items()), columns=["user_id", j])
        df_in = pd.read_excel(csv_filename)
        result = pd.merge(
            df_in, df_group, left_on="Username", right_on="user_id", how="left"
        )
        result = result.drop(columns=["user_id"])
        result.to_excel(csv_filename, index=False)


def send_email_with_attachment(recipient_email, csv_filename):
    # Prepare email message
    msg = MIMEMultipart()
    msg["From"] = SENDER_EMAIL
    msg["To"] = recipient_email
    msg["Subject"] = "Cognito Users Report"

    # Attach the body of the email
    body = "Please find the attached CSV file containing the list of Cognito users."
    msg.attach(MIMEText(body, "plain"))

    # Attach the CSV file
    with open(csv_filename, "rb") as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={csv_filename}")
        msg.attach(part)

    # Send the email via AWS SES
    try:
        response = ses_client.send_raw_email(
            Source=SENDER_EMAIL,
            Destinations=[recipient_email],
            RawMessage={"Data": msg.as_string()},
        )
        print("Email sent successfully!")
    except Exception as e:
        print(f"Error sending email via SES: {e}")


def main(email_address):
    # Fetch users from AWS Cognito and save to Users.json
    fetch_users_from_aws()

    # Convert the fetched JSON to Excel file
    json_filename = "Users.json"
    csv_filename = f"Users_{current_date}.xlsx"

    print(f"Converting JSON to CSV file: {csv_filename}...")
    convert_json_to_csv(json_filename, csv_filename)

    # Fetch user groups from AWS Cognito
    # extract_user_groups()
    user_groups(csv_filename)

    # Send the CSV as an email attachment
    print(f"Sending email with attached CSV report...")
    send_email_with_attachment(email_address, csv_filename)


@click.command("sandbox-users-report")
@click.option("--email", help="Recipient's Email Address", required=True)
def cli(email):
    main(email)


if __name__ == "__main__":
    cli()
