import json
import csv
import boto3
import click
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
import subprocess
import os

AWS_REGION_COGNITO = "us-west-2"  # AWS region for Cognito User Pool
AWS_REGION_SES = "af-south-1"  # AWS region for SES
USER_POOL_ID = "us-west-2_v9nJrst3o"  # User pool ID from environment

# Email Configuration
SENDER_EMAIL = "info@digitalearthafrica.org"  # SES-verified sender email
RECEIVER_EMAIL = "kenneth.mubea@digitalearthafrica.org"

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

    # Open a CSV file for writing
    with open(csv_filename, mode="w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(
            [
                "Name",
                "Email",
                "Email Verified",
                "Phone Number Verified",
                "MFA Enabled",
                "Username",
                "User Status",
                "Enabled",
                "User Create Date",
                "User Last Modified Date",
            ]
        )  # Add more headers as needed

        # Write the user data to the CSV
        for user in data["Users"]:
            username = user["Username"]
            name = next(
                (
                    attr["Value"]
                    for attr in user["Attributes"]
                    if attr["Name"] == "name"
                ),
                None,
            )
            email = next(
                (
                    attr["Value"]
                    for attr in user["Attributes"]
                    if attr["Name"] == "email"
                ),
                None,
            )
            email_verified = next(
                (
                    attr["Value"]
                    for attr in user["Attributes"]
                    if attr["Name"] == "email_verified"
                ),
                "FALSE",
            )
            phone_number_verified = next(
                (
                    attr["Value"]
                    for attr in user["Attributes"]
                    if attr["Name"] == "phone_number_verified"
                ),
                "FALSE",
            )
            mfa_enabled = "FALSE"

            # Extract additional fields
            user_status = user["UserStatus"]
            enabled = user["Enabled"]
            user_create_date = user["UserCreateDate"]
            user_last_modified_date = user["UserLastModifiedDate"]

            # Write the row to the CSV
            writer.writerow(
                [
                    name,
                    email,
                    email_verified,
                    phone_number_verified,
                    mfa_enabled,
                    username,
                    user_status,
                    enabled,
                    user_create_date,
                    user_last_modified_date,
                ]
            )


def send_email_with_attachment(csv_filename):
    # Prepare email message
    msg = MIMEMultipart()
    msg["From"] = SENDER_EMAIL
    msg["To"] = RECEIVER_EMAIL
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
            Destinations=[RECEIVER_EMAIL],
            RawMessage={"Data": msg.as_string()},
        )
        print("Email sent successfully!")
    except Exception as e:
        print(f"Error sending email via SES: {e}")


def main():
    # Fetch users from AWS Cognito and save to Users.json
    fetch_users_from_aws()

    # Convert the fetched JSON to CSV
    json_filename = "Users.json"
    csv_filename = f"Users_{current_date}.csv"

    print(f"Converting JSON to CSV file: {csv_filename}...")
    convert_json_to_csv(json_filename, csv_filename)

    # Send the CSV as an email attachment
    print(f"Sending email with attached CSV report...")
    send_email_with_attachment(csv_filename)


@click.command("sandbox-users-report")
def cli():
    main()


if __name__ == "__main__":
    cli()
