import click

google_chat_url = click.option(
    "--google-chat-url",
    help="Google Chat incoming webhook URL to use to send a notification",
    default=None,
)

slack_url = click.option(
    "--slack_url",
    help="Slack url to use to send a notification",
    default=None,
)

update_stac = click.option(
    "--update_stac",
    is_flag=True,
    default=False,
    help="Will fill a special report within all scenes from the source",
)
limit = click.option(
    "--limit",
    "-l",
    help="Limit the number of messages to transfer.",
    default=None,
)
