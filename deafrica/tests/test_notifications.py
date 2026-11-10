from unittest.mock import Mock

import click
import pytest
import requests
from click.testing import CliRunner

from deafrica.click_options import google_chat_url, slack_url
from deafrica.utils import send_google_chat_notification, send_slack_notification


def test_google_chat_payload(monkeypatch):
    post = Mock()
    monkeypatch.setattr("deafrica.utils.requests.post", post)

    send_google_chat_notification(
        "https://example.com/chat", "Gap Report", "Missing: `3`\nDetails"
    )

    post.assert_called_once_with(
        "https://example.com/chat",
        json={"text": "Gap Report\n\nMissing: `3`\nDetails"},
        timeout=30,
    )
    post.return_value.raise_for_status.assert_called_once_with()


def test_google_chat_http_error(monkeypatch):
    post = Mock()
    post.return_value.raise_for_status.side_effect = requests.HTTPError("Failed")
    monkeypatch.setattr("deafrica.utils.requests.post", post)

    with pytest.raises(requests.HTTPError):
        send_google_chat_notification("https://example.com/chat", "Report", "Failed")


def test_slack_payload_unchanged(monkeypatch):
    post = Mock()
    monkeypatch.setattr("deafrica.utils.requests.post", post)

    send_slack_notification("https://example.com/slack", "Report", "Missing: `3`")

    post.assert_called_once_with(
        "https://example.com/slack",
        json={
            "text": "Report",
            "blocks": [
                {"type": "section", "text": {"type": "mrkdwn", "text": "Missing: `3`"}}
            ],
        },
    )
    post.return_value.raise_for_status.assert_called_once_with()


@pytest.mark.parametrize(
    "args, expected",
    [
        ([], (None, None)),
        (["--slack_url", "slack"], ("slack", None)),
        (["--google-chat-url", "chat"], (None, "chat")),
        (["--slack_url", "slack", "--google-chat-url", "chat"], ("slack", "chat")),
    ],
)
def test_webhook_options(args, expected):
    received = []

    @click.command()
    @slack_url
    @google_chat_url
    def cli(slack_url, google_chat_url):
        received.append((slack_url, google_chat_url))

    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 0, result.output
    assert received == [expected]
