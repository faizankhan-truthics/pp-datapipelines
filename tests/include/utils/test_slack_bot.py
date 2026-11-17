"""Tests for include/utils/slack_bot.py: the chat.postMessage request and Slack's ok/error reply."""

import io
import json

import pytest

import include.utils.slack_bot as slack_bot


class _FakeResponse(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _capture(monkeypatch, reply: dict) -> list:
    sent = []

    def fake_urlopen(req, timeout):
        sent.append(req)
        return _FakeResponse(json.dumps(reply).encode("utf-8"))

    monkeypatch.setattr(slack_bot.urllib.request, "urlopen", fake_urlopen)
    return sent


def test_posts_to_channel_with_bearer_token(monkeypatch):
    sent = _capture(monkeypatch, {"ok": True, "ts": "1.2"})

    body = slack_bot.post_message(token="xoxb-test", channel="C123", text="hello")

    assert body["ok"] is True
    req = sent[0]
    assert req.full_url == slack_bot.POST_MESSAGE_URL
    assert req.get_header("Authorization") == "Bearer xoxb-test"
    assert json.loads(req.data) == {
        "channel": "C123",
        "text": "hello",
        "unfurl_links": False,
    }


def test_thread_reply_sets_thread_ts(monkeypatch):
    sent = _capture(monkeypatch, {"ok": True, "ts": "1.3"})

    slack_bot.post_message(
        token="xoxb-test", channel="C123", text="reply", thread_ts="1.2"
    )

    assert json.loads(sent[0].data)["thread_ts"] == "1.2"


def test_slack_error_reply_raises(monkeypatch):
    _capture(monkeypatch, {"ok": False, "error": "not_in_channel"})

    with pytest.raises(RuntimeError, match="not_in_channel"):
        slack_bot.post_message(token="xoxb-test", channel="C123", text="hello")


@pytest.mark.parametrize("token,channel", [("", "C123"), ("xoxb-test", "")])
def test_missing_token_or_channel_raises_before_calling_slack(
    monkeypatch, token, channel
):
    sent = _capture(monkeypatch, {"ok": True})

    with pytest.raises(RuntimeError):
        slack_bot.post_message(token=token, channel=channel, text="hello")
    assert sent == []
