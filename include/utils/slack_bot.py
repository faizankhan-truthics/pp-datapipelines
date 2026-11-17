"""Post messages and files as a Slack app bot with a bot token (xoxb-...).

Unlike an incoming webhook, one bot token can post to any channel the bot was invited to,
so the channel is chosen per message.
"""

import json
import urllib.parse
import urllib.request

POST_MESSAGE_URL = "https://slack.com/api/chat.postMessage"
GET_UPLOAD_URL = "https://slack.com/api/files.getUploadURLExternal"
COMPLETE_UPLOAD_URL = "https://slack.com/api/files.completeUploadExternal"


def post_message(
    *,
    token: str,
    channel: str,
    text: str,
    blocks: list[dict] | None = None,
    thread_ts: str | None = None,
) -> dict:
    """Post to `channel` (a channel ID) and return Slack's response (its "ts" identifies the
    message, e.g. to reply in its thread).

    `text` is Slack mrkdwn; with `blocks` (Block Kit) it is only the notification / fallback
    text and the blocks are what the channel shows. With `thread_ts` the message is a reply in
    that message's thread.

    Slack answers HTTP 200 even when it rejects a message, with {"ok": false, "error": ...};
    that case raises, so a missing invite or a bad channel ID fails the task instead of
    passing silently.
    """
    if not token:
        raise RuntimeError("Slack bot token is not set.")
    if not channel:
        raise RuntimeError("Slack channel ID is not set.")

    message = {"channel": channel, "text": text, "unfurl_links": False}
    if blocks:
        message["blocks"] = blocks
    if thread_ts:
        message["thread_ts"] = thread_ts
    payload = json.dumps(message).encode("utf-8")
    req = urllib.request.Request(
        POST_MESSAGE_URL,
        data=payload,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = json.loads(resp.read().decode("utf-8"))

    if not body.get("ok"):
        raise RuntimeError(
            f"Slack chat.postMessage to {channel} failed: {body.get('error')}"
        )
    return body


def _call(method_url: str, token: str, data: bytes, content_type: str) -> dict:
    req = urllib.request.Request(
        method_url,
        data=data,
        headers={"Content-Type": content_type, "Authorization": f"Bearer {token}"},
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        body = json.loads(resp.read().decode("utf-8"))
    if not body.get("ok"):
        raise RuntimeError(
            f"Slack {method_url.rsplit('/', 1)[-1]} failed: {body.get('error')}"
        )
    return body


def upload_file(
    *,
    token: str,
    channel: str,
    filename: str,
    content: bytes,
    title: str | None = None,
    initial_comment: str | None = None,
) -> dict:
    """Upload `content` as a file to `channel`, with `initial_comment` (mrkdwn) as its message.

    Slack's three-step upload: get an upload URL, send the bytes there, then complete the upload
    to share it in the channel. The bot needs the files:write scope (error missing_scope
    otherwise).
    """
    if not token:
        raise RuntimeError("Slack bot token is not set.")
    if not channel:
        raise RuntimeError("Slack channel ID is not set.")

    upload = _call(
        GET_UPLOAD_URL,
        token,
        urllib.parse.urlencode({"filename": filename, "length": len(content)}).encode(),
        "application/x-www-form-urlencoded",
    )
    req = urllib.request.Request(upload["upload_url"], data=content, method="POST")
    with urllib.request.urlopen(req, timeout=120) as resp:
        if resp.status != 200:
            raise RuntimeError(f"Slack file upload failed: HTTP {resp.status}")

    complete = {
        "files": [{"id": upload["file_id"], "title": title or filename}],
        "channel_id": channel,
    }
    if initial_comment:
        complete["initial_comment"] = initial_comment
    return _call(
        COMPLETE_UPLOAD_URL,
        token,
        json.dumps(complete).encode("utf-8"),
        "application/json; charset=utf-8",
    )
