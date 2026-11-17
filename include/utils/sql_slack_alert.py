"""Generic SQL-driven Slack alerts: run a SQL file and post its (title, message) row to Slack."""

import json
import urllib.request
from pathlib import Path

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from jinja2 import Template

# Slack caps a section block's text at 3000 chars; chunk so long messages
# render as multiple blocks instead of erroring or getting truncated.
SLACK_SECTION_CHAR_LIMIT = 2900


def run_sql_alert(
    *,
    sql_file: Path,
    webhook_url: str,
    gcp_conn_id: str,
    bq_location: str,
    template_params: dict,
) -> None:
    """Run a SQL file that returns a single (title, message) row and post it to Slack.

    All alert-specific logic (thresholds, grouping, wording) lives in the SQL
    file itself -- it renders its own Slack message text. This function is
    generic: a new alert only needs a new SQL file shaped the same way (a
    `title` + `message` column, no rows when there's nothing to alert on)
    plus a DAG task that calls this, instead of a new copy of this Python.
    """
    if not webhook_url:
        raise RuntimeError(f"Slack webhook URL is not set for {sql_file.name}.")

    raw_sql = sql_file.read_text(encoding="utf-8").strip().rstrip(";")
    query = Template(raw_sql).render(params=template_params)

    hook = BigQueryHook(
        gcp_conn_id=gcp_conn_id, use_legacy_sql=False, location=bq_location
    )
    hook.log_sql = (
        False  # the rendered query is the whole SQL file; keep it out of task logs
    )
    rows = hook.get_records(sql=query)

    if not rows:
        print(f"No alert rows from {sql_file.name}; nothing to send.")
        return

    title, message = rows[0]

    blocks = [{"type": "header", "text": {"type": "plain_text", "text": title[:150]}}]
    for chunk_start in range(0, len(message), SLACK_SECTION_CHAR_LIMIT):
        chunk = message[chunk_start : chunk_start + SLACK_SECTION_CHAR_LIMIT]
        blocks.append(
            {"type": "section", "text": {"type": "mrkdwn", "text": f"```{chunk}```"}}
        )

    payload = json.dumps({"text": title, "blocks": blocks}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        print(f"Slack webhook responded HTTP {resp.status}")
