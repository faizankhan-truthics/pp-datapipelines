"""Tests for the daily VAMP Slack report DAG (dags/stripe/stripe_vamp_alerts.py, dag_id
`daily_vamp_status`).

Its schedule Asset (dbt://bigquery_dbt/marts) is already pinned to dbt_dag's emitted URI
in tests/dags/test_dbt_dag.py, so this covers what's left: the one task's behaviour.
  * non-green rows from the gold VAMP table -> one Slack line each, status emoji,
    ratios as percentages.
  * no rows -> an explicit "no alerts" message is still posted (silence would be
    indistinguishable from the DAG not running).
  * a missing webhook URL fails the task loudly instead of quietly posting nowhere.
  * the query only pulls non-green rows from the env's VAMP table.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

import dags.stripe.stripe_vamp_alerts as vamp

WEBHOOK = "https://hooks.slack.test/vamp"


@pytest.fixture(scope="module")
def dag():
    return vamp.dbt_slack_status()


@pytest.fixture
def run_task(dag, monkeypatch):
    """Run build_slack_message with BigQuery returning `rows`; return the posted Slack text."""

    def _run(rows, webhook=WEBHOOK):
        hook_calls = []
        posted = []

        class FakeHook:
            def __init__(self, **kwargs):
                hook_calls.append(kwargs)

            def get_records(self, sql):
                hook_calls.append(sql)
                return rows

        class FakeResponse:
            status = 200

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        def fake_urlopen(req, timeout):
            posted.append((req.full_url, json.loads(req.data.decode("utf-8")), timeout))
            return FakeResponse()

        monkeypatch.setattr(vamp, "BigQueryHook", FakeHook)
        monkeypatch.setattr(vamp, "VAMP_ALERTS_WEBHOOK", webhook)
        monkeypatch.setattr(vamp.urllib.request, "urlopen", fake_urlopen)
        dag.get_task("build_slack_message").python_callable()
        return SimpleNamespace(hook_calls=hook_calls, posted=posted)

    return _run


def _row(
    status="red",
    account="acme",
    ratio=0.0123,
    eom_ratio=0.0456,
    reason="over threshold",
):
    return (account, "ACME*SHOP", "2026-09", 12, ratio, 30, eom_ratio, status, reason)


class TestDagDefinition:
    def test_single_task_dag_with_retries(self, dag):
        assert dag.dag_id == "daily_vamp_status"
        assert list(dag.task_dict) == ["build_slack_message"]
        assert dag.default_args["retries"] == 2

    def test_query_excludes_green_rows_and_targets_the_vamp_estimate_table(self):
        assert "WHERE vamp_status != 'green'" in vamp.BQ_QUERY
        assert f"FROM `{vamp.VAMP_TABLE_NAME}`" in vamp.BQ_QUERY
        assert vamp.VAMP_TABLE_NAME.endswith(".stripe_monthly_vamp_estimate")


class TestBuildSlackMessage:
    def test_posts_one_line_per_row_with_status_emoji_and_percentages(self, run_task):
        result = run_task(
            [_row("red"), _row("yellow", account="globex", ratio=0.005, eom_ratio=0.01)]
        )

        [(url, payload, timeout)] = result.posted
        assert url == WEBHOOK
        assert timeout == 30
        text = payload["text"]
        assert text.startswith(f"*{vamp.ENV} Daily VAMP Status Report*\n\n")
        lines = text.split("\n\n", 1)[1].split("\n")
        assert len(lines) == 2
        assert lines[0].startswith("🔴 *2026-09* | acme | ACME*SHOP")
        assert "VAMP: 12 (1.23%)" in lines[0]
        assert "EOM: 30 (4.56%)" in lines[0]
        assert "Comment: over threshold" in lines[0]
        assert lines[1].startswith("🟡 *2026-09* | globex")
        assert "VAMP: 12 (0.50%)" in lines[1]

    @pytest.mark.parametrize("status", ["RED", "Red"])
    def test_status_match_is_case_insensitive(self, run_task, status):
        text = run_task([_row(status)]).posted[0][1]["text"]

        assert "🔴" in text

    def test_unknown_status_falls_back_to_default_emoji(self, run_task):
        text = run_task([_row("purple")]).posted[0][1]["text"]

        assert f"{vamp.DEFAULT_EMOJI} *2026-09*" in text

    def test_no_rows_still_posts_an_explicit_no_alerts_message(self, run_task):
        text = run_task([]).posted[0][1]["text"]

        assert "No VAMP alerts available for this run." in text

    @pytest.mark.parametrize("webhook", [None, ""])
    def test_missing_webhook_fails_loudly_without_posting(self, run_task, webhook):
        with pytest.raises(RuntimeError, match="webhook"):
            run_task([_row()], webhook=webhook)

    def test_queries_bigquery_with_the_configured_connection(self, run_task):
        result = run_task([])

        hook_kwargs, sql = result.hook_calls
        assert hook_kwargs == {
            "gcp_conn_id": vamp.GCP_CONN_ID,
            "use_legacy_sql": False,
            "location": vamp.BQ_LOCATION,
        }
        assert sql == vamp.BQ_QUERY
