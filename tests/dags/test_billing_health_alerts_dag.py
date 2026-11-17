"""Tests for dags/billing_health/billing_health_alerts_dag.py (dag_id `billing_health_alerts`).

What can break here is the wiring: task order, the SQL files and their placeholders, which
day is reported, where messages go and who is tagged.
"""

from __future__ import annotations

import re
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import dags.billing_health.billing_health_alerts_dag as bh

REPO_SQL_DIR = Path(__file__).parents[2] / "include" / "sql" / "billing_health"


@pytest.fixture(scope="module")
def dag():
    return bh.billing_health_alerts()


def test_dag_definition(dag):
    assert dag.dag_id == "billing_health_alerts"
    assert set(dag.task_dict) == {
        "wait_for_data",
        "evaluate",
        "send_alerts",
        "update_sheet",
        "upload_report",
    }
    assert dag.get_task("evaluate").upstream_task_ids == {"wait_for_data"}
    assert dag.get_task("send_alerts").upstream_task_ids == {
        "wait_for_data",
        "evaluate",
    }
    assert dag.get_task("update_sheet").upstream_task_ids == {
        "wait_for_data",
        "evaluate",
    }
    # the Excel file comes after the section messages, with the sheet link
    assert dag.get_task("upload_report").upstream_task_ids == {
        "wait_for_data",
        "evaluate",
        "send_alerts",
        "update_sheet",
    }
    assert dag.max_active_runs == 1
    assert dag.catchup is False


@pytest.mark.parametrize(
    "sql_file",
    [bh.STATUS_SQL, bh.DAILY_SQL, bh.FRESHNESS_SQL, bh.RETRY_SQL, bh.TRIALS_SQL],
)
def test_sql_files_exist_and_use_only_known_params(sql_file):
    sql = (REPO_SQL_DIR / sql_file.name).read_text(encoding="utf-8")
    used = set(re.findall(r"\{\{\s*params\.(\w+)\s*\}\}", sql))

    assert "report_date" in used
    assert used <= {
        "project",
        "bronze_dataset",
        "silver_dataset",
        "report_date",
        "alert_from",
    }


@pytest.fixture(autouse=True)
def variables(monkeypatch):
    """In-memory Airflow Variables (send_alerts records the alerted date)."""
    store = {}
    monkeypatch.setattr(
        bh.Variable, "get", lambda key, default=None: store.get(key, default)
    )
    monkeypatch.setattr(
        bh.Variable, "set", lambda key, value: store.update({key: value})
    )
    return store


def _alert_from(variables, alerted_through, report_date, params=None):
    if alerted_through:
        variables[bh.ALERTED_THROUGH_VAR] = alerted_through
    return bh.alert_from_for({"params": params or {}}, report_date)


def test_alert_from_is_the_report_date_after_a_normal_daily_run(variables):
    assert _alert_from(variables, "2026-11-01", date(2026, 11, 2)) == date(2026, 11, 2)


def test_alert_from_catches_up_the_day_whose_run_failed(variables):
    # Oct 31 was alerted; the run for Nov 1 failed
    assert _alert_from(variables, "2026-10-31", date(2026, 11, 2)) == date(2026, 11, 1)


def test_alert_from_catches_up_at_most_a_week(variables):
    assert _alert_from(variables, "2026-09-30", date(2026, 11, 2)) == date(2026, 10, 26)


@pytest.mark.parametrize(
    "alerted_through, params",
    [
        (None, None),  # first run
        ("2026-10-10", {"report_date": "2026-10-20"}),  # re-run of a past day
    ],
)
def test_alert_from_is_the_report_date_without_a_usable_previous_run(
    variables, alerted_through, params
):
    assert _alert_from(variables, alerted_through, date(2026, 10, 20), params) == date(
        2026, 10, 20
    )


def test_a_late_run_does_not_skip_the_next_day(dag, monkeypatch, variables):
    # The Nov 2 run starts on Nov 3 and reports Nov 1; the Nov 3 run (Nov 2) fails. The Nov 4
    # run (Nov 3) still alerts Nov 2's trial cohorts.
    monkeypatch.setattr(bh, "post_message", _fake_slack([]))
    monkeypatch.setenv("BILLING_HEALTH_SLACK_CHANNEL_ID", "CLIVE")
    empty = {"status": [], "daily": [], "retry": [], "trials": []}
    dag.get_task("send_alerts").python_callable(empty, "2026-11-01", params={})

    assert bh.alert_from_for({"params": {}}, date(2026, 11, 3)) == date(2026, 11, 2)


@pytest.mark.parametrize(
    "alerted_through, report_date, params, expected",
    [
        ("2026-11-01", "2026-11-02", {}, "2026-11-02"),  # daily run moves it on
        ("2026-11-02", "2026-11-02", {}, "2026-11-02"),  # retried run
        (
            "2026-11-05",
            "2026-11-02",
            {},
            "2026-11-05",
        ),  # cleared old run: never moves back
        (
            "2026-11-01",
            "2026-11-04",
            {"report_date": "2026-11-04"},
            "2026-11-01",
        ),  # re-run
        (None, "2026-11-02", {}, "2026-11-02"),  # first run
    ],
)
def test_mark_alerted(variables, alerted_through, report_date, params, expected):
    if alerted_through:
        variables[bh.ALERTED_THROUGH_VAR] = alerted_through

    bh.mark_alerted({"params": params}, report_date)

    assert variables.get(bh.ALERTED_THROUGH_VAR) == expected


@pytest.mark.parametrize(
    "missing_loads, done",
    [
        (None, True),
        # data through late on the day, but one account's ingest did not run or dbt has not
        # built it yet: wait
        ("int_stripe_charges acct_1", False),
    ],
)
def test_wait_for_data_waits_for_every_daily_account(
    dag, monkeypatch, missing_loads, done
):
    monkeypatch.setattr(
        bh,
        "run_query",
        lambda *_: [
            {
                "solidgate_loaded_through": "2026-11-02T23:59:58+00:00",
                "stripe_loaded_through": "2026-11-02T23:59:59+00:00",
                "missing_loads": missing_loads,
            }
        ],
    )
    context = {
        "params": {},
        "dag_run": SimpleNamespace(
            run_after=datetime(2026, 11, 3, 6, 30, tzinfo=timezone.utc)
        ),
    }

    result = dag.get_task("wait_for_data").python_callable(**context)

    assert result.is_done is done
    assert result.xcom_value == "2026-11-02"


def test_report_date_is_yesterday_utc():
    context = {
        "params": {},
        "dag_run": SimpleNamespace(
            run_after=datetime(2026, 11, 3, 5, 0, tzinfo=timezone.utc)
        ),
    }

    assert bh.report_date_for(context) == date(2026, 11, 2)


def test_report_date_param_overrides():
    context = {
        "params": {"report_date": "2026-10-29"},
        "dag_run": SimpleNamespace(
            run_after=datetime(2026, 11, 3, 5, 0, tzinfo=timezone.utc)
        ),
    }

    assert bh.report_date_for(context) == date(2026, 10, 29)


def _fake_slack(posted: list):
    """post_message stand-in: records each call and answers with a message ts."""

    def post(**kwargs):
        posted.append(kwargs)
        return {"ok": True, "ts": f"ts{len(posted)}"}

    return post


def _send(
    dag,
    monkeypatch,
    env: dict,
    rows: list[dict],
    tag_payments: bool = True,
) -> list[dict]:
    posted = []
    monkeypatch.setattr(bh, "TAG_PAYMENTS_OWNERS", tag_payments)
    monkeypatch.setattr(bh, "post_message", _fake_slack(posted))
    monkeypatch.delenv("BILLING_HEALTH_SLACK_CHANNEL_ID", raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("BILLING_HEALTH_APP_BOT_TOKEN", "xoxb-test")
    dag.get_task("send_alerts").python_callable(
        {"status": rows, "daily": [DAILY_ROW], "retry": [RETRY_ROW], "trials": []},
        "2026-10-29",
    )
    return posted


DAILY_ROW = {
    "product_key": "giftmusic",
    "product_name": "GiftMusic",
    "entity": "Apollo",
    "attempts": 2510,
    "approval_total": 0.322,
    "approval_renewals_all": 0.231,
    "bank_declines_pct": 0.652,
    "attempts_per_order": 1.92,
    "top_decline_reasons": "insufficient_funds 61% | do_not_honor 19%",
}

RETRY_ROW = {
    "product_key": "giftmusic",
    "product_name": "GiftMusic",
    "entity": "Apollo",
    "payments_owner_slack_id": "U07OWNER",
    "cohort_start": "2026-09-23",
    "cohort_end": "2026-09-29",
    "first_charges": 400,
    "first_charge_failures": 155,
    "recovered_pct": 0.129,
    "failed_after_all_retries_pct": 0.871,
    "avg_attempts_per_failed": 7.6,
    "baseline": None,
    "target": 0.2,
    "status": "no_baseline",
    "alert_now": False,
}

RED_ROW = {
    "product_key": "giftmusic",
    "product_name": "GiftMusic",
    "entity": "Apollo",
    "payments_owner_slack_id": "U07OWNER",
    "metric": "retries_after_do_not_retry",
    "value": 44.0,
    "volume": 2775,
    "amount_usd": None,
    "ref_7d": 32.7,
    "rule": "any",
    "amber": None,
    "red": 1,
    "status": "red",
    "alert_now": True,
}


def test_three_section_messages_and_red_products_tagged_in_the_thread(dag, monkeypatch):
    posted = _send(
        dag, monkeypatch, {"BILLING_HEALTH_SLACK_CHANNEL_ID": "CLIVE"}, [RED_ROW]
    )

    # Payment health (ts1), its thread reply for GiftMusic, then Retry and Trial messages
    assert [p["channel"] for p in posted] == ["CLIVE"] * 4
    sections = [p for p in posted if "thread_ts" not in p]
    assert [p["blocks"][0]["text"]["text"].split(" · ")[0] for p in sections] == [
        "Payment health",
        "Retry performance",
        "Trial-to-paid",
    ]
    (reply,) = [p for p in posted if "thread_ts" in p]
    assert reply["thread_ts"] == "ts1"  # under the Payment health message
    assert reply["text"].startswith("🔴 *GiftMusic* · 1 red")
    assert reply["text"].endswith("<@U07OWNER> please take a look.")
    assert "<@" not in str([p["blocks"] for p in sections])  # summaries don't tag
    assert all(p["token"] == "xoxb-test" for p in posted)


def test_owners_are_not_tagged_unless_switched_on(dag, monkeypatch):
    posted = _send(
        dag,
        monkeypatch,
        {"BILLING_HEALTH_SLACK_CHANNEL_ID": "CLIVE"},
        [RED_ROW],
        tag_payments=False,
    )

    assert "<@" not in str(posted)
    (reply,) = [p for p in posted if "thread_ts" in p]
    assert reply["text"].endswith("_Owner tagging is off._")


def test_no_new_red_means_no_thread_replies(dag, monkeypatch):
    posted = _send(
        dag,
        monkeypatch,
        {"BILLING_HEALTH_SLACK_CHANNEL_ID": "CLIVE"},
        [{**RED_ROW, "alert_now": False}],
    )

    assert len(posted) == 3
    assert not any("thread_ts" in p for p in posted)


def test_owner_missing_from_the_seed_is_not_tagged(dag, monkeypatch):
    posted = _send(
        dag,
        monkeypatch,
        {"BILLING_HEALTH_SLACK_CHANNEL_ID": "CLIVE"},
        [{**RED_ROW, "payments_owner_slack_id": None}],
    )

    assert "<@" not in str(posted)
    (reply,) = [p for p in posted if "thread_ts" in p]
    assert (
        "No owner set in seed_billing_health_products for: GiftMusic" in reply["text"]
    )


def test_missing_channel_variable_fails(dag, monkeypatch):
    with pytest.raises(RuntimeError, match="BILLING_HEALTH_SLACK_CHANNEL_ID"):
        _send(dag, monkeypatch, {}, [RED_ROW])


def test_channel_from_the_airflow_variable(dag, monkeypatch, variables):
    # prod sets AIRFLOW_VAR_BILLING_HEALTH_SLACK_CHANNEL_ID, i.e. a Variable, not a plain env var
    variables[bh.SLACK_CHANNEL_VAR] = "CVAR"

    posted = _send(dag, monkeypatch, {}, [RED_ROW])

    assert posted and all(p["channel"] == "CVAR" for p in posted)


def test_falls_back_to_text_when_slack_rejects_the_table(dag, monkeypatch):
    posted = []

    def fake_post(**kwargs):
        if kwargs.get("blocks"):
            raise RuntimeError("Slack chat.postMessage to CTEST failed: invalid_blocks")
        posted.append(kwargs)
        return {"ok": True, "ts": f"ts{len(posted)}"}

    monkeypatch.setattr(bh, "post_message", fake_post)
    monkeypatch.setenv("BILLING_HEALTH_SLACK_CHANNEL_ID", "CTEST")
    monkeypatch.setenv("BILLING_HEALTH_APP_BOT_TOKEN", "xoxb-test")

    dag.get_task("send_alerts").python_callable(
        {"status": [RED_ROW], "daily": [DAILY_ROW], "retry": [RETRY_ROW], "trials": []},
        "2026-10-29",
    )

    # per section: summary text in the channel, its tables and red replies in the thread
    tops = [p for p in posted if "thread_ts" not in p]
    assert [p["text"].split(" · ")[0] for p in tops] == [
        "*Payment health",
        "*Retry performance",
        "*Trial-to-paid",
    ]
    assert len(posted) > len(tops)
    assert any(
        p["text"].startswith("🔴 *GiftMusic*") for p in posted if "thread_ts" in p
    )
    assert all(p["channel"] == "CTEST" and not p.get("blocks") for p in posted)


def test_update_sheet_fails_without_the_spreadsheet_variable(dag, monkeypatch):
    monkeypatch.setattr(bh.Variable, "get", lambda key, default=None: default)

    with pytest.raises(RuntimeError, match=bh.GSHEET_ID_VAR):
        dag.get_task("update_sheet").python_callable(
            {"status": [], "daily": [], "retry": [], "trials": []}, "2026-10-29"
        )


def test_upload_report_posts_the_excel_file_with_the_sheet_link(dag, monkeypatch):
    uploads = []
    monkeypatch.setattr(bh, "upload_file", lambda **kwargs: uploads.append(kwargs))
    monkeypatch.setenv("BILLING_HEALTH_SLACK_CHANNEL_ID", "CLIVE")
    monkeypatch.setenv("BILLING_HEALTH_APP_BOT_TOKEN", "xoxb-test")

    dag.get_task("upload_report").python_callable(
        {"status": [RED_ROW], "daily": [DAILY_ROW], "retry": [RETRY_ROW], "trials": []},
        "2026-10-29",
        "https://docs.google.com/spreadsheets/d/SHEET/edit",
    )

    (upload,) = uploads
    assert upload["channel"] == "CLIVE"
    assert upload["filename"] == "billing_health_2026-10-29.xlsx"
    assert upload["content"][:2] == b"PK"  # an .xlsx is a zip file
    assert (
        "<https://docs.google.com/spreadsheets/d/SHEET/edit|Google Sheet>"
        in (upload["initial_comment"])
    )
