"""Daily billing health alerts for yesterday (D-1, UTC): Sections A (payment health), B (retry
performance) and C (trial-to-paid).

wait_for_data -> evaluate -> send_alerts  -> upload_report
                         -> update_sheet -> upload_report

* wait_for_data: waits (reschedule mode) until yesterday's Solidgate attempts and Stripe charges
  have been built by dbt through late in the day, and every account that loads daily has been
  loaded and built after the day ended in every model the report reads
  (section_a_freshness.sql), so alerts never fire on a partial day or a missing account.
* evaluate: runs include/sql/billing_health/section_a_status.sql (per product x metric value,
  7-day reference, green / amber / red, alert_now) and section_a_daily.sql (the ticket's other
  Section A columns: attempts, bank declines, attempts per order, top 3 decline reasons),
  section_b_retry.sql (the 7-day cohort of first-charge failures whose 30-day retry window just
  closed) and section_c_trials.sql (weekly trial cohorts).
* send_alerts: as the billing health Slack app, ONE message per section (the daily summary):
  status counts, the products with a red, data notes and a table with every column of that
  section (plain-text messages instead if Slack rejects the table). Then, in that message's
  thread, one reply listing every product with a red and its reds, tagging the payments
  team once (the ticket's red alert; they tag the product owner by hand). Nothing is posted in a thread
  when no product has a red. Everything goes to the channel in Variable
  BILLING_HEALTH_SLACK_CHANNEL_ID (or that env var). The payments team in seed_billing_health_products is tagged
  only when TAG_PAYMENTS_OWNERS is on in include/config/billing_health_settings.py. A daily run
  then records its report date in Variable BILLING_HEALTH_ALERTED_THROUGH, where the next run
  starts its trial alert catch-up.
* update_sheet: writes the same tables to the Google Sheet in Variable BILLING_HEALTH_GSHEET_ID,
  one tab per section with a report date column (include/utils/billing_health_sheet.py). The
  report date's rows are replaced, so the tabs keep every day's history and a re-run of a day
  does not duplicate it. Runs next to send_alerts, so one failing does not stop the other.
* upload_report: posts the day's three tabs as an Excel file to the channel, after the section
  messages, with a link to the sheet.

Trigger with {"report_date": "YYYY-MM-DD"} to re-run a past day.
"""

import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.sdk import Param, PokeReturnValue, Variable, dag, task
from jinja2 import Template

from include.config.billing_health_settings import (
    ALERTED_THROUGH_VAR,
    BQ_BRONZE_DATASET,
    BQ_LOCATION,
    BQ_PROJECT_ID,
    BQ_SILVER_DATASET,
    ENV,
    FRESH_THROUGH_HOUR_UTC,
    GCP_CONN_ID,
    GSHEET_ID_VAR,
    SLACK_CHANNEL_VAR,
    SQL_DIR,
    TAG_PAYMENTS_OWNERS,
)
from include.utils.billing_health_alerts import (
    build_summary_blocks,
    build_summary_messages,
)
from include.utils.billing_health_red_alerts import build_red_replies
from include.utils.billing_health_retry_trial_alerts import (
    build_retry_blocks,
    build_retry_messages,
    build_trial_blocks,
    build_trial_messages,
)
from include.utils.billing_health_sheet import (
    build_tabs,
    build_workbook,
    sheet_url,
    write_tabs,
)
from include.utils.slack_bot import post_message, upload_file

STATUS_SQL = Path(SQL_DIR) / "section_a_status.sql"
DAILY_SQL = Path(SQL_DIR) / "section_a_daily.sql"
FRESHNESS_SQL = Path(SQL_DIR) / "section_a_freshness.sql"
RETRY_SQL = Path(SQL_DIR) / "section_b_retry.sql"
TRIALS_SQL = Path(SQL_DIR) / "section_c_trials.sql"
MAX_ALERT_CATCHUP_DAYS = 7


def report_date_for(context) -> date:
    """D-1 of the run (UTC), or the report_date param when one is given."""
    override = (context.get("params") or {}).get("report_date")
    if override:
        return date.fromisoformat(override)
    run_after = context["dag_run"].run_after
    return run_after.astimezone(timezone.utc).date() - timedelta(days=1)


def is_rerun(context) -> bool:
    return bool((context.get("params") or {}).get("report_date"))


def alert_from_for(context, report_date: date) -> date:
    """First day whose newly final trial cohorts are alerted: the day after the last report date
    whose alerts were posted (ALERTED_THROUGH_VAR), so a day whose run failed is caught up
    (catchup is off), at most MAX_ALERT_CATCHUP_DAYS back. Not the previous run's start date: a
    late run reports the day before it was scheduled, not the day before it started. A re-run
    of a past day alerts only that day."""
    alerted_through = Variable.get(ALERTED_THROUGH_VAR, default=None)
    if is_rerun(context) or not alerted_through:
        return report_date
    after_prev = date.fromisoformat(alerted_through) + timedelta(days=1)
    return min(
        report_date,
        max(after_prev, report_date - timedelta(days=MAX_ALERT_CATCHUP_DAYS)),
    )


def mark_alerted(context, report_date: str) -> None:
    """Record report_date as alerted, for the next run's catch-up. Never moves back, so a daily
    run cleared for an older day does not re-open days already alerted."""
    if is_rerun(context):
        return
    alerted_through = Variable.get(ALERTED_THROUGH_VAR, default=None)
    if not alerted_through or report_date > alerted_through:
        Variable.set(ALERTED_THROUGH_VAR, report_date)


def slack_channel() -> str:
    """Slack channel from Variable SLACK_CHANNEL_VAR (env AIRFLOW_VAR_...), else the plain env
    var of the same name."""
    return Variable.get(SLACK_CHANNEL_VAR, default=None) or os.environ.get(
        SLACK_CHANNEL_VAR, ""
    )


def post_section(token: str, channel: str, build_blocks, build_messages, args) -> str:
    """Post a section as one message (summary + table block), or as plain text if Slack rejects
    the table. Returns the ts of the (first) message, to reply in its thread."""
    notification, blocks = build_blocks(*args)
    try:
        return post_message(
            token=token, channel=channel, text=notification, blocks=blocks
        )["ts"]
    except RuntimeError as err:
        if "invalid_blocks" not in str(err) and "invalid_arguments" not in str(err):
            raise
        print(f"Slack rejected the table block ({err}); posting the text version.")
        first, *rest = build_messages(*args)
        ts = post_message(token=token, channel=channel, text=first)["ts"]
        for text in rest:
            post_message(token=token, channel=channel, text=text, thread_ts=ts)
        return ts


def run_query(
    sql_file: Path, report_date: str, alert_from: str | None = None
) -> list[dict]:
    query = Template(sql_file.read_text(encoding="utf-8")).render(
        params={
            "project": BQ_PROJECT_ID,
            "bronze_dataset": BQ_BRONZE_DATASET,
            "silver_dataset": BQ_SILVER_DATASET,
            "report_date": report_date,
            "alert_from": alert_from or report_date,
        }
    )
    client = BigQueryHook(gcp_conn_id=GCP_CONN_ID, location=BQ_LOCATION).get_client(
        project_id=BQ_PROJECT_ID, location=BQ_LOCATION
    )
    return [
        {
            k: (v.isoformat() if isinstance(v, (date, datetime)) else v)
            for k, v in row.items()
        }
        for row in client.query(query).result()
    ]


@dag(
    dag_id="billing_health_alerts",
    # Solidgate and Stripe land 01:45-03:00 UTC and dbt runs after; wait_for_data covers late runs.
    schedule="30 6 * * *",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
        "pool": "standard_mem_pool",
    },
    params={
        "report_date": Param(
            None,
            type=["null", "string"],
            description="YYYY-MM-DD to report on; default is yesterday (UTC).",
        )
    },
    tags=["billing_health", "solidgate", "stripe", "slack", "alerting"],
)
def billing_health_alerts():

    @task.sensor(poke_interval=15 * 60, timeout=4 * 60 * 60, mode="reschedule")
    def wait_for_data(**context) -> PokeReturnValue:
        report_date = report_date_for(context)
        loaded = run_query(FRESHNESS_SQL, report_date.isoformat())[0]
        needed = datetime.combine(
            report_date, datetime.min.time(), tzinfo=timezone.utc
        ) + timedelta(hours=FRESH_THROUGH_HOUR_UTC)
        fresh = (
            all(
                loaded[k] and datetime.fromisoformat(loaded[k]) >= needed
                for k in ("solidgate_loaded_through", "stripe_loaded_through")
            )
            and not loaded["missing_loads"]
        )
        print(
            f"Report {report_date}: need data through {needed.isoformat()}, loaded {loaded}"
        )
        return PokeReturnValue(is_done=fresh, xcom_value=report_date.isoformat())

    @task
    def evaluate(report_date: str, **context) -> dict:
        alert_from = alert_from_for(context, date.fromisoformat(report_date))
        status = run_query(STATUS_SQL, report_date)
        daily = run_query(DAILY_SQL, report_date)
        retry = run_query(RETRY_SQL, report_date)
        trials = run_query(TRIALS_SQL, report_date, alert_from.isoformat())
        print(
            f"{report_date}: A {len(status)} status / {len(daily)} product rows, "
            f"B {len(retry)} rows, C {len(trials)} cohort rows "
            f"(trial alerts for cohorts final since {alert_from})"
        )
        return {"status": status, "daily": daily, "retry": retry, "trials": trials}

    @task
    def send_alerts(results: dict, report_date: str, **context) -> None:
        channel = slack_channel()
        if not channel:
            raise RuntimeError(f"Variable {SLACK_CHANNEL_VAR} is not set.")
        token = os.environ.get("BILLING_HEALTH_APP_BOT_TOKEN", "")

        replies = build_red_replies(
            results["status"], results["retry"], results["trials"], TAG_PAYMENTS_OWNERS
        )
        sections = [
            (
                build_summary_blocks,
                build_summary_messages,
                (results["status"], results["daily"]),
                replies["payment"],
            ),
            (
                build_retry_blocks,
                build_retry_messages,
                (results["retry"],),
                replies["retry"],
            ),
            (
                build_trial_blocks,
                build_trial_messages,
                (results["trials"],),
                replies["trial"],
            ),
        ]
        for build_blocks, build_messages, rows, section_replies in sections:
            ts = post_section(
                token, channel, build_blocks, build_messages, (*rows, report_date, ENV)
            )
            # Red: one reply listing every product with a red, tagging the payments team once.
            for text in section_replies:
                post_message(token=token, channel=channel, text=text, thread_ts=ts)
        mark_alerted(context, report_date)

    @task
    def update_sheet(results: dict, report_date: str) -> str:
        spreadsheet_id = Variable.get(GSHEET_ID_VAR, default="")
        if not spreadsheet_id:
            raise RuntimeError(f"Variable {GSHEET_ID_VAR} is not set.")
        service = GSheetsHook(gcp_conn_id=GCP_CONN_ID).get_conn()
        write_tabs(
            service, spreadsheet_id, build_tabs(results, report_date), report_date
        )
        return sheet_url(spreadsheet_id)

    @task
    def upload_report(results: dict, report_date: str, url: str) -> None:
        channel = slack_channel()
        env = "" if ENV == "Production" else f" · {ENV}"
        upload_file(
            token=os.environ.get("BILLING_HEALTH_APP_BOT_TOKEN", ""),
            channel=channel,
            filename=f"billing_health_{report_date}.xlsx",
            title=f"Billing health {report_date}",
            content=build_workbook(build_tabs(results, report_date)),
            initial_comment=(
                f"*Billing health · {report_date}*{env} · all three sections as Excel. "
                f"Every day's history: <{url}|Google Sheet>"
            ),
        )

    report_date = wait_for_data()
    results = evaluate(report_date)
    alerts = send_alerts(results, report_date)
    upload = upload_report(results, report_date, update_sheet(results, report_date))
    alerts >> upload


billing_health_alerts()
