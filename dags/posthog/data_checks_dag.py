"""Airflow DAG: posthog_data_checks -- Slack alert on the PostHog BigQuery export.

The export runs inside PostHog (hourly), not in Airflow, so Astro never sees it fail. This
DAG checks what it wrote to bronze once a day -- freshness, volume drop and duplicate share
(alerts/data_checks_alert.sql).
Alerts post to the `slack_webhook_url` Variable, in Production only; set send_in_dev to try
it from a dev deployment (lower the thresholds to force a message).
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow.models import Variable
from airflow.sdk import Param, dag, task

from include.config.posthog_settings import (
    ALERTS_SQL_DIR,
    BQ_BRONZE_DATASET,
    BQ_LOCATION,
    BQ_PROJECT_ID,
    CHECK_LOOKBACK_DAYS,
    DATA_CHECKS_SCHEDULE,
    ENV,
    EXPORT_TABLES,
    GCP_CONN_ID,
    LATE_DATA_HOURS,
    VOLUME_DROP_RATIO,
    get_posthog_projects,
)

SEED_CSV = (
    Path(__file__).resolve().parents[1]
    / "dbt"
    / "bigquery_dbt"
    / "seeds"
    / "seed_ga4_payment_account_map.csv"
)


def _alerting_enabled(params: dict) -> None:
    from airflow.sdk.exceptions import AirflowSkipException

    if ENV != "Production" and not params.get("send_in_dev"):
        raise AirflowSkipException(
            f"{ENV}: PostHog data checks alert in Production only (set send_in_dev to test)"
        )


@dag(
    dag_id="posthog_data_checks",
    schedule=DATA_CHECKS_SCHEDULE,
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 2, "pool": "standard_mem_pool"},
    tags=["posthog", "bronze", "slack", "alerting"],
    params={
        "late_hours": Param(
            LATE_DATA_HOURS, type="number", minimum=0, title="Late after (hours)"
        ),
        "volume_drop_ratio": Param(
            VOLUME_DROP_RATIO, type="number", minimum=0, title="Volume drop ratio"
        ),
        "send_in_dev": Param(
            False, type="boolean", title="Send alerts outside Production"
        ),
    },
)
def posthog_data_checks():

    @task
    def check_export(params: dict) -> None:
        from include.utils.sql_slack_alert import run_sql_alert

        _alerting_enabled(params)
        run_sql_alert(
            sql_file=ALERTS_SQL_DIR / "data_checks_alert.sql",
            webhook_url=Variable.get("slack_webhook_url", default_var=""),
            gcp_conn_id=GCP_CONN_ID,
            bq_location=BQ_LOCATION,
            template_params={
                "bq_project": BQ_PROJECT_ID,
                "bq_bronze_dataset": BQ_BRONZE_DATASET,
                "events_table": EXPORT_TABLES["events"],
                "persons_table": EXPORT_TABLES["persons"],
                "sessions_table": EXPORT_TABLES["sessions"],
                "team_ids": [p["project_id"] for p in get_posthog_projects(SEED_CSV)],
                "lookback_days": CHECK_LOOKBACK_DAYS,
                "late_hours": float(params["late_hours"]),
                "volume_drop_ratio": float(params["volume_drop_ratio"]),
                "env": ENV,
            },
        )

    check_export()


posthog_data_checks()
