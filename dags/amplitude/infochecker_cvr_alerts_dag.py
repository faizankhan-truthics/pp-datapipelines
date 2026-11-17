"""Daily InfoChecker conversion-rate (CVR) alert: runs a SQL check over Amplitude events and posts the result to Slack."""

from datetime import datetime
from pathlib import Path

from airflow.sdk import dag, task

from include.config.amplitude_settings import (
    AMPLITUDE_STAGE_DATASET,
    BQ_LOCATION,
    BQ_PROJECT_ID,
    GCP_CONN_ID,
    INFOCHECKER_CVR_ALERTS_WEBHOOK,
    SQL_DIR,
)
from include.utils.sql_slack_alert import run_sql_alert

SQL_FILE = Path(SQL_DIR) / "infochecker_cvr_alerts_live.sql"


@dag(
    dag_id="infochecker_cvr_alerts",
    # 4am UTC, once the previous UTC day's Amplitude events are complete.
    schedule="0 4 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "pool": "standard_mem_pool"},
    tags=["amplitude", "infochecker", "cvr", "slack", "alerting"],
)
def infochecker_cvr_alerts():

    @task
    def check_and_alert() -> None:
        run_sql_alert(
            sql_file=SQL_FILE,
            webhook_url=INFOCHECKER_CVR_ALERTS_WEBHOOK,
            gcp_conn_id=GCP_CONN_ID,
            bq_location=BQ_LOCATION,
            template_params={
                "bq_project": BQ_PROJECT_ID,
                "amplitude_stage_dataset": AMPLITUDE_STAGE_DATASET,
            },
        )

    check_and_alert()


infochecker_cvr_alerts()
