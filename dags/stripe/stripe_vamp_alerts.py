import json
import urllib.request
from datetime import datetime

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.sdk import Asset, dag, task

from include.config.stripe_settings import (
    BQ_LOCATION,
    ENV,
    GCP_CONN_ID,
    VAMP_ALERTS_WEBHOOK,
    VAMP_TABLE_NAME,
)

# Must match the Asset emitted by the dbt DAG exactly (same URI) so this DAG
# gets scheduled the moment that asset is produced.
DBT_MARTS_READY = Asset("dbt://bigquery_dbt/marts")

BQ_QUERY = f"""
    SELECT
        account,
        charge_descriptor,
        Month_,
        vamp_count,
        vamp_ratio,
        estimated_eom_vamp_count,
        estimated_eom_vamp_ratio,
        vamp_status,
        vamp_status_reason
    FROM `{VAMP_TABLE_NAME}`
    WHERE vamp_status != 'green'
"""

STATUS_EMOJI = {
    "red": "🔴",
    "yellow": "🟡",
    "green": "🟢",
}
DEFAULT_EMOJI = "⚪"


@dag(
    dag_id="daily_vamp_status",
    schedule=[DBT_MARTS_READY],  # triggered when the dbt DAG's asset is emitted
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={"retries": 2, "pool": "standard_mem_pool"},
    tags=["dbt", "gold", "stripe", "slack"],
)
def dbt_slack_status():

    @task
    def build_slack_message() -> None:
        hook = BigQueryHook(
            gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False, location=BQ_LOCATION
        )
        rows = hook.get_records(sql=BQ_QUERY)

        if not VAMP_ALERTS_WEBHOOK:
            raise RuntimeError(
                "VAMP alerts Slack webhook URL is not set (Variable VAMP_ALERTS__SLACK_WEBHOOK_URL)."
            )

        if not rows:
            message = f"*{ENV} Daily VAMP Status Report*\n\n{DEFAULT_EMOJI} No VAMP alerts available for this run."
        else:
            lines = [
                f"{STATUS_EMOJI.get(str(vamp_status).lower(), DEFAULT_EMOJI)} "
                f"*{Month_}* | {account} | {charge_descriptor} "
                f" VAMP: {vamp_count} ({vamp_ratio:.2%}) | "
                f"EOM: {estimated_eom_vamp_count} ({estimated_eom_vamp_ratio:.2%})"
                f" Comment: {vamp_status_reason}"
                for (
                    account,
                    charge_descriptor,
                    Month_,
                    vamp_count,
                    vamp_ratio,
                    estimated_eom_vamp_count,
                    estimated_eom_vamp_ratio,
                    vamp_status,
                    vamp_status_reason,
                ) in rows
            ]

            message = f"*{ENV} Daily VAMP Status Report*\n\n" + "\n".join(lines)

        payload = json.dumps({"text": message}).encode("utf-8")

        req = urllib.request.Request(
            VAMP_ALERTS_WEBHOOK,
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            print(f"Slack webhook responded HTTP {resp.status}")
            return None

    build_slack_message()


dbt_slack_status()
