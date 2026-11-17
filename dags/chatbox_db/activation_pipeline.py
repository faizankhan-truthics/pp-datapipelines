from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, dag, get_current_context, task

from include.config.chatbox_db_settings import (
    ACTIVATION_TABLE,
    BQ_LOCATION,
    GCP_CONN_ID,
    GCS_BUCKET,
    SQL_DIR,
    resolve_window,
)
from include.custom_operators.chatbox_db.activation_to_gcs_operator import (
    land_and_release_activation_to_gcs,
)

logger = logging.getLogger(__name__)

BQ_PROJECT = Variable.get("GCP_PROJECT_ID")
BQ_STAGING_DATASET = Variable.get("BQ_STAGING_DATASET")
BQ_BRONZE_DATASET = Variable.get("BQ_BRONZE_DATASET")

CHATBOX_DB_BRONZE_DATASET = Asset("data://bronze/chatbox_db/ai_response_activation")


@dag(
    dag_id="chatbox_db_activation_pipeline",
    schedule="0 5 * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 3,
        "execution_timeout": timedelta(hours=2),
        "pool": "high_mem_pool",
    },
    tags=[
        "ingestion",
        "chatbox_db",
        "starburst",
        "landing",
        "release",
        "staging",
        "bronze",
    ],
    params={"date_from": "", "date_to": ""},
    template_searchpath=[str(SQL_DIR)],
)
def chatbox_db_activation_pipeline():
    """Lands each Chatbox user's first-ever successful AI response timestamp
    into bronze via an aggregate query run at the Starburst source, not a raw
    table mirror. Insert-only (immutable metric). Backfill via date_from/date_to
    params; scheduled runs use a short trailing window -- see
    chatbox_db_settings.resolve_window."""

    @task(task_id="land_release_ai_response_activation")
    def land_release_fn() -> list[str]:
        context = get_current_context()
        ds, date_from, date_to = resolve_window(context)
        result = land_and_release_activation_to_gcs(
            date_from=date_from,
            date_to=date_to,
            ds=ds,
        )
        if result["record_count"] == 0:
            raise AirflowSkipException("No activations in this window.")
        return [result["release_glob"]]

    release_paths = land_release_fn()

    # Runs unconditionally, not gated on land_release_fn finding any rows -- an
    # initial empty-window run would otherwise publish bronze readiness without
    # the table existing yet. See create_ai_response_activation_table.sql.
    ensure_bronze_table = BigQueryInsertJobOperator(
        task_id="ensure_bronze_table_ai_response_activation",
        gcp_conn_id=GCP_CONN_ID,
        location=BQ_LOCATION,
        configuration={
            "query": {
                "query": "create_ai_response_activation_table.sql",
                "useLegacySql": False,
            }
        },
        params={
            "bq_project": BQ_PROJECT,
            "bq_bronze_dataset": BQ_BRONZE_DATASET,
        },
    )

    staging_table = f"{BQ_PROJECT}.{BQ_STAGING_DATASET}.chatbox_{ACTIVATION_TABLE}"

    load_staging = GCSToBigQueryOperator(
        task_id="load_staging_ai_response_activation",
        gcp_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET,
        source_objects=release_paths,
        source_format="AVRO",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        destination_project_dataset_table=staging_table,
        location=BQ_LOCATION,
    )

    merge_bronze = BigQueryInsertJobOperator(
        task_id="merge_bronze_ai_response_activation",
        gcp_conn_id=GCP_CONN_ID,
        location=BQ_LOCATION,
        configuration={
            "query": {
                "query": "merge_ai_response_activation.sql",
                "useLegacySql": False,
            }
        },
        params={
            "bq_project": BQ_PROJECT,
            "bq_staging_dataset": BQ_STAGING_DATASET,
            "bq_bronze_dataset": BQ_BRONZE_DATASET,
        },
    )

    # trigger_rule="none_failed" so the bronze asset event still fires on an empty
    # window (land_release_fn's AirflowSkipException skips merge_bronze too) --
    # otherwise dbt_dag, which requires every listed bronze asset to update, would
    # stall forever on a day with zero new activations. Mirrors
    # files_editor_db_billing_tables_dag.py's bronze_ready task.
    bronze_ready = EmptyOperator(
        task_id="chatbox_db_bronze_ready",
        outlets=[CHATBOX_DB_BRONZE_DATASET],
        trigger_rule="none_failed",
    )

    release_paths >> load_staging >> merge_bronze
    [ensure_bronze_table, merge_bronze] >> bronze_ready


chatbox_db_activation_pipeline()
