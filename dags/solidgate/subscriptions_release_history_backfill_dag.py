"""One-time, manually-triggered backfill.

Loads every retained daily Release Avro file for subscriptions -- every day the
`solidgate_subscriptions` DAG has ever run -- into the append-only
`solidgate_subscriptions_release_history` bronze table in one shot, so
`int_solidgate_subscription_history` has full history to build from on its first
(non-incremental) dbt run.

Run this once, manually, to seed the table. From then on, `solidgate_subscriptions`'
`append_release_history` task keeps it current by appending only each day's new files.
Safe to re-run: anything it reloads just lands as duplicate raw rows, which the
downstream dbt model already dedups (QUALIFY ROW_NUMBER by captured_at) -- re-running
costs extra bytes, not correctness.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.sdk import dag

from include.config.solidgate_settings import BQ_LOCATION, GCP_CONN_ID, GCS_BUCKET

logger = logging.getLogger(__name__)
ENTITY = "subscriptions"

BQ_PROJECT = Variable.get("GCP_PROJECT_ID")
BQ_BRONZE_DATASET = Variable.get("BQ_BRONZE_DATASET")


@dag(
    dag_id=f"solidgate_{ENTITY}_release_history_backfill",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 3,
        "execution_timeout": timedelta(minutes=120),
    },
    tags=["ingestion", "solidgate", "backfill", "manual"],
)
def solidgate_subscriptions_release_history_backfill_pipeline():
    load_full_history = GCSToBigQueryOperator(  # noqa: F841
        task_id="load_full_history",
        gcp_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET,
        source_objects=[f"release/solidgate/{ENTITY}/*.avro"],
        source_format="AVRO",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_BRONZE_DATASET}.solidgate_{ENTITY}_release_history",
        location=BQ_LOCATION,
    )


solidgate_subscriptions_release_history_backfill_pipeline()
