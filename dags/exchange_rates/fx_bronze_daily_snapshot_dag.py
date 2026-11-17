"""
DAG: fx_bronze_daily_snapshot

Pipeline:
    - FxBronzeDailySnapshotExtractToLandingOperator (fx_bronze.operators) -- custom operator
    - LandingToReleaseOperator                       (common.operators)   -- shared, checksum-gated
    - ReleaseToBigQueryOperator                       (common.operators)   -- shared, staging + MERGE

Pulls today's EUR->USD...etc rate from GOOGLEFINANCE (the sample
pair from the request) and MERGEs it into the same bronze table the
Excel backfill DAG writes into (`{BQ_DATASET}.{BQ_TABLE}`), keyed on
`(currency, date)`.

This can only ever capture a live, point-in-time snapshot -- Google blocks
programmatic access to GOOGLEFINANCE's historical output entirely, so
there's no way to backfill past dates here. Run daily and every run adds
one more day's worth of (currency, date, close) rows.
"""

from __future__ import annotations

import datetime

from airflow.models import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.sdk import Asset

from include.config import exchange_rates as config
from include.custom_operators.exchange_rates.daily_snapshot_extract_to_landing import (
    FxBronzeDailySnapshotExtractToLandingOperator,
)
from include.custom_operators.exchange_rates.landing_to_release import (
    LandingToReleaseOperator,
)
from include.custom_operators.exchange_rates.release_to_bigquery import (
    ReleaseToBigQueryOperator,
)

FX_BRONZE_DATASET = Asset("data://bronze/exchange_rates/daily_fx")

with DAG(
    dag_id="fx_bronze_daily_snapshot",
    description="Daily GOOGLEFINANCE snapshot of currencies to usd, merged into the bronze FX history table. Run ate end of the day",
    default_args={"owner": "data-platform", "retries": 3, "pool": "standard_mem_pool"},
    schedule="55 23 * * *",
    start_date=datetime.datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "currency_rates", "bronze"],
    params={
        "run_type": Param(
            "daily",
            type="string",
            enum=["daily", "backfill"],
            title="Run type",
            description="Which Astro worker queue to run this DAG's tasks on.",
        ),
    },
) as dag:
    extract_task = FxBronzeDailySnapshotExtractToLandingOperator(
        task_id="extract_to_landing",
        gcs_bucket=config.GCS_BUCKET,
        landing_prefix=config.GCS_DAILY_LANDING_PREFIX,
        spreadsheet_id=config.SPREADSHEET_ID,
        sheet_name=config.SHEET_NAME,
        base_currency="USD",
        recalc_timeout_seconds=config.RECALC_TIMEOUT_SECONDS,
        recalc_poll_seconds=config.RECALC_POLL_SECONDS,
        gcp_conn_id=config.GCP_CONN_ID,
    )

    release_task = LandingToReleaseOperator(
        task_id="landing_to_release",
        gcs_bucket=config.GCS_BUCKET,
        landing_task_id=extract_task.task_id,
        release_prefix=config.GCS_DAILY_RELEASE_PREFIX,
        manifest_object=config.GCS_DAILY_MANIFEST_OBJECT,
        natural_key_fields=config.NATURAL_KEY_FIELDS,
        gcp_conn_id=config.GCP_CONN_ID,
    )

    load_task = ReleaseToBigQueryOperator(
        task_id="release_to_bigquery",
        gcs_bucket=config.GCS_BUCKET,
        release_task_id=release_task.task_id,
        bq_project=config.BQ_PROJECT,
        bq_bronze_dataset=config.BQ_BRONZE_DATASET,
        bq_staging_dataset=config.BQ_STAGING_DATASET,
        bq_table="daily_fx",
        schema=config.BQ_SCHEMA,
        natural_key_fields=config.NATURAL_KEY_FIELDS,
        partition_field=config.BQ_PARTITION_FIELD,
        partition_type=config.BQ_PARTITION_TYPE,
        gcp_conn_id=config.GCP_CONN_ID,
        location=config.BQ_LOCATION,
    )

    bronze_ready = EmptyOperator(
        task_id="bronze_ready",
        outlets=[FX_BRONZE_DATASET],
        trigger_rule="none_failed",
    )

    extract_task >> release_task >> load_task >> bronze_ready
