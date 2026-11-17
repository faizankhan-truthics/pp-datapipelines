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
from airflow.sdk import dag, get_current_context, task

from include.config.anytrack_settings import (
    BQ_LOCATION,
    ENTITY,
    GCP_CONN_ID,
    GCS_BUCKET,
    PRODUCTS,
    SQL_DIR,
)
from include.custom_operators.anytrack.backfill_utils import (
    backfill_csv_to_release,
    list_csv_files,
)

logger = logging.getLogger(__name__)

BQ_PROJECT = Variable.get("GCP_PROJECT_ID")
BQ_STAGING_DATASET = Variable.get("BQ_STAGING_DATASET")
BQ_BRONZE_DATASET = Variable.get("BQ_BRONZE_DATASET")


@dag(
    dag_id="anytrack_events_backfill",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 3,
        "execution_timeout": timedelta(minutes=60),
        "pool": "high_mem_pool",
    },
    tags=["ingestion", "anytrack", "backfill", "manual"],
    params={"file_path": "", "product": ""},
    template_searchpath=[str(SQL_DIR)],
)
def anytrack_events_backfill_pipeline():
    @task
    def validate_params() -> dict:
        ctx = get_current_context()
        file_path = ctx["params"].get("file_path")
        product = ctx["params"].get("product")
        if not file_path:
            raise ValueError(
                "param 'file_path' is required (GCS prefix to the CSV file(s))"
            )
        if product not in PRODUCTS:
            raise ValueError(
                f"param 'product' must be one of {PRODUCTS}, got {product!r}"
            )
        return {"file_path": file_path, "product": product}

    @task
    def list_files(run_params: dict) -> list[str]:
        return list_csv_files(file_path=run_params["file_path"])

    @task(trigger_rule="all_done")
    def csv_to_release(run_params: dict, csv_blob_names: list[str]) -> dict:
        ctx = get_current_context()
        ds = ctx["ds"]  # today -- when this backfill actually ran, not event dates
        return backfill_csv_to_release(
            product=run_params["product"], csv_blob_names=csv_blob_names, ds=ds
        )

    @task(trigger_rule="all_done")
    def check_records(release_result: dict) -> list[str]:
        if not release_result or release_result.get("record_count", 0) == 0:
            raise AirflowSkipException("No CSV rows produced. Skipping BigQuery load.")
        return release_result["release_paths"]

    run_params = validate_params()
    csv_files = list_files(run_params)
    release_result = csv_to_release(run_params, csv_files)
    release_path = check_records(release_result)

    @task
    def staging_table_name(run_params: dict) -> str:
        return f"{BQ_PROJECT}.{BQ_STAGING_DATASET}.anytrack_{run_params['product']}_{ENTITY}"

    load_staging = GCSToBigQueryOperator(
        task_id="load_staging",
        gcp_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET,
        source_objects=release_path,
        source_format="AVRO",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        destination_project_dataset_table=staging_table_name(run_params),
        location=BQ_LOCATION,
    )

    # Every product ingests every event type -- see merge_events_all_events.sql.
    merge_bronze = BigQueryInsertJobOperator(
        task_id="merge_bronze",
        gcp_conn_id=GCP_CONN_ID,
        location=BQ_LOCATION,
        configuration={
            "query": {"query": "merge_events_all_events.sql", "useLegacySql": False}
        },
        params={
            "bq_project": BQ_PROJECT,
            "bq_staging_dataset": BQ_STAGING_DATASET,
            "bq_bronze_dataset": BQ_BRONZE_DATASET,
            "source": "csv_backfill",
            "product": "{{ params.product }}",
        },
    )

    release_path >> load_staging >> merge_bronze


anytrack_events_backfill_pipeline()
