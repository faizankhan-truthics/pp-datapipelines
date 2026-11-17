from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.sdk import Asset, dag, get_current_context, task

from include.config.anytrack_settings import (
    BQ_LOCATION,
    ENTITY,
    GCP_CONN_ID,
    GCS_BUCKET,
    PRODUCTS,
    SQL_DIR,
    landing_prefix,
    resolve_window,
)

# Every AnyTrack product ingests every event type -- see merge_events_all_events.sql.
MERGE_SQL_FILE = "merge_events_all_events.sql"

logger = logging.getLogger(__name__)

ANYTRACK_BRONZE_DATASET = Asset("data://bronze/anytrack/events")

BQ_PROJECT = Variable.get("GCP_PROJECT_ID")
BQ_STAGING_DATASET = Variable.get("BQ_STAGING_DATASET")
BQ_BRONZE_DATASET = Variable.get("BQ_BRONZE_DATASET")


@dag(
    dag_id="anytrack_events_pipeline",
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 3,
        "execution_timeout": timedelta(minutes=60),
        "pool": "high_mem_pool",
    },
    tags=["ingestion", "anytrack", "bronze"],
    params={"date_from": "", "date_to": ""},
    template_searchpath=[str(SQL_DIR)],
)
def anytrack_events_pipeline():
    bronze_merge_tasks = []
    for product in PRODUCTS:

        def build_product_tasks(prod):
            @task(task_id=f"check_landing_exists_{prod}")
            def check_landing_exists_fn() -> list[str]:
                """Cheap existence check -- max_results=1 never materializes
                the full day's file list, so memory stays bounded no matter
                how many landing files exist. Returns the wildcard glob
                load_staging reads directly; BigQuery does the actual GCS
                reading server-side, not this task."""
                ds, _, _ = resolve_window(get_current_context())
                prefix = landing_prefix(prod, ds)
                bucket = GCSHook(gcp_conn_id=GCP_CONN_ID).get_conn().bucket(GCS_BUCKET)
                if not list(bucket.list_blobs(prefix=prefix, max_results=1)):
                    raise AirflowSkipException(
                        f"No landing files for {prod}/{ds}. Skipping BigQuery load."
                    )
                logger.info(f"Landing files found for {prod}: {ds}")
                return [f"{prefix}*"]

            landing_glob = check_landing_exists_fn()

            staging_table = (
                f"{BQ_PROJECT}.{BQ_STAGING_DATASET}.anytrack_{prod}_{ENTITY}"
            )

            load_staging = GCSToBigQueryOperator(
                task_id=f"load_staging_{prod}",
                gcp_conn_id=GCP_CONN_ID,
                bucket=GCS_BUCKET,
                source_objects=landing_glob,
                source_format="NEWLINE_DELIMITED_JSON",
                schema_fields=[
                    {"name": "_gcs_file_path", "type": "STRING"},
                    {"name": "raw_data", "type": "STRING"},
                ],
                write_disposition="WRITE_TRUNCATE",
                create_disposition="CREATE_IF_NEEDED",
                destination_project_dataset_table=staging_table,
                location=BQ_LOCATION,
            )

            @task(task_id=f"log_staging_count_{prod}")
            def log_staging_count_fn() -> None:
                """BigQuery's own row count for this run's staging load --
                authoritative (straight from the table's metadata, no
                separate query needed) since staging is WRITE_TRUNCATE'd
                every run, so num_rows here is exactly this run's total."""
                table = (
                    BigQueryHook(gcp_conn_id=GCP_CONN_ID)
                    .get_client()
                    .get_table(staging_table)
                )
                logger.info(
                    f"Loaded {table.num_rows} record(s) into staging for {prod}"
                )

            merge_bronze = BigQueryInsertJobOperator(
                task_id=f"merge_bronze_{prod}",
                gcp_conn_id=GCP_CONN_ID,
                location=BQ_LOCATION,
                configuration={
                    "query": {"query": MERGE_SQL_FILE, "useLegacySql": False}
                },
                params={
                    "bq_project": BQ_PROJECT,
                    "bq_staging_dataset": BQ_STAGING_DATASET,
                    "bq_bronze_dataset": BQ_BRONZE_DATASET,
                    "source": "webhook",
                    "product": prod,
                },
            )

            landing_glob >> load_staging >> log_staging_count_fn() >> merge_bronze

            return merge_bronze

        bronze_merge_tasks.append(build_product_tasks(product))

    anytrack_bronze_ready = EmptyOperator(
        task_id="anytrack_bronze_ready",
        outlets=[ANYTRACK_BRONZE_DATASET],
        trigger_rule="none_failed",
    )

    bronze_merge_tasks >> anytrack_bronze_ready


anytrack_events_pipeline()
