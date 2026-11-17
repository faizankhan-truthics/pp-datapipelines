from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.sdk import Asset, dag, get_current_context, task

from include.config.files_editor_db_settings import (
    BQ_LOCATION,
    GCP_CONN_ID,
    GCS_BUCKET,
    SQL_DIR,
    TABLES,
    resolve_window,
)
from include.custom_operators.files_editor_db.files_editor_db_to_gcs_operator import (
    land_and_release_table_to_gcs,
)

logger = logging.getLogger(__name__)

BQ_PROJECT = Variable.get("GCP_PROJECT_ID")
BQ_STAGING_DATASET = Variable.get("BQ_STAGING_DATASET")
BQ_BRONZE_DATASET = Variable.get("BQ_BRONZE_DATASET")

FILES_EDITOR_DB_BRONZE_DATASET = Asset("data://bronze/files_editor_db/billing")


@dag(
    dag_id="files_editor_db_billing_tables_pipeline",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 3,
        "execution_timeout": timedelta(hours=3),
        "pool": "high_mem_pool",
    },
    tags=[
        "ingestion",
        "files_editor_db",
        "starburst",
        "landing",
        "release",
        "staging",
        "bronze",
    ],
    params={"date_from": "", "date_to": ""},
    template_searchpath=[str(SQL_DIR)],
)
def files_editor_db_billing_tables_pipeline():
    """Lands the 5 files_editor billing_* Postgres tables (reached via Starburst) into
    their own bronze tables -- 1:1 raw mirrors, no joins. See TABLES in
    files_editor_db_settings.py. Joins/classification for the AnyTrack reconciliation
    happen downstream in dbt silver models, not here.
    """
    bronze_merge_tasks = []
    previous_release_paths = None

    for table in TABLES:

        def build_table_tasks(tbl, previous_release_paths):
            @task(task_id=f"land_release_{tbl}")
            def land_release_fn() -> list[str]:
                context = get_current_context()
                ds, date_from, date_to = resolve_window(context)
                result = land_and_release_table_to_gcs(
                    table=tbl,
                    date_from=date_from,
                    date_to=date_to,
                    ds=ds,
                )
                if result["record_count"] == 0:
                    raise AirflowSkipException(f"No rows for {tbl} in this window.")
                return [result["release_glob"]]

            release_paths = land_release_fn()

            # Force extractions to run one table at a time -- each is memory-heavy
            # on its own (see files_editor_db_to_gcs_operator.py), and running
            # several concurrently (allowed by high_mem_pool's 3 slots) was still
            # enough to OOM the shared worker even with per-table backpressure.
            # Only the extraction is serialized; staging/merge below (BigQuery-side,
            # not memory-heavy here) stay free to run in parallel across tables.
            if previous_release_paths is not None:
                previous_release_paths >> release_paths

            staging_table = f"{BQ_PROJECT}.{BQ_STAGING_DATASET}.files_editor_{tbl}"

            load_staging = GCSToBigQueryOperator(
                task_id=f"load_staging_{tbl}",
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
                task_id=f"merge_bronze_{tbl}",
                gcp_conn_id=GCP_CONN_ID,
                location=BQ_LOCATION,
                configuration={
                    "query": {"query": f"merge_{tbl}.sql", "useLegacySql": False}
                },
                params={
                    "bq_project": BQ_PROJECT,
                    "bq_staging_dataset": BQ_STAGING_DATASET,
                    "bq_bronze_dataset": BQ_BRONZE_DATASET,
                },
            )

            release_paths >> load_staging >> merge_bronze
            return merge_bronze, release_paths

        merge_bronze, previous_release_paths = build_table_tasks(
            table, previous_release_paths
        )
        bronze_merge_tasks.append(merge_bronze)

    bronze_ready = EmptyOperator(
        task_id="files_editor_db_bronze_ready",
        outlets=[FILES_EDITOR_DB_BRONZE_DATASET],
        trigger_rule="none_failed",
    )

    bronze_merge_tasks >> bronze_ready


files_editor_db_billing_tables_pipeline()
