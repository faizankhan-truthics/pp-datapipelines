"""
DAG: gcs_external_to_bigquery_bronze

Generic CSV -> BigQuery bronze loader. Dynamically discovers CSV files under
gs://{GCS_BUCKET}/{GCS_PREFIX} and loads each file into its own BigQuery table

Manually triggered (schedule=None).
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import timedelta

import pendulum
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.sdk import dag, task

from include.config.external_setting import (
    BQ_BRONZE_DATASET,
    BQ_PROJECT,
    GCP_CONN_ID,
    GCS_BUCKET,
    GCS_PREFIX,
    build_schema_fields,
    has_csv_files,
)

logger = logging.getLogger(__name__)


@dag(
    dag_id="gcs_external_to_bigquery_bronze",
    description="Discovers CSV reference files dropped in GCS and loads each into its own BigQuery bronze table.",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    default_args={
        "owner": "data-platform",
        "retries": 3,
        "execution_timeout": timedelta(minutes=120),
        "pool": "standard_mem_pool",
    },
    tags=["external", "metadata", "bronze"],
)
def gcs_external_to_bigquery_bronze():

    @task
    def get_csv_files() -> list[dict]:
        """List every CSV under GCS_PREFIX, read its header, and map it to its destination bronze table.

        These are small, manually-maintained reference/lookup files, so
        downloading each one in full to read its header row is cheap and
        keeps this simple -- there's no need for a ranged/partial read.
        """
        gcs_hook = GCSHook(gcp_conn_id=GCP_CONN_ID)

        files = gcs_hook.list(bucket_name=GCS_BUCKET, prefix=GCS_PREFIX)

        csv_files = []
        for file in files:
            if not file.lower().endswith(".csv"):
                continue

            content = gcs_hook.download(bucket_name=GCS_BUCKET, object_name=file)
            first_line = content.decode("utf-8").splitlines()[0]
            header_row = next(csv.reader(io.StringIO(first_line)))
            schema_fields = build_schema_fields(header_row)

            file_name = file.split("/")[-1]
            table_name = file_name.rsplit(".", 1)[0].lower()

            csv_files.append(
                {
                    "source_objects": [file],
                    "destination_project_dataset_table": (
                        f"{BQ_PROJECT}.{BQ_BRONZE_DATASET}.{table_name}"
                    ),
                    "schema_fields": schema_fields,
                }
            )

        logger.info(
            "Found %d CSV file(s) under gs://%s/%s: %s",
            len(csv_files),
            GCS_BUCKET,
            GCS_PREFIX,
            [f["source_objects"][0] for f in csv_files],
        )
        return csv_files

    files = get_csv_files()

    check_files = ShortCircuitOperator(
        task_id="check_csv_files",
        python_callable=has_csv_files,
        op_kwargs={"csv_files": files},
    )

    load_to_bronze = GCSToBigQueryOperator.partial(
        task_id="load_to_bronze",
        bucket=GCS_BUCKET,
        source_format="CSV",
        autodetect=False,
        skip_leading_rows=1,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id=GCP_CONN_ID,
    ).expand_kwargs(files)

    files >> check_files >> load_to_bronze


gcs_external_to_bigquery_bronze()
