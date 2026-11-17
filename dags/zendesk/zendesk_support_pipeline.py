"""Daily Zendesk Support pipeline: tickets, users and ticket comments from the Zendesk API to BigQuery bronze."""

from datetime import datetime, timedelta

from airflow.models import DAG, Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.sdk import Asset
from airflow.utils.trigger_rule import TriggerRule

from include.config.zendesk_settings import (
    ENDPOINTS,
    GCP_CONN_ID,
    SQL_DIR,
    ZENDESK_API_TOKEN,
    ZENDESK_EMAIL,
    ZENDESK_SUBDOMAIN,
)
from include.custom_operators.zendesk.zendesk_to_gcs_operator import (
    ZendeskToGCSOperator,
)

ZENDESK_BRONZE_DATASET = Asset("zendesk://financial/bronze")

with (
    DAG(
        dag_id="zendesk_support_pipeline",
        start_date=datetime(2026, 1, 1),
        schedule="15 3 * * *",
        catchup=False,
        default_args={
            "owner": "data-platform",
            "retries": 3,
            "execution_timeout": timedelta(minutes=120),
            "pool": "standard_mem_pool",
        },
        tags=["ingestion", "zendesk", "bronze"],
        max_active_runs=1,
        max_active_tasks=15,
        template_searchpath=[str(SQL_DIR)],
        params={
            "date_from": "",  # e.g. "2026-08-10" -- test run on a small slice, runs up to now
        },
    ) as dag
):
    GCS_BUCKET_NAME = Variable.get("GCS_LANDING_BUCKET")
    BQ_PROJECT = Variable.get("GCP_PROJECT_ID")
    BQ_STAGING_DATASET = Variable.get("BQ_STAGING_DATASET")
    BQ_BRONZE_DATASET = Variable.get("BQ_BRONZE_DATASET")

    ds_macro = "{% if data_interval_start == data_interval_end %}{{ logical_date.strftime('%Y-%m-%d') }}{% else %}{{ data_interval_start.strftime('%Y-%m-%d') }}{% endif %}"

    bronze_tasks = []
    for endpoint in ENDPOINTS:
        # 1. EXTRACT (API -> Landing JSON -> Release Avro), resuming from the checkpointed cursor for this endpoint.
        extract_task = ZendeskToGCSOperator(
            task_id=f"extract_{endpoint}",
            endpoint=endpoint,
            subdomain=ZENDESK_SUBDOMAIN,
            email=ZENDESK_EMAIL,
            api_token=ZENDESK_API_TOKEN,
            gcs_bucket=GCS_BUCKET_NAME,
            gcp_conn_id=GCP_CONN_ID,
        )

        # 2. LOAD (Release Avro -> BigQuery Staging)
        load_staging = GCSToBigQueryOperator(
            task_id=f"load_staging_{endpoint}",
            gcp_conn_id=GCP_CONN_ID,
            bucket=GCS_BUCKET_NAME,
            source_objects=[f"release/zendesk/{endpoint}/{ds_macro}/*.avro"],
            destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_STAGING_DATASET}.zendesk_{endpoint}",
            source_format="AVRO",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            ignore_unknown_values=True,
        )

        # 3. TRANSFORM (Merge Staging -> Bronze)
        transform_bronze = BigQueryInsertJobOperator(
            task_id=f"transform_bronze_{endpoint}",
            gcp_conn_id=GCP_CONN_ID,
            configuration={
                "query": {
                    "query": f"merge_{endpoint}.sql",
                    "useLegacySql": False,
                }
            },
            params={
                "bq_project": BQ_PROJECT,
                "bq_staging_dataset": BQ_STAGING_DATASET,
                "bq_bronze_dataset": BQ_BRONZE_DATASET,
                "endpoint": endpoint,
            },
        )

        extract_task >> load_staging >> transform_bronze

        bronze_tasks.append(transform_bronze)

    # Publish only after all endpoint Bronze merges complete.
    zendesk_bronze_ready = EmptyOperator(
        task_id="zendesk_bronze_ready",
        outlets=[ZENDESK_BRONZE_DATASET],
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    bronze_tasks >> zendesk_bronze_ready
