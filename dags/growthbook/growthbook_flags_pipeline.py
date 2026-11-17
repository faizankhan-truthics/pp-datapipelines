"""Daily GrowthBook pipeline: feature flags and projects from the GrowthBook API to BigQuery bronze."""

from datetime import datetime, timedelta

from airflow.models import DAG, Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.sdk import Asset
from airflow.utils.trigger_rule import TriggerRule

from include.config.growthbook_settings import (
    ENDPOINTS,
    GCP_CONN_ID,
    GROWTHBOOK_API_KEY_TEMPLATE,
    SQL_DIR,
)
from include.custom_operators.growthbook.growthbook_to_gcs_operator import (
    GrowthBookToGCSOperator,
)

# No DAG consumes this Asset on purpose: GrowthBook data must never block other DAGs. Do NOT add
# it to dbt_dag's schedule: that list is an AND over every bronze Asset, so one failed GrowthBook
# run would hold back the dbt refresh of every other pipeline.
GROWTHBOOK_FLAGS_BRONZE_DATASET = Asset("growthbook://features/bronze")

with DAG(
    dag_id="growthbook_flags_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    default_args={
        "owner": "data-platform",
        "retries": 3,
        "execution_timeout": timedelta(minutes=30),
        "pool": "standard_mem_pool",
    },
    tags=["ingestion", "growthbook", "bronze"],
    max_active_runs=1,
    template_searchpath=[str(SQL_DIR)],
) as dag:
    GCS_BUCKET_NAME = Variable.get("GCS_LANDING_BUCKET")
    BQ_PROJECT = Variable.get("GCP_PROJECT_ID")
    BQ_STAGING_DATASET = Variable.get("BQ_STAGING_DATASET")
    BQ_BRONZE_DATASET = Variable.get("BQ_BRONZE_DATASET")

    bronze_tasks = []
    for endpoint in ENDPOINTS:
        # 1. EXTRACT (API -> Landing JSON -> Release Avro): a full snapshot every run.
        extract_task = GrowthBookToGCSOperator(
            task_id=f"extract_{endpoint}",
            endpoint=endpoint,
            api_key=GROWTHBOOK_API_KEY_TEMPLATE,
            gcs_bucket=GCS_BUCKET_NAME,
            gcp_conn_id=GCP_CONN_ID,
        )

        # 2. LOAD (Release Avro -> BigQuery Staging)
        load_staging = GCSToBigQueryOperator(
            task_id=f"load_staging_{endpoint}",
            gcp_conn_id=GCP_CONN_ID,
            bucket=GCS_BUCKET_NAME,
            # the exact file this run's extract wrote (its path comes back as the task's return value),
            # so the load needs no date logic of its own and works for manual runs too
            source_objects=[
                f"{{{{ ti.xcom_pull(task_ids='extract_{endpoint}')['release_path'] }}}}"
            ],
            destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_STAGING_DATASET}.growthbook_{endpoint}",
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
    growthbook_flags_bronze_ready = EmptyOperator(
        task_id="growthbook_flags_bronze_ready",
        outlets=[GROWTHBOOK_FLAGS_BRONZE_DATASET],
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    bronze_tasks >> growthbook_flags_bronze_ready
