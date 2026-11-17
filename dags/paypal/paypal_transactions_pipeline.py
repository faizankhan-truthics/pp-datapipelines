"""Daily PayPal sandbox Transaction Search API ingestion to BigQuery bronze."""

from datetime import datetime

from airflow.exceptions import AirflowSkipException
from airflow.models import DAG, Variable
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.sdk import Asset, task
from airflow.utils.trigger_rule import TriggerRule

from include.config.paypal_settings import GCP_CONN_ID, SQL_DIR
from include.custom_operators.paypal.paypal_to_gcs_operator import (
    PaypalToGCSOperator,
)

PAYPAL_BRONZE_DATASET = Asset("paypal://transactions/bronze")


with DAG(
    dag_id="paypal_transactions_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "data-platform", "retries": 3, "pool": "standard_mem_pool"},
    tags=["ingestion", "paypal", "sandbox", "bronze"],
    max_active_runs=1,
    template_searchpath=[str(SQL_DIR)],
    params={
        "date_from": "",
        "date_to": "",
        "run_type": Param(
            "daily",
            type="string",
            enum=["daily", "backfill"],
            title="Run type",
            description="Which Astro worker queue to run this DAG's tasks on.",
        ),
    },
) as dag:
    GCS_BUCKET_NAME = Variable.get("GCS_LANDING_BUCKET")
    BQ_PROJECT = Variable.get("GCP_PROJECT_ID")
    BQ_STAGING_DATASET = Variable.get("BQ_STAGING_DATASET")
    BQ_BRONZE_DATASET = Variable.get("BQ_BRONZE_DATASET")

    extract_transactions = PaypalToGCSOperator(
        task_id="extract_transactions",
        gcs_bucket=GCS_BUCKET_NAME,
        gcp_conn_id=GCP_CONN_ID,
    )

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def collect_release_paths(result: dict) -> list[str]:
        paths = result.get("release_paths", [])
        if not paths:
            raise AirflowSkipException(
                "PayPal returned no transaction pages; skipping the BigQuery load."
            )
        directories = sorted({path.rsplit("/", 1)[0] for path in paths})
        return [f"{directory}/*.avro" for directory in directories]

    release_paths = collect_release_paths(extract_transactions.output)

    load_staging = GCSToBigQueryOperator(
        task_id="load_staging",
        gcp_conn_id=GCP_CONN_ID,
        bucket=GCS_BUCKET_NAME,
        source_objects=release_paths,
        destination_project_dataset_table=(
            f"{BQ_PROJECT}.{BQ_STAGING_DATASET}.paypal_transactions"
        ),
        source_format="AVRO",
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        ignore_unknown_values=True,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    merge_bronze = BigQueryInsertJobOperator(
        task_id="merge_bronze",
        gcp_conn_id=GCP_CONN_ID,
        configuration={
            "query": {"query": "merge_transactions.sql", "useLegacySql": False}
        },
        params={
            "bq_project": BQ_PROJECT,
            "bq_staging_dataset": BQ_STAGING_DATASET,
            "bq_bronze_dataset": BQ_BRONZE_DATASET,
        },
    )

    bronze_ready = EmptyOperator(
        task_id="bronze_ready",
        outlets=[PAYPAL_BRONZE_DATASET],
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    release_paths >> load_staging >> merge_bronze >> bronze_ready
