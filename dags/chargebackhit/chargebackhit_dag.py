import os
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

from include.config.chargebackhit_settings import ENDPOINTS, GCP_CONN_ID, SQL_DIR
from include.custom_operators.chargebackhit.chargebackhit_to_gcs import (
    ChargebackhitToGCSOperator,
)

CHARGEBACKHIT_BRONZE_DATASET = Asset("data://bronze/chargebackhit")


def discover_chargebackhit_accounts() -> list:
    prefix = "CHARGEBACKHIT_"
    result = {}

    for key, value in os.environ.items():
        if not key.startswith(prefix) or not value:
            continue

        parts = key[len(prefix) :].split("_API_")

        if len(parts) != 2:
            continue

        account = parts[0].lower()
        key_type = parts[1].lower()

        if key_type == "public_key":
            result.setdefault(account, {})["public_key_env"] = key

        elif key_type == "secret_key":
            result.setdefault(account, {})["secret_key_env"] = key

    accounts = []
    for account, values in result.items():
        if "public_key_env" not in values or "secret_key_env" not in values:
            raise ValueError(
                f"Incomplete ChargebackHit credentials for account '{account}'. "
                "Both *_API_PUBLIC_KEY and *_API_SECRET_KEY environment variables are required."
            )
        accounts.append({"account_name": account, **values})

    return accounts


with DAG(
    dag_id="chargebackhit_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "data-platform", "retries": 3, "pool": "standard_mem_pool"},
    tags=["ingestion", "chargebackhit", "bronze"],
    max_active_runs=1,
    max_active_tasks=15,
    template_searchpath=[SQL_DIR],
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

    accounts_list = discover_chargebackhit_accounts()

    if not accounts_list:
        raise ValueError("No ChargebackHit accounts found in environment variables")

    def build_endpoint_pipeline(endpoint):
        # 1. EXTRACT (API -> Landing JSON -> Release Avro, one file per API page)
        extract_tasks = ChargebackhitToGCSOperator.partial(
            task_id=f"extract_{endpoint}",
            endpoint=endpoint,
            gcs_bucket=GCS_BUCKET_NAME,
            gcp_conn_id=GCP_CONN_ID,
        ).expand_kwargs(accounts_list)

        # One wildcard per account+day folder, not one exact URI per page -- avoids
        # BigQuery's 10,000-sourceUris-per-job limit on large backfills.
        @task(
            task_id=f"collect_release_paths_{endpoint}",
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )
        def collect_release_paths(results: list) -> list:
            paths = [path for r in results if r for path in r.get("release_paths", [])]
            if not paths:
                raise AirflowSkipException(
                    f"No release files produced for '{endpoint}'. Skipping BigQuery load."
                )
            directories = sorted({path.rsplit("/", 1)[0] for path in paths})
            return [f"{directory}/*.avro" for directory in directories]

        release_paths = collect_release_paths(extract_tasks.output)

        # 2. LOAD (Release Avro -> BigQuery Staging)
        load_staging = GCSToBigQueryOperator(
            task_id=f"load_staging_{endpoint}",
            gcp_conn_id=GCP_CONN_ID,
            bucket=GCS_BUCKET_NAME,
            source_objects=release_paths,
            destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_STAGING_DATASET}.chargebackhit_{endpoint}",
            source_format="AVRO",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            ignore_unknown_values=True,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
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

        release_paths >> load_staging >> transform_bronze

        return transform_bronze

    bronze_tasks = [build_endpoint_pipeline(endpoint) for endpoint in ENDPOINTS]

    # Publish only after all endpoint Bronze merges complete.
    chargebackhit_bronze_ready = EmptyOperator(
        task_id="chargebackhit_bronze_ready",
        outlets=[CHARGEBACKHIT_BRONZE_DATASET],
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    bronze_tasks >> chargebackhit_bronze_ready
