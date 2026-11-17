"""Daily Stripe pipeline: extracts each endpoint per account to GCS and loads it to BigQuery bronze."""

import logging
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

from include.config.stripe_settings import (
    ENDPOINT_MAX_WORKERS_OVERRIDES,
    GCP_CONN_ID,
    LEGACY_ENDPOINTS,
    NEW_ENDPOINTS,
    SQL_DIR,
    discover_stripe_accounts,
)
from include.custom_operators.stripe.stripe_to_gcs_operator import StripeToGCSOperator

logger = logging.getLogger(__name__)
STRIPE_BRONZE_DATASET = Asset("stripe://financial/bronze")


with DAG(
    dag_id="stripe_financial_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "data-platform", "retries": 3, "pool": "standard_mem_pool"},
    tags=["ingestion", "stripe", "bronze"],
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

    accounts_list = discover_stripe_accounts()

    ALL_ENDPOINTS = LEGACY_ENDPOINTS + NEW_ENDPOINTS

    # GATE: conf = {"endpoints": [...]} limits the run to those endpoints. One branch task
    # picks which extract_<endpoint> tasks run; the rest are skipped before any account
    # instance starts, and the skip cascades via trigger rules to collect/load/transform.
    # stripe_bronze_ready (NONE_FAILED) still runs.
    @task.branch(task_id="select_endpoints")
    def select_endpoints(**context) -> list:
        dag_run = context.get("dag_run")
        requested_endpoints = (dag_run.conf or {}).get("endpoints") if dag_run else None
        selected = [
            e
            for e in ALL_ENDPOINTS
            if not requested_endpoints or e in requested_endpoints
        ]
        skipped = sorted(set(ALL_ENDPOINTS) - set(selected))
        if skipped:
            logger.info(
                "Skipping endpoints not in conf['endpoints'] allowlist %s: %s",
                requested_endpoints,
                skipped,
            )
        return [f"extract_{e}" for e in selected]

    endpoint_gate = select_endpoints()

    def build_endpoint_pipeline(endpoint, accounts_for_endpoint):
        # 1. EXTRACT (API -> Landing JSON -> Release Avro, one file per Stripe API page)
        extract_tasks = StripeToGCSOperator.partial(
            task_id=f"extract_{endpoint}",
            endpoint=endpoint,
            gcs_bucket=GCS_BUCKET_NAME,
            gcp_conn_id=GCP_CONN_ID,
            max_workers=ENDPOINT_MAX_WORKERS_OVERRIDES.get(endpoint, 10),
        ).expand_kwargs(accounts_for_endpoint)

        endpoint_gate >> extract_tasks

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
            destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_STAGING_DATASET}.stripe_{endpoint}",
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
                "source": "api",
            },
        )

        release_paths >> load_staging >> transform_bronze

        return transform_bronze

    bronze_tasks = []

    for endpoint in ALL_ENDPOINTS:
        bronze_tasks.append(build_endpoint_pipeline(endpoint, accounts_list))

    bronze_ready = EmptyOperator(
        task_id="stripe_bronze_ready",
        outlets=[STRIPE_BRONZE_DATASET],
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    bronze_tasks >> bronze_ready
