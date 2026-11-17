"""Stripe /v1/events pipeline: runs after stripe_financial_pipeline and loads the previous UTC day to BigQuery bronze."""

from datetime import datetime

from airflow.exceptions import AirflowSkipException
from airflow.models import DAG, Variable
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.sdk import Asset, get_current_context, task
from airflow.utils.trigger_rule import TriggerRule

from include.config.stripe_settings import (
    EVENTS_ENDPOINT,
    GCP_CONN_ID,
    SQL_DIR,
    discover_stripe_accounts,
)
from include.custom_operators.stripe.stripe_to_gcs_operator import extract_events_to_gcs

# Emitted by stripe_financial_pipeline when its bronze load finishes; triggers this DAG.
STRIPE_BRONZE_DATASET = Asset("stripe://financial/bronze")
STRIPE_EVENTS_BRONZE_DATASET = Asset("stripe://events/bronze")

HOURS_PER_DAY = 24
EVENT_SOURCED_ENDPOINTS = [
    "charges",
    "disputes",
    "refunds",
    "payment_intents",
    "radar_early_fraud_warnings",
    "customers",
    "products",
    "prices",
    "subscriptions",
    "invoices",
]


def collapse_release_dirs(release_paths: list) -> list:
    """Turn release paths into BQ wildcard URIs. Full days collapse to <day>/*.avro to
    stay under BigQuery's 10k source URI limit, partial days stay per hour."""
    hours_by_day = {}
    for path in release_paths:
        hour_dir = path.rsplit("/", 1)[0]
        day_dir, _hour = hour_dir.rsplit("/", 1)
        hours_by_day.setdefault(day_dir, set()).add(hour_dir)

    uris = []
    for day_dir, hour_dirs in sorted(hours_by_day.items()):
        if len(hour_dirs) == HOURS_PER_DAY:
            uris.append(f"{day_dir}/*.avro")
        else:
            uris.extend(f"{hour_dir}/*.avro" for hour_dir in sorted(hour_dirs))
    return uris


# Stripe events pipeline, runs after stripe_financial_pipeline and loads the previous UTC day.
# The API load inserts new objects first; the event merges then update their state.
# Backfill conf: {"date_from": ..., "date_to": ..., "accounts": [...]}, date_to excluded
with DAG(
    dag_id="stripe_events_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=STRIPE_BRONZE_DATASET,
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

    endpoint = EVENTS_ENDPOINT

    # 1. EXTRACT (API -> Landing JSON -> Release Avro)
    # api key is fetched inside the task so it doesn't show up in the UI
    @task(
        task_id=f"extract_{endpoint}", map_index_template="account_{{ account_name }}"
    )
    def extract_events(account_name: str) -> dict:
        context = get_current_context()
        context["account_name"] = account_name
        account = next(
            a for a in discover_stripe_accounts() if a["account_name"] == account_name
        )
        return extract_events_to_gcs(
            context=context,
            account_name=account_name,
            account_id=account["account_id"],
            api_key_env=account["api_key_env"],
            gcs_bucket=GCS_BUCKET_NAME,
            gcp_conn_id=GCP_CONN_ID,
        )

    extract_results = extract_events.expand(
        account_name=[a["account_name"] for a in discover_stripe_accounts()]
    )

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
        return collapse_release_dirs(paths)

    release_paths = collect_release_paths(extract_results)

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
        },
    )

    object_merges = [
        BigQueryInsertJobOperator(
            task_id=f"merge_{object_endpoint}_from_events",
            gcp_conn_id=GCP_CONN_ID,
            configuration={
                "query": {
                    "query": f"merge_{object_endpoint}.sql",
                    "useLegacySql": False,
                }
            },
            params={
                "bq_project": BQ_PROJECT,
                "bq_staging_dataset": BQ_STAGING_DATASET,
                "bq_bronze_dataset": BQ_BRONZE_DATASET,
                "endpoint": object_endpoint,
                "source": "events",
            },
        )
        for object_endpoint in EVENT_SOURCED_ENDPOINTS
    ]

    @task.branch(task_id="select_object_endpoints")
    def select_object_endpoints(**context) -> list[str]:
        dag_run = context.get("dag_run")
        requested_endpoints = (dag_run.conf or {}).get("endpoints") if dag_run else None
        if not requested_endpoints:
            selected = EVENT_SOURCED_ENDPOINTS
        else:
            if not isinstance(requested_endpoints, list) or not all(
                isinstance(endpoint, str) for endpoint in requested_endpoints
            ):
                raise ValueError(
                    "conf['endpoints'] must be a list of event object endpoint names"
                )
            unknown = sorted(set(requested_endpoints) - set(EVENT_SOURCED_ENDPOINTS))
            if unknown:
                raise ValueError(
                    f"Unknown event object endpoint(s) in conf['endpoints']: {unknown}. "
                    f"Allowed endpoints: {EVENT_SOURCED_ENDPOINTS}"
                )
            selected = [
                endpoint
                for endpoint in EVENT_SOURCED_ENDPOINTS
                if endpoint in requested_endpoints
            ]
        return [f"merge_{endpoint}_from_events" for endpoint in selected]

    endpoint_gate = select_object_endpoints()

    # separate asset so it doesn't retrigger the financial dbt DAGs
    events_bronze_ready = EmptyOperator(
        task_id="stripe_events_bronze_ready",
        outlets=[STRIPE_EVENTS_BRONZE_DATASET],
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    release_paths >> load_staging >> transform_bronze >> endpoint_gate >> object_merges
    object_merges >> events_bronze_ready
