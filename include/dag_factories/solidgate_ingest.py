"""Builder for the Solidgate ingest DAGs (dags/solidgate/<entity>_dag.py).

Every entity runs the same pipeline -- per-account ingest to Landing/Release, a failure
watcher leaf, Release Avro -> staging, staging -> bronze MERGE, then the bronze Asset the
dbt DAG waits on. The per-entity DAG files only pass in what actually differs (hook,
schedule, Asset, report vs. paginated landing, and subscriptions' release-history append),
so a fix to the shared wiring lands on all six DAGs at once instead of drifting.

Kept under include/ (not dags/) so the scheduler doesn't parse it as a DAG file; each
entity keeps its own *_dag.py so a parse error in one stays isolated to that DAG.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import pendulum
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.sdk import DAG, Asset, Param, dag, get_current_context, task

from include.config.solidgate_settings import (
    BQ_LOCATION,
    GCP_CONN_ID,
    GCS_BUCKET,
    SQL_DIR,
    account_allowed,
    get_solidgate_accounts,
    resolve_window,
)
from include.custom_hooks.solidgate.base import SolidgateBaseHook
from include.custom_operators.solidgate.solidgate_to_gcs_operator import (
    land_and_release_to_gcs,
    report_land_and_release_to_gcs,
)

logger = logging.getLogger(__name__)

BQ_PROJECT = Variable.get("GCP_PROJECT_ID")
BQ_STAGING_DATASET = Variable.get("BQ_STAGING_DATASET")
BQ_BRONZE_DATASET = Variable.get("BQ_BRONZE_DATASET")


def build_solidgate_ingest_dag(
    *,
    entity: str,
    hook_cls: type[SolidgateBaseHook],
    bronze_asset_uri: str,
    schedule: str,
    start_date: datetime = pendulum.datetime(2024, 1, 1, tz="UTC"),
    report: bool = False,
    append_release_history: bool = False,
) -> DAG:
    """Build the ``solidgate_<entity>`` DAG.

    ``report`` lands via the async CSV report flow (``report_land_and_release_to_gcs``,
    financial_entries) instead of the paginated JSON flow. ``append_release_history``
    also appends every Release file to ``solidgate_<entity>_release_history`` in bronze.
    """
    staging_table = f"{BQ_PROJECT}.{BQ_STAGING_DATASET}.solidgate_{entity}"
    bronze_asset = Asset(bronze_asset_uri)

    @dag(
        dag_id=f"solidgate_{entity}",
        schedule=schedule,
        start_date=start_date,
        catchup=False,
        max_active_runs=1,
        default_args={
            "owner": "data-platform",
            "retries": 3,
            "execution_timeout": timedelta(minutes=120),
            "pool": "standard_mem_pool",
        },
        tags=["ingestion", "solidgate", "landing", "release", "staging", "bronze"],
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
            "split_by_day": Param(
                True,
                type="boolean",
                title="Split by day",
                description="Split the date_from/date_to window into per-day requests. "
                "Disable to send the whole window as a single request.",
            ),
        },
        template_searchpath=[str(SQL_DIR)],
    )
    def solidgate_ingest_pipeline():
        accounts = get_solidgate_accounts()

        ingest_tasks = []

        for account in accounts:

            def build_account_tasks(acc):
                acc_name = acc["account_name"]

                @task(task_id=f"ingest_{acc_name}")
                def ingest_fn() -> dict:
                    context = get_current_context()
                    if not account_allowed(context, acc_name):
                        from airflow.exceptions import AirflowSkipException

                        raise AirflowSkipException(
                            f"Skipping account '{acc_name}' - not in conf['accounts'] allowlist"
                        )
                    _ds, date_from, date_to = resolve_window(context)
                    split_by_day = context["params"].get("split_by_day", True)
                    logger.info(
                        f"Ingesting {entity} for account {acc_name}: {date_from} -> {date_to}"
                    )
                    hook = hook_cls(
                        public_key_env=acc["public_key_env"],
                        secret_key_env=acc["secret_key_env"],
                    )

                    # Resolved at call time (not bound at build time) so tests can stub
                    # either land function in this module's globals.
                    land_fn = (
                        report_land_and_release_to_gcs
                        if report
                        else land_and_release_to_gcs
                    )
                    return land_fn(
                        hook=hook,
                        date_from=date_from,
                        date_to=date_to,
                        entity=entity,
                        account_name=acc_name,
                        gcs_bucket=GCS_BUCKET,
                        split_by_day=split_by_day,
                    )

                ingest_tasks.append(ingest_fn())

            build_account_tasks(account)

        @task(trigger_rule="all_done")
        def get_release_paths(results: list[dict]) -> list[str]:
            from airflow.exceptions import AirflowSkipException

            paths = [p for r in results if r for p in r.get("release_paths", [])]
            if not paths:
                raise AirflowSkipException(
                    "No records across all accounts. Skipping BigQuery load."
                )
            directories = sorted({p.rsplit("/", 1)[0] for p in paths})
            return [f"{directory}/*.avro" for directory in directories]

        release_paths = get_release_paths(ingest_tasks)

        # Run state is decided by leaf tasks only, and get_release_paths runs all_done, so with
        # bronze_ready as the sole leaf a permanently failing account would end in a green run. This leaf fails the run (no
        # retries) when any ingest fails, while the other accounts still load and emit.
        @task(trigger_rule="one_failed")
        def ingest_failure_watcher():
            from airflow.sdk.exceptions import AirflowFailException

            raise AirflowFailException(
                "One or more account ingests failed; bronze was refreshed for the remaining accounts only."
            )

        ingest_tasks >> ingest_failure_watcher()

        # Release Avro -> BigQuery staging (Avro carries its own schema; no autodetect).
        load_staging = GCSToBigQueryOperator(
            task_id="load_staging",
            gcp_conn_id=GCP_CONN_ID,
            bucket=GCS_BUCKET,
            source_objects=release_paths,
            source_format="AVRO",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
            destination_project_dataset_table=staging_table,
            location=BQ_LOCATION,
        )

        # Staging -> bronze via the static, typed MERGE (keyed on id).
        merge_bronze = BigQueryInsertJobOperator(
            task_id="merge_bronze",
            gcp_conn_id=GCP_CONN_ID,
            location=BQ_LOCATION,
            configuration={
                "query": {"query": f"merge_{entity}.sql", "useLegacySql": False}
            },
            params={
                "bq_project": BQ_PROJECT,
                "bq_staging_dataset": BQ_STAGING_DATASET,
                "bq_bronze_dataset": BQ_BRONZE_DATASET,
                "endpoint": entity,
            },
        )

        bronze_ready = EmptyOperator(
            task_id="bronze_ready",
            outlets=[bronze_asset],
            trigger_rule="none_failed",
        )

        release_paths >> load_staging >> merge_bronze >> bronze_ready

        if append_release_history:
            append_history = GCSToBigQueryOperator(
                task_id="append_release_history",
                gcp_conn_id=GCP_CONN_ID,
                bucket=GCS_BUCKET,
                source_objects=release_paths,
                source_format="AVRO",
                write_disposition="WRITE_APPEND",
                create_disposition="CREATE_IF_NEEDED",
                destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_BRONZE_DATASET}.solidgate_{entity}_release_history",
                location=BQ_LOCATION,
            )
            release_paths >> append_history >> bronze_ready

    return solidgate_ingest_pipeline()
