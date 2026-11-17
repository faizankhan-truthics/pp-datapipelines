"""Builder for the Microsoft Ads report-ingest DAGs (dags/microsoft_ads/<report>_pipeline.py).

Mirrors include/dag_factories/solidgate_ingest.py's shape: per-account ingest to
Landing/Release, a failure watcher leaf, Release Avro -> staging, staging -> bronze
MERGE, then the bronze Asset the dbt DAG waits on. Every Phase 1 report (Account/
Campaign/AdGroup/Ad/Keyword performance) differs only in report_type/columns/merge SQL,
so a fix to the shared wiring lands on all of them at once instead of drifting.

Kept under include/ (not dags/) so the scheduler doesn't parse it as a DAG file.
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

from include.config.microsoft_ads_settings import (
    BQ_LOCATION,
    GCP_CONN_ID,
    GCS_BUCKET,
    SQL_DIR,
    account_allowed,
    discover_microsoft_ads_accounts,
    resolve_window,
)
from include.custom_hooks.microsoft_ads.base import MicrosoftAdsHook
from include.custom_operators.microsoft_ads.microsoft_ads_to_gcs_operator import (
    report_land_and_release_to_gcs,
)

logger = logging.getLogger(__name__)

BQ_PROJECT = Variable.get("GCP_PROJECT_ID")
BQ_STAGING_DATASET = Variable.get("BQ_STAGING_DATASET")
BQ_BRONZE_DATASET = Variable.get("BQ_BRONZE_DATASET")


def _check_no_conflicting_aggregation(
    *,
    entity: str,
    account_id: str,
    date_from: str,
    date_to: str,
    requested_aggregation: str,
) -> None:
    """Guard against silently mixing Daily and Hourly rows for the same account/date
    range in the same bronze table. time_period's shape differs by grain
    ("yyyy-mm-dd" vs "yyyy-mm-dd|hour"), so the MERGE's ON clause (account_id x
    time_period) never actually collides between the two -- they'd just both exist
    as separate rows, and any downstream consumer that aggregates without filtering
    by aggregation_type would silently double-count that window.

    The Daily override exists only for backfills older than Microsoft's 6-month
    Hourly retention, where Hourly rows can never have existed either -- this fails
    loudly instead of running quietly if asked to produce one grain somewhere the
    other already exists, forcing a deliberate decision rather than a silent
    mixed-grain table. Read-only (no writes), so unlike the stale-row-reconciliation
    gap documented in the merge SQL, there's no destructive-DELETE risk here.
    """
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    from airflow.sdk.exceptions import AirflowFailException
    from google.api_core.exceptions import NotFound
    from google.cloud import bigquery

    table = f"{BQ_PROJECT}.{BQ_BRONZE_DATASET}.microsoft_ads_{entity}"
    client = BigQueryHook(gcp_conn_id=GCP_CONN_ID).get_client(project_id=BQ_PROJECT)
    query = f"""
        SELECT DISTINCT aggregation_type
        FROM `{table}`
        WHERE account_id = @account_id
          AND SUBSTR(time_period, 1, 10) >= @date_from
          AND SUBSTR(time_period, 1, 10) < @date_to
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("account_id", "STRING", account_id),
            bigquery.ScalarQueryParameter("date_from", "STRING", date_from[:10]),
            bigquery.ScalarQueryParameter(
                "date_to",
                "STRING",
                (
                    (
                        datetime.fromisoformat(date_to.strip()) - timedelta(seconds=1)
                    ).date()
                    + timedelta(days=1)
                ).isoformat(),
            ),
        ]
    )
    try:
        rows = list(client.query(query, job_config=job_config).result())
    except NotFound:
        return  # Table doesn't exist yet (first-ever run) -- nothing to conflict with.

    existing = {r["aggregation_type"] for r in rows if r["aggregation_type"]}
    conflicting = existing - {requested_aggregation.lower()}
    if conflicting:
        raise AirflowFailException(
            f"Refusing to run aggregation={requested_aggregation} for account_id="
            f"{account_id} over {date_from[:10]}..{date_to[:10]}: bronze already has "
            f"aggregation_type={sorted(conflicting)} rows in this window. Mixing Daily "
            "and Hourly rows for the same account/date range would silently "
            "double-count downstream -- they never collide/merge into one row, they'd "
            "just both exist side by side. Resolve by not overriding aggregation here, "
            "or by explicitly handling the conflicting rows first."
        )


def build_microsoft_ads_report_dag(
    *,
    entity: str,
    report_type: str,
    columns: list[str],
    bronze_asset_uri: str,
    schedule: str,
    aggregation: str = "Daily",
    start_date: datetime = pendulum.datetime(2026, 10, 1, tz="UTC"),
) -> DAG:
    """Build the ``microsoft_ads_<entity>`` DAG for one Reporting API report type."""
    staging_table = f"{BQ_PROJECT}.{BQ_STAGING_DATASET}.microsoft_ads_{entity}"
    bronze_asset = Asset(bronze_asset_uri)

    # Discovered once at DAG-parse time so the "accounts" param's enum (below) always
    # reflects whatever MSADS_ACCOUNT_ID_<NAME> env vars actually exist in this
    # environment -- adding a new account picks it up on the next parse, no code change.
    account_names = [acc["account_name"] for acc in discover_microsoft_ads_accounts()]

    @dag(
        dag_id=f"microsoft_ads_{entity}",
        schedule=schedule,
        start_date=start_date,
        catchup=False,
        max_active_runs=1,
        default_args={
            "owner": "data-platform",
            "retries": 3,
            "execution_timeout": timedelta(minutes=60),
            "pool": "standard_mem_pool",
        },
        tags=["ingestion", "microsoft_ads", "landing", "release", "staging", "bronze"],
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
                description="Split the date_from/date_to window into per-day report "
                "requests, one per date folder -- matches the Solidgate/GA4 convention "
                "so landing/release folders are always keyed by the actual day, including "
                "today. Disable only to force a single whole-window request (e.g. "
                "troubleshooting); reports are server-aggregated, so either way is cheap.",
            ),
            "aggregation": Param(
                aggregation,
                type="string",
                enum=["Daily", "Hourly"],
                title="Aggregation",
                description="Overrides this DAG's default Aggregation for this run only. "
                "Microsoft only retains Hourly data for 6 months (vs. 36 months for "
                "Daily) -- use Daily for a backfill window older than that, since Hourly "
                "data simply no longer exists past 6 months regardless of what's "
                "requested. Only Daily/Hourly are offered here -- other ReportAggregation "
                "values aren't handled by this pipeline's time_period parsing.",
            ),
            "accounts": Param(
                None,
                type=["array", "null"],
                items={"type": "string", "enum": account_names},
                # Confirmed against Airflow's own params docs: an array param only
                # renders as a free-text "one string per line" textarea by default --
                # the `examples` attribute is what specifically makes the Trigger DAG UI
                # render a multi-select widget instead (items.enum alone, and
                # uniqueItems, don't affect widget choice -- only validate the value).
                examples=account_names,
                title="Accounts",
                description="Restrict this run to specific accounts (by account_name, "
                "the lowercased suffix of their MSADS_ACCOUNT_ID_<NAME> env var). Leave "
                "empty/unset to run every configured account, same as today.",
            ),
        },
        template_searchpath=[str(SQL_DIR)],
    )
    def microsoft_ads_report_pipeline():
        accounts = discover_microsoft_ads_accounts()

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
                    split_by_day = context["params"].get("split_by_day", False)
                    # Per-run override -- falls back to this DAG's own default
                    # (e.g. "Hourly" for campaign_performance) when not explicitly set.
                    run_aggregation = (
                        context["params"].get("aggregation") or aggregation
                    )
                    _check_no_conflicting_aggregation(
                        entity=entity,
                        account_id=acc["account_id"],
                        date_from=date_from,
                        date_to=date_to,
                        requested_aggregation=run_aggregation,
                    )
                    logger.info(
                        "Ingesting %s for account %s (id=%s): %s -> %s (aggregation=%s)",
                        entity,
                        acc_name,
                        acc["account_id"],
                        date_from,
                        date_to,
                        run_aggregation,
                    )
                    hook = MicrosoftAdsHook(
                        account_id_env=acc["account_id_env"],
                        account_name=acc_name,
                    )
                    return report_land_and_release_to_gcs(
                        hook=hook,
                        report_type=report_type,
                        columns=columns,
                        date_from=date_from,
                        date_to=date_to,
                        entity=entity,
                        account_name=acc_name,
                        account_id=acc["account_id"],
                        aggregation=run_aggregation,
                        gcs_bucket=GCS_BUCKET,
                        split_by_day=split_by_day,
                    )

                ingest_tasks.append(ingest_fn())

            build_account_tasks(account)

        @task(trigger_rule="all_done")
        def get_release_paths(results: list[dict]) -> list[str]:
            from airflow.exceptions import AirflowSkipException
            from airflow.sdk.exceptions import AirflowFailException

            # A failed ingest task never pushes an XCom, so it shows up here as None --
            # distinguishing "every account failed" (fail, so bronze_ready's none_failed
            # trigger rule correctly blocks and the asset is never emitted) from "every
            # account succeeded but genuinely had zero rows this window" (skip, which
            # none_failed still tolerates -- a legitimate run, not a false signal).
            successful_results = [r for r in results if r is not None]
            if not successful_results:
                raise AirflowFailException(
                    "All account ingests failed -- no successful results, so bronze "
                    "cannot be refreshed. Failing (not skipping) so the downstream "
                    "bronze_ready asset is not falsely emitted on a total outage."
                )

            paths = [p for r in successful_results for p in r.get("release_paths", [])]
            if not paths:
                raise AirflowSkipException(
                    "No records across all accounts. Skipping BigQuery load."
                )
            directories = sorted({p.rsplit("/", 1)[0] for p in paths})
            return [f"{directory}/*.avro" for directory in directories]

        release_paths = get_release_paths(ingest_tasks)

        # Run state is decided by leaf tasks only, so this fails the run (no retries) when
        # any account ingest fails, while the other accounts still load and emit bronze.
        @task(trigger_rule="one_failed")
        def ingest_failure_watcher():
            from airflow.sdk.exceptions import AirflowFailException

            raise AirflowFailException(
                "One or more account ingests failed; bronze was refreshed for the remaining accounts only."
            )

        ingest_tasks >> ingest_failure_watcher()

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

    return microsoft_ads_report_pipeline()
