"""Airflow DAG: solidgate_financial_entries. Pipeline wiring is shared via build_solidgate_ingest_dag."""

import pendulum

from include.custom_hooks.solidgate.financial_entries import FinancialEntriesHook
from include.dag_factories.solidgate_ingest import build_solidgate_ingest_dag

build_solidgate_ingest_dag(
    entity="financial_entries",
    hook_cls=FinancialEntriesHook,
    bronze_asset_uri="solidgate://financial/entries/bronze",
    schedule="30 2 * * *",
    start_date=pendulum.datetime(2022, 1, 1, tz="UTC"),
    # Async CSV report (generate -> poll -> presigned S3 URL), not paginated JSON.
    report=True,
)
