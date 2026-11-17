"""Airflow DAG: solidgate_apm_orders. Pipeline wiring is shared via build_solidgate_ingest_dag."""

from include.custom_hooks.solidgate.apm_orders import ApmOrdersHook
from include.dag_factories.solidgate_ingest import build_solidgate_ingest_dag

build_solidgate_ingest_dag(
    entity="apm_orders",
    hook_cls=ApmOrdersHook,
    bronze_asset_uri="solidgate://financial/apm/bronze",
    schedule="45 1 * * *",
)
