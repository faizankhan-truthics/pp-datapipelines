"""Airflow DAG: solidgate_chargebacks. Pipeline wiring is shared via build_solidgate_ingest_dag."""

from include.custom_hooks.solidgate.chargebacks import ChargebacksHook
from include.dag_factories.solidgate_ingest import build_solidgate_ingest_dag

build_solidgate_ingest_dag(
    entity="chargebacks",
    hook_cls=ChargebacksHook,
    bronze_asset_uri="solidgate://financial/chargebacks/bronze",
    schedule="15 2 * * *",
)
