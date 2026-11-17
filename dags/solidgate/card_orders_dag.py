"""Airflow DAG: solidgate_card_orders. Pipeline wiring is shared via build_solidgate_ingest_dag."""

from include.custom_hooks.solidgate.card_orders import CardOrdersHook
from include.dag_factories.solidgate_ingest import build_solidgate_ingest_dag

build_solidgate_ingest_dag(
    entity="card_orders",
    hook_cls=CardOrdersHook,
    bronze_asset_uri="solidgate://financial/card/bronze",
    schedule="0 2 * * *",
)
