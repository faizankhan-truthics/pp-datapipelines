"""Airflow DAG: solidgate_subscriptions. Pipeline wiring is shared via build_solidgate_ingest_dag."""

from include.custom_hooks.solidgate.subscriptions import SubscriptionsHook
from include.dag_factories.solidgate_ingest import build_solidgate_ingest_dag

build_solidgate_ingest_dag(
    entity="subscriptions",
    hook_cls=SubscriptionsHook,
    bronze_asset_uri="solidgate://financial/subscriptions/bronze",
    schedule="0 3 * * *",
    # Feeds int_solidgate_subscription_history (seeded once by
    # subscriptions_release_history_backfill_dag.py).
    append_release_history=True,
)
