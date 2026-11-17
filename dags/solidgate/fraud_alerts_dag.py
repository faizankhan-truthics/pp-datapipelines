"""Airflow DAG: solidgate_fraud_alerts. Pipeline wiring is shared via build_solidgate_ingest_dag."""

from include.custom_hooks.solidgate.fraud_alerts import FraudAlertsHook
from include.dag_factories.solidgate_ingest import build_solidgate_ingest_dag

build_solidgate_ingest_dag(
    entity="fraud_alerts",
    hook_cls=FraudAlertsHook,
    bronze_asset_uri="solidgate://financial/fraud/bronze",
    schedule="45 2 * * *",
)
