"""Wiring tests for the dbt DAGs in dags/dbt/.

`dbt_dag` runs off bronze Assets and emits dbt://bigquery_dbt/marts, which
`dbt_observability` and `daily_vamp_status` schedule on. Assets match by URI string, so a
typo on either side silently never fires — these tests pin every URI to a real producer.
"""

from __future__ import annotations

import logging

import pytest
from airflow.models import DagBag
from airflow.sdk import AssetAll

from include.config.observability_settings import DBT_MARTS_READY_URI

DBT_DAG_BRONZE_URIS = {
    "stripe://financial/bronze",
    "stripe://events/bronze",
    "data://bronze/chargebackhit",
    "data://bronze/exchange_rates/daily_fx",
    "data://bronze/ga4/analytics",
    "solidgate://financial/apm/bronze",
    "solidgate://financial/card/bronze",
    "solidgate://financial/chargebacks/bronze",
    "solidgate://financial/entries/bronze",
    "solidgate://financial/fraud/bronze",
    "solidgate://financial/subscriptions/bronze",
    "paypal://transactions/bronze",
    "zendesk://financial/bronze",
    "data://bronze/anytrack/events",
    "data://bronze/files_editor_db/billing",
    "data://bronze/microsoft_ads/campaign_performance",
    "data://bronze/microsoft_ads/account_performance",
    "data://bronze/microsoft_ads/ad_group_performance",
    "data://bronze/microsoft_ads/ad_performance",
    "data://bronze/microsoft_ads/keyword_performance",
    "data://bronze/chatbox_db/ai_response_activation",
    "data://bronze/microsoft_ads/conversion_performance",
}

MARTS_CONSUMERS = ["dbt_observability", "daily_vamp_status"]


@pytest.fixture(scope="module")
def dag_bag():
    logger = logging.getLogger("airflow")
    old_value, logger.disabled = logger.disabled, True
    try:
        yield DagBag(include_examples=False)
    finally:
        logger.disabled = old_value


def _asset_uris(condition) -> set[str]:
    # AssetAll / AssetAny nest their members under .objects; a plain Asset has .uri.
    if hasattr(condition, "objects"):
        return set().union(*(_asset_uris(o) for o in condition.objects))
    return {condition.uri}


def _schedule_uris(dag) -> set[str]:
    return _asset_uris(dag.timetable.asset_condition)


def _emitted_uris(dag) -> set[str]:
    return {
        outlet.uri
        for task in dag.tasks
        for outlet in task.outlets
        if hasattr(outlet, "uri")
    }


def test_dbt_dag_waits_on_all_bronze_assets(dag_bag):
    dag = dag_bag.dags["dbt_dag"]

    assert isinstance(dag.timetable.asset_condition, AssetAll)
    assert _schedule_uris(dag) == DBT_DAG_BRONZE_URIS


def test_every_bronze_asset_is_emitted_by_an_ingestion_dag(dag_bag):
    emitted = set().union(*(_emitted_uris(d) for d in dag_bag.dags.values()))

    assert DBT_DAG_BRONZE_URIS - emitted == set()


def test_dbt_done_is_the_only_leaf_and_emits_the_marts_asset(dag_bag):
    dag = dag_bag.dags["dbt_dag"]

    assert [t.task_id for t in dag.leaves] == ["dbt_done"]
    dbt_done = dag.get_task("dbt_done")
    assert dbt_done.trigger_rule == "all_done"
    assert _emitted_uris(dag) == {DBT_MARTS_READY_URI}


def test_only_dbt_dag_emits_the_marts_asset(dag_bag):
    # A second emitter fires dbt_observability / daily_vamp_status twice per cycle.
    emitters = sorted(
        d.dag_id
        for d in dag_bag.dags.values()
        if DBT_MARTS_READY_URI in _emitted_uris(d)
    )

    assert emitters == ["dbt_dag"]


@pytest.mark.parametrize("dag_id", MARTS_CONSUMERS)
def test_downstream_consumers_run_off_the_marts_asset(dag_bag, dag_id):
    assert _schedule_uris(dag_bag.dags[dag_id]) == {DBT_MARTS_READY_URI}


def test_dbt_manual_run_is_trigger_only(dag_bag):
    assert dag_bag.dags["dbt_manual_run"].schedule is None
