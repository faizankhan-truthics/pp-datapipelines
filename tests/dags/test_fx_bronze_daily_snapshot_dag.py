"""DAG-level tests for dags/exchange_rates/fx_bronze_daily_snapshot_dag.py.

Each of the three operators has its own unit tests under
tests/include/custom_operators/exchange_rates/. What only the DAG decides is how they're
chained: each step pulls its input from the previous step's XCom by task_id, and the
release (dedupe) and load (MERGE) steps must agree on the natural key, or the bronze
MERGE stops being idempotent.
"""

from __future__ import annotations

import pytest

import dags.exchange_rates.fx_bronze_daily_snapshot_dag as fx
from include.config import exchange_rates as config


@pytest.fixture(scope="module")
def dag():
    return fx.dag


def test_dag_definition(dag):
    assert dag.dag_id == "fx_bronze_daily_snapshot"
    assert dag.schedule == "55 23 * * *"
    assert dag.max_active_runs == 1
    assert dag.catchup is False


def test_linear_chain(dag):
    assert dag.get_task("landing_to_release").upstream_task_ids == {
        "extract_to_landing"
    }
    assert dag.get_task("release_to_bigquery").upstream_task_ids == {
        "landing_to_release"
    }
    assert dag.get_task("bronze_ready").upstream_task_ids == {"release_to_bigquery"}


def test_each_step_pulls_from_the_task_directly_upstream(dag):
    assert dag.get_task("landing_to_release").landing_task_id == "extract_to_landing"
    assert dag.get_task("release_to_bigquery").release_task_id == "landing_to_release"


def test_release_and_load_agree_on_the_natural_key(dag):
    release = dag.get_task("landing_to_release")
    load = dag.get_task("release_to_bigquery")

    assert (
        release.natural_key_fields
        == load.natural_key_fields
        == config.NATURAL_KEY_FIELDS
    )


def test_loads_the_daily_fx_bronze_table(dag):
    load = dag.get_task("release_to_bigquery")

    assert load.bq_table == "daily_fx"
    assert load.partition_field == config.BQ_PARTITION_FIELD


def test_bronze_ready_emits_the_asset_dbt_waits_on(dag):
    ready = dag.get_task("bronze_ready")

    assert [o.uri for o in ready.outlets] == ["data://bronze/exchange_rates/daily_fx"]
    assert ready.trigger_rule == "none_failed"
