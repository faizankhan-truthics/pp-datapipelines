"""DAG-level tests for dags/zendesk/zendesk_support_pipeline.py.

The extract operator, hook and merge SQL already have their own tests; this covers the
DAG wiring between them:
  * the `ds_macro` the staging load uses to find the release folder -- it must match
    the folder of the run's data interval on a scheduled run, and the logical date on a
    manual run (where Airflow 3 collapses the interval to a single instant).
  * one extract -> load -> merge chain per endpoint, each merge SQL file existing.
  * zendesk_bronze_ready only firing after every endpoint's merge.
"""

from __future__ import annotations

import pendulum
import pytest

import dags.zendesk.zendesk_support_pipeline as zendesk
from include.config.zendesk_settings import ENDPOINTS, SQL_DIR


@pytest.fixture(scope="module")
def dag():
    return zendesk.dag


def _render_ds_macro(dag, start, end, logical_date):
    return (
        dag.get_template_env()
        .from_string(zendesk.ds_macro)
        .render(
            data_interval_start=start, data_interval_end=end, logical_date=logical_date
        )
    )


class TestDsMacro:
    def test_scheduled_run_uses_the_data_interval_start(self, dag):
        start = pendulum.datetime(2026, 9, 1, tz="UTC")
        end = pendulum.datetime(2026, 9, 2, tz="UTC")

        assert _render_ds_macro(dag, start, end, logical_date=end) == "2026-09-01"

    def test_manual_run_with_collapsed_interval_uses_the_logical_date(self, dag):
        instant = pendulum.datetime(2026, 9, 5, 13, 30, tz="UTC")

        assert (
            _render_ds_macro(dag, instant, instant, logical_date=instant)
            == "2026-09-05"
        )

    @pytest.mark.parametrize("endpoint", ENDPOINTS)
    def test_staging_load_reads_that_endpoints_release_folder(self, dag, endpoint):
        [source] = dag.get_task(f"load_staging_{endpoint}").source_objects

        assert source == f"release/zendesk/{endpoint}/{zendesk.ds_macro}/*.avro"


class TestWiring:
    def test_dag_definition(self, dag):
        assert dag.dag_id == "zendesk_support_pipeline"
        assert dag.schedule == "15 3 * * *"
        assert dag.max_active_runs == 1
        assert "date_from" in dag.params

    @pytest.mark.parametrize("endpoint", ENDPOINTS)
    def test_endpoint_chain(self, dag, endpoint):
        assert dag.get_task(f"load_staging_{endpoint}").upstream_task_ids == {
            f"extract_{endpoint}"
        }
        assert dag.get_task(f"transform_bronze_{endpoint}").upstream_task_ids == {
            f"load_staging_{endpoint}"
        }

    @pytest.mark.parametrize("endpoint", ENDPOINTS)
    def test_merge_sql_and_params_per_endpoint(self, dag, endpoint):
        merge = dag.get_task(f"transform_bronze_{endpoint}")

        assert merge.configuration["query"]["query"] == f"merge_{endpoint}.sql"
        assert (SQL_DIR / f"merge_{endpoint}.sql").is_file()
        assert merge.params["endpoint"] == endpoint

    @pytest.mark.parametrize("endpoint", ENDPOINTS)
    def test_staging_table_matches_endpoint(self, dag, endpoint):
        assert dag.get_task(
            f"load_staging_{endpoint}"
        ).destination_project_dataset_table.endswith(f".zendesk_{endpoint}")

    def test_bronze_ready_waits_for_every_endpoint_merge(self, dag):
        ready = dag.get_task("zendesk_bronze_ready")

        assert ready.upstream_task_ids == {f"transform_bronze_{e}" for e in ENDPOINTS}
        assert ready.trigger_rule == "none_failed"
        assert [o.uri for o in ready.outlets] == ["zendesk://financial/bronze"]
