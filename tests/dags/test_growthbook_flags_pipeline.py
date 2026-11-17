"""DAG-level tests for dags/growthbook/growthbook_flags_pipeline.py.

The extract operator, hook and merge SQL have their own tests; this covers the wiring:
  * one extract -> load -> merge chain per endpoint (flags and projects), each merge SQL file existing;
  * the staging load reading exactly the release file that endpoint's extract wrote;
  * growthbook_flags_bronze_ready only firing after every endpoint's merge;
  * the API key only being read at run time.
"""

from __future__ import annotations

from types import SimpleNamespace

import jinja2
import pytest

import dags.growthbook.growthbook_flags_pipeline as growthbook
from include.config.growthbook_settings import ENDPOINTS, SQL_DIR


@pytest.fixture(scope="module")
def dag():
    return growthbook.dag


class TestWiring:
    def test_dag_definition(self, dag):
        assert dag.dag_id == "growthbook_flags_pipeline"
        assert dag.schedule == "0 6 * * *"
        assert dag.max_active_runs == 1

    def test_endpoints_are_the_flags_and_their_projects(self):
        assert list(ENDPOINTS) == ["features", "projects"]

    def test_the_two_chains_do_not_depend_on_each_other(self, dag):
        # a failing projects lookup must not hold back the flags (the project name is a join, not a copy)
        assert dag.get_task("transform_bronze_features").upstream_task_ids == {
            "load_staging_features"
        }
        assert dag.get_task("transform_bronze_projects").upstream_task_ids == {
            "load_staging_projects"
        }

    @pytest.mark.parametrize("endpoint", list(ENDPOINTS))
    def test_endpoint_chain(self, dag, endpoint):
        assert dag.get_task(f"extract_{endpoint}").upstream_task_ids == set()
        assert dag.get_task(f"load_staging_{endpoint}").upstream_task_ids == {
            f"extract_{endpoint}"
        }
        assert dag.get_task(f"transform_bronze_{endpoint}").upstream_task_ids == {
            f"load_staging_{endpoint}"
        }

    @pytest.mark.parametrize("endpoint", list(ENDPOINTS))
    def test_merge_sql_and_params_per_endpoint(self, dag, endpoint):
        merge = dag.get_task(f"transform_bronze_{endpoint}")

        assert merge.configuration["query"]["query"] == f"merge_{endpoint}.sql"
        assert (SQL_DIR / f"merge_{endpoint}.sql").is_file()
        assert merge.params["endpoint"] == endpoint

    @pytest.mark.parametrize("endpoint", list(ENDPOINTS))
    def test_staging_load_reads_the_file_its_extract_wrote(self, dag, endpoint):
        load = dag.get_task(f"load_staging_{endpoint}")

        assert load.source_objects == [
            f"{{{{ ti.xcom_pull(task_ids='extract_{endpoint}')['release_path'] }}}}"
        ]
        # the template resolves to the path the extract task returned, with no date logic involved
        path = f"release/growthbook/{endpoint}/2026-10-20/growthbook_manual__x_release.avro"
        ti = SimpleNamespace(
            xcom_pull=lambda task_ids: (
                {"release_path": path} if task_ids == f"extract_{endpoint}" else None
            )
        )
        assert jinja2.Template(load.source_objects[0]).render(ti=ti) == path
        assert load.destination_project_dataset_table.endswith(
            f".growthbook_{endpoint}"
        )

    def test_bronze_ready_waits_for_every_endpoint_merge(self, dag):
        ready = dag.get_task("growthbook_flags_bronze_ready")

        assert ready.upstream_task_ids == {f"transform_bronze_{e}" for e in ENDPOINTS}
        assert ready.trigger_rule == "none_failed"
        assert [o.uri for o in ready.outlets] == ["growthbook://features/bronze"]

    @pytest.mark.parametrize("endpoint", list(ENDPOINTS))
    def test_api_key_is_only_read_at_run_time(self, dag, endpoint):
        # Templated, so parsing the DAG (locally or in CI) needs no GROWTHBOOK_API_KEY variable.
        extract = dag.get_task(f"extract_{endpoint}")

        assert extract.api_key == "{{ var.value.GROWTHBOOK_API_KEY }}"
        assert "api_key" in extract.template_fields
