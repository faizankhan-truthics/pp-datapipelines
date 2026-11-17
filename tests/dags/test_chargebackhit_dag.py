"""DAG-level tests for dags/chargebackhit/chargebackhit_dag.py (dag_id `chargebackhit_pipeline`).

Account discovery itself is covered in test_chargebackhit_accounts.py. This covers the
rest of the DAG:
  * collect_release_paths_<endpoint>: turning every account's mapped extract output into
    one wildcard per release folder (the BigQuery 10,000-sourceUris guard noted in the
    DAG), skipping the load when nothing was produced, and tolerating accounts whose
    extract returned nothing.
  * per-endpoint chain wiring and the merge SQL file each endpoint renders.
  * the bronze_ready Asset only being emitted after every endpoint's merge.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from airflow.exceptions import AirflowSkipException

import dags.chargebackhit.chargebackhit_dag as cbh
from include.config.chargebackhit_settings import ENDPOINTS

REPO_SQL_DIR = Path(__file__).parents[2] / "include" / "sql" / "chargebackhit"


@pytest.fixture(scope="module")
def dag():
    return cbh.dag


def _collect(dag, endpoint="alerts"):
    return dag.get_task(f"collect_release_paths_{endpoint}").python_callable


class TestCollectReleasePaths:
    def test_collapses_page_files_into_one_wildcard_per_folder(self, dag):
        results = [
            {
                "release_paths": [
                    "release/chargebackhit/alerts/acme/2026-09-01/page_0.avro",
                    "release/chargebackhit/alerts/acme/2026-09-01/page_1.avro",
                    "release/chargebackhit/alerts/acme/2026-09-02/page_0.avro",
                ]
            },
            {
                "release_paths": [
                    "release/chargebackhit/alerts/globex/2026-09-01/page_0.avro"
                ]
            },
        ]

        assert _collect(dag)(results) == [
            "release/chargebackhit/alerts/acme/2026-09-01/*.avro",
            "release/chargebackhit/alerts/acme/2026-09-02/*.avro",
            "release/chargebackhit/alerts/globex/2026-09-01/*.avro",
        ]

    def test_accounts_with_no_output_are_ignored(self, dag):
        results = [
            None,
            {},
            {"release_paths": []},
            {"release_paths": ["release/x/2026-09-01/p.avro"]},
        ]

        assert _collect(dag)(results) == ["release/x/2026-09-01/*.avro"]

    @pytest.mark.parametrize("results", [[], [None], [{"release_paths": []}, {}]])
    def test_skips_the_load_when_no_account_produced_files(self, dag, results):
        with pytest.raises(AirflowSkipException):
            _collect(dag)(results)

    def test_many_pages_stay_well_under_the_bigquery_source_uri_limit(self, dag):
        results = [
            {
                "release_paths": [
                    f"release/a/{acct}/2026-09-01/page_{i}.avro" for i in range(5000)
                ]
            }
            for acct in ("acme", "globex", "initech")
        ]

        assert len(_collect(dag)(results)) == 3


class TestWiring:
    def test_dag_definition(self, dag):
        assert dag.dag_id == "chargebackhit_pipeline"
        assert dag.max_active_runs == 1
        assert dag.params["run_type"] == "daily"

    @pytest.mark.parametrize("endpoint", ENDPOINTS)
    def test_endpoint_chain(self, dag, endpoint):
        assert (
            f"extract_{endpoint}"
            in dag.get_task(f"collect_release_paths_{endpoint}").upstream_task_ids
        )
        assert dag.get_task(f"load_staging_{endpoint}").upstream_task_ids == {
            f"collect_release_paths_{endpoint}"
        }
        assert dag.get_task(f"transform_bronze_{endpoint}").upstream_task_ids == {
            f"load_staging_{endpoint}"
        }

    @pytest.mark.parametrize("endpoint", ENDPOINTS)
    def test_merge_sql_file_exists_for_every_endpoint(self, dag, endpoint):
        query = dag.get_task(f"transform_bronze_{endpoint}").configuration["query"][
            "query"
        ]

        assert query == f"merge_{endpoint}.sql"
        assert (REPO_SQL_DIR / query).is_file()

    @pytest.mark.parametrize("endpoint", ENDPOINTS)
    def test_staging_table_matches_endpoint(self, dag, endpoint):
        load = dag.get_task(f"load_staging_{endpoint}")

        assert load.destination_project_dataset_table.endswith(
            f".chargebackhit_{endpoint}"
        )
        assert load.source_format == "AVRO"
        assert load.write_disposition == "WRITE_TRUNCATE"

    def test_bronze_ready_waits_for_every_endpoint_merge(self, dag):
        ready = dag.get_task("chargebackhit_bronze_ready")

        assert ready.upstream_task_ids == {f"transform_bronze_{e}" for e in ENDPOINTS}
        assert ready.trigger_rule == "none_failed"
        assert [o.uri for o in ready.outlets] == ["data://bronze/chargebackhit"]

    def test_extract_is_mapped_over_every_discovered_account(self, dag):
        accounts = cbh.discover_chargebackhit_accounts()
        expand_input = dag.get_task(f"extract_{ENDPOINTS[0]}").expand_input

        assert accounts
        assert list(expand_input.value) == accounts
