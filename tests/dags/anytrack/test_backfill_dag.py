"""Tests for the manual AnyTrack CSV backfill DAG (dags/anytrack/backfill_dag.py).

CHANGELOG v7.8.0 records 8f2faf1, "route AnyTrack backfill merges through the correct
SQL by product". Every product now shares merge_events_all_events.sql, but `product` is
still a runtime param (it comes from the trigger conf, not a parse-time loop like
events_dag.py), so the thing that can silently go wrong is which product's tables the
merge actually renders against. These tests render the real merge_bronze SQL through
Airflow's own param merge (process_params: DAG params < task params < run conf) for
every product and check it targets that product's staging/bronze tables, as the
`csv_backfill` source.

Note: merge_bronze's task-level `"product": "{{ params.product }}"` is never itself
rendered -- Jinja doesn't re-render param values. It works only because run conf
overrides task params, and validate_params guarantees `product` is in conf before
anything reaches the merge. Both halves of that are pinned below.
"""

from __future__ import annotations

import re

import pytest
from airflow.exceptions import AirflowSkipException
from airflow.sdk.definitions.param import process_params

import dags.anytrack.backfill_dag as backfill_dag
from include.config.anytrack_settings import PRODUCTS


@pytest.fixture(scope="module")
def dag():
    return backfill_dag.anytrack_events_backfill_pipeline()


def _callable(dag, task_id):
    return dag.get_task(task_id).python_callable


def _patch_params(monkeypatch, **params):
    monkeypatch.setattr(
        backfill_dag,
        "get_current_context",
        lambda: {"params": params, "ds": "2026-09-01"},
    )


def _rendered_merge_sql(conf: dict) -> str:
    # Rendering mutates the task in place, so use a fresh DAG rather than the shared fixture.
    dag = backfill_dag.anytrack_events_backfill_pipeline()
    task = dag.get_task("merge_bronze")
    params = process_params(dag, task, conf, suppress_exception=False)
    task.render_template_fields({"params": params}, jinja_env=dag.get_template_env())
    return task.configuration["query"]["query"]


class TestDagDefinition:
    def test_is_manual_only_with_file_path_and_product_params(self, dag):
        assert dag.dag_id == "anytrack_events_backfill"
        assert dag.schedule is None
        assert set(dag.params.keys()) == {"file_path", "product"}
        assert dag.max_active_runs == 1

    def test_task_chain(self, dag):
        assert dag.get_task("validate_params").downstream_task_ids == {
            "list_files",
            "csv_to_release",
            "staging_table_name",
        }
        assert dag.get_task("csv_to_release").downstream_task_ids == {"check_records"}
        assert "check_records" in dag.get_task("load_staging").upstream_task_ids
        assert dag.get_task("merge_bronze").upstream_task_ids == {"load_staging"}

    def test_merge_uses_the_shared_all_events_sql(self, dag):
        assert (
            dag.get_task("merge_bronze").configuration["query"]["query"]
            == "merge_events_all_events.sql"
        )


class TestValidateParams:
    def test_valid_params_pass_through(self, dag, monkeypatch):
        _patch_params(
            monkeypatch,
            file_path="external/anytrack/iqcenter/2026-09-01/",
            product="iqcenter",
        )

        assert _callable(dag, "validate_params")() == {
            "file_path": "external/anytrack/iqcenter/2026-09-01/",
            "product": "iqcenter",
        }

    @pytest.mark.parametrize("file_path", ["", None])
    def test_missing_file_path_fails(self, dag, monkeypatch, file_path):
        _patch_params(monkeypatch, file_path=file_path, product="iqcenter")

        with pytest.raises(ValueError, match="file_path"):
            _callable(dag, "validate_params")()

    @pytest.mark.parametrize(
        "product", ["", "not_a_product", "IQCENTER", "{{ params.product }}"]
    )
    def test_unknown_product_fails_before_anything_reaches_the_merge(
        self, dag, monkeypatch, product
    ):
        _patch_params(monkeypatch, file_path="some/prefix/", product=product)

        with pytest.raises(ValueError, match="product"):
            _callable(dag, "validate_params")()


class TestCheckRecords:
    @pytest.mark.parametrize(
        "result", [None, {}, {"release_paths": [], "record_count": 0}]
    )
    def test_skips_the_load_when_no_rows_were_produced(self, dag, result):
        with pytest.raises(AirflowSkipException):
            _callable(dag, "check_records")(result)

    def test_returns_release_paths_when_rows_were_produced(self, dag):
        result = {
            "release_paths": ["release/a.avro", "release/b.avro"],
            "record_count": 7,
        }

        assert _callable(dag, "check_records")(result) == [
            "release/a.avro",
            "release/b.avro",
        ]


class TestCsvToRelease:
    def test_passes_product_files_and_run_ds_through(self, dag, monkeypatch):
        calls = []
        monkeypatch.setattr(
            backfill_dag,
            "backfill_csv_to_release",
            lambda **kw: calls.append(kw) or {"ok": 1},
        )
        _patch_params(monkeypatch)

        result = _callable(dag, "csv_to_release")(
            {"product": "tracelo", "file_path": "p/"}, ["p/a.csv"]
        )

        assert result == {"ok": 1}
        assert calls == [
            {"product": "tracelo", "csv_blob_names": ["p/a.csv"], "ds": "2026-09-01"}
        ]


class TestMergeRoutingByProduct:
    """Regression area for 8f2faf1."""

    @pytest.mark.parametrize("product", PRODUCTS)
    def test_merge_targets_the_triggering_products_tables(self, dag, product):
        sql = _rendered_merge_sql({"product": product, "file_path": "p/"})

        tables = set(re.findall(r"anytrack_[A-Za-z0-9_{}. ]+?_events", sql))
        assert tables == {f"anytrack_{product}_events"}, tables
        assert "{{" not in sql and "}}" not in sql

    def test_merge_tags_rows_as_csv_backfill_not_webhook(self, dag):
        sql = _rendered_merge_sql({"product": "iqcenter", "file_path": "p/"})

        assert re.search(r"'csv_backfill'\s+AS _source", sql)

    def test_staging_load_writes_where_the_merge_reads(self, dag):
        staging_table = _callable(dag, "staging_table_name")(
            {"product": "qga", "file_path": "p/"}
        )
        sql = _rendered_merge_sql({"product": "qga", "file_path": "p/"})

        assert staging_table.endswith(".anytrack_qga_events")
        assert f"`{staging_table}`" in sql
