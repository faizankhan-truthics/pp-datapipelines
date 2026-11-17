"""Tests for the manual AnyTrack Conversion API backfill DAG (dags/anytrack/capi_dag.py)."""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from airflow.exceptions import AirflowSkipException
from airflow.sdk.definitions.param import process_params

import dags.anytrack.capi_dag as capi_dag
from include.config.anytrack_settings import CAPI_PROPERTIES


@pytest.fixture(autouse=True)
def _fixed_today(monkeypatch):
    # validate_params refuses windows reaching the last two UTC days
    monkeypatch.setattr(capi_dag, "_today_utc", lambda: date(2026, 11, 6))


@pytest.fixture(scope="module")
def dag():
    return capi_dag.anytrack_capi_events_pipeline()


def _callable(dag, task_id):
    return dag.get_task(task_id).python_callable


def _patch_params(monkeypatch, **params):
    monkeypatch.setattr(capi_dag, "get_current_context", lambda: {"params": params})


def _conf(product="fitday"):
    return {"product": product, "date_from": "2026-10-01", "date_to": "2026-10-08"}


class TestDagDefinition:
    def test_is_manual_only(self, dag):
        assert dag.dag_id == "anytrack_capi_events"
        assert dag.schedule is None
        assert set(dag.params.keys()) == {
            "product",
            "date_from",
            "date_to",
            "chunk_days",
        }
        assert dag.max_active_runs == 1

    def test_task_chain(self, dag):
        assert dag.get_task("load_staging").upstream_task_ids == {
            "landing_globs",
            "staging_table_name",
        }
        assert dag.get_task("append_missing").upstream_task_ids == {"load_staging"}

    def test_pull_runs_one_at_a_time(self, dag):
        # No dedicated pool: one run at a time and one pull task at a time keep the
        # account-wide AnyTrack rate limit to a single puller.
        assert dag.max_active_runs == 1
        assert dag.get_task("pull_chunk").max_active_tis_per_dag == 1

    def test_pull_tasks_are_labelled_in_the_grid(self, dag):
        assert dag.get_task("pull_chunk").map_index_template == "{{ chunk_label }}"

    def test_pull_timeout_covers_the_largest_accepted_chunk(self, dag):
        # ~26,000 pages of 200 at ~114 requests/minute is about 228 minutes
        assert dag.get_task("pull_chunk").execution_timeout >= timedelta(minutes=300)

    @pytest.mark.parametrize(
        "rpm,page,hours",
        [(120, 200, 6), (60, 200, 12), (120, 100, 12), (60, 100, 24), (240, 400, 6)],
    )
    def test_pull_timeout_scales_with_the_configured_limits(self, rpm, page, hours):
        assert capi_dag.pull_timeout(rpm, page) == timedelta(hours=hours)

    def test_bad_input_fails_without_retries(self, dag):
        assert dag.get_task("validate_params").retries == 0

    def test_the_insert_is_not_retried(self, dag):
        assert dag.get_task("append_missing").retries == 0

    def test_dag_has_no_task_that_creates_another_table(self, dag):
        assert set(dag.task_ids) == {
            "validate_params",
            "chunk_list",
            "pull_chunk",
            "landing_globs",
            "staging_table_name",
            "load_staging",
            "append_missing",
        }


class TestValidateParams:
    def test_valid_params_build_the_chunk_list(self, dag, monkeypatch):
        _patch_params(
            monkeypatch,
            product="fitday",
            date_from="2026-10-01",
            date_to="2026-10-16",
            chunk_days=7,
        )
        assert _callable(dag, "validate_params")() == {
            "product": "fitday",
            "chunks": [
                {
                    "start": start,
                    "end": end,
                    "index": i,
                    "total": 3,
                    "window_start": "2026-10-01",
                    "window_end": "2026-10-16",
                }
                for i, (start, end) in enumerate(
                    [
                        ("2026-10-01", "2026-10-08"),
                        ("2026-10-08", "2026-10-15"),
                        ("2026-10-15", "2026-10-16"),
                    ],
                    start=1,
                )
            ],
            "date_from": "2026-10-01",
            "date_to": "2026-10-16",
        }

    @pytest.mark.parametrize(
        "product", ["", "files_convert", "FITDAY", "{{ params.product }}"]
    )
    def test_product_without_a_capi_property_fails(self, dag, monkeypatch, product):
        _patch_params(monkeypatch, **{**_conf(), "product": product})
        with pytest.raises(ValueError, match="product"):
            _callable(dag, "validate_params")()

    @pytest.mark.parametrize(
        "date_from,date_to",
        [
            ("", ""),
            ("2026-10-01", ""),
            ("2026-10-01'; DROP", "2026-10-02"),
            ("x", "y"),
            # valid for date.fromisoformat, but the SQL would read them differently
            ("20261001", "20261008"),
            ("2026-W41-4", "2026-W42-4"),
        ],
    )
    def test_bad_dates_fail_before_reaching_the_sql(
        self, dag, monkeypatch, date_from, date_to
    ):
        _patch_params(
            monkeypatch, product="fitday", date_from=date_from, date_to=date_to
        )
        with pytest.raises(ValueError, match="YYYY-MM-DD"):
            _callable(dag, "validate_params")()

    @pytest.mark.parametrize("chunk_days", [30, 31, -1])
    def test_chunk_days_out_of_range_fails(self, dag, monkeypatch, chunk_days):
        _patch_params(monkeypatch, **_conf(), chunk_days=chunk_days)
        with pytest.raises(ValueError, match="chunk_days"):
            _callable(dag, "validate_params")()

    @pytest.mark.parametrize(
        "reserved",
        ["bq_bronze_dataset", "bq_project", "source", "match_tolerance_seconds"],
    )
    def test_run_conf_cannot_override_trusted_sql_settings(
        self, dag, monkeypatch, reserved
    ):
        _patch_params(monkeypatch, **_conf(), **{reserved: "x"})
        with pytest.raises(ValueError, match="may not set"):
            _callable(dag, "validate_params")()

    def test_a_window_reaching_the_last_two_days_is_refused(self, dag, monkeypatch):
        # today is 2026-11-06, so the latest allowed date_to is 2026-11-05
        _patch_params(
            monkeypatch, product="fitday", date_from="2026-10-06", date_to="2026-11-06"
        )
        with pytest.raises(ValueError, match="too recent"):
            _callable(dag, "validate_params")()

    def test_the_latest_allowed_window_passes(self, dag, monkeypatch):
        _patch_params(
            monkeypatch, product="fitday", date_from="2026-10-06", date_to="2026-11-05"
        )
        assert _callable(dag, "validate_params")()["date_to"] == "2026-11-05"

    def test_chunks_carry_what_the_progress_log_needs(self, dag, monkeypatch):
        _patch_params(
            monkeypatch,
            product="fitday",
            date_from="2026-10-01",
            date_to="2026-10-16",
            chunk_days=7,
        )
        chunks = _callable(dag, "validate_params")()["chunks"]
        assert [(c["index"], c["total"]) for c in chunks] == [(1, 3), (2, 3), (3, 3)]
        assert {(c["window_start"], c["window_end"]) for c in chunks} == {
            ("2026-10-01", "2026-10-16")
        }


class TestLandingGlobs:
    def test_skips_the_load_when_nothing_was_pulled(self, dag):
        with pytest.raises(AirflowSkipException):
            _callable(dag, "landing_globs")(
                "fitday", [{"events": 0, "days_written": []}]
            )

    def test_globs_only_cover_this_runs_shards(self, dag):
        # a day folder can hold shards of an earlier run with other chunking;
        # the globs must name this run's chunk start so those are not loaded
        results = [
            {
                "events": 3,
                "chunk_start": "2026-10-01",
                "days_written": ["2026-10-01", "2026-10-02"],
            },
            {
                "events": 1,
                "chunk_start": "2026-10-02",
                "days_written": ["2026-10-02", "2026-10-08"],
            },
        ]
        assert _callable(dag, "landing_globs")("fitday", results) == [
            "landing/anytrack/fitday/capi_events/2026-10-01/2026-10-01_*",
            "landing/anytrack/fitday/capi_events/2026-10-02/2026-10-01_*",
            "landing/anytrack/fitday/capi_events/2026-10-02/2026-10-02_*",
            "landing/anytrack/fitday/capi_events/2026-10-08/2026-10-02_*",
        ]


class TestRenderedSql:
    @pytest.mark.parametrize("product", sorted(CAPI_PROPERTIES))
    def test_append_inserts_only_into_the_products_events_table(self, product):
        dag = capi_dag.anytrack_capi_events_pipeline()
        task = dag.get_task("append_missing")
        params = process_params(dag, task, _conf(product), suppress_exception=False)
        task.render_template_fields(
            {"params": params}, jinja_env=dag.get_template_env()
        )
        sql = task.configuration["query"]["query"]
        assert f"anytrack_{product}_events` (" in sql
        assert f"anytrack_{product}_capi_events`" in sql
        assert CAPI_PROPERTIES[product]["property_id"] in sql
        assert "'capi_backfill'" in sql
        assert "TIMESTAMP '2026-10-01'" in sql and "TIMESTAMP '2026-10-08'" in sql
        assert sql.count("INSERT INTO") == 1
        other = [p for p in CAPI_PROPERTIES if p != product]
        # exact table names: `files_editor` is a prefix of `files_editor_preprod`
        assert all(
            f"anytrack_{o}_events`" not in sql
            and f"anytrack_{o}_capi_events`" not in sql
            for o in other
        )
