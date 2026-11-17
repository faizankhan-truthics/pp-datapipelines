"""DAG-level tests for dags/ga4/ga4_daily_source_snapshots.py.

The GA4 hook, settings helpers and merge SQL have their own tests; this covers the logic
that lives only in the DAG file:
  * build_windows -- backfill ranges, a pinned event_date, and the last-complete-day
    fallback for scheduled vs manual (collapsed-interval) runs. GA4 only finalizes past
    days, so a manual run must never query "today".
  * the pure helpers (_to_float, _normalize_ga4_date, discover_ga4_properties,
    _render_merge_sql) the extract/merge steps depend on.
  * extract's landing CSV + release Avro output for one window, including derived ratios
    and divide-by-zero.
  * one extract -> gather -> load -> merge chain per report, plus the period report on
    its own window-grain merge key.
"""

from __future__ import annotations

import csv
import io
import os
from datetime import datetime, timezone
from types import SimpleNamespace

import fastavro
import pendulum
import pytest

import dags.ga4.ga4_daily_source_snapshots as ga4
import include.config.ga4_settings as settings

ALL_SPECS = [*ga4._REPORTS, ga4._PERIOD_REPORT]


@pytest.fixture(scope="module")
def dag():
    return ga4.ga4_daily_source_snapshots()


def _build_windows(dag, monkeypatch, params=None, start=None, end=None, run_after=None):
    dag_run = SimpleNamespace(
        data_interval_start=start, data_interval_end=end, run_after=run_after
    )
    monkeypatch.setattr(
        ga4,
        "get_current_context",
        lambda: {
            "dag_run": dag_run,
            "params": {
                "date_from": "",
                "date_to": "",
                "event_date": "",
                **(params or {}),
            },
        },
    )
    return dag.get_task("build_windows").python_callable()


class TestBuildWindows:
    def test_backfill_yields_one_single_day_window_per_day_date_to_excluded(
        self, dag, monkeypatch
    ):
        windows = _build_windows(
            dag, monkeypatch, {"date_from": "2026-02-27", "date_to": "2026-03-03"}
        )

        assert windows == [
            {"start_date": d, "end_date": d}
            for d in ["2026-02-27", "2026-02-28", "2026-03-01", "2026-03-02"]
        ]

    def test_backfill_with_date_to_equal_to_date_from_yields_nothing(
        self, dag, monkeypatch
    ):
        assert (
            _build_windows(
                dag, monkeypatch, {"date_from": "2026-03-02", "date_to": "2026-03-02"}
            )
            == []
        )

    def test_backfill_needs_both_ends_otherwise_falls_back(self, dag, monkeypatch):
        windows = _build_windows(
            dag, monkeypatch, {"date_from": "2026-01-01", "event_date": "2026-05-05"}
        )

        assert windows == [{"start_date": "2026-05-05", "end_date": "2026-05-05"}]

    def test_event_date_targets_a_single_day(self, dag, monkeypatch):
        assert _build_windows(dag, monkeypatch, {"event_date": "2026-07-04"}) == [
            {"start_date": "2026-07-04", "end_date": "2026-07-04"}
        ]

    def test_scheduled_run_uses_the_data_interval_start(self, dag, monkeypatch):
        windows = _build_windows(
            dag,
            monkeypatch,
            start=pendulum.datetime(2026, 9, 1, tz="UTC"),
            end=pendulum.datetime(2026, 9, 2, tz="UTC"),
        )

        assert windows == [{"start_date": "2026-09-01", "end_date": "2026-09-01"}]

    def test_manual_run_with_collapsed_interval_uses_the_previous_day(
        self, dag, monkeypatch
    ):
        # Airflow 3 hands stdlib datetimes here, not pendulum -- that's the case the
        # pendulum.instance() wrap in build_windows exists for.
        instant = datetime(2026, 9, 5, 13, 30, tzinfo=timezone.utc)

        windows = _build_windows(dag, monkeypatch, start=instant, end=instant)

        assert windows == [{"start_date": "2026-09-04", "end_date": "2026-09-04"}]

    def test_manual_run_without_interval_falls_back_to_run_after(
        self, dag, monkeypatch
    ):
        windows = _build_windows(
            dag, monkeypatch, run_after=datetime(2026, 9, 5, 1, tzinfo=timezone.utc)
        )

        assert windows == [{"start_date": "2026-09-04", "end_date": "2026-09-04"}]


class TestHelpers:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("12", 12.0),
            ("3.5", 3.5),
            ("0", 0.0),
            ("", None),
            (None, None),
            ("(not set)", None),
        ],
    )
    def test_to_float(self, value, expected):
        assert ga4._to_float(value) == expected

    @pytest.mark.parametrize(
        "value,expected",
        [
            ("20260901", "2026-09-01"),
            ("2026-09-01", "2026-09-01"),
            ("(other)", "(other)"),
            ("", ""),
        ],
    )
    def test_normalize_ga4_date(self, value, expected):
        assert ga4._normalize_ga4_date(value) == expected

    def test_discover_properties_is_sorted_lowercased_and_skips_blanks(
        self, monkeypatch
    ):
        for key in [k for k in os.environ if k.startswith("GA4_PROPERTY_ID_")]:
            monkeypatch.delenv(key)
        monkeypatch.setenv("GA4_PROPERTY_ID_ZETA", " 222 ")
        monkeypatch.setenv("GA4_PROPERTY_ID_ALPHA", "111")
        monkeypatch.setenv("GA4_PROPERTY_ID_EMPTY", "  ")

        assert ga4.discover_ga4_properties() == [
            {"property_name": "alpha", "property_id": "111"},
            {"property_name": "zeta", "property_id": "222"},
        ]

    @pytest.mark.parametrize("spec", ga4._REPORTS, ids=lambda s: s["entity"])
    def test_render_merge_sql_fills_every_placeholder(self, spec):
        sql = ga4._render_merge_sql("proj", "stg", "brz", spec)

        # The template documents its placeholders in comments; only the SQL body must be filled.
        body = "\n".join(
            line for line in sql.splitlines() if not line.lstrip().startswith("--")
        )
        for placeholder in (
            "__STAGING_TABLE__",
            "__BRONZE_TABLE__",
            "__STAGING_DATASET__",
            "__ENTITY__",
            "__KEY_COLS__",
        ):
            assert placeholder not in body
        assert f"`proj.brz.ga4_{spec['entity']}`" in sql
        expected_keys = ", ".join(f"'{c}'" for c in settings.merge_key_columns(spec))
        assert f"[{expected_keys}]" in sql

    def test_period_report_merges_on_its_window_key_not_date(self):
        sql = ga4._render_merge_sql(
            "proj",
            "stg",
            "brz",
            ga4._PERIOD_REPORT,
            key_cols=settings.period_merge_key_columns(),
        )

        expected_keys = ", ".join(f"'{c}'" for c in settings.period_merge_key_columns())
        assert f"[{expected_keys}]" in sql
        assert "'date'" not in expected_keys


class TestExtract:
    @pytest.fixture
    def run_extract(self, dag, monkeypatch, tmp_path):
        def _run(rows, spec, task_id="extract_overview_daily"):
            uploads = {}
            hook_calls = []

            class FakeGA4Hook:
                def __init__(self, gcp_conn_id, property_id):
                    hook_calls.append(
                        {"gcp_conn_id": gcp_conn_id, "property_id": property_id}
                    )

                def run_report(self, **kwargs):
                    hook_calls.append(kwargs)
                    return rows, {"sampled": False}

            class FakeGCSHook:
                def __init__(self, gcp_conn_id):
                    pass

                def upload(self, bucket_name, object_name, filename, **kwargs):
                    with open(filename, "rb") as fh:
                        uploads[object_name] = fh.read()

            monkeypatch.setattr(ga4, "GA4Hook", FakeGA4Hook)
            monkeypatch.setattr(ga4, "GCSHook", FakeGCSHook)
            monkeypatch.setattr(ga4, "_OUTPUT_DIR", str(tmp_path))
            release_object = dag.get_task(task_id).python_callable(
                window={"start_date": "2026-09-01", "end_date": "2026-09-01"},
                prop={"property_name": "acme", "property_id": "123"},
                spec=spec,
                bucket="test-bucket",
                gcp_conn_id="test-conn",
            )
            return SimpleNamespace(
                release_object=release_object, uploads=uploads, hook_calls=hook_calls
            )

        return _run

    def test_daily_report_lands_csv_and_releases_typed_avro(self, run_extract):
        spec = ga4._REPORTS[0]  # overview_daily, has a derived ratio
        rows = [
            {
                "date": "20260901",
                "activeUsers": "10",
                "newUsers": "4",
                "userEngagementDuration": "250",
                "sessions": "12",
            },
            {
                "date": "20260901",
                "activeUsers": "0",
                "newUsers": "0",
                "userEngagementDuration": "5",
                "sessions": "",
            },
        ]

        result = run_extract(rows, spec)

        assert result.hook_calls[1]["dimensions"] == ["date"]
        assert result.release_object == settings.release_object(
            "analytics_123", "overview_daily", "2026-09-01", "overview_daily.avro"
        )
        landing = settings.landing_object(
            "analytics_123", "overview_daily", "2026-09-01", "overview_daily.csv"
        )
        assert set(result.uploads) == {landing, result.release_object}

        landed = list(
            csv.DictReader(io.StringIO(result.uploads[landing].decode("utf-8")))
        )
        assert [r["date"] for r in landed] == ["2026-09-01", "2026-09-01"]
        assert {r["property_id"] for r in landed} == {"123"}

        records = list(
            fastavro.reader(io.BytesIO(result.uploads[result.release_object]))
        )
        assert records[0]["date"] == "2026-09-01"
        assert records[0]["activeUsers"] == 10.0
        assert records[0]["averageEngagementTimePerActiveUser"] == 25.0
        # Zero active users: derived ratio is 0, not a ZeroDivisionError.
        assert records[1]["averageEngagementTimePerActiveUser"] == 0.0
        assert records[1]["sessions"] is None
        assert (
            records[0]["_gcs_file_path"] == f"gs://test-bucket/{result.release_object}"
        )

    def test_dimension_reports_query_date_plus_their_dimensions(self, run_extract):
        spec = ga4._REPORTS[1]
        rows = [
            {
                "date": "20260901",
                "sessionSourceMedium": "google / cpc",
                "sessions": "3",
                "keyEvents": "1",
                "totalRevenue": "9.5",
            }
        ]

        result = run_extract(rows, spec, task_id=f"extract_{spec['entity']}")

        assert result.hook_calls[1]["dimensions"] == ["date", "sessionSourceMedium"]
        [record] = fastavro.reader(io.BytesIO(result.uploads[result.release_object]))
        assert record["sessionSourceMedium"] == "google / cpc"
        assert record["totalRevenue"] == 9.5

    def test_period_report_is_queried_without_a_date_dimension(self, run_extract):
        rows = [{"activeUsers": "40", "sessions": "55"}]

        result = run_extract(
            rows, ga4._PERIOD_REPORT, task_id="extract_overview_period"
        )

        assert result.hook_calls[1]["dimensions"] == []
        [record] = fastavro.reader(io.BytesIO(result.uploads[result.release_object]))
        assert record["period_start_date"] == record["period_end_date"] == "2026-09-01"
        assert record["activeUsers"] == 40.0

    def test_empty_report_still_writes_empty_files(self, run_extract):
        result = run_extract(
            [], ga4._REPORTS[2], task_id=f"extract_{ga4._REPORTS[2]['entity']}"
        )

        assert (
            list(fastavro.reader(io.BytesIO(result.uploads[result.release_object])))
            == []
        )


class TestWiring:
    @pytest.mark.parametrize("spec", ALL_SPECS, ids=lambda s: s["entity"])
    def test_chain_per_report(self, dag, spec):
        entity = spec["entity"]

        assert dag.get_task(f"gather_{entity}").upstream_task_ids == {
            f"extract_{entity}"
        }
        assert dag.get_task(f"load_staging_{entity}").upstream_task_ids == {
            f"gather_{entity}"
        }
        assert dag.get_task(f"merge_bronze_{entity}").upstream_task_ids == {
            f"load_staging_{entity}"
        }

    def test_report_entities_are_unique(self):
        entities = [s["entity"] for s in ALL_SPECS]

        assert len(entities) == len(set(entities))

    def test_reports_never_list_date_as_a_dimension(self):
        # `date` is added automatically; listing it again would duplicate the column.
        assert all("date" not in s["dimensions"] for s in ALL_SPECS)

    def test_extract_concurrency_stays_under_the_ga4_per_property_limit(self, dag):
        assert dag.max_active_tasks < 10
        for spec in ALL_SPECS:
            assert (
                dag.get_task(f"extract_{spec['entity']}").max_active_tis_per_dagrun < 10
            )

    def test_bronze_ready_waits_for_every_merge(self, dag):
        ready = dag.get_task("ga4_bronze_ready")

        assert ready.upstream_task_ids == {
            f"merge_bronze_{s['entity']}" for s in ALL_SPECS
        }
        assert ready.trigger_rule == "none_failed"
        assert [o.uri for o in ready.outlets] == ["data://bronze/ga4/analytics"]
