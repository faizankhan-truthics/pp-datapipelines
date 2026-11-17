"""Unit tests for collapse_release_dirs (dags/stripe/stripe_events_pipeline.py): release
avro paths from the events extract (daily run, one-hour windows) -> BigQuery wildcard source URIs."""

from __future__ import annotations

from dags.stripe.stripe_events_pipeline import (
    EVENT_SOURCED_ENDPOINTS,
    STRIPE_BRONZE_DATASET,
    collapse_release_dirs,
    dag,
)


def _path(account, day, hour, page=1):
    return f"release/stripe/events/{account}/{day}/hour-{hour:02d}/{account}_events_{page:05d}.avro"


class TestCollapseReleaseDirs:
    def test_single_hour_backfill_loads_only_that_hour_folder(self):
        paths = [_path("acme", "2026-05-10", 14, 1), _path("acme", "2026-05-10", 14, 2)]

        assert collapse_release_dirs(paths) == [
            "release/stripe/events/acme/2026-05-10/hour-14/*.avro"
        ]

    def test_a_full_day_collapses_to_one_day_wildcard(self):
        paths = [_path("acme", "2026-05-10", h) for h in range(24)]

        assert collapse_release_dirs(paths) == [
            "release/stripe/events/acme/2026-05-10/*.avro"
        ]

    def test_partial_days_stay_per_hour_and_accounts_stay_separate(self):
        paths = (
            [_path("acme", "2026-05-10", h) for h in range(24)]
            + [_path("acme", "2026-05-11", 0), _path("acme", "2026-05-11", 1)]
            + [_path("zebra", "2026-05-10", 5)]
        )

        assert collapse_release_dirs(paths) == [
            "release/stripe/events/acme/2026-05-10/*.avro",
            "release/stripe/events/acme/2026-05-11/hour-00/*.avro",
            "release/stripe/events/acme/2026-05-11/hour-01/*.avro",
            "release/stripe/events/zebra/2026-05-10/hour-05/*.avro",
        ]


def test_events_pipeline_runs_after_financial_and_merges_objects_before_emitting_bronze():
    assert dag.schedule == STRIPE_BRONZE_DATASET
    events_merge = dag.get_task("transform_bronze_events")
    endpoint_gate = dag.get_task("select_object_endpoints")
    bronze_ready = dag.get_task("stripe_events_bronze_ready")
    assert endpoint_gate.task_id in events_merge.downstream_task_ids
    assert {
        f"merge_{endpoint}_from_events" for endpoint in EVENT_SOURCED_ENDPOINTS
    } == endpoint_gate.downstream_task_ids
    assert bronze_ready.upstream_task_ids == {
        f"merge_{endpoint}_from_events" for endpoint in EVENT_SOURCED_ENDPOINTS
    }
    assert all(
        dag.get_task(f"merge_{endpoint}_from_events").params["source"] == "events"
        for endpoint in EVENT_SOURCED_ENDPOINTS
    )


def test_endpoint_selection_defaults_to_all_and_accepts_allowlist():
    select_endpoints = dag.get_task("select_object_endpoints").python_callable

    assert select_endpoints(dag_run=None) == [
        f"merge_{endpoint}_from_events" for endpoint in EVENT_SOURCED_ENDPOINTS
    ]
    assert select_endpoints(
        dag_run=type("DagRun", (), {"conf": {"endpoints": ["charges", "invoices"]}})()
    ) == ["merge_charges_from_events", "merge_invoices_from_events"]


def test_empty_endpoint_allowlist_runs_all_and_unknown_endpoint_fails():
    select_endpoints = dag.get_task("select_object_endpoints").python_callable

    dag_run = type("DagRun", (), {"conf": {"endpoints": []}})()
    assert select_endpoints(dag_run=dag_run) == [
        f"merge_{endpoint}_from_events" for endpoint in EVENT_SOURCED_ENDPOINTS
    ]

    dag_run.conf = {"endpoints": ["not_an_endpoint"]}
    import pytest

    with pytest.raises(ValueError, match="Unknown event object endpoint"):
        select_endpoints(dag_run=dag_run)
