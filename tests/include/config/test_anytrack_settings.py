"""Unit tests for include/config/anytrack_settings.py.

Covers:
  * ``resolve_window`` - byte-for-byte mirrors solidgate_settings.resolve_window (see
    tests/include/config/test_solidgate_settings.py), including the same-instant
    collapse for manually triggered runs.
  * ``landing_prefix``/``release_object`` - the GCS path helpers the DAG and
    include/custom_operators/anytrack/utils.py both rely on to scope objects by
    product.
  * ``PRODUCTS`` - the list of onboarded AnyTrack products the DAG maps over. This is
    hand-edited config, not derived logic, and it has already regressed once: a prior
    commit temporarily shipped with ``iqcenter`` commented out and PRODUCTS reduced to
    just ``["infochecker"]`` before being restored. The pin below is meant to fail the
    day the list changes again, as a deliberate reminder to update this test (and the
    rest of the AnyTrack test suite) alongside it.
"""

from __future__ import annotations

import pendulum
import pytest

from include.config.anytrack_settings import (
    ENTITY,
    PRODUCTS,
    SOURCE,
    landing_prefix,
    release_object,
    resolve_window,
)


def _context(*, date_from=None, date_to=None, start, end):
    return {
        "params": {"date_from": date_from, "date_to": date_to},
        "data_interval_start": start,
        "data_interval_end": end,
    }


class TestResolveWindowBackfill:
    def test_uses_params_verbatim_when_both_dates_present(self):
        start = pendulum.datetime(2026, 1, 1, tz="UTC")
        end = pendulum.datetime(2026, 1, 1, tz="UTC")
        ctx = _context(
            date_from="2025-03-01 00:00:00",
            date_to="2025-03-05 23:59:59",
            start=start,
            end=end,
        )

        ds, date_from, date_to = resolve_window(ctx)

        assert ds == "2025-03-01"
        assert date_from == "2025-03-01 00:00:00"
        assert date_to == "2025-03-05 23:59:59"

    @pytest.mark.parametrize(
        "date_from,date_to",
        [
            (None, None),
            ("2025-03-01 00:00:00", None),
            (None, "2025-03-05 00:00:00"),
            ("", ""),
        ],
    )
    def test_falls_back_to_data_interval_unless_both_dates_present(
        self, date_from, date_to
    ):
        start = pendulum.datetime(2026, 5, 10, 3, 0, tz="UTC")
        end = pendulum.datetime(2026, 5, 11, 3, 0, tz="UTC")
        ctx = _context(date_from=date_from, date_to=date_to, start=start, end=end)

        ds, resolved_from, resolved_to = resolve_window(ctx)

        assert ds == "2026-05-10"
        assert resolved_from == "2026-05-10 03:00:00"
        assert resolved_to == "2026-05-11 03:00:00"


class TestResolveWindowScheduled:
    def test_normal_data_interval_is_used_directly(self):
        start = pendulum.datetime(2026, 5, 10, 1, 0, tz="UTC")
        end = pendulum.datetime(2026, 5, 11, 1, 0, tz="UTC")
        ctx = _context(start=start, end=end)

        ds, date_from, date_to = resolve_window(ctx)

        assert ds == "2026-05-10"
        assert date_from == "2026-05-10 01:00:00"
        assert date_to == "2026-05-11 01:00:00"

    def test_data_interval_is_converted_to_utc(self):
        start = pendulum.datetime(2026, 5, 10, 20, 0, tz="US/Pacific")
        end = pendulum.datetime(2026, 5, 11, 20, 0, tz="US/Pacific")
        ctx = _context(start=start, end=end)

        ds, date_from, date_to = resolve_window(ctx)

        # US/Pacific is UTC-7 in May (DST) -> 20:00 local == 03:00 UTC next day.
        assert ds == "2026-05-11"
        assert date_from == "2026-05-11 03:00:00"
        assert date_to == "2026-05-12 03:00:00"

    def test_same_instant_manual_trigger_collapses_to_previous_day(self):
        # A manually triggered run has no real data_interval, so Airflow sets
        # start == end == "now". Without the collapse this would resolve into a
        # zero-width window; the fix walks it back to yesterday's full day.
        now = pendulum.datetime(2026, 8, 18, 14, 30, 5, tz="UTC")
        ctx = _context(start=now, end=now)

        ds, date_from, date_to = resolve_window(ctx)

        assert ds == "2026-08-17"
        assert date_from == "2026-08-17 00:00:00"
        assert date_to == "2026-08-18 00:00:00"


class TestGcsPathHelpers:
    def test_landing_prefix_path_shape(self):
        assert (
            landing_prefix("iqcenter", "2026-08-19")
            == "landing/anytrack/iqcenter/events/2026-08-19/"
        )

    def test_landing_prefix_uses_source_and_entity_constants(self):
        # If SOURCE/ENTITY are ever renamed, the prefix must move with them -- the
        # webhook receiver (cloud_runs/webhook-service/) and this DAG must always
        # agree on where landing files live.
        assert (
            landing_prefix("infochecker", "2026-08-19")
            == f"landing/{SOURCE}/infochecker/{ENTITY}/2026-08-19/"
        )

    def test_release_object_path_shape(self):
        assert release_object(
            "iqcenter", "2026-08-19", "iqcenter_events_2026-08-19.avro"
        ) == (
            "release/anytrack/iqcenter/events/2026-08-19/iqcenter_events_2026-08-19.avro"
        )

    def test_landing_and_release_agree_on_product_and_ds_segment(self):
        prefix = landing_prefix("infochecker", "2026-08-19")
        release = release_object(
            "infochecker", "2026-08-19", "infochecker_events_2026-08-19.avro"
        )
        # Both paths share the same "<product>/events/<ds>/" segment -- only the
        # landing/release root and the trailing filename differ.
        assert prefix == "landing/anytrack/infochecker/events/2026-08-19/"
        assert (
            release
            == "release/anytrack/infochecker/events/2026-08-19/infochecker_events_2026-08-19.avro"
        )


class TestProducts:
    def test_currently_onboarded_products(self):
        assert PRODUCTS == [
            "iqcenter",
            "infochecker",
            "files_editor_preprod",
            "files_editor",
            "file_editor",
            "files_convert",
            "tracelo",
            "reversly",
            "opengen_ai",
            "giftmusic",
            "oqg",
            "location_tool",
            "qci",
            "qga",
            "chatbox_ai",
            "simpl_ai",
            "publicregistry",
            "fitday",
            "delete_background",
            "docuhero",
        ]

    def test_products_are_unique(self):
        assert len(PRODUCTS) == len(set(PRODUCTS))
