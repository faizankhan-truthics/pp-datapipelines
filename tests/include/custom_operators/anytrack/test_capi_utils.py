"""Tests for include/custom_operators/anytrack/capi_utils.py (CAPI -> GCS landing)."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

from include.config.anytrack_settings import capi_landing_prefix
from include.custom_operators.anytrack.capi_utils import (
    chunk_fraction_read,
    overall_progress_pct,
    pull_chunk_to_landing,
)


class FakeHook:
    def __init__(self, names, pages):
        self.names = names
        self.pages = pages
        self.pulled_names = []
        self.ranges = []

    def list_event_names(self, property_id, date_from, date_to):
        self.ranges.append((date_from, date_to))
        return self.names

    def iter_conversion_pages(self, property_id, date_from, date_to, event_names):
        self.pulled_names.append(event_names)
        yield from self.pages


class FakeSink:
    def __init__(self):
        self.deleted = []
        self.objects = {}

    def delete_prefix(self, prefix):
        self.deleted.append(prefix)

    def upload(self, object_name, data):
        self.objects[object_name] = data


def _event(i, time):
    return {"id": f"id-{i}", "time": time, "eventName": "Lead"}


def _run(hook, sink, **kw):
    return pull_chunk_to_landing(
        hook,
        sink,
        product="fitday",
        property_id="P1",
        chunk_start=date(2026, 11, 2),
        chunk_end=date(2026, 11, 4),
        **kw,
    )


def _lines(sink):
    return [
        json.loads(line) for data in sink.objects.values() for line in data.splitlines()
    ]


class TestPullChunkToLanding:
    def test_pageview_is_never_requested(self):
        hook = FakeHook(["Lead", "PageView", "Signup"], [])
        _run(hook, FakeSink())
        assert hook.pulled_names == [["Lead", "Signup"]]

    def test_only_pageview_means_no_pull_and_no_files(self):
        hook = FakeHook(["PageView"], [])
        sink = FakeSink()
        result = _run(hook, sink)
        assert hook.pulled_names == []
        assert sink.objects == {}
        assert result["events"] == 0 and result["days_written"] == []

    def test_range_is_padded_for_the_property_timezone(self):
        hook = FakeHook([], [])
        _run(hook, FakeSink())
        assert hook.ranges == [("2026-11-01", "2026-11-04")]

    def test_events_outside_the_utc_chunk_are_dropped(self):
        pages = [
            [
                _event(1, "2026-11-01T23:59:59.000Z"),  # before the chunk
                _event(2, "2026-11-02T00:00:00.000Z"),  # first instant, kept
                _event(3, "2026-11-03T23:59:59.000Z"),  # last instant, kept
                _event(4, "2026-11-04T00:00:00.000Z"),  # end is exclusive
            ]
        ]
        sink = FakeSink()
        result = _run(FakeHook(["Lead"], pages), sink)
        assert result["events"] == 2
        assert result["out_of_window"] == 2
        assert {
            e["raw_data"] and json.loads(e["raw_data"])["id"] for e in _lines(sink)
        } == {
            "id-2",
            "id-3",
        }

    def test_files_are_split_per_utc_day_under_the_capi_prefix(self):
        pages = [[_event(1, "2026-11-02T10:00:00Z"), _event(2, "2026-11-03T10:00:00Z")]]
        sink = FakeSink()
        result = _run(FakeHook(["Lead"], pages), sink)
        assert result["days_written"] == ["2026-11-02", "2026-11-03"]
        for name in sink.objects:
            assert name.startswith(capi_landing_prefix("fitday", name.split("/")[4]))
            assert name.endswith(".ndjson")
        assert len(sink.objects) == 2

    def test_each_line_has_the_webhook_landing_shape(self):
        sink = FakeSink()
        _run(FakeHook(["Lead"], [[_event(1, "2026-11-02T10:00:00Z")]]), sink)
        ((name, data),) = sink.objects.items()
        (line,) = [json.loads(x) for x in data.splitlines()]
        assert set(line) == {"_gcs_file_path", "raw_data"}
        assert line["_gcs_file_path"] == name
        assert json.loads(line["raw_data"])["id"] == "id-1"

    def test_buffer_is_flushed_every_shard_events(self):
        events = [_event(i, "2026-11-02T10:00:00Z") for i in range(5)]
        sink = FakeSink()
        result = _run(
            FakeHook(["Lead"], [events[:3], events[3:]]), sink, shard_events=2
        )
        # 5 events, flushed at 2, 4 and the remainder -> 3 files
        assert result["files"] == 3
        assert result["events"] == 5
        assert len(_lines(sink)) == 5

    def test_a_rerun_clears_the_chunks_earlier_shards_first(self):
        sink = FakeSink()
        _run(FakeHook(["Lead"], []), sink)
        assert sink.deleted == [
            f"{capi_landing_prefix('fitday', '2026-11-02')}2026-11-02_",
            f"{capi_landing_prefix('fitday', '2026-11-03')}2026-11-02_",
        ]

    def test_more_than_fifty_names_are_pulled_in_batches(self):
        names = [f"e{i:02d}" for i in range(60)]
        hook = FakeHook(names, [])
        _run(hook, FakeSink())
        assert [len(b) for b in hook.pulled_names] == [50, 10]


class TestProgress:
    def test_overall_percent_counts_finished_chunks_and_the_part_read(self):
        # 60-day window, 7-day chunks: chunk 3 starts after 14 days; half read
        assert overall_progress_pct(14, 7, 60, 0.5) == round((14 + 3.5) / 60 * 100, 1)
        assert overall_progress_pct(0, 7, 60, 0.0) == 0.0
        assert overall_progress_pct(53, 7, 60, 1.0) == 100.0

    def test_overall_percent_is_clamped(self):
        assert overall_progress_pct(0, 7, 7, 5.0) == 100.0
        assert overall_progress_pct(0, 7, 0, 0.0) == 100.0

    def test_chunk_fraction_follows_the_newest_first_order(self):
        lo = datetime(2026, 10, 1, tzinfo=timezone.utc)
        hi = datetime(2026, 10, 11, tzinfo=timezone.utc)
        assert chunk_fraction_read(None, lo, hi, 0, 1) == 0.0
        assert chunk_fraction_read(hi, lo, hi, 0, 1) == 0.0
        assert (
            chunk_fraction_read(
                datetime(2026, 10, 6, tzinfo=timezone.utc), lo, hi, 0, 1
            )
            == 0.5
        )
        assert chunk_fraction_read(lo, lo, hi, 0, 1) == 1.0

    def test_name_batches_split_the_fraction(self):
        lo = datetime(2026, 10, 1, tzinfo=timezone.utc)
        hi = datetime(2026, 10, 11, tzinfo=timezone.utc)
        # second of two batches, halfway through it -> 75% of the chunk
        mid = datetime(2026, 10, 6, tzinfo=timezone.utc)
        assert chunk_fraction_read(mid, lo, hi, 1, 2) == 0.75

    def test_logs_a_start_a_progress_and_a_done_line(self, caplog):
        # 4 days read as 2 pages of 1 event each, logging every page
        pages = [
            [_event(1, "2026-11-03T12:00:00Z")],
            [_event(2, "2026-11-02T12:00:00Z")],
        ]
        with caplog.at_level("INFO"):
            pull_chunk_to_landing(
                FakeHook(["Lead"], pages),
                FakeSink(),
                product="fitday",
                property_id="P1",
                chunk_start=date(2026, 11, 2),
                chunk_end=date(2026, 11, 4),
                window_start=date(2026, 10, 21),
                window_end=date(2026, 11, 4),
                chunk_index=2,
                chunk_total=2,
                log_every_pages=1,
            )
        text = "\n".join(r.getMessage() for r in caplog.records)
        assert "[capi fitday] chunk 2/2" in text
        assert "starting: overall 85.7%" in text  # 12 of 14 days done before it
        assert "overall 89.3% | this chunk 25.0%" in text  # read down to 11-03 12:00
        assert "overall 96.4% | this chunk 75.0%" in text  # read down to 11-02 12:00
        assert "done in" in text and "Overall 100.0%" in text
