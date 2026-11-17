"""Unit tests for the Zendesk GCS operator (include/custom_operators/zendesk/zendesk_to_gcs_operator.py).

Two things are exercised, matching Solidgate's operator test template but extended
for Zendesk's added checkpointing:

  * ``land_and_release_to_gcs`` -- the "no records this window" branch (still lands
    raw pages losslessly, skips the Release Avro), the per-page ``save_cursor``
    checkpoint (so a failed/timed-out run resumes near where it left off, not from
    the start of a possibly-hours-long backfill), and the ``persist_checkpoint=False``
    escape hatch used by a manual ``date_from`` test run.
  * ``ZendeskToGCSOperator.execute`` -- resolving ``date_from`` vs. the
    saved cursor, and the ``ds`` folder-date pick (data_interval end-vs-start).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import include.custom_operators.zendesk.zendesk_to_gcs_operator as zg_operator
from include.config.zendesk_settings import parse_date_from
from include.custom_hooks.zendesk.ticket_comments import TicketCommentsHook
from include.custom_hooks.zendesk.tickets import TicketsHook
from include.custom_hooks.zendesk.users import UsersHook
from include.custom_operators.zendesk.zendesk_to_gcs_operator import (
    ZendeskToGCSOperator,
    land_and_release_to_gcs,
)


class FakeBlob:
    def __init__(self):
        self.content_type = None
        self.chunk_size = None
        self.written_lines: list[str] = []
        self.uploaded_from_filename = None

    def open(self, mode):
        blob = self

        class _Ctx:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

            def write(self, data):
                blob.written_lines.append(data)

        return _Ctx()

    def upload_from_filename(self, path):
        self.uploaded_from_filename = path


class FakeBucket:
    def __init__(self):
        self._blobs: dict[str, FakeBlob] = {}

    def blob(self, path):
        return self._blobs.setdefault(path, FakeBlob())


class FakeHook:
    def __init__(self, pages, subdomain="acme"):
        self._pages = pages
        self.subdomain = subdomain
        self.received_resume_token = "UNSET"
        self.received_max_pages = "UNSET"

    def iter_raw_pages(self, resume_token, *, max_pages=None):
        self.received_resume_token = resume_token
        self.received_max_pages = max_pages
        yield from self._pages

    @staticmethod
    def extract_records(payload):
        return payload.get("tickets", [])

    @staticmethod
    def next_resume_token(payload):
        return (payload or {}).get("after_cursor")


def _patch_gcs(monkeypatch, bucket: FakeBucket):
    fake_client = SimpleNamespace(bucket=lambda name: bucket)
    monkeypatch.setattr(
        zg_operator,
        "GCSHook",
        lambda **kwargs: SimpleNamespace(get_conn=lambda: fake_client),
    )


def _patch_save_cursor(monkeypatch):
    calls = []
    monkeypatch.setattr(
        zg_operator, "save_cursor", lambda entity, value: calls.append((entity, value))
    )
    return calls


class TestLandAndReleaseToGcsNoRecords:
    def test_lands_raw_pages_but_skips_release_avro(self, monkeypatch):
        bucket = FakeBucket()
        _patch_gcs(monkeypatch, bucket)
        _patch_save_cursor(monkeypatch)
        pages = [{"tickets": [], "after_cursor": None, "end_of_stream": True}]
        hook = FakeHook(pages)

        result = land_and_release_to_gcs(
            hook=hook,
            entity="tickets",
            ds="2026-08-10",
            run_id="manual__run",
            resume_token=None,
            gcs_bucket="test-bucket",
            gcp_conn_id="test-conn",
        )

        assert result["record_count"] == 0
        assert result["page_count"] == 1
        landing_blob = bucket.blob(result["landing_path"])
        landed = [
            json.loads(line)
            for line in "".join(landing_blob.written_lines).splitlines()
        ]
        assert landed == pages
        release_blob = bucket.blob(result["release_path"])
        assert release_blob.uploaded_from_filename is None


class TestLandAndReleaseToGcsWithRecords:
    def test_lands_every_page_and_uploads_release_avro_with_expected_record_shape(
        self, monkeypatch
    ):
        bucket = FakeBucket()
        _patch_gcs(monkeypatch, bucket)
        _patch_save_cursor(monkeypatch)
        pages = [
            {"tickets": [{"id": 1}], "after_cursor": "c1", "end_of_stream": False},
            {
                "tickets": [{"id": 2}, {"id": 3}],
                "after_cursor": None,
                "end_of_stream": True,
            },
        ]
        hook = FakeHook(pages, subdomain="acme")

        result = land_and_release_to_gcs(
            hook=hook,
            entity="tickets",
            ds="2026-08-10",
            run_id="manual__run",
            resume_token=None,
            gcs_bucket="test-bucket",
            gcp_conn_id="test-conn",
        )

        assert result["record_count"] == 3
        assert result["page_count"] == 2
        release_blob = bucket.blob(result["release_path"])
        assert release_blob.uploaded_from_filename is not None

    def test_non_dict_records_in_the_extracted_list_are_skipped(self, monkeypatch):
        bucket = FakeBucket()
        _patch_gcs(monkeypatch, bucket)
        _patch_save_cursor(monkeypatch)

        class OddHook(FakeHook):
            @staticmethod
            def extract_records(payload):
                return [{"id": 1}, "not-a-dict", None]

        hook = OddHook([{"tickets": [], "end_of_stream": True}])

        result = land_and_release_to_gcs(
            hook=hook,
            entity="tickets",
            ds="2026-08-10",
            run_id="run",
            resume_token=None,
            gcs_bucket="test-bucket",
            gcp_conn_id="test-conn",
        )

        assert result["record_count"] == 1


class TestLandAndReleaseToGcsCheckpointing:
    def test_save_cursor_is_called_once_per_page_with_that_pages_resume_token(
        self, monkeypatch
    ):
        bucket = FakeBucket()
        _patch_gcs(monkeypatch, bucket)
        calls = _patch_save_cursor(monkeypatch)
        pages = [
            {"tickets": [{"id": 1}], "after_cursor": "c1", "end_of_stream": False},
            {"tickets": [{"id": 2}], "after_cursor": "c2", "end_of_stream": False},
            {"tickets": [], "after_cursor": None, "end_of_stream": True},
        ]
        hook = FakeHook(pages)

        land_and_release_to_gcs(
            hook=hook,
            entity="tickets",
            ds="2026-08-10",
            run_id="run",
            resume_token=None,
            gcs_bucket="b",
            gcp_conn_id="c",
            persist_checkpoint=True,
        )

        assert calls == [("tickets", "c1"), ("tickets", "c2"), ("tickets", None)]

    def test_persist_checkpoint_false_never_calls_save_cursor(self, monkeypatch):
        # Used for a manual date_from test run against a small slice --
        # the real backfill cursor must stay untouched.
        bucket = FakeBucket()
        _patch_gcs(monkeypatch, bucket)
        calls = _patch_save_cursor(monkeypatch)
        pages = [{"tickets": [{"id": 1}], "after_cursor": "c1", "end_of_stream": True}]
        hook = FakeHook(pages)

        land_and_release_to_gcs(
            hook=hook,
            entity="tickets",
            ds="2026-08-10",
            run_id="run",
            resume_token=1700000000,
            gcs_bucket="b",
            gcp_conn_id="c",
            persist_checkpoint=False,
        )

        assert calls == []

    def test_result_next_resume_token_falls_back_to_the_input_resume_token_when_no_pages_are_yielded(
        self, monkeypatch
    ):
        bucket = FakeBucket()
        _patch_gcs(monkeypatch, bucket)
        _patch_save_cursor(monkeypatch)
        hook = FakeHook([])  # empty generator -- e.g. an already-exhausted export

        result = land_and_release_to_gcs(
            hook=hook,
            entity="tickets",
            ds="2026-08-10",
            run_id="run",
            resume_token="last-known-cursor",
            gcs_bucket="b",
            gcp_conn_id="c",
        )

        assert result["next_resume_token"] == "last-known-cursor"
        assert result["page_count"] == 0


class TestLandAndReleaseToGcsPathsAndPlumbing:
    def test_paths_are_scoped_by_entity_ds_account_and_sanitized_run_id(
        self, monkeypatch
    ):
        bucket = FakeBucket()
        _patch_gcs(monkeypatch, bucket)
        _patch_save_cursor(monkeypatch)
        hook = FakeHook([{"tickets": [], "end_of_stream": True}], subdomain="acme")

        result = land_and_release_to_gcs(
            hook=hook,
            entity="tickets",
            ds="2026-08-10",
            run_id="scheduled__2026-08-10T04:00:00+00:00",
            resume_token=None,
            gcs_bucket="test-bucket",
            gcp_conn_id="test-conn",
        )

        expected_safe_run_id = "scheduled__2026-08-10T04_00_00_00_00"
        assert (
            result["landing_path"]
            == f"landing/zendesk/tickets/2026-08-10/acme_{expected_safe_run_id}_raw_compiled.json"
        )
        assert (
            result["release_path"]
            == f"release/zendesk/tickets/2026-08-10/acme_{expected_safe_run_id}_release.avro"
        )

    def test_resume_token_and_max_pages_are_passed_through_to_the_hook(
        self, monkeypatch
    ):
        bucket = FakeBucket()
        _patch_gcs(monkeypatch, bucket)
        _patch_save_cursor(monkeypatch)
        monkeypatch.setattr(zg_operator, "max_pages_for", lambda entity: 42)
        hook = FakeHook([{"tickets": [], "end_of_stream": True}])

        land_and_release_to_gcs(
            hook=hook,
            entity="tickets",
            ds="2026-08-10",
            run_id="run",
            resume_token="saved-cursor",
            gcs_bucket="b",
            gcp_conn_id="c",
        )

        assert hook.received_resume_token == "saved-cursor"
        assert hook.received_max_pages == 42


class TestZendeskToGcsOperatorExecute:
    def _operator(self, endpoint="tickets"):
        return ZendeskToGCSOperator(
            task_id=f"extract_{endpoint}",
            subdomain="acme",
            email="bot@acme.com",
            api_token="tok",
            endpoint=endpoint,
        )

    def _context(self, *, start, end, params=None, run_id="manual__run"):
        return {
            "data_interval_start": start,
            "data_interval_end": end,
            "params": params or {},
            "run_id": run_id,
        }

    def test_date_from_present_skips_saved_cursor_and_disables_checkpoint_persist(
        self, monkeypatch
    ):
        captured = {}

        def fake_land(**kwargs):
            captured.update(kwargs)
            return {
                "landing_path": "l",
                "release_path": "r",
                "record_count": 0,
                "page_count": 0,
                "next_resume_token": None,
            }

        monkeypatch.setattr(zg_operator, "land_and_release_to_gcs", fake_land)
        monkeypatch.setattr(
            zg_operator,
            "get_saved_cursor",
            lambda endpoint: pytest.fail(
                "must not consult saved cursor when override is set"
            ),
        )

        op = self._operator()
        start = end = datetime(2026, 8, 10, tzinfo=timezone.utc)
        ctx = self._context(start=start, end=end, params={"date_from": "2026-08-10"})

        op.execute(ctx)

        assert captured["resume_token"] == parse_date_from("2026-08-10")
        assert captured["persist_checkpoint"] is False

    def test_no_override_uses_the_saved_cursor_and_persists_checkpoint(
        self, monkeypatch
    ):
        captured = {}

        def fake_land(**kwargs):
            captured.update(kwargs)
            return {
                "landing_path": "l",
                "release_path": "r",
                "record_count": 0,
                "page_count": 0,
                "next_resume_token": None,
            }

        monkeypatch.setattr(zg_operator, "land_and_release_to_gcs", fake_land)
        monkeypatch.setattr(
            zg_operator, "get_saved_cursor", lambda endpoint: "cursor-from-variable"
        )

        op = self._operator()
        start = end = datetime(2026, 8, 10, tzinfo=timezone.utc)
        ctx = self._context(start=start, end=end, params={"date_from": ""})

        op.execute(ctx)

        assert captured["resume_token"] == "cursor-from-variable"
        assert captured["persist_checkpoint"] is True

    def test_ds_uses_data_interval_end_when_start_equals_end(self, monkeypatch):
        # A manually triggered run has start == end (no real data interval).
        captured = {}
        monkeypatch.setattr(
            zg_operator,
            "land_and_release_to_gcs",
            lambda **kw: (
                captured.update(kw),
                {
                    "landing_path": "l",
                    "release_path": "r",
                    "record_count": 0,
                    "page_count": 0,
                    "next_resume_token": None,
                },
            )[1],
        )
        monkeypatch.setattr(zg_operator, "get_saved_cursor", lambda endpoint: None)

        op = self._operator()
        same = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
        ctx = self._context(start=same, end=same)

        op.execute(ctx)

        assert captured["ds"] == "2026-08-10"

    def test_ds_uses_data_interval_start_when_start_differs_from_end(self, monkeypatch):
        captured = {}
        monkeypatch.setattr(
            zg_operator,
            "land_and_release_to_gcs",
            lambda **kw: (
                captured.update(kw),
                {
                    "landing_path": "l",
                    "release_path": "r",
                    "record_count": 0,
                    "page_count": 0,
                    "next_resume_token": None,
                },
            )[1],
        )
        monkeypatch.setattr(zg_operator, "get_saved_cursor", lambda endpoint: None)

        op = self._operator()
        start = datetime(2026, 8, 10, 4, 0, tzinfo=timezone.utc)
        end = datetime(2026, 8, 11, 4, 0, tzinfo=timezone.utc)
        ctx = self._context(start=start, end=end)

        op.execute(ctx)

        assert captured["ds"] == "2026-08-10"  # the start of the interval, not the end

    @pytest.mark.parametrize(
        "endpoint,hook_cls",
        [
            ("tickets", TicketsHook),
            ("users", UsersHook),
            ("ticket_comments", TicketCommentsHook),
        ],
    )
    def test_endpoint_resolves_to_the_matching_hook_class(
        self, monkeypatch, endpoint, hook_cls
    ):
        captured = {}
        monkeypatch.setattr(
            zg_operator,
            "land_and_release_to_gcs",
            lambda **kw: (
                captured.update(kw),
                {
                    "landing_path": "l",
                    "release_path": "r",
                    "record_count": 0,
                    "page_count": 0,
                    "next_resume_token": None,
                },
            )[1],
        )
        monkeypatch.setattr(zg_operator, "get_saved_cursor", lambda ep: None)

        op = self._operator(endpoint=endpoint)
        same = datetime(2026, 8, 10, tzinfo=timezone.utc)
        ctx = self._context(start=same, end=same)

        op.execute(ctx)

        assert type(captured["hook"]) is hook_cls
