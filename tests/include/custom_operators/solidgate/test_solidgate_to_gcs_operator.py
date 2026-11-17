"""Unit tests for include/custom_operators/solidgate/solidgate_to_gcs_operator.py.

Covers ``land_and_release_to_gcs`` (paginated JSON entities) and
``report_land_and_release_to_gcs`` (financial_entries' generate+poll+CSV report flow),
including the "no records this window" case, which is easy to get backwards: the
Release Avro is ALWAYS written -- even for zero records -- so the release path is
deterministic and downstream GCSToBigQueryOperator never references a nonexistent
object for an empty window.
"""

from __future__ import annotations

import io
from types import SimpleNamespace

import requests

import include.custom_operators.solidgate.solidgate_to_gcs_operator as sg_operator
from include.custom_operators.solidgate.solidgate_to_gcs_operator import (
    land_and_release_to_gcs,
    report_land_and_release_to_gcs,
)


class FakeBlob:
    def __init__(self):
        self.content_type = None
        self.chunk_size = None
        self.uploaded_from_filename = None
        self._raw: bytes | None = None

    def upload_from_string(self, data, content_type=None):
        self.content_type = content_type
        self._raw = data if isinstance(data, bytes) else data.encode("utf-8")

    def upload_from_file(self, file_obj, content_type=None):
        self.content_type = content_type
        data = file_obj.read()
        self._raw = data if isinstance(data, bytes) else data.encode("utf-8")

    def upload_from_filename(self, path):
        self.uploaded_from_filename = path

    def exists(self):
        return self._raw is not None

    def open(self, mode):
        blob = self

        class _Ctx:
            def __enter__(self):
                return io.StringIO((blob._raw or b"").decode("utf-8"))

            def __exit__(self, *exc):
                return False

        return _Ctx()


class FakeBucket:
    def __init__(self):
        self._blobs: dict[str, FakeBlob] = {}

    def blob(self, path):
        return self._blobs.setdefault(path, FakeBlob())

    def list_blobs(self, prefix):
        return [
            SimpleNamespace(name=name)
            for name in self._blobs
            if name.startswith(prefix)
        ]

    def delete_blobs(self, blobs):
        for blob in blobs:
            self._blobs.pop(blob.name, None)


class FakePaginatedHook:
    def __init__(self, pages: list[dict]):
        self._pages = pages

    def iter_raw_pages(self, date_from, date_to, *, filter="updated_at"):
        yield from self._pages

    @staticmethod
    def _extract_records(payload):
        return payload.get("orders", [])


def _patch_gcs(monkeypatch, bucket: FakeBucket):
    fake_client = SimpleNamespace(bucket=lambda name: bucket)
    monkeypatch.setattr(
        sg_operator,
        "GCSHook",
        lambda **kwargs: SimpleNamespace(get_conn=lambda: fake_client),
    )


class TestLandAndReleaseToGcs:
    def test_no_records_still_lands_page_and_writes_empty_release_avro(
        self, monkeypatch
    ):
        bucket = FakeBucket()
        _patch_gcs(monkeypatch, bucket)
        hook = FakePaginatedHook([{"orders": [], "metadata": {}}])

        result = land_and_release_to_gcs(
            hook=hook,
            date_from="2026-01-01 00:00:00",
            date_to="2026-01-02 00:00:00",
            entity="card_orders",
            account_name="acme",
            gcs_bucket="test-bucket",
            gcp_conn_id="test-conn",
        )

        assert result["record_count"] == 0
        # The empty page is still landed and released -- one deterministic Avro path.
        assert result["release_paths"] == [
            "release/solidgate/card_orders/acme/2026-01-01/acme__card_orders_00001.avro",
        ]

    def test_records_present_lands_every_page_and_uploads_release_avro(
        self, monkeypatch
    ):
        bucket = FakeBucket()
        _patch_gcs(monkeypatch, bucket)
        pages = [
            {"orders": [{"order_id": "o1"}], "metadata": {"next_page_iterator": "c1"}},
            {"orders": [{"order_id": "o2"}, {"order_id": "o3"}], "metadata": {}},
        ]
        hook = FakePaginatedHook(pages)

        result = land_and_release_to_gcs(
            hook=hook,
            date_from="2026-01-01 00:00:00",
            date_to="2026-01-02 00:00:00",
            entity="card_orders",
            account_name="acme",
            gcs_bucket="test-bucket",
            gcp_conn_id="test-conn",
        )

        assert result["record_count"] == 3
        assert result["release_paths"] == [
            "release/solidgate/card_orders/acme/2026-01-01/acme__card_orders_00001.avro",
            "release/solidgate/card_orders/acme/2026-01-01/acme__card_orders_00002.avro",
        ]

    def test_mixed_case_account_name_is_lowercased_in_gcs_paths_only(self, monkeypatch):
        bucket = FakeBucket()
        _patch_gcs(monkeypatch, bucket)
        hook = FakePaginatedHook([{"orders": [{"order_id": "o1"}], "metadata": {}}])

        result = land_and_release_to_gcs(
            hook=hook,
            date_from="2026-01-01 00:00:00",
            date_to="2026-01-02 00:00:00",
            entity="card_orders",
            account_name="PS_PROD_ACME_LIVE",
            gcs_bucket="test-bucket",
            gcp_conn_id="test-conn",
        )

        assert result["release_paths"] == [
            "release/solidgate/card_orders/ps_prod_acme_live/2026-01-01/"
            "ps_prod_acme_live__card_orders_00001.avro",
        ]

    def test_multi_day_window_splits_by_day_and_scopes_paths(self, monkeypatch):
        bucket = FakeBucket()
        _patch_gcs(monkeypatch, bucket)
        hook = FakePaginatedHook([{"orders": [{"order_id": "o1"}], "metadata": {}}])

        result = land_and_release_to_gcs(
            hook=hook,
            date_from="2026-01-01 00:00:00",
            date_to="2026-01-03 00:00:00",
            entity="card_orders",
            account_name="acme",
            gcs_bucket="test-bucket",
            gcp_conn_id="test-conn",
        )

        # One page per day, each landed under its own day folder.
        assert sorted(result["release_paths"]) == [
            "release/solidgate/card_orders/acme/2026-01-01/acme__card_orders_00001.avro",
            "release/solidgate/card_orders/acme/2026-01-02/acme__card_orders_00001.avro",
        ]


class FakeReportHook:
    def __init__(self, csv_by_window: dict[tuple[str, str], str]):
        self._csv_by_window = csv_by_window
        self.calls: list[tuple[str, str, str]] = []

    def resolve_report_url(self, date_from, date_to, *, filter="created_at"):
        self.calls.append((date_from, date_to, filter))
        return f"https://reports.solidgate.com/fake/{date_from}_{date_to}"


class _FakeStreamedResponse:
    def __init__(self, csv_text: str):
        self.raw = io.BytesIO(csv_text.encode("utf-8"))

    def raise_for_status(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _patch_requests_get(monkeypatch, csv_by_window: dict[tuple[str, str], str]):
    def fake_get(url, *, stream, timeout):
        # url encodes "<date_from>_<date_to>" -- recover the window to look up its CSV.
        window = url.rsplit("/", 1)[-1]
        date_from, date_to = window.split("_", 1)
        return _FakeStreamedResponse(csv_by_window[(date_from, date_to)])

    monkeypatch.setattr(requests, "get", fake_get)


class TestReportLandAndReleaseToGcs:
    def test_single_day_window_produces_one_release_file(self, monkeypatch):
        bucket = FakeBucket()
        _patch_gcs(monkeypatch, bucket)
        window = ("2026-01-01 00:00:00", "2026-01-02 00:00:00")
        _patch_requests_get(monkeypatch, {window: "id,amount\n1,100\n2,200\n"})
        hook = FakeReportHook({window: "csv-url"})

        result = report_land_and_release_to_gcs(
            hook=hook,
            date_from=window[0],
            date_to=window[1],
            entity="financial_entries",
            account_name="acme",
            gcs_bucket="test-bucket",
            gcp_conn_id="test-conn",
        )

        assert result["record_count"] == 2
        assert result["release_paths"] == [
            "release/solidgate/financial_entries/acme/2026-01-01/acme__financial_entries.avro",
        ]
        assert hook.calls == [
            ("2026-01-01 00:00:00", "2026-01-02 00:00:00", "created_at")
        ]

    def test_mixed_case_account_name_is_lowercased_in_gcs_paths_only(self, monkeypatch):
        bucket = FakeBucket()
        _patch_gcs(monkeypatch, bucket)
        window = ("2026-01-01 00:00:00", "2026-01-02 00:00:00")
        _patch_requests_get(monkeypatch, {window: "id,amount\n1,100\n"})
        hook = FakeReportHook({window: "csv-url"})

        result = report_land_and_release_to_gcs(
            hook=hook,
            date_from=window[0],
            date_to=window[1],
            entity="financial_entries",
            account_name="PS_PROD_ACME_LIVE",
            gcs_bucket="test-bucket",
            gcp_conn_id="test-conn",
        )

        assert result["release_paths"] == [
            "release/solidgate/financial_entries/ps_prod_acme_live/2026-01-01/"
            "ps_prod_acme_live__financial_entries.avro",
        ]

    def test_empty_csv_still_lands_but_skips_release_avro(self, monkeypatch):
        bucket = FakeBucket()
        _patch_gcs(monkeypatch, bucket)
        window = ("2026-01-01 00:00:00", "2026-01-02 00:00:00")
        _patch_requests_get(monkeypatch, {window: "id,amount\n"})
        hook = FakeReportHook({window: "csv-url"})

        result = report_land_and_release_to_gcs(
            hook=hook,
            date_from=window[0],
            date_to=window[1],
            entity="financial_entries",
            account_name="acme",
            gcs_bucket="test-bucket",
            gcp_conn_id="test-conn",
        )

        assert result["record_count"] == 0
        assert result["release_paths"] == [
            "release/solidgate/financial_entries/acme/2026-01-01/acme__financial_entries.avro",
        ]

    def test_multi_day_backfill_requests_one_report_per_day(self, monkeypatch):
        bucket = FakeBucket()
        _patch_gcs(monkeypatch, bucket)
        windows = {
            ("2026-01-01 00:00:00", "2026-01-02 00:00:00"): "id\n1\n",
            ("2026-01-02 00:00:00", "2026-01-03 00:00:00"): "id\n2\n3\n",
        }
        _patch_requests_get(monkeypatch, windows)
        hook = FakeReportHook(windows)

        result = report_land_and_release_to_gcs(
            hook=hook,
            date_from="2026-01-01 00:00:00",
            date_to="2026-01-03 00:00:00",
            entity="financial_entries",
            account_name="acme",
            gcs_bucket="test-bucket",
            gcp_conn_id="test-conn",
        )

        assert result["record_count"] == 3
        assert sorted(result["release_paths"]) == [
            "release/solidgate/financial_entries/acme/2026-01-01/acme__financial_entries.avro",
            "release/solidgate/financial_entries/acme/2026-01-02/acme__financial_entries.avro",
        ]
        assert sorted((f, t) for f, t, _ in hook.calls) == sorted(windows.keys())
