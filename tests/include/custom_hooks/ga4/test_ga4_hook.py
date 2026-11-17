"""Unit tests for include/custom_hooks/ga4/ga4_hook.py (GA4Hook).

``run_report`` drives every GA4 extract task, paging through ``runReport`` with
``limit``/``offset`` until the API's reported ``row_count`` is exhausted -- a bug here
would silently truncate a wide date range or a high-cardinality dimension the same way
a broken cursor would for Solidgate. ``get_conn`` also has hand-written credential
scoping (narrowing to the analytics.readonly scope) that is easy to get backwards.

No real GA4 API client or Airflow connection is built anywhere in this file: hooks are
constructed with ``GA4Hook.__new__`` (bypassing ``__init__``/connection resolution,
mirroring tests/include/custom_hooks/exchange_rates/test_google_finance_hook.py's
``_make_hook`` convention) and a hand-rolled ``SimpleNamespace`` fake mirrors the shape
``google-analytics-data``'s ``RunReportResponse`` exposes: ``rows`` (each with
``dimension_values``/``metric_values`` lists of ``.value`` strings), ``row_count`` and
``metadata``.

Note: GA4Hook.run_report does NOT convert metric strings to floats -- it returns them
exactly as the API gives them (numeric strings). That conversion (``_to_float`` in
dags/ga4/ga4_daily_source_snapshots.py) happens later, at Avro-write time, not in this
hook -- see the report notes for why it isn't covered here.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import include.custom_hooks.ga4.ga4_hook as ga4_hook_module
from include.custom_hooks.ga4.ga4_hook import _GA4_SCOPES, GA4Hook


def _make_hook(property_id=None):
    """Build a GA4Hook without running GoogleBaseHook.__init__ (no real connection)."""
    hook = GA4Hook.__new__(GA4Hook)
    hook.property_id = property_id
    hook._client = None
    return hook


def _dim_value(v):
    return SimpleNamespace(value=v)


def _metric_value(v):
    return SimpleNamespace(value=v)


def _row(dim_values, metric_values):
    return SimpleNamespace(
        dimension_values=[_dim_value(v) for v in dim_values],
        metric_values=[_metric_value(v) for v in metric_values],
    )


def _metadata(**overrides):
    base = dict(
        sampling_metadatas=[],
        subject_to_thresholding=False,
        data_loss_from_other_row=False,
        currency_code="USD",
        time_zone="America/New_York",
        empty_reason="",
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _response(rows, row_count, metadata=None):
    return SimpleNamespace(
        rows=rows,
        row_count=row_count,
        metadata=metadata if metadata is not None else _metadata(),
    )


class _FakeClient:
    """Records every RunReportRequest it receives and returns queued responses in order."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.requests: list = []

    def run_report(self, request):
        self.requests.append(request)
        return self._responses.pop(0)


class TestRunReportSingleAndEmptyPage:
    def test_single_page_maps_dimensions_and_metrics_by_position_as_raw_strings(self):
        client = _FakeClient([_response([_row(["2026-08-01"], ["10"])], row_count=1)])
        hook = _make_hook()
        hook.get_conn = lambda: client

        rows = hook.run_report(
            dimensions=["date"], metrics=["sessions"], property_id="123"
        )

        assert rows == [{"date": "2026-08-01", "sessions": "10"}]
        # never converted to float/number here -- exact API string is preserved.
        assert isinstance(rows[0]["sessions"], str)
        assert len(client.requests) == 1

    def test_multiple_dimensions_and_metrics_are_zipped_by_position_not_by_name(self):
        client = _FakeClient(
            [
                _response(
                    [_row(["2026-08-01", "google / cpc"], ["5", "1.5"])], row_count=1
                )
            ]
        )
        hook = _make_hook()
        hook.get_conn = lambda: client

        rows = hook.run_report(
            dimensions=["date", "sessionSourceMedium"],
            metrics=["sessions", "totalRevenue"],
            property_id="123",
        )

        assert rows == [
            {
                "date": "2026-08-01",
                "sessionSourceMedium": "google / cpc",
                "sessions": "5",
                "totalRevenue": "1.5",
            }
        ]

    def test_empty_report_returns_empty_list_after_a_single_request(self):
        client = _FakeClient([_response([], row_count=0)])
        hook = _make_hook()
        hook.get_conn = lambda: client

        rows = hook.run_report(
            dimensions=["date"], metrics=["sessions"], property_id="123"
        )

        assert rows == []
        assert len(client.requests) == 1


class TestRunReportPagination:
    def test_pages_until_offset_reaches_total_row_count(self):
        pages = [
            _response([_row(["d1"], ["1"]), _row(["d2"], ["2"])], row_count=5),
            _response([_row(["d3"], ["3"]), _row(["d4"], ["4"])], row_count=5),
            _response([_row(["d5"], ["5"])], row_count=5),
        ]
        client = _FakeClient(pages)
        hook = _make_hook()
        hook.get_conn = lambda: client

        rows = hook.run_report(
            dimensions=["date"], metrics=["sessions"], property_id="123", page_size=2
        )

        assert [r["date"] for r in rows] == ["d1", "d2", "d3", "d4", "d5"]
        assert len(client.requests) == 3
        assert [req.offset for req in client.requests] == [0, 2, 4]
        assert all(req.limit == 2 for req in client.requests)

    def test_stops_immediately_if_a_page_returns_zero_rows_even_below_row_count(self):
        # Guards against ever looping forever on a GA4 response inconsistency: an
        # empty page always ends pagination, regardless of the reported row_count.
        pages = [_response([], row_count=10)]
        client = _FakeClient(pages)
        hook = _make_hook()
        hook.get_conn = lambda: client

        rows = hook.run_report(
            dimensions=["date"], metrics=["sessions"], property_id="123"
        )

        assert rows == []
        assert len(client.requests) == 1

    def test_property_and_date_range_are_forwarded_on_every_page(self):
        pages = [
            _response([_row(["d1"], ["1"])], row_count=2),
            _response([_row(["d2"], ["2"])], row_count=2),
        ]
        client = _FakeClient(pages)
        hook = _make_hook()
        hook.get_conn = lambda: client

        hook.run_report(
            dimensions=["date"],
            metrics=["sessions"],
            start_date="2026-08-01",
            end_date="2026-08-02",
            property_id="999",
            page_size=1,
        )

        for req in client.requests:
            assert req.property == "properties/999"
            assert len(req.date_ranges) == 1
            assert req.date_ranges[0].start_date == "2026-08-01"
            assert req.date_ranges[0].end_date == "2026-08-02"


class TestRunReportPropertyIdResolution:
    def test_explicit_property_id_overrides_hook_default(self):
        client = _FakeClient([_response([], row_count=0)])
        hook = _make_hook(property_id="default-prop")
        hook.get_conn = lambda: client

        hook.run_report(
            dimensions=["date"], metrics=["sessions"], property_id="explicit-prop"
        )

        assert client.requests[0].property == "properties/explicit-prop"

    def test_falls_back_to_the_hooks_default_property_id(self):
        client = _FakeClient([_response([], row_count=0)])
        hook = _make_hook(property_id="default-prop")
        hook.get_conn = lambda: client

        hook.run_report(dimensions=["date"], metrics=["sessions"])

        assert client.requests[0].property == "properties/default-prop"

    def test_missing_property_id_everywhere_raises_before_touching_the_client(self):
        hook = _make_hook(property_id=None)
        hook.get_conn = lambda: pytest.fail("get_conn should not be called")

        with pytest.raises(ValueError, match="property_id"):
            hook.run_report(dimensions=["date"], metrics=["sessions"])


class _FakeClientCtor:
    """Fakes the BetaAnalyticsDataClient(credentials=...) constructor."""

    def __init__(self):
        self.calls: list = []

    def __call__(self, credentials):
        self.calls.append(credentials)
        return SimpleNamespace(built_with=credentials)


class TestGetConn:
    def test_builds_and_caches_a_single_client_across_calls(self, monkeypatch):
        hook = _make_hook()
        creds = SimpleNamespace(with_scopes=lambda scopes: creds)
        monkeypatch.setattr(
            hook, "get_credentials_and_project_id", lambda: (creds, "proj-1")
        )
        fake_ctor = _FakeClientCtor()
        monkeypatch.setattr(ga4_hook_module, "BetaAnalyticsDataClient", fake_ctor)

        client1 = hook.get_conn()
        client2 = hook.get_conn()

        assert client1 is client2
        assert len(fake_ctor.calls) == 1

    def test_narrows_credentials_to_the_analytics_readonly_scope_when_supported(
        self, monkeypatch
    ):
        hook = _make_hook()
        scoped = SimpleNamespace(name="scoped")
        creds = SimpleNamespace(
            with_scopes=lambda scopes: scoped if scopes == _GA4_SCOPES else None
        )
        monkeypatch.setattr(
            hook, "get_credentials_and_project_id", lambda: (creds, "proj-1")
        )
        fake_ctor = _FakeClientCtor()
        monkeypatch.setattr(ga4_hook_module, "BetaAnalyticsDataClient", fake_ctor)

        hook.get_conn()

        assert fake_ctor.calls[0] is scoped

    def test_falls_back_to_the_connections_credentials_when_with_scopes_is_unavailable(
        self, monkeypatch
    ):
        hook = _make_hook()
        creds = SimpleNamespace()  # deliberately has no with_scopes attribute
        monkeypatch.setattr(
            hook, "get_credentials_and_project_id", lambda: (creds, "proj-1")
        )
        fake_ctor = _FakeClientCtor()
        monkeypatch.setattr(ga4_hook_module, "BetaAnalyticsDataClient", fake_ctor)

        hook.get_conn()

        assert fake_ctor.calls[0] is creds
