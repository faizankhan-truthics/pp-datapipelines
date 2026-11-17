"""Unit tests for FxBronzeDailySnapshotExtractToLandingOperator and its helper
``get_daily_currencies`` (include/custom_operators/exchange_rates/daily_snapshot_extract_to_landing.py).

This operator can only ever capture *today's* live GOOGLEFINANCE snapshot (see the
module docstring), so the interesting hand-written logic isn't date-window math --
it's:
  * currency-list resolution: explicit ``self.currencies`` short-circuits the
    BigQuery-driven auto-discovery in ``get_daily_currencies``.
  * the "zero usable rates" guardrail that turns a fully-#N/A sheet into a loud
    RuntimeError instead of silently landing nothing.
  * the landing payload/manifest shape (date + source stamped onto every row,
    run_id sanitized for use as a GCS path segment, `_SUCCESS` marker written
    alongside the page).

No real Sheets/BigQuery/GCS call is made: GoogleFinanceCurrencyHook and GCSHook
are monkeypatched to fakes at the module level, mirroring
tests/include/custom_operators/solidgate/test_solidgate_to_gcs_operator.py.
"""

from __future__ import annotations

import json

import pytest

import include.custom_operators.exchange_rates.daily_snapshot_extract_to_landing as mod
from include.custom_operators.exchange_rates.daily_snapshot_extract_to_landing import (
    FxBronzeDailySnapshotExtractToLandingOperator,
    get_daily_currencies,
)


class FakeSheetHook:
    def __init__(self, rows):
        self.rows = rows
        self.write_calls = []
        self.wait_calls = []
        self.read_calls = []

    def write_currency_formulas(self, **kwargs):
        self.write_calls.append(kwargs)
        return len(kwargs["currencies"])

    def wait_for_fresh_values(self, *args, **kwargs):
        self.wait_calls.append((args, kwargs))

    def read_currency_rates(self, *args, **kwargs):
        self.read_calls.append((args, kwargs))
        return self.rows


class FakeGCSHook:
    def __init__(self):
        self.uploads: list[dict] = []

    def upload(self, *, bucket_name, object_name, data, mime_type=None):
        self.uploads.append(
            {
                "bucket_name": bucket_name,
                "object_name": object_name,
                "data": data,
                "mime_type": mime_type,
            }
        )


def _patch_hooks(monkeypatch, sheet_hook, gcs_hook):
    monkeypatch.setattr(mod, "GoogleFinanceCurrencyHook", lambda **kwargs: sheet_hook)
    monkeypatch.setattr(mod, "GCSHook", lambda **kwargs: gcs_hook)


def _make_operator(**overrides):
    kwargs = dict(
        task_id="extract_to_landing",
        gcs_bucket="test-bucket",
        landing_prefix="landing/fx_bronze/daily_snapshot",
        spreadsheet_id="SSID",
        sheet_name="Sheet1",
        base_currency="USD",
    )
    kwargs.update(overrides)
    return FxBronzeDailySnapshotExtractToLandingOperator(**kwargs)


class TestCurrencyListResolution:
    def test_explicit_currencies_short_circuits_auto_discovery(self, monkeypatch):
        sheet_hook = FakeSheetHook(rows=[{"currency": "EUR", "close": 1.08}])
        gcs_hook = FakeGCSHook()
        _patch_hooks(monkeypatch, sheet_hook, gcs_hook)

        def _boom():
            raise AssertionError(
                "get_daily_currencies should not be called when self.currencies is set"
            )

        monkeypatch.setattr(mod, "get_daily_currencies", _boom)

        op = _make_operator()
        op.currencies = ["eur"]
        op.execute(
            {"ds": "2026-08-18", "run_id": "scheduled__2026-08-18T00:00:00+00:00"}
        )

        assert sheet_hook.write_calls[0]["currencies"] == ["eur"]

    def test_falls_back_to_get_daily_currencies_when_none_set(self, monkeypatch):
        sheet_hook = FakeSheetHook(rows=[{"currency": "EUR", "close": 1.08}])
        gcs_hook = FakeGCSHook()
        _patch_hooks(monkeypatch, sheet_hook, gcs_hook)
        monkeypatch.setattr(mod, "get_daily_currencies", lambda: ["eur", "pln"])

        op = _make_operator()
        assert op.currencies is None
        op.execute(
            {"ds": "2026-08-18", "run_id": "scheduled__2026-08-18T00:00:00+00:00"}
        )

        assert sheet_hook.write_calls[0]["currencies"] == ["eur", "pln"]


class TestZeroRatesGuardrail:
    def test_raises_runtime_error_when_read_currency_rates_returns_nothing(
        self, monkeypatch
    ):
        sheet_hook = FakeSheetHook(rows=[])
        gcs_hook = FakeGCSHook()
        _patch_hooks(monkeypatch, sheet_hook, gcs_hook)

        op = _make_operator()
        op.currencies = ["eur"]

        with pytest.raises(RuntimeError, match="zero usable rates"):
            op.execute(
                {"ds": "2026-08-18", "run_id": "manual__2026-08-18T00:00:00+00:00"}
            )

        # Guardrail fires before any GCS upload happens.
        assert gcs_hook.uploads == []


class TestLandingPayloadShape:
    def test_rows_are_stamped_with_run_date_and_source(self, monkeypatch):
        sheet_hook = FakeSheetHook(
            rows=[{"currency": "EUR", "close": 1.08}, {"currency": "PLN", "close": 4.1}]
        )
        gcs_hook = FakeGCSHook()
        _patch_hooks(monkeypatch, sheet_hook, gcs_hook)

        op = _make_operator()
        op.currencies = ["eur", "pln"]
        op.execute(
            {"ds": "2026-08-18", "run_id": "scheduled__2026-08-18T00:00:00+00:00"}
        )

        page_upload = next(
            u for u in gcs_hook.uploads if u["object_name"].endswith("page_0001.json")
        )
        payload = json.loads(page_upload["data"])
        assert payload["meta"] == {"total-pages": 1}
        assert payload["data"] == [
            {
                "currency": "EUR",
                "close": 1.08,
                "date": "2026-08-18",
                "source": "google_finance",
            },
            {
                "currency": "PLN",
                "close": 4.1,
                "date": "2026-08-18",
                "source": "google_finance",
            },
        ]

    def test_success_marker_records_page_and_row_counts(self, monkeypatch):
        sheet_hook = FakeSheetHook(rows=[{"currency": "EUR", "close": 1.08}])
        gcs_hook = FakeGCSHook()
        _patch_hooks(monkeypatch, sheet_hook, gcs_hook)

        op = _make_operator()
        op.currencies = ["eur"]
        op.execute(
            {"ds": "2026-08-18", "run_id": "scheduled__2026-08-18T00:00:00+00:00"}
        )

        success_upload = next(
            u for u in gcs_hook.uploads if u["object_name"].endswith("_SUCCESS")
        )
        assert json.loads(success_upload["data"]) == {"pages": 1, "rows": 1}

    def test_run_id_is_sanitized_for_use_as_a_gcs_path_segment(self, monkeypatch):
        sheet_hook = FakeSheetHook(rows=[{"currency": "EUR", "close": 1.08}])
        gcs_hook = FakeGCSHook()
        _patch_hooks(monkeypatch, sheet_hook, gcs_hook)

        op = _make_operator()
        op.currencies = ["eur"]
        landing_run_prefix = op.execute(
            {"ds": "2026-08-18", "run_id": "manual__2026-08-18T14:30:05+00:00"}
        )

        assert ":" not in landing_run_prefix
        assert "+" not in landing_run_prefix
        assert (
            landing_run_prefix
            == "landing/fx_bronze/daily_snapshot/manual__2026-08-18T14_30_05_00_00"
        )
        assert all(
            u["object_name"].startswith(landing_run_prefix) for u in gcs_hook.uploads
        )

    def test_execute_returns_the_landing_run_prefix(self, monkeypatch):
        sheet_hook = FakeSheetHook(rows=[{"currency": "EUR", "close": 1.08}])
        gcs_hook = FakeGCSHook()
        _patch_hooks(monkeypatch, sheet_hook, gcs_hook)

        op = _make_operator()
        op.currencies = ["eur"]
        result = op.execute(
            {"ds": "2026-08-18", "run_id": "scheduled__2026-08-18T00:00:00+00:00"}
        )

        assert (
            result
            == "landing/fx_bronze/daily_snapshot/scheduled__2026-08-18T00_00_00_00_00"
        )


class TestGetDailyCurrencies:
    class _FakeRow:
        def __init__(self, currency):
            self.currency = currency

    class _FakeQueryJob:
        def __init__(self, rows):
            self._rows = rows

        def result(self):
            return self._rows

    class _FakeClient:
        def __init__(self, rows):
            self._rows = rows
            self.queries: list[str] = []

        def query(self, query):
            self.queries.append(query)
            return TestGetDailyCurrencies._FakeQueryJob(self._rows)

    class _FakeBigQueryHook:
        def __init__(self, gcp_conn_id, location):
            self.gcp_conn_id = gcp_conn_id
            self.location = location

        def get_client(self, project_id):
            self.project_id = project_id
            return TestGetDailyCurrencies._FakeClient(
                [
                    TestGetDailyCurrencies._FakeRow("EUR"),
                    TestGetDailyCurrencies._FakeRow("PLN"),
                ]
            )

    def _patch_variables_and_bigquery(self, monkeypatch):
        variables = {
            "GCP_PROJECT_ID": "proj",
            "BQ_BRONZE_DATASET": "bronze",
            "BQ_LOCATION": "US",
            "GCP_CONN_ID": "gcp_default",
        }
        monkeypatch.setattr(
            mod.Variable,
            "get",
            staticmethod(lambda key, default_var=None: variables.get(key, default_var)),
        )
        monkeypatch.setattr(mod, "BigQueryHook", self._FakeBigQueryHook)

    def test_queries_stripe_charges_table_in_the_bronze_dataset_and_returns_currency_list(
        self, monkeypatch
    ):
        self._patch_variables_and_bigquery(monkeypatch)

        currencies = get_daily_currencies()

        assert currencies == ["EUR", "PLN"]

    def test_query_text_targets_the_configured_project_and_dataset(self, monkeypatch):
        self._patch_variables_and_bigquery(monkeypatch)
        captured_client = {}

        original_get_client = self._FakeBigQueryHook.get_client

        def spying_get_client(self, project_id):
            client = original_get_client(self, project_id)
            captured_client["client"] = client
            return client

        monkeypatch.setattr(self._FakeBigQueryHook, "get_client", spying_get_client)

        get_daily_currencies()

        [query] = captured_client["client"].queries
        assert "proj.bronze.stripe_charges" in query
        assert "upper(currency)!='USD'" in query
