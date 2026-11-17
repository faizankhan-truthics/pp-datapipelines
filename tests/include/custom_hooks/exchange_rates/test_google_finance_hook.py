"""Unit tests for include/custom_hooks/exchange_rates/google_finance_hook.py.

GOOGLEFINANCE has no REST API; this hook drives it indirectly through the Sheets
API (write a formula, poll until it resolves, read back the value). These tests
cover the three pieces of hand-written logic that could silently corrupt a rate
or hang a DAG run:

  * ``write_currency_formulas`` -- ticker-direction templating (currency->base vs
    base->currency) and case-normalization of currency codes.
  * ``wait_for_fresh_values`` -- the poll loop's "is this row done recalculating"
    test, which only recognizes explicit ``#``-prefixed error strings as "still
    pending" (see ``test_blank_not_yet_computed_cell_is_not_recognized_as_pending``
    below -- a suspected real gap, not something these tests paper over).
  * ``read_currency_rates`` -- the numeric/error-string row filter that decides
    which rows survive into the landing payload.

No real Sheets API or network call is made anywhere in this file: the hook is
built with ``__new__`` (bypassing ``GSheetsHook.__init__``/credential lookup
entirely) and every collaborator it touches (``get_conn``, ``get_values``,
``log``, ``time.sleep``/``time.monotonic``) is a fake or monkeypatch.

Note on ``log``: airflow's ``LoggingMixin.log`` is a read-only ``@property`` (no
setter), so it cannot be assigned on an instance (``hook.log = ...`` raises
AttributeError). ``_make_hook`` therefore shadows it at the class level via
``monkeypatch``, which monkeypatch cleanly reverts after each test.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

import include.custom_hooks.exchange_rates.google_finance_hook as gfh
from include.custom_hooks.exchange_rates.google_finance_hook import (
    HEADER_ROW,
    GoogleFinanceCurrencyHook,
)


def _make_hook(monkeypatch):
    """Build a GoogleFinanceCurrencyHook without running GSheetsHook.__init__.

    __init__ would try to resolve a real Airflow connection; every method under
    test only touches self.get_conn()/self.get_values()/self.log, so those are
    stubbed directly instead. ``log`` is patched at the class level because it's
    a property with no setter (see module docstring).
    """
    hook = GoogleFinanceCurrencyHook.__new__(GoogleFinanceCurrencyHook)
    fake_log = SimpleNamespace(warning=lambda *a, **k: None, info=lambda *a, **k: None)
    monkeypatch.setattr(GoogleFinanceCurrencyHook, "log", fake_log, raising=False)
    return hook


class FakeValuesResource:
    def __init__(self):
        self.update_calls: list[dict] = []

    def update(self, spreadsheetId, range, valueInputOption, body):
        self.update_calls.append(
            {
                "spreadsheetId": spreadsheetId,
                "range": range,
                "valueInputOption": valueInputOption,
                "body": body,
            }
        )
        return SimpleNamespace(execute=lambda: {"updatedCells": 0})


class FakeSpreadsheetsResource:
    def __init__(self, values_resource):
        self._values_resource = values_resource

    def values(self):
        return self._values_resource


class FakeSheetsService:
    def __init__(self):
        self.values_resource = FakeValuesResource()

    def spreadsheets(self):
        return FakeSpreadsheetsResource(self.values_resource)


class TestWriteCurrencyFormulas:
    def test_default_template_builds_currency_then_base_ticker_and_formula(
        self, monkeypatch
    ):
        hook = _make_hook(monkeypatch)
        service = FakeSheetsService()
        hook.get_conn = lambda: service

        count = hook.write_currency_formulas(
            spreadsheet_id="SSID",
            sheet_name="Sheet1",
            currencies=["eur", "pln"],
            base_currency="usd",
        )

        assert count == 2
        [call] = service.values_resource.update_calls
        assert call["spreadsheetId"] == "SSID"
        assert call["range"] == "Sheet1!A1"
        assert call["valueInputOption"] == "USER_ENTERED"
        values = call["body"]["values"]
        assert values[0] == HEADER_ROW
        assert values[1][0] == "EUR"
        assert values[1][1] == "CURRENCY:EURUSD"
        assert values[1][2] == '=IF(A2="USD",1,GOOGLEFINANCE(B2))'
        assert values[2][0] == "PLN"
        assert values[2][1] == "CURRENCY:PLNUSD"
        assert values[2][2] == '=IF(A3="USD",1,GOOGLEFINANCE(B3))'

    def test_reverse_ticker_template_puts_base_before_currency(self, monkeypatch):
        hook = _make_hook(monkeypatch)
        service = FakeSheetsService()
        hook.get_conn = lambda: service

        hook.write_currency_formulas(
            spreadsheet_id="SSID",
            sheet_name="Sheet1",
            currencies=["eur"],
            base_currency="usd",
            ticker_template="{base}{currency}",
        )

        values = service.values_resource.update_calls[0]["body"]["values"]
        assert values[1][1] == "CURRENCY:USDEUR"

    def test_currency_and_base_currency_are_uppercased_regardless_of_input_case(
        self, monkeypatch
    ):
        hook = _make_hook(monkeypatch)
        service = FakeSheetsService()
        hook.get_conn = lambda: service

        hook.write_currency_formulas(
            spreadsheet_id="SSID",
            sheet_name="Sheet1",
            currencies=["EuR", "pLn"],
            base_currency="uSd",
        )

        values = service.values_resource.update_calls[0]["body"]["values"]
        assert [row[0] for row in values[1:]] == ["EUR", "PLN"]
        assert all("USD" in row[1] for row in values[1:])

    def test_empty_currency_list_writes_header_only_and_returns_zero(self, monkeypatch):
        hook = _make_hook(monkeypatch)
        service = FakeSheetsService()
        hook.get_conn = lambda: service

        count = hook.write_currency_formulas(
            spreadsheet_id="SSID", sheet_name="Sheet1", currencies=[]
        )

        assert count == 0
        values = service.values_resource.update_calls[0]["body"]["values"]
        assert values == [HEADER_ROW]

    def test_row_numbers_advance_correctly_for_each_currency(self, monkeypatch):
        hook = _make_hook(monkeypatch)
        service = FakeSheetsService()
        hook.get_conn = lambda: service

        hook.write_currency_formulas(
            spreadsheet_id="SSID", sheet_name="Sheet1", currencies=["eur", "pln", "gbp"]
        )

        values = service.values_resource.update_calls[0]["body"]["values"]
        # header is row 1, so data rows are 2, 3, 4
        assert values[1][2] == '=IF(A2="USD",1,GOOGLEFINANCE(B2))'
        assert values[2][2] == '=IF(A3="USD",1,GOOGLEFINANCE(B3))'
        assert values[3][2] == '=IF(A4="USD",1,GOOGLEFINANCE(B4))'


class _Cycle:
    """Returns each value in order, then keeps returning the last one forever.

    Using a plain ``iter(...)`` here would raise StopIteration on any call beyond
    what a test anticipates (e.g. an extra timing check caused by a refactor),
    turning a behavior change into a confusing test-harness error instead of a
    clear assertion failure.
    """

    def __init__(self, values):
        self._values = list(values)
        self._i = 0

    def __call__(self, *args, **kwargs):
        value = self._values[min(self._i, len(self._values) - 1)]
        self._i += 1
        return value


class TestWaitForFreshValues:
    def _patch_time(self, monkeypatch, monotonic_values):
        monkeypatch.setattr(gfh.time, "monotonic", _Cycle(monotonic_values))
        sleeps = []
        monkeypatch.setattr(gfh.time, "sleep", lambda s: sleeps.append(s))
        return sleeps

    def test_returns_immediately_when_all_rows_present_and_no_pending_errors(
        self, monkeypatch
    ):
        hook = _make_hook(monkeypatch)
        sleeps = self._patch_time(monkeypatch, [0])
        hook.get_values = lambda **kwargs: [[1.1], [2.2]]

        hook.wait_for_fresh_values(
            "SSID", "Sheet1", num_rows=2, timeout_seconds=60, poll_seconds=5
        )

        assert sleeps == []  # no polling needed

    def test_polls_and_succeeds_once_the_pending_error_clears(self, monkeypatch):
        hook = _make_hook(monkeypatch)
        sleeps = self._patch_time(monkeypatch, [0, 1, 2])
        hook.get_values = _Cycle([[["#N/A"], [2.2]], [[1.1], [2.2]]])

        hook.wait_for_fresh_values(
            "SSID", "Sheet1", num_rows=2, timeout_seconds=60, poll_seconds=5
        )

        assert sleeps == [5]  # slept exactly once, between the two polls

    def test_polls_while_row_count_is_still_short_of_num_rows(self, monkeypatch):
        hook = _make_hook(monkeypatch)
        sleeps = self._patch_time(monkeypatch, [0, 1])
        hook.get_values = _Cycle([[[1.1]], [[1.1], [2.2]]])

        hook.wait_for_fresh_values(
            "SSID", "Sheet1", num_rows=2, timeout_seconds=60, poll_seconds=5
        )

        assert sleeps == [5]

    def test_times_out_and_returns_without_raising_when_errors_persist(
        self, monkeypatch
    ):
        hook = _make_hook(monkeypatch)
        warnings = []
        monkeypatch.setattr(
            GoogleFinanceCurrencyHook,
            "log",
            SimpleNamespace(
                warning=lambda *a, **k: warnings.append((a, k)),
                info=lambda *a, **k: None,
            ),
            raising=False,
        )
        # First check: before deadline (0 < 10). Second check: at/after deadline (10 >= 10).
        self._patch_time(monkeypatch, [0, 10])
        hook.get_values = lambda **kwargs: [["#N/A"], [2.2]]

        # Must not raise -- timeout is a logged warning, not an exception.
        hook.wait_for_fresh_values(
            "SSID", "Sheet1", num_rows=2, timeout_seconds=10, poll_seconds=5
        )

        assert len(warnings) == 1

    def test_uses_unformatted_value_render_option_and_column_c_range(self, monkeypatch):
        hook = _make_hook(monkeypatch)
        self._patch_time(monkeypatch, [0])
        captured = {}

        def fake_get_values(**kwargs):
            captured.update(kwargs)
            return [[1.1], [2.2], [3.3]]

        hook.get_values = fake_get_values

        hook.wait_for_fresh_values("SSID", "Sheet1", num_rows=3)

        assert captured["spreadsheet_id"] == "SSID"
        assert captured["range_"] == "Sheet1!C2:C4"
        assert captured["value_render_option"] == "UNFORMATTED_VALUE"

    def test_blank_not_yet_computed_cell_is_not_recognized_as_pending(
        self, monkeypatch
    ):
        """Documents a suspected real gap (google_finance_hook.py:102-106): only
        strings starting with "#" are treated as "still resolving". A cell that
        Sheets returns as an empty string "" (present, but not yet recalculated,
        and not an error) sails through as if it were a valid finished value --
        the function returns immediately instead of continuing to poll. This test
        pins down the CURRENT behavior; it is not asserting that behavior is
        correct.
        """
        hook = _make_hook(monkeypatch)
        sleeps = self._patch_time(monkeypatch, [0])
        hook.get_values = lambda **kwargs: [[""], [2.2]]

        # No exception, no extra poll -- the blank cell is silently accepted as "done".
        hook.wait_for_fresh_values(
            "SSID", "Sheet1", num_rows=2, timeout_seconds=60, poll_seconds=5
        )

        assert sleeps == []


class TestReadCurrencyRates:
    def test_skips_incomplete_rows(self, monkeypatch):
        hook = _make_hook(monkeypatch)
        hook.get_values = lambda **kwargs: [
            ["EUR", "CURRENCY:EURUSD"],
            ["PLN", "CURRENCY:PLNUSD", 4.5],
        ]

        results = hook.read_currency_rates("SSID", "Sheet1", num_rows=2)

        assert results == [{"currency": "PLN", "value": 4.5}]

    def test_skips_non_numeric_string_rate_such_as_a_sheets_error(self, monkeypatch):
        hook = _make_hook(monkeypatch)
        hook.get_values = lambda **kwargs: [
            ["EUR", "CURRENCY:EURUSD", "#N/A"],
            ["PLN", "CURRENCY:PLNUSD", 4.5],
        ]

        results = hook.read_currency_rates("SSID", "Sheet1", num_rows=2)

        assert results == [{"currency": "PLN", "value": 4.5}]

    def test_uses_the_given_value_field_name_in_the_output_dict(self, monkeypatch):
        hook = _make_hook(monkeypatch)
        hook.get_values = lambda **kwargs: [["EUR", "CURRENCY:EURUSD", 1.08]]

        results = hook.read_currency_rates(
            "SSID", "Sheet1", num_rows=1, value_field="close"
        )

        assert results == [{"currency": "EUR", "close": 1.08}]

    def test_integer_rate_is_coerced_to_float(self, monkeypatch):
        hook = _make_hook(monkeypatch)
        hook.get_values = lambda **kwargs: [["JPY", "CURRENCY:JPYUSD", 150]]

        results = hook.read_currency_rates("SSID", "Sheet1", num_rows=1)

        assert results == [{"currency": "JPY", "value": 150.0}]
        assert isinstance(results[0]["value"], float)

    def test_none_values_from_get_values_yields_empty_list(self, monkeypatch):
        hook = _make_hook(monkeypatch)
        hook.get_values = lambda **kwargs: None

        assert hook.read_currency_rates("SSID", "Sheet1", num_rows=3) == []

    def test_range_and_render_option_used_for_the_read(self, monkeypatch):
        hook = _make_hook(monkeypatch)
        captured = {}

        def fake_get_values(**kwargs):
            captured.update(kwargs)
            return [["EUR", "CURRENCY:EURUSD", 1.08]]

        hook.get_values = fake_get_values

        hook.read_currency_rates("SSID", "Sheet1", num_rows=1)

        assert captured["spreadsheet_id"] == "SSID"
        assert captured["range_"] == "Sheet1!A2:C2"
        assert captured["value_render_option"] == "UNFORMATTED_VALUE"

    def test_none_rate_raises_instead_of_being_skipped(self, monkeypatch):
        """Documents a suspected real gap (google_finance_hook.py:132-135): the
        "skip if non-numeric" guard only checks ``isinstance(rate, str)``. A rate
        that comes back as ``None`` (e.g. a genuinely empty/blank cell, as
        opposed to a "#"-prefixed error string) is not a str, so it falls through
        to ``float(None)`` and raises TypeError instead of being logged and
        skipped like every other "not ready yet" row. This test pins down the
        CURRENT behavior (an uncaught TypeError propagating out of the hook); it
        is not asserting that behavior is desirable.
        """
        hook = _make_hook(monkeypatch)
        hook.get_values = lambda **kwargs: [["EUR", "CURRENCY:EURUSD", None]]

        with pytest.raises(TypeError):
            hook.read_currency_rates("SSID", "Sheet1", num_rows=1)
