"""
GoogleFinanceCurrencyHook

GOOGLEFINANCE has no public REST API -- it only evaluates inside a live
Google Sheet. Its *real-time* "price" attribute reads back fine via the
Sheets API (that's what this hook relies on); its *historical* attribute
explicitly does not -- Google's own docs state historical GOOGLEFINANCE
output "cannot be downloaded or accessed via the Sheets API or Apps
Script" (any attempt returns #N/A), with no documented workaround. That
means the only Google-only way to build up a time series is to run a
real-time snapshot daily and let it accumulate into history over time --
this hook is the piece of infrastructure that makes today's live rate
reachable from Airflow: it (1) (re)writes one GOOGLEFINANCE formula per
currency into a spreadsheet you control, (2) polls until Sheets has
recalculated them, and (3) reads back the computed values.

Reused by two pipelines with different ticker directions:
  - google_finance_fx:  ticker "{currency}{base}"  -> price of 1 <currency> in <base>  (to_usd_conversion)
  - fx_history:          ticker "{base}{currency}"  -> price of 1 <base> in <currency>  (usd_to_currency "close")

Built on top of Airflow's standard GSheetsHook (Sheets API v4 wrapper from
apache-airflow-providers-google) for auth/transport, the same way the
Treasury pipeline builds TreasuryFiscalDataHook on top of `requests`.
"""

from __future__ import annotations

import time
from datetime import date

from airflow.providers.google.suite.hooks.sheets import GSheetsHook

HEADER_ROW = ["currency", "ticker", "value", "sheet_date"]


class GoogleFinanceCurrencyHook(GSheetsHook):
    """Hook for reading daily currency rates via GOOGLEFINANCE("...", "price").

    :param gcp_conn_id: Airflow connection id whose service account has
        Editor access to the target spreadsheet (share the sheet with
        that service account's email) and the Sheets API scope enabled.
    """

    def write_currency_formulas(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        currencies: list[str],
        base_currency: str = "USD",
        ticker_template: str = "{currency}{base}",
    ) -> int:
        """(Re)write the header + one row per currency with a formula like:

            =IF(A2="USD", 1, GOOGLEFINANCE("CURRENCY:"&B2))

        where the ticker in column B is built from `ticker_template`
        (default: currency-then-base, e.g. "EURUSD" = price of 1 EUR in
        USD; pass ticker_template="{base}{currency}" for the reverse, e.g.
        "USDEUR" = price of 1 USD in EUR).

        Writing (rather than assuming the sheet is already populated)
        makes each run force a fresh recalculation, and keeps the sheet
        as a disposable "scratchpad" driven entirely by this hook rather
        than something someone has to remember to set up correctly by
        hand. Returns the number of data rows written (excluding header).
        """
        base_currency = base_currency.upper()
        values = [HEADER_ROW]
        today = date.today().isoformat()
        for i, currency in enumerate(currencies):
            row_num = i + 2  # 1-indexed, row 1 is the header
            currency = currency.upper()
            ticker = "CURRENCY:" + ticker_template.format(
                currency=currency, base=base_currency
            )
            formula = f'=IF(A{row_num}="{base_currency}",1,GOOGLEFINANCE(B{row_num}))'
            values.append([currency, ticker, formula, today])

        service = self.get_conn()
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="USER_ENTERED",
            body={"values": values},
        ).execute()
        return len(currencies)

    def wait_for_fresh_values(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        num_rows: int,
        timeout_seconds: int = 60,
        poll_seconds: int = 5,
    ) -> None:
        """Poll column C until no cell still shows an error (e.g. `#N/A`
        while GOOGLEFINANCE is still resolving) or the timeout is hit.
        """
        deadline = time.monotonic() + timeout_seconds
        rng = f"{sheet_name}!C2:C{num_rows + 1}"
        while True:
            values = (
                self.get_values(
                    spreadsheet_id=spreadsheet_id,
                    range_=rng,
                    value_render_option="UNFORMATTED_VALUE",
                )
                or []
            )
            flat = [row[0] if row else None for row in values]
            has_pending_error = any(
                isinstance(v, str) and v.startswith("#") for v in flat
            )
            has_all_rows = len(flat) >= num_rows
            if has_all_rows and not has_pending_error:
                return
            if time.monotonic() >= deadline:
                self.log.warning(
                    "Timed out after %ss waiting for GOOGLEFINANCE to recalculate; "
                    "proceeding with whatever values are currently present "
                    "(errors, if any, will surface as skipped rows downstream).",
                    timeout_seconds,
                )
                return
            time.sleep(poll_seconds)

    def read_currency_rates(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        num_rows: int,
        value_field: str = "value",
    ) -> list[dict]:
        """Read back today's computed (currency, <value_field>) pairs."""
        rng = f"{sheet_name}!A2:C{num_rows + 1}"
        rows = (
            self.get_values(
                spreadsheet_id=spreadsheet_id,
                range_=rng,
                value_render_option="UNFORMATTED_VALUE",
            )
            or []
        )

        results = []
        for row in rows:
            if len(row) < 3:
                self.log.warning("Skipping incomplete row: %r", row)
                continue
            currency, _ticker, rate = row[0], row[1], row[2]
            if isinstance(rate, str):
                self.log.warning(
                    "Skipping %s: non-numeric value %r (likely still resolving)",
                    currency,
                    rate,
                )
                continue
            results.append({"currency": currency, value_field: float(rate)})
        return results
