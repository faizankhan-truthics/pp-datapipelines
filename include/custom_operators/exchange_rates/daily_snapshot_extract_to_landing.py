"""
FxBronzeDailySnapshotExtractToLandingOperator

Pulls today's real-time GOOGLEFINANCE rate for a set of currencies
 and lands it in the same (date, currency, close, source, source_file) shape .

Reuses `GoogleFinanceCurrencyHook` from google_finance_fx.hooks (the same
hook backing google_finance_fx_rates_to_usd and fx_history_daily_close) --
same caveat applies: only today's live value is readable via the Sheets
API, so this operator (like the other two GOOGLEFINANCE-based DAGs) can
only ever add *today's* row, never backfill.
"""

from __future__ import annotations

import json

from airflow.models import BaseOperator, Variable
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

from include.custom_hooks.exchange_rates.google_finance_hook import (
    GoogleFinanceCurrencyHook,
)


def get_daily_currencies(**context) -> list[str]:
    """get a list of all the currencies from a table with different currencies.
    using the charges table from stripe for now"""
    QUERY = """
    SELECT DISTINCT currency
    FROM `{project}.{dataset}.{table}`
    WHERE upper(currency)!='USD'
    order by 1
    """

    project = Variable.get("GCP_PROJECT_ID")
    dataset = Variable.get("BQ_BRONZE_DATASET")
    datatable_with_currency = "stripe_charges"
    location = Variable.get("BQ_LOCATION")
    conn_id = Variable.get("GCP_CONN_ID")

    hook = BigQueryHook(gcp_conn_id=conn_id, location=location)
    client = hook.get_client(project_id=project)

    query = QUERY.format(
        project=project, dataset=dataset, table=datatable_with_currency
    )
    rows = client.query(query).result()
    currencies = [row.currency for row in rows]

    return currencies


class FxBronzeDailySnapshotExtractToLandingOperator(BaseOperator):
    """Land today's currency->USD GOOGLEFINANCE snapshot to GCS.

    :param gcs_bucket: destination GCS bucket
    :param landing_prefix: GCS prefix under which each run gets its own
        ``<run_id>/`` subfolder
    :param spreadsheet_id: id of the Google Sheet used as the GOOGLEFINANCE
        scratchpad (must be shared with the service account behind
        ``gcp_conn_id``); use a tab not shared with the other two
        GOOGLEFINANCE DAGs
    :param sheet_name: tab name within that spreadsheet
    :param base_currency: the "USD" side of currency->USD, e.g. ``"usd"``
    :param currencies: target currency codes, e.g. ``["eur", "pln"]``
    :param recalc_timeout_seconds: max time to wait for Sheets to recalculate
    :param recalc_poll_seconds: polling interval while waiting
    :param gcp_conn_id: Airflow connection id (used for both Sheets and GCS)
    """

    template_fields = ("gcs_bucket", "landing_prefix", "spreadsheet_id")

    def __init__(
        self,
        *,
        gcs_bucket: str,
        landing_prefix: str,
        spreadsheet_id: str,
        sheet_name: str,
        base_currency: str,
        recalc_timeout_seconds: int = 60,
        recalc_poll_seconds: int = 5,
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.gcs_bucket = gcs_bucket
        self.landing_prefix = landing_prefix
        self.spreadsheet_id = spreadsheet_id
        self.sheet_name = sheet_name
        self.base_currency = base_currency
        self.currencies: list[str] | None = None
        self.recalc_timeout_seconds = recalc_timeout_seconds
        self.recalc_poll_seconds = recalc_poll_seconds
        self.gcp_conn_id = gcp_conn_id

    def execute(self, context) -> str:
        sheet_hook = GoogleFinanceCurrencyHook(gcp_conn_id=self.gcp_conn_id)
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id)

        currencies = self.currencies or get_daily_currencies()

        num_rows = sheet_hook.write_currency_formulas(
            spreadsheet_id=self.spreadsheet_id,
            sheet_name=self.sheet_name,
            currencies=currencies,
            base_currency=self.base_currency,
            ticker_template="{currency}{base}",  # e.g. "EURUSD": price of 1 EUR in USD
        )
        sheet_hook.wait_for_fresh_values(
            self.spreadsheet_id,
            self.sheet_name,
            num_rows,
            timeout_seconds=self.recalc_timeout_seconds,
            poll_seconds=self.recalc_poll_seconds,
        )
        rows = sheet_hook.read_currency_rates(
            self.spreadsheet_id, self.sheet_name, num_rows, value_field="close"
        )
        if not rows:
            raise RuntimeError(
                "GOOGLEFINANCE returned zero usable rates -- check that the "
                "spreadsheet is shared with the service account and that "
                "the Sheets API is enabled."
            )

        run_date = context["ds"]
        for r in rows:
            r["date"] = run_date
            r["source"] = "google_finance"

        run_id = context["run_id"].replace(":", "_").replace("+", "_")
        landing_run_prefix = f"{self.landing_prefix}/{run_id}"

        payload = {"data": rows, "meta": {"total-pages": 1}}
        gcs_hook.upload(
            bucket_name=self.gcs_bucket,
            object_name=f"{landing_run_prefix}/page_0001.json",
            data=json.dumps(payload),
            mime_type="application/json",
        )
        gcs_hook.upload(
            bucket_name=self.gcs_bucket,
            object_name=f"{landing_run_prefix}/_SUCCESS",
            data=json.dumps({"pages": 1, "rows": len(rows)}),
            mime_type="application/json",
        )
        self.log.info(
            "Landed %d currency rows for %s to gs://%s/%s",
            len(rows),
            run_date,
            self.gcs_bucket,
            landing_run_prefix,
        )
        return landing_run_prefix
