"""
Configuration for the fx_bronze pipelines.

Two DAGs feed one shared BigQuery "bronze" table
(`{BQ_DATASET}.{BQ_TABLE}`, natural key `(currency, date)`):

  1. fx_bronze_excel_backfill  -- ingests a user-provided Excel workbook
     (one tab per currency, already sitting in GCS) exactly once per file
     version; never reprocesses a file it has already landed.
  2. fx_bronze_daily_snapshot  -- pulls today's EUR->USD and PLN->USD rate
     from GOOGLEFINANCE and merges it in alongside the historical data.

Both write rows shaped as (date, currency, close, source, source_file),
so the same MERGE-based `common.operators.ReleaseToBigQueryOperator`
loads either one into the same table without needing to know which
pipeline produced a given row.
"""

from __future__ import annotations

from airflow.models import Variable
from google.cloud import bigquery


def _var(name: str, default: str) -> str:
    return Variable.get(name, default_var=default)


GCP_CONN_ID = _var("GCP_CONN_ID", default="")
# --- Shared: GCS bucket & BigQuery bronze table -----------------------------
GCS_BUCKET = _var("GCS_LANDING_BUCKET", "")

BQ_PROJECT = _var("GCP_PROJECT_ID", "")
BQ_STAGING_DATASET = _var("BQ_STAGING_DATASET", "")
BQ_BRONZE_DATASET = _var("BQ_BRONZE_DATASET", "")
BQ_LOCATION = _var("BQ_LOCATION", "")
BQ_PARTITION_FIELD = "date"
BQ_PARTITION_TYPE = "DAY"

# Natural key: one close price per currency per calendar date, regardless
# of which of the two pipelines produced it.
NATURAL_KEY_FIELDS = ["currency", "date"]

BQ_SCHEMA = [
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("currency", "STRING", mode="REQUIRED"),
    bigquery.SchemaField(
        "close",
        "FLOAT64",
        mode="REQUIRED",
        description="Price of 1 unit of `currency` in USD",
    ),
    bigquery.SchemaField("source", "STRING", description="'google_finance'"),
]
# --- Daily Google Finance snapshot pipeline ---------------------------------
SPREADSHEET_ID = _var("FX_CURRENCY_GSHEET_ID", "")
SHEET_NAME = _var("FX_CURRENCY_GSHEET_SHEET_NAME", "")

RECALC_TIMEOUT_SECONDS = 60
RECALC_POLL_SECONDS = 5

GCS_DAILY_LANDING_PREFIX = "landing/fx_bronze/daily_snapshot"
GCS_DAILY_RELEASE_PREFIX = "release/fx_bronze/daily_snapshot"
GCS_DAILY_MANIFEST_OBJECT = f"{GCS_DAILY_RELEASE_PREFIX}/_manifest/latest_checksum.txt"
