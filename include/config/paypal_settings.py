"""Configuration for the PayPal sandbox transactions ingestion pipeline."""

from pathlib import Path

from airflow.models import Variable

BASE_URL = "https://api-m.sandbox.paypal.com"
CLIENT_ID_ENV = "PAYPAL_SANDBOX_CLIENT_ID"
CLIENT_SECRET_ENV = "PAYPAL_SANDBOX_CLIENT_SECRET"
ACCOUNT_NAME = "sandbox"
ENDPOINT = "transactions"
PAGE_SIZE = 500
MAX_WINDOW_DAYS = 30
RATE_LIMIT_PER_SECOND = 5
GCP_CONN_ID = Variable.get("GCP_CONN_ID")
SQL_DIR = Path(__file__).parent.parent / "sql" / "paypal"

AVRO_SCHEMA = {
    "type": "record",
    "name": "PayPalReleaseRecord",
    "fields": [
        {"name": "_elt_account_name", "type": "string"},
        {"name": "_elt_endpoint", "type": "string"},
        {"name": "_elt_insert_timestamp", "type": "string"},
        {"name": "_elt_update_timestamp", "type": "string"},
        {"name": "_gcs_file_path", "type": "string"},
        {"name": "raw_data", "type": "string"},
    ],
}
