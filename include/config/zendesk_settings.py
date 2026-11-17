"""Configuration for the Zendesk Support ingestion pipeline."""

from datetime import datetime, timezone
from pathlib import Path

from airflow.models import Variable

GCP_CONN_ID = Variable.get("GCP_CONN_ID")
GCS_BUCKET = Variable.get("GCS_LANDING_BUCKET")
BQ_LOCATION = "US"
SOURCE = "zendesk"

ZENDESK_SUBDOMAIN = Variable.get("ZENDESK_API_SUBDOMAIN")
ZENDESK_EMAIL = Variable.get("ZENDESK_API_EMAIL")
ZENDESK_API_TOKEN = Variable.get("ZENDESK_API_TOKEN")

RATE_LIMIT_PER_SECOND = 10 / 60

CONNECT_TIMEOUT = 10
READ_TIMEOUT = 120
TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

GCS_UPLOAD_CHUNK = 8 * 1024 * 1024  # 8 MiB streamed upload (memory-bounded)

MAX_PAGES_PER_RUN = 5000
MAX_PAGES_PER_RUN_OVERRIDES = {"ticket_comments": 150}


def max_pages_for(endpoint: str) -> int:
    return MAX_PAGES_PER_RUN_OVERRIDES.get(endpoint, MAX_PAGES_PER_RUN)


ENDPOINTS = ["tickets", "users", "ticket_comments"]

SQL_DIR = Path(__file__).parent.parent / "sql" / "zendesk"

AVRO_SCHEMA = {
    "type": "record",
    "name": "ZendeskReleaseRecord",
    "fields": [
        {"name": "_elt_endpoint", "type": "string"},
        {"name": "_elt_zendesk_account_id", "type": "string"},
        {"name": "_elt_insert_timestamp", "type": "string"},
        {"name": "_elt_update_timestamp", "type": "string"},
        {"name": "_gcs_file_path", "type": "string"},
        {"name": "raw_data", "type": "string"},
    ],
}


def _cursor_variable_name(endpoint: str) -> str:
    return f"ZENDESK_{endpoint}_CURSOR".upper()


def get_saved_cursor(endpoint: str) -> str | None:
    return Variable.get(_cursor_variable_name(endpoint), default_var=None)


def save_cursor(endpoint: str, value: str | int | None) -> None:
    """`value` is a cursor string for tickets/users, but a raw JSON int
    (`end_time`) for ticket_comments -- Airflow Variables are strings
    regardless, so coerce here rather than relying on every hook to remember.
    """
    if value is None:
        return
    Variable.set(_cursor_variable_name(endpoint), str(value))


def parse_date_from(value: str | None) -> int | None:
    """Parse the DAG's `date_from` param into a Unix timestamp."""
    if not value:
        return None
    value = value.strip()
    if value.isdigit():
        return int(value)
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())
