"""Configuration for the ChargebackHit pipeline (dags/chargebackhit/)."""

import os

GCP_CONN_ID = "gcp_connection_data_project"

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
SQL_DIR = os.path.join(AIRFLOW_HOME, "include", "sql", "chargebackhit")

ENDPOINTS = [
    "alerts",
    # "disputes",
]

# Requests/sec per task instance, shared by its backfill worker threads (see
# include/utils/rate_limiter.py). Same cap as Stripe and Solidgate; 429s are still
# retried with backoff.
RATE_LIMIT_PER_SECOND = 100

# Per-endpoint API contract, read by ChargebackhitHook.get_raw_pages. "disputes" is
# parked (commented out of ENDPOINTS) but kept here on purpose: re-enabling it is just
# uncommenting it above and adding merge_disputes.sql.
PAGINATION_STYLE = {
    "alerts": "offset",
    "disputes": "token",
}

# (from, to) request-filter field names for each endpoint's date window.
DATE_FILTER_FIELDS = {
    "alerts": ("created_at_from", "created_at_to"),
    "disputes": ("dispute_date_from", "dispute_date_to"),
}

AVRO_SCHEMA = {
    "type": "record",
    "name": "ChargebackHitRawPage",
    "fields": [
        {"name": "_elt_endpoint", "type": "string"},
        {"name": "_elt_chargebackhit_account_name", "type": "string"},
        {"name": "_elt_insert_timestamp", "type": "string"},
        {"name": "_elt_update_timestamp", "type": "string"},
        {"name": "_gcs_file_path", "type": "string"},
        {"name": "raw_data", "type": "string"},
    ],
}
