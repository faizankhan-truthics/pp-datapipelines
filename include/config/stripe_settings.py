"""Configuration for the Stripe pipelines (dags/stripe/): account discovery, endpoints and rate limits."""

import logging
import os

from airflow.models import Variable

logger = logging.getLogger(__name__)

BQ_LOCATION = "US"
GCP_CONN_ID = Variable.get("GCP_CONN_ID")
VAMP_ALERTS_WEBHOOK = os.environ.get("VAMP_ALERTS__SLACK_WEBHOOK_URL")
VAMP_TABLE_NAME = (
    f"{os.environ.get('AIRFLOW_VAR_GCP_PROJECT_ID')}."
    f"{os.environ.get('AIRFLOW_VAR_BQ_GOLD_DATASET')}."
    "stripe_monthly_vamp_estimate"
)

ENV = (
    "Development"
    if os.environ.get("AIRFLOW_VAR_BQ_GOLD_DATASET") == "gold_sandbox"
    else "Production"
)

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
SQL_DIR = os.path.join(AIRFLOW_HOME, "include", "sql", "stripe")

RATE_LIMIT_PER_SECOND = 100


def discover_stripe_accounts() -> list:
    """Pairs every STRIPE_API_KEY_<SUFFIX> env var with STRIPE_ACCOUNT_ID_<SUFFIX>. Shared by
    the financial and events DAGs."""
    accounts = []
    excluded_keys = ["STRIPE_API_KEY_READ_ONLY"]
    for key, value in os.environ.items():
        if (
            key.startswith("STRIPE_API_KEY_")
            and value.strip()
            and key not in excluded_keys
        ):
            account_suffix = key.replace("STRIPE_API_KEY_", "")
            account_id = os.environ.get(
                f"STRIPE_ACCOUNT_ID_{account_suffix}", "unknown"
            )

            if account_id == "unknown":
                logger.warning(
                    "Missing Stripe account ID for API key variable: %s. Ensure you have "
                    "STRIPE_ACCOUNT_ID_insertaccount and STRIPE_API_KEY_insertaccount variable pairs",
                    account_suffix,
                )
            else:
                accounts.append(
                    {
                        "account_name": account_suffix.lower(),
                        "api_key_env": key,
                        "account_id": account_id,
                    }
                )

    return accounts


# Endpoints cleared for every discovered Stripe account.
LEGACY_ENDPOINTS = [
    "balance_transactions",
    "charges",
    "disputes",
    "payment_intents",
    "balance",
    "refunds",
    "radar_early_fraud_warnings",  # endpoint is radar/early_fraud_warnings
]

# Endpoints requiring extra API key scopes (customer_read, plan_read, subscription_read).
# New Stripe accounts are granted these scopes by default, so they run for every
# discovered account alongside LEGACY_ENDPOINTS.
NEW_ENDPOINTS = [
    "customers",
    "products",
    "prices",
    "subscriptions",
    "invoices",
]

# stripe_financial_pipeline pulls every endpoint: it inserts new objects (including ones Stripe
# never emits an event for) and only fills rows no event has written; stripe_events_pipeline
# then runs and updates their state in the same bronze tables.

# /v1/events runs in its own daily DAG (dags/stripe/stripe_events_pipeline.py), not with
# the endpoints above. Every event type is pulled (no `type` filter); dbt filters downstream.
# Unfiltered, one busy account ran past 1,500 pages a day, so extract_events_to_gcs
# (include/custom_operators/stripe/stripe_to_gcs_operator.py) fetches each day as 24 one-hour windows and GCS files land in per-hour folders. Stripe
# keeps events for 30 days only. Needs the "Events: read" scope.
EVENTS_ENDPOINT = "events"
# Hour windows fetched in parallel per account task (days themselves run one after another).
EVENTS_HOUR_WORKERS = 6

ENDPOINT_EXTRA_PARAMS = {
    # Stripe's default list only returns active subscriptions; a customer refunded
    # after canceling would otherwise never show up.
    "subscriptions": {"status": "all"},
}

# StripeToGCSOperator backfills run up to `max_workers` days concurrently, each thread
# buffering that day's data in memory. Endpoints with heavy per-day payloads (nested line
# items, etc.) can OOM the task pod at the operator's default of 10 concurrent days;
# override those here. Unlisted endpoints keep the operator default.
ENDPOINT_MAX_WORKERS_OVERRIDES = {
    "invoices": 10,
}

AVRO_SCHEMA = {
    "type": "record",
    "name": "StripeReleaseRecord",
    "fields": [
        {"name": "_elt_endpoint", "type": "string"},
        {"name": "_elt_stripe_account_id", "type": "string"},
        {"name": "_elt_insert_timestamp", "type": "string"},
        {"name": "_elt_update_timestamp", "type": "string"},
        {"name": "_gcs_file_path", "type": "string"},
        {"name": "raw_data", "type": "string"},
    ],
}
