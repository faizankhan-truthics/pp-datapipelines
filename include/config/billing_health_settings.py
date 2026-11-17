"""Configuration for the billing health alerts DAG (dags/billing_health/)."""

import os

from airflow.models import Variable

BQ_LOCATION = "US"
GCP_CONN_ID = Variable.get("GCP_CONN_ID")
BQ_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
BQ_BRONZE_DATASET = Variable.get("BQ_BRONZE_DATASET")
BQ_SILVER_DATASET = Variable.get("BQ_SILVER_DATASET")

ENV = "Development" if BQ_SILVER_DATASET.endswith("_sandbox") else "Production"

# Whether red alerts tag the payments team (seed_billing_health_products.payments_owner_slack_id).
# Every red tags only them, cancellations before trial end included; they tag the product owner
# by hand. Off, the red replies are still posted, without tagging anyone.
TAG_PAYMENTS_OWNERS = True

# Spreadsheet ID (the part between /d/ and /edit of its URL) the report is written to, one tab
# per section. Create it by hand and share it as Editor with the service account of GCP_CONN_ID;
# the DAG creates the tabs. Use a separate spreadsheet for sandbox and prod.
GSHEET_ID_VAR = "BILLING_HEALTH_GSHEET_ID"

# Slack channel ID every message goes to, as a Variable (e.g. env
# AIRFLOW_VAR_BILLING_HEALTH_SLACK_CHANNEL_ID). A plain env var of this name is the fallback.
SLACK_CHANNEL_VAR = "BILLING_HEALTH_SLACK_CHANNEL_ID"

# Last report date whose alerts were posted by a daily run (not a report_date re-run); the DAG
# keeps it. The next run alerts trial cohorts that became final after it, so a day whose run
# failed is caught up. Missing (first run), the run alerts only its own report date.
ALERTED_THROUGH_VAR = "BILLING_HEALTH_ALERTED_THROUGH"

# Yesterday must be loaded through this hour (UTC) before the report runs.
FRESH_THROUGH_HOUR_UTC = 23

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
SQL_DIR = os.path.join(AIRFLOW_HOME, "include", "sql", "billing_health")
