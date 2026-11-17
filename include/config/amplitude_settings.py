"""Configuration for the Amplitude DAGs (dags/amplitude/)."""

import os

from airflow.models import Variable

BQ_LOCATION = "US"
GCP_CONN_ID = Variable.get("GCP_CONN_ID")
BQ_PROJECT_ID = Variable.get("GCP_PROJECT_ID")

# Amplitude's native BigQuery export lands here directly -- the table name
# (EVENTS_839561) is fixed by that sync; project and dataset vary by env.
# Rendered into the {{ params.bq_project }} / {{ params.amplitude_stage_dataset }}
# placeholders in infochecker_cvr_alerts_live.sql.
AMPLITUDE_STAGE_DATASET = Variable.get("AMPLITUDE_STAGE_DATASET")

INFOCHECKER_CVR_ALERTS_WEBHOOK = os.environ.get("CVR_ALERTS_SLACK_WEBHOOK_URL")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
SQL_DIR = os.path.join(AIRFLOW_HOME, "include", "sql", "amplitude")
