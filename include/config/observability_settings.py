"""Config for the dbt_observability DAG (standalone dbt docs + Elementary observability)."""

import os

# GCP connection (same one the ingestion + dbt_dag use). Constant here, like stripe_settings.
GCP_CONN_ID = "gcp_connection_data_project"

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
DBT_VENV_BIN = os.path.join(AIRFLOW_HOME, "dbt_venv", "bin")
DBT_BIN = os.path.join(DBT_VENV_BIN, "dbt")
EDR_BIN = os.path.join(DBT_VENV_BIN, "edr")


# Descriptive GCS object names.
DOCS_OBJECT = "dbt_docs.html"
REPORT_OBJECT = "elementary_report.html"

# Asset emitted by dbt_dag's dbt_done task (dbt_dag imports this constant);
# dbt_observability and daily_vamp_status schedule on it.
DBT_MARTS_READY_URI = "dbt://bigquery_dbt/marts"
