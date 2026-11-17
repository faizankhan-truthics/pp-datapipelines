"""Unit tests for include/config/monitoring_settings.py.

API_REQUEST_LOG_TABLE is where include/monitoring/api_request_monitor.py creates and
streams its per-request log rows; it must be a fully-qualified project.dataset.table
built from the deployment's Variables.
"""

from __future__ import annotations

from airflow.models import Variable

from include.config import monitoring_settings as settings


def test_api_request_log_table_is_fully_qualified_from_variables():
    expected = f"{Variable.get('GCP_PROJECT_ID')}.{Variable.get('BQ_MONITORING_DATASET')}.api_requests"

    assert settings.API_REQUEST_LOG_TABLE == expected
    assert len(settings.API_REQUEST_LOG_TABLE.split(".")) == 3
