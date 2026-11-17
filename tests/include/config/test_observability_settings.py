"""Unit tests for include/config/observability_settings.py.

DBT_MARTS_READY_URI's producer/consumer wiring is covered in tests/dags/test_dbt_dag.py.
This pins the dbt/edr binary paths to the virtualenv the Dockerfile builds
(/usr/local/airflow/dbt_venv) -- dbt_observability and dbt_manual_run shell out to them.
"""

from __future__ import annotations

import importlib

import pytest

import include.config.observability_settings as settings


@pytest.fixture
def reload_with_airflow_home(monkeypatch):
    def _reload(value):
        if value is None:
            monkeypatch.delenv("AIRFLOW_HOME", raising=False)
        else:
            monkeypatch.setenv("AIRFLOW_HOME", value)
        return importlib.reload(settings)

    yield _reload
    monkeypatch.undo()
    importlib.reload(settings)


def test_binaries_default_to_the_dockerfile_dbt_venv(reload_with_airflow_home):
    reloaded = reload_with_airflow_home(None)

    assert reloaded.DBT_VENV_BIN == "/usr/local/airflow/dbt_venv/bin"
    assert reloaded.DBT_BIN == "/usr/local/airflow/dbt_venv/bin/dbt"
    assert reloaded.EDR_BIN == "/usr/local/airflow/dbt_venv/bin/edr"


def test_airflow_home_is_respected(reload_with_airflow_home):
    assert reload_with_airflow_home("/opt/af").DBT_BIN == "/opt/af/dbt_venv/bin/dbt"


def test_published_object_names_are_html():
    assert settings.DOCS_OBJECT.endswith(".html")
    assert settings.REPORT_OBJECT.endswith(".html")
