"""Unit tests for include/config/amplitude_settings.py.

SQL_DIR is resolved from AIRFLOW_HOME (not __file__), so it only lines up with the repo's
include/sql/amplitude/ when AIRFLOW_HOME is the project root -- as it is in the Astro
image. Pinned here so a move of either side is caught.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

import include.config.amplitude_settings as settings

REPO_ROOT = Path(__file__).parents[3]


@pytest.fixture
def reload_with_env(monkeypatch):
    def _reload(**env):
        for key, value in env.items():
            monkeypatch.setenv(key, value)
        return importlib.reload(settings)

    yield _reload
    monkeypatch.undo()
    importlib.reload(settings)


def test_sql_dir_resolves_to_the_repo_amplitude_sql_folder(reload_with_env):
    sql_dir = Path(reload_with_env(AIRFLOW_HOME=str(REPO_ROOT)).SQL_DIR)

    assert sql_dir == REPO_ROOT / "include" / "sql" / "amplitude"
    assert (sql_dir / "infochecker_cvr_alerts_live.sql").is_file()


def test_webhook_comes_from_env(reload_with_env):
    reloaded = reload_with_env(
        CVR_ALERTS_SLACK_WEBHOOK_URL="https://hooks.slack.test/cvr"
    )

    assert reloaded.INFOCHECKER_CVR_ALERTS_WEBHOOK == "https://hooks.slack.test/cvr"
