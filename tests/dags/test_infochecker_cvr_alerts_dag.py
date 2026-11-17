"""Tests for dags/amplitude/infochecker_cvr_alerts_dag.py (dag_id `infochecker_cvr_alerts`).

The DAG is a thin wrapper around include.utils.sql_slack_alert.run_sql_alert, so what
can break here is the wiring: the SQL file it points at, the template params that SQL
expects, and the schedule (it must run after Amplitude's previous UTC day is complete).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

import dags.amplitude.infochecker_cvr_alerts_dag as cvr

REPO_SQL = (
    Path(__file__).parents[2] / "include" / "sql" / "amplitude" / cvr.SQL_FILE.name
)


@pytest.fixture(scope="module")
def dag():
    return cvr.infochecker_cvr_alerts()


def test_dag_definition(dag):
    assert dag.dag_id == "infochecker_cvr_alerts"
    assert list(dag.task_dict) == ["check_and_alert"]
    assert dag.max_active_runs == 1
    assert dag.schedule == "0 4 * * *"


def test_sql_file_exists_in_the_repo():
    assert cvr.SQL_FILE.name == "infochecker_cvr_alerts_live.sql"
    assert REPO_SQL.is_file()


def test_template_params_cover_every_placeholder_the_sql_uses():
    used = set(
        re.findall(r"\{\{\s*params\.(\w+)\s*\}\}", REPO_SQL.read_text(encoding="utf-8"))
    )

    assert used, "expected the SQL to use {{ params.* }} placeholders"
    assert used <= {"bq_project", "amplitude_stage_dataset"}


def test_check_and_alert_calls_run_sql_alert_with_settings(dag, monkeypatch):
    calls = []
    monkeypatch.setattr(cvr, "run_sql_alert", lambda **kwargs: calls.append(kwargs))

    dag.get_task("check_and_alert").python_callable()

    assert calls == [
        {
            "sql_file": cvr.SQL_FILE,
            "webhook_url": cvr.INFOCHECKER_CVR_ALERTS_WEBHOOK,
            "gcp_conn_id": cvr.GCP_CONN_ID,
            "bq_location": cvr.BQ_LOCATION,
            "template_params": {
                "bq_project": cvr.BQ_PROJECT_ID,
                "amplitude_stage_dataset": cvr.AMPLITUDE_STAGE_DATASET,
            },
        }
    ]
