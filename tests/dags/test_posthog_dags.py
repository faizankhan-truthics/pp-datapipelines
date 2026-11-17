"""DAG-level tests for dags/posthog/data_checks_dag.py (posthog_data_checks).

The checks must stay silent outside Production unless asked, or every dev run posts to
the shared Slack channel.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest
from airflow.models import DagBag
from airflow.sdk.exceptions import AirflowSkipException

POSTHOG_DAG_DIR = Path(__file__).parents[2] / "dags" / "posthog"


@pytest.fixture(scope="module")
def posthog_dags():
    logger = logging.getLogger("airflow")
    old_value, logger.disabled = logger.disabled, True
    try:
        dag_bag = DagBag(dag_folder=str(POSTHOG_DAG_DIR), include_examples=False)
    finally:
        logger.disabled = old_value

    assert not dag_bag.import_errors, dag_bag.import_errors
    return dag_bag.dags


def test_one_check_task(posthog_dags):
    assert {t.task_id for t in posthog_dags["posthog_data_checks"].tasks} == {
        "check_export"
    }


def test_skips_outside_production_unless_asked(posthog_dags, monkeypatch):
    # DagBag imports DAG files under a generated module name; find it by file.
    dag_module = next(
        m
        for m in list(sys.modules.values())
        if getattr(m, "__file__", None)
        and m.__file__.endswith("posthog/data_checks_dag.py")
    )
    monkeypatch.setattr(dag_module, "ENV", "Development")
    with pytest.raises(AirflowSkipException):
        dag_module._alerting_enabled({"send_in_dev": False})
    dag_module._alerting_enabled({"send_in_dev": True})

    monkeypatch.setattr(dag_module, "ENV", "Production")
    dag_module._alerting_enabled({"send_in_dev": False})
