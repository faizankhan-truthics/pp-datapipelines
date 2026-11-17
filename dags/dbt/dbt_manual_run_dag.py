"""Ad-hoc dbt runs — schedule=None, trigger manually from the Airflow UI.

Cosmos builds `dbt_dag`'s task graph at parse time, before any Airflow param exists, so
it can't take a "which model" param at trigger time. This DAG covers "rebuild just this
model/folder" (debugging, backfilling one mart, etc.) as a single plain `dbt` subprocess
call, same pattern `observability_dag.py` already uses.

Runs as one task, so there's no risk of racing `dbt_dag` on shared tables as long as it's
triggered while `dbt_dag` is idle.
"""

from __future__ import annotations

import os
import subprocess
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

from include.config.observability_settings import DBT_BIN, DBT_VENV_BIN, GCP_CONN_ID

DBT_PROJECT_DIR = str(Path(__file__).resolve().parent / "bigquery_dbt")


@contextmanager
def gcp_cli_env():
    """Yield env with GCP creds materialised from the connection (ADC)."""
    hook = GoogleBaseHook(gcp_conn_id=GCP_CONN_ID)
    with hook.provide_gcp_credential_file_as_context() as _cred_path:
        env = {
            **os.environ,
            "DBT_GCP_PROJECT": Variable.get("GCP_PROJECT_ID"),
            "DBT_BQ_DATASET": "dbt_analytics",  # matches dbt_dag's Cosmos ProfileConfig
            "DBT_BQ_LOCATION": Variable.get("BQ_LOCATION", default_var="US"),
            "PATH": DBT_VENV_BIN + os.pathsep + os.environ.get("PATH", ""),
        }
        yield env


def _dbt(*args):
    return [
        DBT_BIN,
        *args,
        "--project-dir",
        DBT_PROJECT_DIR,
        "--profiles-dir",
        DBT_PROJECT_DIR,
    ]


@dag(
    dag_id="dbt_manual_run",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={"retries": 2, "pool": "standard_mem_pool"},
    tags=["dbt", "bigquery", "manual"],
    params={
        "select": Param(
            "",
            type="string",
            title="dbt selector",
            description="Required. e.g. int_stripe_charges, path:models/silver/stripe, or +stripe_mrr_usd_daily",
        ),
        "full_refresh": Param(
            False,
            type="boolean",
            title="Full Refresh",
            description="Enable to run dbt with --full-refresh.",
        ),
    },
)
def dbt_manual_run():
    @task
    def run_selected(params: dict):
        select = params["select"]
        if not select:
            raise ValueError(
                "select param is required — refusing to run the whole project"
            )

        args = ["build", "-s", select]
        if params["full_refresh"]:
            args.append("--full-refresh")

        with gcp_cli_env() as env:
            subprocess.run(_dbt("deps"), env=env, check=True)
            subprocess.run(_dbt(*args), env=env, check=True)

    run_selected()


dbt_manual_run()
