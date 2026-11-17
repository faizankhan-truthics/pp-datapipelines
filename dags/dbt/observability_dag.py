"""Standalone dbt docs + Elementary observability.

Runs after `dbt_dag` finishes (via the shared Asset). Everything happens in ONE task on purpose.

The task: deps -> build/test -> docs -> report -> upload to GCS -> Slack. docs/report/upload/Slack
are best-effort so a failed build still ships them; a real build failure fails the task at the end.

"""

from __future__ import annotations

import json
import os
import subprocess
import urllib.request
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

try:
    from airflow.sdk import Asset
except ImportError:  # pragma: no cover - older Airflow fallback
    from airflow.datasets import Dataset as Asset

from include.config.observability_settings import (
    DBT_BIN,
    DBT_MARTS_READY_URI,
    DBT_VENV_BIN,
    DOCS_OBJECT,
    EDR_BIN,
    GCP_CONN_ID,
    REPORT_OBJECT,
)

DBT_PROJECT_DIR = str(Path(__file__).resolve().parent / "bigquery_dbt")
TARGET_DIR = str(Path(DBT_PROJECT_DIR) / "target")
DOCS_HTML = str(Path(TARGET_DIR) / "static_index.html")
EDR_HTML = str(Path(TARGET_DIR) / "elementary" / "elementary_report.html")

DBT_MARTS_READY = Asset(DBT_MARTS_READY_URI)


@contextmanager
def gcp_cli_env():
    """Yield (env, cred_path) with GCP creds materialised from the connection (ADC)."""
    hook = GoogleBaseHook(gcp_conn_id=GCP_CONN_ID)
    with hook.provide_gcp_credential_file_as_context() as cred_path:
        env = {
            **os.environ,  # includes GOOGLE_APPLICATION_CREDENTIALS + the AIRFLOW_VAR_* schemas
            "DBT_GCP_PROJECT": Variable.get("GCP_PROJECT_ID"),
            "DBT_BQ_DATASET": "dbt_analytics",  # matches dbt_dag's Cosmos ProfileConfig
            "DBT_BQ_LOCATION": Variable.get("BQ_LOCATION", default_var="US"),
            "PATH": DBT_VENV_BIN + os.pathsep + os.environ.get("PATH", ""),
        }
        yield env, cred_path


def _dbt(*args):
    return [
        DBT_BIN,
        *args,
        "--project-dir",
        DBT_PROJECT_DIR,
        "--profiles-dir",
        DBT_PROJECT_DIR,
    ]


def _parse_counts(run_results_path):
    counts = {"passed": 0, "warnings": 0, "errors": 0, "skipped": 0}
    try:
        for r in json.loads(Path(run_results_path).read_text()).get("results", []):
            s = (r.get("status") or "").lower()
            if s in ("error", "fail", "runtime error"):
                counts["errors"] += 1
            elif s == "warn":
                counts["warnings"] += 1
            elif s == "skipped":
                counts["skipped"] += 1
            else:
                counts["passed"] += 1
    except (FileNotFoundError, ValueError):
        pass
    return counts


def _upload_to_gcs(bucket):
    hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
    for local_path, object_name in [
        (DOCS_HTML, DOCS_OBJECT),
        (EDR_HTML, REPORT_OBJECT),
    ]:
        if Path(local_path).exists():
            hook.upload(
                bucket_name=bucket,
                object_name=object_name,
                filename=local_path,
                mime_type="text/html",
                cache_control="no-cache",
            )
            print(f"Uploaded {local_path} -> gs://{bucket}/{object_name}")
        else:
            print(f"Missing {local_path}; skipped.")


def _notify_slack(webhook, bucket, build_rc, counts):
    """One custom webhook message: status + counts + access-controlled report links.

    Links are authenticated console URLs (storage.cloud.google.com) that only people with GCS
    access to the bucket can open — NOT signed/bearer URLs — so external channel members can't
    view the reports. They open as a download; the inline experience is the ClickUp embed.
    """
    if counts["errors"]:
        headline = f"❌ {counts['errors']} error(s)"
    elif build_rc != 0:
        headline = "❌ dbt run failed"  # fatal — dbt couldn't run, no test results
    elif counts["warnings"]:
        headline = f"⚠️ {counts['warnings']} warning(s)"
    else:
        headline = "✅ all passed"
    summary = f"{counts['passed']} passed · {counts['warnings']} warning(s) · {counts['errors']} error(s)"
    if counts["skipped"]:
        summary += f" · {counts['skipped']} skipped"
    lines = [
        f":bar_chart: *Data observability run* — {headline}",
        f"Results: {summary}",
    ]

    if bucket:
        base = f"https://storage.cloud.google.com/{bucket}"
        lines.append(
            f"<{base}/{DOCS_OBJECT}|📖 dbt docs>  ·  <{base}/{REPORT_OBJECT}|📊 Elementary report>"
        )
    else:
        lines.append("_(gcs_docs_bucket not set — artifacts not published)_")

    payload = json.dumps({"text": "\n".join(lines)}).encode("utf-8")
    req = urllib.request.Request(
        webhook, data=payload, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        print(f"Slack webhook responded HTTP {resp.status}")


@dag(
    dag_id="dbt_observability",
    schedule=[DBT_MARTS_READY],  # runs when dbt_dag's dbt_done task updates the asset
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={"retries": 3, "pool": "high_mem_pool"},
    tags=["dbt", "bigquery", "observability", "elementary"],
    params={
        "run_type": Param(
            "daily",
            type="string",
            enum=["daily", "backfill"],
            title="Run type",
            description="Which Astro worker queue to run this DAG's tasks on.",
        ),
    },
)
def observability():
    @task
    def run_observability():
        """deps -> build/test -> docs -> report -> upload -> Slack, all in one process."""
        Path(TARGET_DIR).mkdir(parents=True, exist_ok=True)
        mode = Variable.get("observability_dbt_mode", default_var="test")
        bucket = Variable.get("gcs_docs_bucket", default_var="")
        webhook = Variable.get("slack_webhook_url", default_var="")

        build_rc = 0
        counts = {"passed": 0, "warnings": 0, "errors": 0, "skipped": 0}

        with gcp_cli_env() as (env, _):
            # 1) deps — REQUIRED, and must run in THIS task (separate tasks don't share the FS).
            subprocess.run(_dbt("deps"), env=env, check=True)

            # 2) populate the elementary schema; capture rc + counts in-process.
            if mode == "build":
                build_rc = subprocess.run(_dbt("build"), env=env).returncode
            else:  # "test": run Elementary's models, then tests against the existing marts
                run_elem = subprocess.run(
                    _dbt("run", "--select", "elementary"), env=env
                ).returncode
                test_rc = subprocess.run(_dbt("test"), env=env).returncode
                build_rc = test_rc if run_elem == 0 else run_elem
            counts = _parse_counts(Path(TARGET_DIR, "run_results.json"))

            # 3) dbt docs — best-effort (a failed build shouldn't stop docs/report/Slack).
            try:
                subprocess.run(
                    _dbt("docs", "generate", "--static"), env=env, check=True
                )
            except Exception as e:
                print(f"dbt docs generate failed: {e}")

            # 4) Elementary report — best-effort.
            try:
                Path(EDR_HTML).parent.mkdir(parents=True, exist_ok=True)
                subprocess.run(
                    [
                        EDR_BIN,
                        "report",
                        "--project-dir",
                        DBT_PROJECT_DIR,
                        "--profiles-dir",
                        DBT_PROJECT_DIR,
                        "--file-path",
                        EDR_HTML,
                    ],
                    env=env,
                    check=True,
                )
            except Exception as e:
                print(f"edr report failed: {e}")

            # 5) publish to GCS.
            if bucket:
                _upload_to_gcs(bucket)
            else:
                print("gcs_docs_bucket not set; skipping upload.")

        # 6) Slack (console links need no GCP creds).
        if webhook:
            _notify_slack(webhook, bucket, build_rc, counts)
        else:
            print("slack_webhook_url not set; skipping Slack notification.")

        # 7) Only a FATAL dbt error (couldn't run at all: rc>=2) fails the task. Test/node
        #    failures (rc==1) are the monitoring signal — already surfaced in Slack + the report —
        #    so they do NOT turn the observability DAG red (and don't trigger a wasteful retry).
        if build_rc >= 2:
            raise RuntimeError(f"dbt {mode} could not run (rc={build_rc}); see logs.")

    run_observability()


observability()
