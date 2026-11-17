import logging
from pathlib import Path

from airflow.models import Variable

logger = logging.getLogger(__name__)

GCP_CONN_ID = Variable.get("GCP_CONN_ID")
GCS_BUCKET = Variable.get("GCS_LANDING_BUCKET")
GCS_UPLOAD_CHUNK = 8 * 1024 * 1024  # 8 MiB streamed upload (memory-bounded)
BQ_LOCATION = "US"
SOURCE = "solidgate"

BASE_URL = "https://reports.solidgate.com/api/v1"
SOLIDGATE_PUBLIC_KEY_ENV = "SOLIDGATE_PUBLIC_KEY"
SOLIDGATE_SECRET_KEY_ENV = "SOLIDGATE_SECRET_KEY"

CONNECT_TIMEOUT = 10
READ_TIMEOUT = 120
TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

RATE_LIMIT_PER_SECOND = 100

SQL_DIR = Path(__file__).parent.parent / "sql" / "solidgate"

AVRO_SCHEMA = {
    "type": "record",
    "name": "SolidgateReleaseRecord",
    "fields": [
        {"name": "_elt_account_name", "type": "string"},
        {"name": "_elt_endpoint", "type": "string"},
        {"name": "_elt_insert_timestamp", "type": "string"},
        {"name": "_elt_update_timestamp", "type": "string"},
        {"name": "_gcs_file_path", "type": "string"},
        {"name": "raw_data", "type": "string"},
    ],
}


def get_solidgate_accounts() -> list[dict]:
    """Dynamically auto-discovers merchant configurations from env variables."""
    import json
    import os

    accounts_json = Variable.get("SOLIDGATE_ACCOUNTS", default_var=None)
    if accounts_json:
        try:
            return json.loads(accounts_json)
        except json.JSONDecodeError:
            logger.exception("SOLIDGATE_ACCOUNTS Variable is set but is not valid JSON")
            raise

    accounts_map = {}
    for key in os.environ.keys():
        if key.startswith("SOLIDGATE_PUBLIC_KEY_"):
            acc_name = key.replace("SOLIDGATE_PUBLIC_KEY_", "", 1)
            accounts_map.setdefault(acc_name, set()).add("public")
        elif key.startswith("SOLIDGATE_SECRET_KEY_"):
            acc_name = key.replace("SOLIDGATE_SECRET_KEY_", "", 1)
            accounts_map.setdefault(acc_name, set()).add("secret")

    discovered_accounts = []
    for acc_name in sorted(accounts_map.keys()):
        found = accounts_map[acc_name]
        if found != {"public", "secret"}:
            missing_env = (
                f"SOLIDGATE_SECRET_KEY_{acc_name}"
                if "public" in found
                else f"SOLIDGATE_PUBLIC_KEY_{acc_name}"
            )
            raise RuntimeError(
                f"Solidgate account '{acc_name}' is missing env var {missing_env}: "
                "both public and secret keys are required to schedule an account"
            )
        discovered_accounts.append(
            {
                "account_name": acc_name,
                "public_key_env": f"SOLIDGATE_PUBLIC_KEY_{acc_name}",
                "secret_key_env": f"SOLIDGATE_SECRET_KEY_{acc_name}",
                "public_key": os.environ.get(f"SOLIDGATE_PUBLIC_KEY_{acc_name}", ""),
            }
        )

    if not discovered_accounts:
        raise RuntimeError(
            "No Solidgate accounts configured: set the SOLIDGATE_ACCOUNTS Airflow Variable "
            "or SOLIDGATE_PUBLIC_KEY_<NAME>/SOLIDGATE_SECRET_KEY_<NAME> env var pairs"
        )
    return discovered_accounts


def account_allowed(context, account_name: str) -> bool:
    """True if this account should run in this DAG run."""
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    requested = conf.get("accounts") if conf else None
    if not requested:
        return True
    return account_name.lower() in {a.lower() for a in requested}


def resolve_window(context) -> tuple[str, str, str]:
    """Resolve (ds, date_from, date_to) for an ingest run.

    Backfill runs pass ``date_from``/``date_to`` via params/conf — use those directly
    and take ``ds`` (the GCS landing/release partition) from the window start. This
    keeps backfills independent of the run's ``logical_date``/``data_interval`` (which
    are ``now`` or null for triggered runs), so each window lands in its own ``ds``.

    Scheduled runs (no conf) fall back to the data interval, collapsing a same-instant
    manual interval to the previous day.
    """
    params = context["params"]
    date_from = params.get("date_from")
    date_to = params.get("date_to")
    if date_from and date_to:
        return date_from[:10], date_from, date_to

    start_dt = context["data_interval_start"].in_timezone("UTC")
    end_dt = context["data_interval_end"].in_timezone("UTC")
    if start_dt == end_dt:
        end_dt = start_dt.start_of("day")
        start_dt = end_dt.subtract(days=1)
    return (
        start_dt.format("YYYY-MM-DD"),
        start_dt.format("YYYY-MM-DD HH:mm:ss"),
        end_dt.format("YYYY-MM-DD HH:mm:ss"),
    )
