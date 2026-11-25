"""Configuration for the multi-account PayPal transactions ingestion pipeline."""

import json
import logging
import os
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.models import Variable

logger = logging.getLogger(__name__)

BASE_URLS = {
    "sandbox": "https://api-m.sandbox.paypal.com",
    "production": "https://api-m.paypal.com",
}
ENDPOINT = "transactions"
PAGE_SIZE = 500
MAX_WINDOW_DAYS = 30
RATE_LIMIT_PER_SECOND = 5
GCP_CONN_ID = Variable.get("GCP_CONN_ID")
SQL_DIR = Path(__file__).parent.parent / "sql" / "paypal"


def _validate_accounts(accounts: list[dict]) -> list[dict]:
    if not isinstance(accounts, list) or not accounts:
        raise AirflowException("PAYPAL_ACCOUNTS must contain at least one account")

    validated = []
    seen_names = set()
    required_fields = {
        "account_name",
        "environment",
        "client_id_env",
        "client_secret_env",
    }

    for account in accounts:
        if not isinstance(account, dict):
            raise AirflowException("Every PAYPAL_ACCOUNTS entry must be an object")
        missing_fields = required_fields - account.keys()
        if missing_fields:
            raise AirflowException(
                "PayPal account configuration is missing required field(s): "
                + ", ".join(sorted(missing_fields))
            )

        account_name = account["account_name"]
        environment = account["environment"]
        client_id_env = account["client_id_env"]
        client_secret_env = account["client_secret_env"]
        if not isinstance(account_name, str) or not account_name.strip():
            raise AirflowException("PayPal account_name must be a non-empty string")
        account_name = account_name.strip().lower()
        if not all(char.isalnum() or char in "_-" for char in account_name):
            raise AirflowException(
                f"PayPal account_name {account_name!r} may contain only letters, "
                "numbers, underscores, and hyphens"
            )
        if account_name in seen_names:
            raise AirflowException(f"Duplicate PayPal account_name: {account_name!r}")
        seen_names.add(account_name)

        if not isinstance(environment, str) or environment not in BASE_URLS:
            raise AirflowException(
                f"Invalid PayPal environment {environment!r} for {account_name}; "
                "expected 'sandbox' or 'production'"
            )
        if not all(
            isinstance(value, str) and value.strip()
            for value in (client_id_env, client_secret_env)
        ):
            raise AirflowException(
                f"PayPal account {account_name!r} must specify non-empty "
                "client_id_env and client_secret_env names"
            )

        validated.append(
            {
                "account_name": account_name,
                "environment": environment,
                "client_id_env": client_id_env.strip(),
                "client_secret_env": client_secret_env.strip(),
            }
        )
    return validated


def get_paypal_accounts() -> list[dict]:
    """Load named PayPal accounts from an Airflow Variable or paired env vars."""
    accounts_json = Variable.get("PAYPAL_ACCOUNTS", default_var=None)
    if accounts_json:
        try:
            return _validate_accounts(json.loads(accounts_json))
        except json.JSONDecodeError:
            logger.exception("PAYPAL_ACCOUNTS Variable is not valid JSON")
            raise

    discovered: dict[str, set[str]] = {}
    prefixes = {
        "PAYPAL_CLIENT_ID_": "client_id",
        "PAYPAL_CLIENT_SECRET_": "client_secret",
        "PAYPAL_ENVIRONMENT_": "environment",
    }
    for key, value in os.environ.items():
        if not value.strip():
            continue
        for prefix, field in prefixes.items():
            if key.startswith(prefix):
                account_suffix = key[len(prefix) :]
                discovered.setdefault(account_suffix, set()).add(field)
                break

    accounts = []
    for suffix in sorted(discovered):
        found = discovered[suffix]
        if found != {"client_id", "client_secret", "environment"}:
            missing = {
                "client_id": f"PAYPAL_CLIENT_ID_{suffix}",
                "client_secret": f"PAYPAL_CLIENT_SECRET_{suffix}",
                "environment": f"PAYPAL_ENVIRONMENT_{suffix}",
            }
            missing_env = next(
                variable for field, variable in missing.items() if field not in found
            )
            raise AirflowException(
                f"PayPal account {suffix!r} is missing {missing_env}; configure "
                "a client ID, client secret, and environment for every account"
            )
        accounts.append(
            {
                "account_name": suffix.lower(),
                "environment": os.environ[f"PAYPAL_ENVIRONMENT_{suffix}"].strip(),
                "client_id_env": f"PAYPAL_CLIENT_ID_{suffix}",
                "client_secret_env": f"PAYPAL_CLIENT_SECRET_{suffix}",
            }
        )

    if not accounts:
        raise AirflowException(
            "No PayPal accounts configured: set the PAYPAL_ACCOUNTS Airflow "
            "Variable or PAYPAL_CLIENT_ID_<NAME>, PAYPAL_CLIENT_SECRET_<NAME>, "
            "and PAYPAL_ENVIRONMENT_<NAME> environment variables"
        )
    return _validate_accounts(accounts)


AVRO_SCHEMA = {
    "type": "record",
    "name": "PayPalReleaseRecord",
    "fields": [
        {"name": "_elt_account_name", "type": "string"},
        {"name": "_elt_environment", "type": "string"},
        {"name": "_elt_endpoint", "type": "string"},
        {"name": "_elt_insert_timestamp", "type": "string"},
        {"name": "_elt_update_timestamp", "type": "string"},
        {"name": "_gcs_file_path", "type": "string"},
        {"name": "raw_data", "type": "string"},
    ],
}
