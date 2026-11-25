from __future__ import annotations

import json
import os

import pytest
from airflow.exceptions import AirflowException

from include.config import paypal_settings
from include.config.paypal_settings import get_paypal_accounts


@pytest.fixture
def clean_paypal_env(monkeypatch):
    for key in list(os.environ):
        if key.startswith(
            ("PAYPAL_CLIENT_ID_", "PAYPAL_CLIENT_SECRET_", "PAYPAL_ENVIRONMENT_")
        ):
            monkeypatch.delenv(key, raising=False)


def _patch_accounts_variable(monkeypatch, value):
    monkeypatch.setattr(
        paypal_settings.Variable,
        "get",
        staticmethod(
            lambda key, default_var=None: (
                value if key == "PAYPAL_ACCOUNTS" else default_var
            )
        ),
    )


def test_loads_multiple_accounts_from_airflow_variable(clean_paypal_env, monkeypatch):
    accounts = [
        {
            "account_name": "sandbox_main",
            "environment": "sandbox",
            "client_id_env": "PAYPAL_CLIENT_ID_SANDBOX",
            "client_secret_env": "PAYPAL_CLIENT_SECRET_SANDBOX",
        },
        {
            "account_name": "production_main",
            "environment": "production",
            "client_id_env": "PAYPAL_CLIENT_ID_PRODUCTION",
            "client_secret_env": "PAYPAL_CLIENT_SECRET_PRODUCTION",
        },
    ]
    _patch_accounts_variable(monkeypatch, json.dumps(accounts))

    assert get_paypal_accounts() == accounts


def test_discovers_accounts_from_client_secret_and_environment_env_vars(
    clean_paypal_env, monkeypatch
):
    _patch_accounts_variable(monkeypatch, None)
    monkeypatch.setenv("PAYPAL_CLIENT_ID_PROD", "secret-client-id")
    monkeypatch.setenv("PAYPAL_CLIENT_SECRET_PROD", "secret-client-secret")
    monkeypatch.setenv("PAYPAL_ENVIRONMENT_PROD", "production")

    accounts = get_paypal_accounts()

    assert accounts == [
        {
            "account_name": "prod",
            "environment": "production",
            "client_id_env": "PAYPAL_CLIENT_ID_PROD",
            "client_secret_env": "PAYPAL_CLIENT_SECRET_PROD",
        }
    ]
    assert "secret-client-id" not in repr(accounts)
    assert "secret-client-secret" not in repr(accounts)


def test_rejects_missing_environment_for_discovered_account(
    clean_paypal_env, monkeypatch
):
    _patch_accounts_variable(monkeypatch, None)
    monkeypatch.setenv("PAYPAL_CLIENT_ID_SANDBOX", "client-id")
    monkeypatch.setenv("PAYPAL_CLIENT_SECRET_SANDBOX", "client-secret")

    with pytest.raises(AirflowException, match="PAYPAL_ENVIRONMENT_SANDBOX"):
        get_paypal_accounts()


def test_rejects_unknown_environment_in_variable(clean_paypal_env, monkeypatch):
    _patch_accounts_variable(
        monkeypatch,
        json.dumps(
            [
                {
                    "account_name": "test",
                    "environment": "staging",
                    "client_id_env": "PAYPAL_CLIENT_ID_TEST",
                    "client_secret_env": "PAYPAL_CLIENT_SECRET_TEST",
                }
            ]
        ),
    )

    with pytest.raises(AirflowException, match="sandbox.*production"):
        get_paypal_accounts()
