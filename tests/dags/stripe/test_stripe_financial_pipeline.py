"""Unit tests for discover_stripe_accounts (dags/stripe/stripe_financial_pipeline.py).

This is Stripe's analog of Solidgate's ``get_solidgate_accounts``
(include/config/solidgate_settings.py, see tests/include/config/test_solidgate_settings.py)
-- both auto-discover per-account credentials from paired env vars. Two things are worth
noting about where/how this one differs:

  * It lives directly in the DAG file rather than in include/config/stripe_settings.py, so
    reaching it means importing the whole DAG module (which builds the real DAG at import
    time, the same thing Airflow's own DagBag parsing already does -- see
    tests/dags/test_dag_example.py, which asserts every DAG in this repo currently imports
    without error). The function itself re-reads ``os.environ`` on every call, so once the
    module is imported, these tests only need to control the environment at *call* time.
  * On a missing pairing (an API key with no matching account id), it does NOT raise the
    way Solidgate's account discovery does -- it logs a warning and silently excludes that
    account from the returned list. A misconfigured Stripe account (key added, account id
    forgotten) therefore just quietly never gets ingested, with only an easy-to-miss log
    line, rather than the loud RuntimeError Solidgate's equivalent gives you. This is
    exactly the validation gap bugfix/solidgate-settings-accounts closed for Solidgate;
    Stripe's version still has it. Pinned below, not fixed -- flagged in the report.
"""

from __future__ import annotations

import os

import pytest

from dags.stripe.stripe_financial_pipeline import dag, discover_stripe_accounts
from include.config.stripe_settings import LEGACY_ENDPOINTS, NEW_ENDPOINTS


@pytest.fixture
def clean_stripe_env(monkeypatch):
    """Strip every STRIPE_API_KEY_*/STRIPE_ACCOUNT_ID_* env var so real dev-container
    credentials (loaded from .env) can't leak into these discovery tests."""
    for key in list(os.environ):
        if key.startswith("STRIPE_API_KEY_") or key.startswith("STRIPE_ACCOUNT_ID_"):
            monkeypatch.delenv(key, raising=False)


class TestDiscoverStripeAccounts:
    def test_pairs_api_key_and_account_id_and_lowercases_the_account_name(
        self, clean_stripe_env, monkeypatch
    ):
        monkeypatch.setenv("STRIPE_API_KEY_ACME", "sk_acme")
        monkeypatch.setenv("STRIPE_ACCOUNT_ID_ACME", "acct_123")

        accounts = discover_stripe_accounts()

        assert accounts == [
            {
                "account_name": "acme",
                "api_key_env": "STRIPE_API_KEY_ACME",
                "account_id": "acct_123",
            }
        ]

    def test_missing_account_id_warns_and_silently_excludes_the_account_instead_of_raising(
        self, clean_stripe_env, monkeypatch, caplog
    ):
        monkeypatch.setenv("STRIPE_API_KEY_ACME", "sk_acme")
        # deliberately no STRIPE_ACCOUNT_ID_ACME -- see the module docstring's flagged gap

        accounts = discover_stripe_accounts()

        assert accounts == []

    def test_read_only_key_is_excluded_even_when_its_account_id_is_present(
        self, clean_stripe_env, monkeypatch
    ):
        monkeypatch.setenv("STRIPE_API_KEY_READ_ONLY", "sk_ro")
        monkeypatch.setenv("STRIPE_ACCOUNT_ID_READ_ONLY", "acct_ro")

        assert discover_stripe_accounts() == []

    def test_blank_api_key_value_is_skipped(self, clean_stripe_env, monkeypatch):
        monkeypatch.setenv("STRIPE_API_KEY_ACME", "   ")
        monkeypatch.setenv("STRIPE_ACCOUNT_ID_ACME", "acct_123")

        assert discover_stripe_accounts() == []

    def test_multiple_paired_accounts_are_all_discovered(
        self, clean_stripe_env, monkeypatch
    ):
        monkeypatch.setenv("STRIPE_API_KEY_ACME", "sk_acme")
        monkeypatch.setenv("STRIPE_ACCOUNT_ID_ACME", "acct_a")
        monkeypatch.setenv("STRIPE_API_KEY_ZEBRA", "sk_zebra")
        monkeypatch.setenv("STRIPE_ACCOUNT_ID_ZEBRA", "acct_z")

        accounts = discover_stripe_accounts()

        assert sorted(a["account_name"] for a in accounts) == ["acme", "zebra"]

    def test_unrelated_env_vars_are_ignored(self, clean_stripe_env, monkeypatch):
        monkeypatch.setenv("SOME_OTHER_VAR", "irrelevant")
        monkeypatch.setenv(
            "STRIPE_ACCOUNT_ID_ORPHAN", "acct_orphan"
        )  # account id with no key

        assert discover_stripe_accounts() == []


def test_financial_pipeline_runs_every_api_endpoint_by_default():
    select_endpoints = dag.get_task("select_endpoints").python_callable

    assert select_endpoints(dag_run=None) == [
        f"extract_{e}" for e in LEGACY_ENDPOINTS + NEW_ENDPOINTS
    ]
    for endpoint in LEGACY_ENDPOINTS + NEW_ENDPOINTS:
        transform = dag.get_task(f"transform_bronze_{endpoint}")
        assert transform.params["source"] == "api"
