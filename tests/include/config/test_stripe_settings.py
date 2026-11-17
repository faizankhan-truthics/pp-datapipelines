"""Unit tests for include/config/stripe_settings.py.

discover_stripe_accounts() is covered in tests/dags/stripe/test_stripe_financial_pipeline.py
(the DAG imports it from here). This covers the rest of the module:
  * the VAMP table name and the Development/Production label on the VAMP Slack report,
    both derived from env at import time -- a wrong label on a prod alert is how a real
    one gets ignored.
  * the endpoint lists and their per-endpoint override maps staying consistent: an
    override keyed on an endpoint that isn't scheduled silently does nothing.
"""

from __future__ import annotations

import importlib

import pytest

import include.config.stripe_settings as stripe_settings


@pytest.fixture
def reload_with_env(monkeypatch):
    def _reload(**env):
        for key, value in env.items():
            if value is None:
                monkeypatch.delenv(key, raising=False)
            else:
                monkeypatch.setenv(key, value)
        return importlib.reload(stripe_settings)

    yield _reload
    monkeypatch.undo()
    importlib.reload(stripe_settings)


class TestVampSettings:
    def test_vamp_table_is_built_from_project_and_gold_dataset(self, reload_with_env):
        settings = reload_with_env(
            AIRFLOW_VAR_GCP_PROJECT_ID="proj", AIRFLOW_VAR_BQ_GOLD_DATASET="gold"
        )

        assert settings.VAMP_TABLE_NAME == "proj.gold.stripe_monthly_vamp_estimate"

    def test_sandbox_gold_dataset_is_labelled_development(self, reload_with_env):
        assert (
            reload_with_env(AIRFLOW_VAR_BQ_GOLD_DATASET="gold_sandbox").ENV
            == "Development"
        )

    @pytest.mark.parametrize("dataset", ["gold", None])
    def test_anything_else_is_labelled_production(self, reload_with_env, dataset):
        assert reload_with_env(AIRFLOW_VAR_BQ_GOLD_DATASET=dataset).ENV == "Production"

    def test_webhook_comes_from_env(self, reload_with_env):
        settings = reload_with_env(
            VAMP_ALERTS__SLACK_WEBHOOK_URL="https://hooks.slack.test/x"
        )

        assert settings.VAMP_ALERTS_WEBHOOK == "https://hooks.slack.test/x"


class TestEndpointConfig:
    def test_legacy_and_new_endpoints_do_not_overlap(self):
        assert not set(stripe_settings.LEGACY_ENDPOINTS) & set(
            stripe_settings.NEW_ENDPOINTS
        )

    def test_events_runs_in_its_own_dag_not_the_financial_endpoint_lists(self):
        all_endpoints = set(stripe_settings.LEGACY_ENDPOINTS) | set(
            stripe_settings.NEW_ENDPOINTS
        )

        assert stripe_settings.EVENTS_ENDPOINT not in all_endpoints

    @pytest.mark.parametrize(
        "overrides", ["ENDPOINT_EXTRA_PARAMS", "ENDPOINT_MAX_WORKERS_OVERRIDES"]
    )
    def test_overrides_only_reference_scheduled_endpoints(self, overrides):
        all_endpoints = set(stripe_settings.LEGACY_ENDPOINTS) | set(
            stripe_settings.NEW_ENDPOINTS
        )

        assert set(getattr(stripe_settings, overrides)) <= all_endpoints

    def test_subscriptions_pulls_every_status(self):
        # Stripe's default list only returns active subscriptions.
        assert stripe_settings.ENDPOINT_EXTRA_PARAMS["subscriptions"] == {
            "status": "all"
        }

    def test_max_worker_overrides_are_positive_ints(self):
        assert all(
            isinstance(v, int) and v > 0
            for v in stripe_settings.ENDPOINT_MAX_WORKERS_OVERRIDES.values()
        )

    def test_avro_schema_carries_the_account_id(self):
        names = [f["name"] for f in stripe_settings.AVRO_SCHEMA["fields"]]

        assert "_elt_stripe_account_id" in names
        assert "raw_data" in names
