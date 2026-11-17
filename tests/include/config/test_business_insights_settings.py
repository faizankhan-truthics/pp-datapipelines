"""Tests for include/config/business_insights_settings.py -- product -> Slack channel routing.
A wrong route posts one product's numbers into another product's channel."""

from __future__ import annotations

import importlib

import pytest

import include.config.business_insights_settings as settings


@pytest.fixture
def reload_with_env(monkeypatch):
    def _reload(**env):
        for key, value in env.items():
            if value is None:
                monkeypatch.delenv(key, raising=False)
            else:
                monkeypatch.setenv(key, value)
        return importlib.reload(settings)

    yield _reload
    monkeypatch.undo()
    importlib.reload(settings)


@pytest.mark.parametrize(
    "product, expected",
    [
        ("QR", "BUSINESS_INSIGHTS_CHANNEL_ID__QR"),
        ("Location-Tool", "BUSINESS_INSIGHTS_CHANNEL_ID__LOCATION_TOOL"),
        ("Simpl.ai", "BUSINESS_INSIGHTS_CHANNEL_ID__SIMPL_AI"),
    ],
)
def test_product_channel_env_var_name(product, expected):
    assert settings.product_channel_env_var(product) == expected


def test_every_product_goes_to_the_one_configured_channel(reload_with_env):
    s = reload_with_env(
        BUSINESS_INSIGHTS_CHANNEL_ID="CSHARED",
        BUSINESS_INSIGHTS_CHANNEL_ID__QR=None,
        BUSINESS_INSIGHTS_CHANNEL_ID__TRACELO=None,
    )

    assert s.channel_for("QR") == "CSHARED" and s.channel_for("Tracelo") == "CSHARED"


def test_a_product_channel_overrides_the_shared_one(reload_with_env):
    s = reload_with_env(
        BUSINESS_INSIGHTS_CHANNEL_ID="CSHARED",
        BUSINESS_INSIGHTS_CHANNEL_ID__QR="CQR",
        BUSINESS_INSIGHTS_CHANNEL_ID__TRACELO=None,
    )

    assert s.channel_for("QR") == "CQR"
    assert s.channel_for("Tracelo") == "CSHARED"


def test_nothing_configured_means_no_destination(reload_with_env):
    s = reload_with_env(
        BUSINESS_INSIGHTS_CHANNEL_ID=None, BUSINESS_INSIGHTS_CHANNEL_ID__QR=None
    )

    assert s.channel_for("QR") is None


def test_there_is_no_mode_switch_or_webhook_path_left():
    # dev and prod differ only by their env vars; a leftover flag or a second delivery route would be a silent rule
    for name in (
        "MODE",
        "TEST_CHANNEL_ID",
        "WEBHOOK",
        "webhook_for",
        "product_webhook_env_var",
    ):
        assert not hasattr(settings, name), name
