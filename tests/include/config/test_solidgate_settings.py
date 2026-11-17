"""Unit tests for include/config/solidgate_settings.py.

Covers the two hand-written pieces of logic that have previously shipped bugs:
  * ``resolve_window`` - backfill vs. scheduled date-window math, including the
    same-instant collapse for manually triggered runs (bugfix/solidgate-financial-entries-date-window).
  * ``get_solidgate_accounts`` - the SOLIDGATE_ACCOUNTS Variable vs. env-var
    auto-discovery fallback, including its partial-pair/no-accounts guardrails
    (bugfix/solidgate-settings-accounts).
"""

import json
import os

import pendulum
import pytest

from include.config import solidgate_settings
from include.config.solidgate_settings import (
    get_solidgate_accounts,
    resolve_window,
)


def _context(*, date_from=None, date_to=None, start, end):
    return {
        "params": {"date_from": date_from, "date_to": date_to},
        "data_interval_start": start,
        "data_interval_end": end,
    }


class TestResolveWindowBackfill:
    def test_uses_params_verbatim_when_both_dates_present(self):
        start = pendulum.datetime(2026, 1, 1, tz="UTC")
        end = pendulum.datetime(2026, 1, 1, tz="UTC")
        ctx = _context(
            date_from="2025-03-01 00:00:00",
            date_to="2025-03-05 23:59:59",
            start=start,
            end=end,
        )

        ds, date_from, date_to = resolve_window(ctx)

        assert ds == "2025-03-01"
        assert date_from == "2025-03-01 00:00:00"
        assert date_to == "2025-03-05 23:59:59"

    @pytest.mark.parametrize(
        "date_from,date_to",
        [
            (None, None),
            ("2025-03-01 00:00:00", None),
            (None, "2025-03-05 00:00:00"),
            ("", ""),
        ],
    )
    def test_falls_back_to_data_interval_unless_both_dates_present(
        self, date_from, date_to
    ):
        start = pendulum.datetime(2026, 5, 10, 3, 0, tz="UTC")
        end = pendulum.datetime(2026, 5, 11, 3, 0, tz="UTC")
        ctx = _context(date_from=date_from, date_to=date_to, start=start, end=end)

        ds, resolved_from, resolved_to = resolve_window(ctx)

        assert ds == "2026-05-10"
        assert resolved_from == "2026-05-10 03:00:00"
        assert resolved_to == "2026-05-11 03:00:00"


class TestResolveWindowScheduled:
    def test_normal_data_interval_is_used_directly(self):
        start = pendulum.datetime(2026, 5, 10, 1, 0, tz="UTC")
        end = pendulum.datetime(2026, 5, 11, 1, 0, tz="UTC")
        ctx = _context(start=start, end=end)

        ds, date_from, date_to = resolve_window(ctx)

        assert ds == "2026-05-10"
        assert date_from == "2026-05-10 01:00:00"
        assert date_to == "2026-05-11 01:00:00"

    def test_data_interval_is_converted_to_utc(self):
        start = pendulum.datetime(2026, 5, 10, 20, 0, tz="US/Pacific")
        end = pendulum.datetime(2026, 5, 11, 20, 0, tz="US/Pacific")
        ctx = _context(start=start, end=end)

        ds, date_from, date_to = resolve_window(ctx)

        # US/Pacific is UTC-7 in May (DST) -> 20:00 local == 03:00 UTC next day.
        assert ds == "2026-05-11"
        assert date_from == "2026-05-11 03:00:00"
        assert date_to == "2026-05-12 03:00:00"

    def test_same_instant_manual_trigger_collapses_to_previous_day(self):
        # A manually triggered run has no real data_interval, so Airflow sets
        # start == end == "now". Without the collapse this would resolve into a
        # zero-width window; the fix walks it back to yesterday's full day.
        now = pendulum.datetime(2026, 8, 18, 14, 30, 5, tz="UTC")
        ctx = _context(start=now, end=now)

        ds, date_from, date_to = resolve_window(ctx)

        assert ds == "2026-08-17"
        assert date_from == "2026-08-17 00:00:00"
        assert date_to == "2026-08-18 00:00:00"


@pytest.fixture
def clean_solidgate_env(monkeypatch):
    """Strip every SOLIDGATE_PUBLIC_KEY_*/SOLIDGATE_SECRET_KEY_* env var.

    The real dev environment has real merchant credentials loaded (see .env), which
    would otherwise leak into get_solidgate_accounts()'s auto-discovery scan and make
    these tests depend on whatever accounts happen to be configured on this machine.
    """
    for key in list(os.environ):
        if key.startswith("SOLIDGATE_PUBLIC_KEY_") or key.startswith(
            "SOLIDGATE_SECRET_KEY_"
        ):
            monkeypatch.delenv(key, raising=False)


def _patch_accounts_variable(monkeypatch, value):
    monkeypatch.setattr(
        solidgate_settings.Variable,
        "get",
        staticmethod(
            lambda key, default_var=None: (
                value if key == "SOLIDGATE_ACCOUNTS" else default_var
            )
        ),
    )


class TestGetSolidgateAccounts:
    def test_uses_solidgate_accounts_variable_when_set(
        self, clean_solidgate_env, monkeypatch
    ):
        accounts = [
            {"account_name": "acme", "public_key_env": "X", "secret_key_env": "Y"}
        ]
        _patch_accounts_variable(monkeypatch, json.dumps(accounts))

        assert get_solidgate_accounts() == accounts

    def test_invalid_json_in_variable_raises(self, clean_solidgate_env, monkeypatch):
        _patch_accounts_variable(monkeypatch, "{not valid json")

        with pytest.raises(json.JSONDecodeError):
            get_solidgate_accounts()

    def test_env_var_discovery_pairs_public_and_secret_keys(
        self, clean_solidgate_env, monkeypatch
    ):
        _patch_accounts_variable(monkeypatch, None)
        monkeypatch.setenv("SOLIDGATE_PUBLIC_KEY_ZEBRA", "pk_zebra")
        monkeypatch.setenv("SOLIDGATE_SECRET_KEY_ZEBRA", "sk_zebra")
        monkeypatch.setenv("SOLIDGATE_PUBLIC_KEY_ACME", "pk_acme")
        monkeypatch.setenv("SOLIDGATE_SECRET_KEY_ACME", "sk_acme")

        accounts = get_solidgate_accounts()

        # Sorted by account name, not discovery order.
        assert [a["account_name"] for a in accounts] == ["ACME", "ZEBRA"]
        acme = accounts[0]
        assert acme == {
            "account_name": "ACME",
            "public_key_env": "SOLIDGATE_PUBLIC_KEY_ACME",
            "secret_key_env": "SOLIDGATE_SECRET_KEY_ACME",
            "public_key": "pk_acme",
        }
        # The secret value itself must never be surfaced in the discovered account dict.
        assert "sk_acme" not in acme.values()
        assert "secret_key" not in acme

    def test_missing_secret_key_raises_with_the_missing_env_var_name(
        self, clean_solidgate_env, monkeypatch
    ):
        _patch_accounts_variable(monkeypatch, None)
        monkeypatch.setenv("SOLIDGATE_PUBLIC_KEY_ACME", "pk_acme")

        with pytest.raises(RuntimeError, match="SOLIDGATE_SECRET_KEY_ACME"):
            get_solidgate_accounts()

    def test_missing_public_key_raises_with_the_missing_env_var_name(
        self, clean_solidgate_env, monkeypatch
    ):
        _patch_accounts_variable(monkeypatch, None)
        monkeypatch.setenv("SOLIDGATE_SECRET_KEY_ACME", "sk_acme")

        with pytest.raises(RuntimeError, match="SOLIDGATE_PUBLIC_KEY_ACME"):
            get_solidgate_accounts()

    def test_no_accounts_configured_anywhere_raises(
        self, clean_solidgate_env, monkeypatch
    ):
        _patch_accounts_variable(monkeypatch, None)

        with pytest.raises(RuntimeError, match="No Solidgate accounts configured"):
            get_solidgate_accounts()
