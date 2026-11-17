"""Unit tests for discover_chargebackhit_accounts (dags/chargebackhit/chargebackhit_dag.py).

Unlike Solidgate, ChargebackHit has no include/config/chargebackhit_settings.py helper
for account auto-discovery -- the ``CHARGEBACKHIT_<NAME>_API_PUBLIC_KEY`` /
``CHARGEBACKHIT_<NAME>_API_SECRET_KEY`` env-var scan lives directly in the DAG module
(dags/chargebackhit/chargebackhit_dag.py). This is the same bug class already fixed once
in Solidgate (bugfix/solidgate-settings-accounts): an incomplete credential pair for one
account must fail loudly with the missing env var name, not silently drop that account
or blow up the whole DAG with an unhelpful error.

The function is extracted from the DAG file via ``ast`` and ``exec``'d in isolation
INSTEAD OF importing ``dags.chargebackhit.chargebackhit_dag`` directly, because that
module's top level constructs the actual DAG (``with DAG(...) as dag: ...``), which
calls ``Variable.get(...)`` for several Airflow Variables and calls
``discover_chargebackhit_accounts()`` itself using whatever real credentials happen to
be in the container's ``.env`` -- making test collection depend on live Airflow Variable
state (see tests/dags/test_dag_example.py, which already accepts that dependency via
DagBag for "no DAG import errors" checks, but that's a broader integration concern than
what these tests need). Extracting just the function keeps these tests hermetic and
independent of Airflow Variables while still exercising the real, unmodified source.
"""

from __future__ import annotations

import ast
import os
from pathlib import Path

import pytest

DAG_FILE = Path(__file__).parents[2] / "dags" / "chargebackhit" / "chargebackhit_dag.py"


def _load_discover_accounts_function():
    source = DAG_FILE.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(DAG_FILE))
    func_node = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "discover_chargebackhit_accounts"
    )
    module_ast = ast.Module(body=[func_node], type_ignores=[])
    namespace = {"os": __import__("os")}
    exec(compile(module_ast, filename=str(DAG_FILE), mode="exec"), namespace)
    return namespace["discover_chargebackhit_accounts"]


discover_chargebackhit_accounts = _load_discover_accounts_function()


@pytest.fixture
def clean_chargebackhit_env(monkeypatch):
    """Strip every CHARGEBACKHIT_*_API_PUBLIC_KEY/_API_SECRET_KEY env var.

    The real dev environment has real credentials loaded (see .env), which would
    otherwise leak into discover_chargebackhit_accounts()'s scan and make these tests
    depend on whatever accounts happen to be configured on this machine.
    """
    for key in list(os.environ):
        if key.startswith("CHARGEBACKHIT_") and (
            "_API_PUBLIC_KEY" in key or "_API_SECRET_KEY" in key
        ):
            monkeypatch.delenv(key, raising=False)


class TestDiscoverChargebackhitAccounts:
    def test_pairs_public_and_secret_keys_by_account(
        self, clean_chargebackhit_env, monkeypatch
    ):
        monkeypatch.setenv("CHARGEBACKHIT_ACME_API_PUBLIC_KEY", "pk_acme")
        monkeypatch.setenv("CHARGEBACKHIT_ACME_API_SECRET_KEY", "sk_acme")

        accounts = discover_chargebackhit_accounts()

        assert accounts == [
            {
                "account_name": "acme",
                "public_key_env": "CHARGEBACKHIT_ACME_API_PUBLIC_KEY",
                "secret_key_env": "CHARGEBACKHIT_ACME_API_SECRET_KEY",
            }
        ]

    def test_multiple_accounts_are_all_discovered(
        self, clean_chargebackhit_env, monkeypatch
    ):
        monkeypatch.setenv("CHARGEBACKHIT_ACME_API_PUBLIC_KEY", "pk_acme")
        monkeypatch.setenv("CHARGEBACKHIT_ACME_API_SECRET_KEY", "sk_acme")
        monkeypatch.setenv("CHARGEBACKHIT_ZEBRA_API_PUBLIC_KEY", "pk_zebra")
        monkeypatch.setenv("CHARGEBACKHIT_ZEBRA_API_SECRET_KEY", "sk_zebra")

        accounts = discover_chargebackhit_accounts()

        assert {a["account_name"] for a in accounts} == {"acme", "zebra"}
        assert len(accounts) == 2

    def test_no_accounts_configured_returns_empty_list(self, clean_chargebackhit_env):
        # Unlike get_solidgate_accounts (which raises RuntimeError when nothing is
        # configured), this function itself returns [] -- the caller
        # (chargebackhit_dag.py's module body) is responsible for raising ValueError on
        # an empty list. Pinned so that invariant isn't silently assumed by callers.
        assert discover_chargebackhit_accounts() == []

    def test_missing_secret_key_raises_with_the_missing_env_var_name(
        self, clean_chargebackhit_env, monkeypatch
    ):
        monkeypatch.setenv("CHARGEBACKHIT_ACME_API_PUBLIC_KEY", "pk_acme")

        with pytest.raises(ValueError, match="acme"):
            discover_chargebackhit_accounts()

    def test_missing_public_key_raises_with_the_missing_env_var_name(
        self, clean_chargebackhit_env, monkeypatch
    ):
        monkeypatch.setenv("CHARGEBACKHIT_ACME_API_SECRET_KEY", "sk_acme")

        with pytest.raises(ValueError, match="acme"):
            discover_chargebackhit_accounts()

    def test_empty_string_secret_value_is_treated_as_not_set_and_still_raises(
        self, clean_chargebackhit_env, monkeypatch
    ):
        # `if not key.startswith(prefix) or not value: continue` skips the empty-string
        # env var outright, as if it were never set at all -- so this does NOT hit some
        # separate "blank secret" code path. It falls through to the exact same
        # incomplete-pair check as a wholly-missing secret key, and raises the same way.
        monkeypatch.setenv("CHARGEBACKHIT_ACME_API_PUBLIC_KEY", "pk_acme")
        monkeypatch.setenv("CHARGEBACKHIT_ACME_API_SECRET_KEY", "")

        with pytest.raises(ValueError, match="acme"):
            discover_chargebackhit_accounts()

    def test_unrelated_env_vars_are_ignored(self, clean_chargebackhit_env, monkeypatch):
        monkeypatch.setenv("CHARGEBACKHIT_ACME_API_PUBLIC_KEY", "pk_acme")
        monkeypatch.setenv("CHARGEBACKHIT_ACME_API_SECRET_KEY", "sk_acme")
        monkeypatch.setenv("SOME_OTHER_VAR", "irrelevant")
        monkeypatch.setenv(
            "CHARGEBACKHIT_WEBHOOK_URL", "https://example.com"
        )  # no _API_ marker

        accounts = discover_chargebackhit_accounts()

        assert accounts == [
            {
                "account_name": "acme",
                "public_key_env": "CHARGEBACKHIT_ACME_API_PUBLIC_KEY",
                "secret_key_env": "CHARGEBACKHIT_ACME_API_SECRET_KEY",
            }
        ]

    def test_account_name_containing_extra_api_marker_is_silently_skipped(
        self, clean_chargebackhit_env, monkeypatch
    ):
        # key[len(prefix):].split("_API_") is an unbounded split (no maxsplit=1). An
        # account name that itself contains the literal substring "_API_" produces more
        # than 2 parts and is silently ignored (`if len(parts) != 2: continue`) instead
        # of raising or being parsed with the LAST "_API_" as the separator. This is a
        # narrow edge case (real account names are unlikely to contain "_API_") but is
        # worth pinning as documented, current behavior.
        monkeypatch.setenv("CHARGEBACKHIT_FOO_API_BAR_API_PUBLIC_KEY", "pk")
        monkeypatch.setenv("CHARGEBACKHIT_FOO_API_BAR_API_SECRET_KEY", "sk")

        assert discover_chargebackhit_accounts() == []
