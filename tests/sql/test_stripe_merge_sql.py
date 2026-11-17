"""Regression tests for include/sql/stripe/merge_*.sql.

Modeled on tests/sql/test_solidgate_merge_sql.py, but Stripe's merge scripts do NOT all
share Solidgate's shape, so the assertions are adapted per what's actually in each file
rather than forcing one template:

  * Every merge scopes its ``ON`` clause by ``T._elt_stripe_account_id = S._elt_stripe_account_id``
    (Stripe's analog of Solidgate's ``_elt_account_name`` scoping) -- this is the invariant
    that matters most, since a batch mixing two Stripe accounts sharing a business id
    would otherwise dedup/upsert into the wrong account's row.
  * ``merge_balance.sql`` is an account-level snapshot (one row per Stripe account, no
    Stripe object id to join on) and its ``QUALIFY`` dedup partitions only by
    ``_elt_stripe_account_id``, matching its account-only ``ON`` clause.
  * Every other merge (balance_transactions, charges, disputes, payment_intents,
    radar_early_fraud_warnings, refunds, customers, invoices, prices, products,
    subscriptions, events) joins on ``(id, _elt_stripe_account_id)`` and, since
    "Replicate balance dedup logic in all Stripe merge" (PR #107), also dedups its
    staging batch with ``QUALIFY ROW_NUMBER() OVER (PARTITION BY
    JSON_VALUE(json_payload.id), _elt_stripe_account_id ORDER BY _elt_insert_timestamp
    DESC) = 1`` -- the same key-parity invariant test_solidgate_merge_sql.py pins. If the
    QUALIFY partition key and the MERGE ON key ever diverge, a duplicate row in a batch
    -- e.g. an extraction retry that lands overlapping pages twice -- can slip past the
    dedup and make BigQuery's MERGE raise "UPDATE/DELETE statement affects a row in the
    target table more than once" at runtime.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

SQL_DIR = Path(__file__).parents[2] / "include" / "sql" / "stripe"
MERGE_SQL_FILES = sorted(SQL_DIR.glob("merge_*.sql"))

assert MERGE_SQL_FILES, f"no merge_*.sql files found under {SQL_DIR}"

# merge_balance.sql is an account-level snapshot: no Stripe object id to join/dedup on.
ACCOUNT_SNAPSHOT_ONLY_FILES = {"merge_balance.sql"}
NON_SNAPSHOT_FILES = [
    p for p in MERGE_SQL_FILES if p.name not in ACCOUNT_SNAPSHOT_ONLY_FILES
]

ACCOUNT_SCOPE_RE = re.compile(
    r"T\._elt_stripe_account_id\s*=\s*S\._elt_stripe_account_id", re.IGNORECASE
)
ID_ON_CLAUSE_RE = re.compile(
    r"ON\s+T\.id\s*=\s*JSON_VALUE\(S\.json_payload\.id\)\s+AND\s+T\._elt_stripe_account_id\s*=\s*"
    r"S\._elt_stripe_account_id",
    re.IGNORECASE,
)
ACCOUNT_ONLY_ON_CLAUSE_RE = re.compile(
    r"ON\s+T\._elt_stripe_account_id\s*=\s*S\._elt_stripe_account_id\s*\n?\s*WHEN",
    re.IGNORECASE,
)
QUALIFY_ID_ACCOUNT_PARTITION_RE = re.compile(
    r"QUALIFY\s+ROW_NUMBER\(\)\s+OVER\s*\(\s*PARTITION BY\s+JSON_VALUE\(json_payload\.id\)\s*,\s*"
    r"_elt_stripe_account_id\s+ORDER",
    re.IGNORECASE,
)
QUALIFY_ACCOUNT_ONLY_PARTITION_RE = re.compile(
    r"QUALIFY\s+ROW_NUMBER\(\)\s+OVER\s*\(\s*PARTITION BY\s+_elt_stripe_account_id\s+ORDER",
    re.IGNORECASE,
)


@pytest.mark.parametrize(
    "sql_path", MERGE_SQL_FILES, ids=[p.name for p in MERGE_SQL_FILES]
)
def test_merge_on_clause_is_always_scoped_by_stripe_account_id(sql_path):
    text = sql_path.read_text(encoding="utf-8")
    assert ACCOUNT_SCOPE_RE.search(text), (
        f"{sql_path.name}: MERGE ON clause must include "
        "T._elt_stripe_account_id = S._elt_stripe_account_id -- otherwise two Stripe accounts "
        "sharing a business id would upsert into each other's rows"
    )


@pytest.mark.parametrize(
    "sql_path", NON_SNAPSHOT_FILES, ids=[p.name for p in NON_SNAPSHOT_FILES]
)
def test_non_snapshot_merges_also_join_on_the_stripe_object_id(sql_path):
    text = sql_path.read_text(encoding="utf-8")
    assert ID_ON_CLAUSE_RE.search(text), (
        f"{sql_path.name}: expected 'ON T.id = JSON_VALUE(S.json_payload.id) AND "
        "T._elt_stripe_account_id = S._elt_stripe_account_id'"
    )


@pytest.mark.parametrize(
    "sql_path", NON_SNAPSHOT_FILES, ids=[p.name for p in NON_SNAPSHOT_FILES]
)
def test_non_snapshot_merges_dedup_partition_matches_their_id_and_account_on_clause(
    sql_path,
):
    text = sql_path.read_text(encoding="utf-8")
    assert QUALIFY_ID_ACCOUNT_PARTITION_RE.search(text), (
        f"{sql_path.name}: QUALIFY dedup partition must be (JSON_VALUE(json_payload.id), "
        "_elt_stripe_account_id) to match its MERGE ON join key -- a mismatch here is exactly "
        "the bug class bugfix/solidgate-merge-sql fixed for Solidgate"
    )


def test_balance_snapshot_dedup_partition_matches_its_account_only_on_clause():
    text = (SQL_DIR / "merge_balance.sql").read_text(encoding="utf-8")
    assert ACCOUNT_ONLY_ON_CLAUSE_RE.search(text), (
        "merge_balance.sql: expected an account-only ON clause (it's a one-row-per-account "
        "snapshot, not a per-object merge)"
    )
    assert QUALIFY_ACCOUNT_ONLY_PARTITION_RE.search(text), (
        "merge_balance.sql: QUALIFY dedup must partition by _elt_stripe_account_id alone to "
        "match its account-only ON clause, keeping only the latest snapshot per account per batch"
    )
