"""Regression tests for include/sql/chargebackhit/merge_alerts.sql's MERGE key/dedup
consistency.

Same bug class as tests/sql/test_solidgate_merge_sql.py (bugfix/solidgate-merge-sql,
bugfix/solidgate-settings-accounts): the staging batch is deduplicated with
``QUALIFY ROW_NUMBER() OVER (PARTITION BY <business_key>, <account_column> ...)`` and
then the MERGE joins on ``ON T.<business_key> = S.<business_key> AND T.<account_column>
= S.<account_column>``. If those two key sets ever drift apart, two different
ChargebackHit accounts that happen to share an ``alert.id`` would dedup against each
other's row instead of being kept independent.

Chargebackhit's natural key differs from Solidgate's in two ways worth calling out
explicitly (so this test doesn't quietly assume Solidgate's exact shape):
  * the business key is nested under ``item.alert.id`` (``JSON_VALUE(item, '$.alert.id')``),
    not a top-level ``id``/``order_id`` like Solidgate's per-entity keys;
  * the account column in the *source* CTE is aliased ``account`` (``_elt_chargebackhit_account_name
    AS account``), while the QUALIFY partition (evaluated before the alias exists, in
    the same SELECT) still has to reference the raw ``_elt_chargebackhit_account_name``
    column. The alias is transparent to callers of this test but the regex below checks
    both spellings so a future rename of either side is still caught.
"""

import re
from pathlib import Path

SQL_PATH = (
    Path(__file__).parents[2] / "include" / "sql" / "chargebackhit" / "merge_alerts.sql"
)


def _text() -> str:
    assert SQL_PATH.exists(), f"expected merge SQL at {SQL_PATH}"
    return SQL_PATH.read_text(encoding="utf-8")


class TestMergeJoinKey:
    def test_on_clause_joins_on_alert_id_and_account(self):
        text = _text()
        match = re.search(
            r"ON\s+T\.alert_id\s*=\s*S\.alert_id\s+AND\s+T\.account\s*=\s*S\.account",
            text,
        )
        assert match, "MERGE ON clause must join on both alert_id AND account"

    def test_qualify_partitions_on_alert_id_json_path_and_raw_account_column(self):
        text = _text()
        match = re.search(
            r"QUALIFY\s+ROW_NUMBER\(\)\s+OVER\s*\(\s*"
            r"PARTITION BY\s+JSON_VALUE\(item,\s*'\$\.alert\.id'\)\s*,\s*_elt_chargebackhit_account_name",
            text,
            re.IGNORECASE,
        )
        assert match, (
            "QUALIFY dedup must partition by (JSON_VALUE(item, '$.alert.id'), "
            "_elt_chargebackhit_account_name) to match the account-scoped MERGE ON clause"
        )

    def test_source_query_filters_out_rows_with_no_alert_id(self):
        text = _text()
        assert re.search(
            r"WHERE\s+NULLIF\(JSON_VALUE\(item,\s*'\$\.alert\.id'\),\s*''\)\s+IS NOT NULL",
            text,
        ), (
            "source query must filter out rows where alert.id is missing/empty, "
            "otherwise every such row collapses into a single QUALIFY partition"
        )
