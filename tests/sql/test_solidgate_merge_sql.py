"""Regression tests for the Solidgate Bronze MERGE key/dedup consistency.

Every include/sql/solidgate/merge_*.sql file dedups its staging batch with
``QUALIFY ROW_NUMBER() OVER (PARTITION BY <business_key>, _elt_account_name ...)``
and then joins on ``ON T.<business_key> = S.<business_key> AND T._elt_account_name =
S._elt_account_name``. Those two key sets MUST match: if the QUALIFY partition is
missing ``_elt_account_name`` (or the ON clause is), two different merchant accounts
that happen to share a business key (order_id/id) will dedup or upsert into each
other's rows. This is exactly the class of bug fixed in
bugfix/solidgate-merge-sql/bugfix/solidgate-settings-accounts -- these tests pin the
invariant so a future edit to any one merge script can't silently drop the account
scoping from only one of the two clauses.
"""

import re
from pathlib import Path

import pytest

SQL_DIR = Path(__file__).parents[2] / "include" / "sql" / "solidgate"
MERGE_SQL_FILES = sorted(SQL_DIR.glob("merge_*.sql"))

ON_CLAUSE_RE = re.compile(
    r"ON\s+T\.(\w+)\s*=\s*S\.\1\s+AND\s+T\._elt_account_name\s*=\s*S\._elt_account_name"
)
QUALIFY_PARTITION_RE = re.compile(
    r"QUALIFY\s+ROW_NUMBER\(\)\s+OVER\s*\(\s*"
    r"PARTITION BY\s+(?:SAFE_CAST\()?JSON_VALUE\(jp,\s*'\$\.(\w+)'\)(?:\s+AS\s+\w+\))?"
    r"\s*,\s*_elt_account_name",
    re.IGNORECASE,
)

assert MERGE_SQL_FILES, f"no merge_*.sql files found under {SQL_DIR}"


@pytest.mark.parametrize(
    "sql_path", MERGE_SQL_FILES, ids=[p.name for p in MERGE_SQL_FILES]
)
def test_dedup_partition_key_matches_merge_join_key(sql_path):
    text = sql_path.read_text(encoding="utf-8")

    on_match = ON_CLAUSE_RE.search(text)
    assert on_match, (
        f"{sql_path.name}: MERGE ON clause must join on <key> AND _elt_account_name"
    )

    qualify_match = QUALIFY_PARTITION_RE.search(text)
    assert qualify_match, (
        f"{sql_path.name}: QUALIFY dedup must partition by <key>, _elt_account_name"
    )

    on_key, qualify_key = on_match.group(1), qualify_match.group(1)
    assert on_key == qualify_key, (
        f"{sql_path.name}: dedup partitions on '{qualify_key}' but MERGE joins on '{on_key}' -- "
        "a batch with two accounts sharing a business key would dedup against the wrong row"
    )


@pytest.mark.parametrize(
    "sql_path", MERGE_SQL_FILES, ids=[p.name for p in MERGE_SQL_FILES]
)
def test_source_query_only_considers_rows_with_a_non_null_key(sql_path):
    text = sql_path.read_text(encoding="utf-8")
    on_key = ON_CLAUSE_RE.search(text).group(1)

    assert re.search(
        rf"WHERE\s+(?:SAFE_CAST\()?JSON_VALUE\(jp,\s*'\$\.{on_key}'\)(?:\s+AS\s+\w+\))?\s+IS NOT NULL",
        text,
    ), (
        f"{sql_path.name}: source query must filter out rows where '{on_key}' failed to parse, "
        "otherwise every NULL-keyed row collapses into a single QUALIFY partition"
    )
