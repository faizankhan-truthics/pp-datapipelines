"""Regression tests for the Zendesk Bronze MERGE key/dedup consistency.

Every include/sql/zendesk/merge_*.sql file dedups its staging batch with
``QUALIFY ROW_NUMBER() OVER (PARTITION BY <business_key> ...)`` and then joins on
``ON T.<business_key> = S.<business_key>``. Those two key sets MUST match -- if the
QUALIFY partition key and the MERGE ON key ever diverge, the dedup collapses rows
that the MERGE then can't correctly match up, silently dropping or misattributing
records. This is exactly the class of bug already fixed once for Solidgate (see
tests/sql/test_solidgate_merge_sql.py, bugfix/solidgate-merge-sql).

Unlike Solidgate's merge SQL, Zendesk's has no ``_elt_account_name``-equivalent
scoping column in the ON clause or QUALIFY partition: this pipeline only ever talks
to a single Zendesk subdomain (``ZENDESK_API_SUBDOMAIN`` is one value, not a list of
accounts like Solidgate's per-merchant credentials), confirmed by reading
include/config/zendesk_settings.py (a single ZENDESK_SUBDOMAIN/EMAIL/API_TOKEN, no
per-account discovery). So these tests intentionally do NOT require account scoping
the way the Solidgate ones do -- only that the natural key (``id``) matches between
QUALIFY and ON.
"""

import re
from pathlib import Path

import pytest

SQL_DIR = Path(__file__).parents[2] / "include" / "sql" / "zendesk"
MERGE_SQL_FILES = sorted(SQL_DIR.glob("merge_*.sql"))

ON_CLAUSE_RE = re.compile(r"ON\s+T\.(\w+)\s*=\s*S\.\1\b")
QUALIFY_PARTITION_RE = re.compile(
    r"QUALIFY\s+ROW_NUMBER\(\)\s+OVER\s*\(\s*PARTITION BY\s+JSON_VALUE\(jp,\s*'\$\.(\w+)'\)",
    re.IGNORECASE,
)

assert MERGE_SQL_FILES, f"no merge_*.sql files found under {SQL_DIR}"
assert len(MERGE_SQL_FILES) == 3, (
    f"expected 3 Zendesk merge SQL files, found {[p.name for p in MERGE_SQL_FILES]}"
)


@pytest.mark.parametrize(
    "sql_path", MERGE_SQL_FILES, ids=[p.name for p in MERGE_SQL_FILES]
)
def test_dedup_partition_key_matches_merge_join_key(sql_path):
    text = sql_path.read_text(encoding="utf-8")

    on_match = ON_CLAUSE_RE.search(text)
    assert on_match, f"{sql_path.name}: MERGE ON clause must join on T.<key> = S.<key>"

    qualify_match = QUALIFY_PARTITION_RE.search(text)
    assert qualify_match, (
        f"{sql_path.name}: QUALIFY dedup must partition by JSON_VALUE(jp, '$.<key>')"
    )

    on_key, qualify_key = on_match.group(1), qualify_match.group(1)
    assert on_key == qualify_key, (
        f"{sql_path.name}: dedup partitions on '{qualify_key}' but MERGE joins on '{on_key}' -- "
        "rows would be deduped against the wrong partition before the MERGE ever sees them"
    )


@pytest.mark.parametrize(
    "sql_path", MERGE_SQL_FILES, ids=[p.name for p in MERGE_SQL_FILES]
)
def test_source_query_only_considers_rows_with_a_non_null_key(sql_path):
    text = sql_path.read_text(encoding="utf-8")
    on_key = ON_CLAUSE_RE.search(text).group(1)

    assert re.search(
        rf"WHERE\s+JSON_VALUE\(jp,\s*'\$\.{on_key}'\)\s+IS NOT NULL", text
    ), (
        f"{sql_path.name}: source query must filter out rows where '{on_key}' failed to parse, "
        "otherwise every NULL-keyed row collapses into a single QUALIFY partition"
    )


@pytest.mark.parametrize(
    "sql_path", MERGE_SQL_FILES, ids=[p.name for p in MERGE_SQL_FILES]
)
def test_join_key_is_id_for_every_zendesk_entity(sql_path):
    # Every Zendesk entity here (tickets, ticket comments, users) has its own
    # numeric `id` as the natural key -- unlike, say, ticket_comments' `ticket_id`,
    # which identifies the parent, not the comment row itself.
    text = sql_path.read_text(encoding="utf-8")
    on_key = ON_CLAUSE_RE.search(text).group(1)
    assert on_key == "id", (
        f"{sql_path.name}: expected MERGE ON key 'id', got '{on_key}'"
    )


def test_merge_ticket_comments_has_no_when_matched_clause():
    """Comments are immutable once posted (no edit event in the Zendesk audit
    trail this pipeline consumes), so merge_ticket_comments.sql intentionally has
    only a WHEN NOT MATCHED THEN INSERT -- no WHEN MATCHED THEN UPDATE. If a future
    edit adds one without a real update-in-place use case, that's worth a second
    look rather than assuming it's a mistake this test should silently allow either
    way -- so pin the current, deliberate shape.
    """
    text = (SQL_DIR / "merge_ticket_comments.sql").read_text(encoding="utf-8")
    assert "WHEN NOT MATCHED THEN" in text
    assert "WHEN MATCHED THEN" not in text


_MUTABLE_ENTITY_FILES = [
    p for p in MERGE_SQL_FILES if p.name != "merge_ticket_comments.sql"
]


@pytest.mark.parametrize(
    "sql_path", _MUTABLE_ENTITY_FILES, ids=[p.name for p in _MUTABLE_ENTITY_FILES]
)
def test_mutable_entities_have_a_when_matched_update_clause(sql_path):
    # tickets and users can change status/fields after creation, so their merge
    # scripts must update in place, not only insert-once-then-ignore.
    text = sql_path.read_text(encoding="utf-8")
    assert "WHEN MATCHED THEN" in text, (
        f"{sql_path.name}: mutable entity must support WHEN MATCHED UPDATE"
    )
