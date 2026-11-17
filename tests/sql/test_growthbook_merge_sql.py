"""Regression tests for the GrowthBook Bronze MERGE.

Same key/dedup invariant as tests/sql/test_zendesk_merge_sql.py: the QUALIFY dedup partition key
and the MERGE ON key must match, or rows are deduped against the wrong partition before the
MERGE sees them. Every GrowthBook feature is keyed on its own `id` (the flag key), and the
pipeline talks to a single GrowthBook organization, so no account scoping is needed.

The privacy tests are text checks; the redaction itself was run against BigQuery with fake flags
holding emails in conditions and in an environment definition (see dags/growthbook/README.md).
"""

import re
from pathlib import Path

import pytest

SQL_DIR = Path(__file__).parents[2] / "include" / "sql" / "growthbook"
MERGE_SQL_FILES = sorted(SQL_DIR.glob("merge_*.sql"))
FEATURES_SQL = SQL_DIR / "merge_features.sql"

ON_CLAUSE_RE = re.compile(r"ON\s+T\.(\w+)\s*=\s*S\.\1\b")
QUALIFY_PARTITION_RE = re.compile(
    r"QUALIFY\s+ROW_NUMBER\(\)\s+OVER\s*\(\s*PARTITION BY\s+JSON_VALUE\(jp,\s*'\$\.(\w+)'\)",
    re.IGNORECASE,
)

PROJECTS_SQL = SQL_DIR / "merge_projects.sql"

assert [p.name for p in MERGE_SQL_FILES] == [
    "merge_features.sql",
    "merge_projects.sql",
], [p.name for p in MERGE_SQL_FILES]


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

    assert on_match.group(1) == qualify_match.group(1) == "id"


@pytest.mark.parametrize(
    "sql_path", MERGE_SQL_FILES, ids=[p.name for p in MERGE_SQL_FILES]
)
def test_source_query_only_considers_rows_with_a_non_null_key(sql_path):
    assert "WHERE JSON_VALUE(jp, '$.id') IS NOT NULL" in sql_path.read_text(
        encoding="utf-8"
    )


@pytest.mark.parametrize(
    "sql_path", MERGE_SQL_FILES, ids=[p.name for p in MERGE_SQL_FILES]
)
def test_never_overwrites_newer_state_with_older(sql_path):
    text = sql_path.read_text(encoding="utf-8")
    assert (
        "WHEN MATCHED AND (T.date_updated IS NULL OR S.date_updated >= T.date_updated) THEN"
        in text
    )


def test_projects_never_store_the_owner_email_or_settings():
    text = PROJECTS_SQL.read_text(encoding="utf-8")
    for field in ("ownerEmail", "settings"):
        assert f"'$.{field}'" not in text, field
    assert "owner_email" not in text
    assert "'$.owner'" in text  # the user id is kept, like in growthbook_features


def test_rule_targeting_is_dropped():
    # Rule conditions, saved groups and prerequisites can hold user ids or emails.
    text = FEATURES_SQL.read_text(encoding="utf-8")
    assert "JSON_QUERY(jp, '$.rules') AS rules" not in text
    assert (
        "JSON_REMOVE(r, '$.condition', '$.savedGroupTargeting', '$.prerequisites')"
        in text
    )


def test_feature_level_prerequisite_conditions_are_dropped():
    # Feature-level prerequisites are documented as flag ids, but a condition on one would carry the same
    # user ids or emails as a rule-level one, so it is stripped if it ever shows up.
    text = FEATURES_SQL.read_text(encoding="utf-8")
    assert "JSON_QUERY(jp, '$.prerequisites') AS prerequisites" not in text
    assert "JSON_REMOVE(p, '$.condition')" in text


def test_missing_or_malformed_rules_land_as_null_not_as_an_empty_array():
    # Without the IF, UNNEST of a NULL array gives `[]`, which looks valid and hides the problem
    # from the dbt shape check. (Verified in BigQuery: a non-array `rules` does not raise.)
    text = FEATURES_SQL.read_text(encoding="utf-8")
    assert "UNNEST(JSON_QUERY_ARRAY(jp, '$.rules'))" not in text
    assert "SAFE.JSON_QUERY_ARRAY(jp, '$.rules')" in text


def test_environment_definition_is_blanked():
    # `definition` is the compiled copy of the rules, conditions included.
    text = FEATURES_SQL.read_text(encoding="utf-8")
    assert "JSON_QUERY(jp, '$.environments') AS environments" not in text
    assert '"definition":null' in text


def test_personal_fields_are_not_stored():
    text = FEATURES_SQL.read_text(encoding="utf-8")
    for field in ("ownerEmail", "customFields", "createdBy", "publishedBy"):
        assert f"'$.{field}'" not in text and f"'$.revision.{field}'" not in text, field
    assert "owner_email" not in text


@pytest.mark.parametrize(
    "sql_path", MERGE_SQL_FILES, ids=[p.name for p in MERGE_SQL_FILES]
)
def test_owner_is_nulled_when_it_looks_like_an_email(sql_path):
    # `owner` is documented as a user id, but the API field is free text.
    text = sql_path.read_text(encoding="utf-8")
    assert (
        "IF(STRPOS(JSON_VALUE(jp, '$.owner'), '@') > 0, NULL, JSON_VALUE(jp, '$.owner')) AS owner"
        in text
    )
