"""Regression tests for include/sql/ga4/merge_ga4.sql.

Unlike the Solidgate merge_*.sql scripts (which hardcode a ``QUALIFY ROW_NUMBER() OVER
(PARTITION BY <business_key>, _elt_account_name ...)`` dedup and a textually separate
``ON T.<key> = S.<key> AND T._elt_account_name = S._elt_account_name`` join -- two
independent hardcoded key lists that can drift, see tests/sql/test_solidgate_merge_sql.py),
GA4 uses ONE generic script for every report entity. The upsert key columns are not
hardcoded in this file at all: they arrive as the ``__KEY_COLS__`` placeholder,
substituted at render time (``_render_merge_sql`` in dags/ga4/ga4_daily_source_snapshots.py)
directly from ``include.config.ga4_settings.merge_key_columns``/``period_merge_key_columns``.
Both the MERGE's ``ON`` clause and its payload-column (``data_cols``) exclusion are then
derived from that SAME ``key_cols`` array inside the script.

These tests pin that architecture down:
  * the ON clause and the data_cols exclusion both read from ``UNNEST(key_cols)`` (a
    single source of truth -- there is no second, independently hardcoded key list to
    drift from, which is exactly the class of bug the Solidgate merge-sql tests guard
    against).
  * ``settings.merge_key_columns``/``period_merge_key_columns`` render into the
    ``DECLARE key_cols`` line exactly as ``_render_merge_sql`` would substitute them, so
    a future change to either side (the Python key-builder or the SQL template) that
    breaks the pairing is caught here instead of at MERGE-execution time in BigQuery.
  * the empty-staging guard (a report that returned zero rows must not error) is present.

A real observation surfaced while writing these tests (not papered over, see the class
at the bottom): merge_ga4.sql has no dedup step at all before the MERGE -- if staging
ever contains two rows sharing a key (e.g. overlapping backfill windows), BigQuery's
MERGE will raise at execution time rather than being silently deduped like Solidgate's
QUALIFY step would. Under the current ``build_windows`` (non-overlapping daily
windows), this should not occur in practice, but there is no defensive guard for it.
"""

from __future__ import annotations

import re
from pathlib import Path

from include.config import ga4_settings

SQL_PATH = Path(__file__).parents[2] / "include" / "sql" / "ga4" / "merge_ga4.sql"

assert SQL_PATH.exists(), f"merge_ga4.sql not found at {SQL_PATH}"


def _sql_text() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


def _render_key_cols(key_cols: list[str]) -> str:
    """Reproduce _render_merge_sql's __KEY_COLS__ substitution formula exactly.

    Mirrors dags/ga4/ga4_daily_source_snapshots.py::_render_merge_sql's
    ``.replace("__KEY_COLS__", ", ".join(f"'{c}'" for c in key_cols))`` without
    importing the DAG module (which would trigger full DAG parsing/Variable.get calls
    at import time -- out of scope for a settings/SQL-template regression test).
    """
    return ", ".join(f"'{c}'" for c in key_cols)


class TestKeyColsIsTheSingleSourceOfTruth:
    def test_key_cols_placeholder_feeds_a_declared_array_variable(self):
        text = _sql_text()
        assert "DECLARE key_cols ARRAY<STRING> DEFAULT [__KEY_COLS__];" in text

    def test_on_clause_is_built_from_the_key_cols_array_not_hardcoded_columns(self):
        text = _sql_text()
        match = re.search(
            r"SET on_clause = \(\s*"
            r"SELECT STRING_AGG\(FORMAT\('T\.`%s` = S\.`%s`', c, c\), ' AND '\)\s*"
            r"FROM UNNEST\(key_cols\) AS c\s*\);",
            text,
        )
        assert match, (
            "on_clause must be derived from UNNEST(key_cols), not a hardcoded column list"
        )

    def test_data_cols_excludes_exactly_the_key_cols_array_not_a_second_hardcoded_list(
        self,
    ):
        text = _sql_text()
        # This is the drift-prone spot: if a future edit swaps this for a hardcoded
        # column list instead of referencing key_cols, the payload columns and the
        # upsert key could silently diverge from settings.merge_key_columns().
        assert re.search(
            r"AND column_name NOT IN \(SELECT c FROM UNNEST\(key_cols\) AS c\)", text
        ), (
            "data_cols exclusion must reference the same key_cols array used by the ON clause"
        )

    def test_update_clause_and_insert_lists_are_built_from_data_cols_not_key_cols(self):
        text = _sql_text()
        assert re.search(
            r"SET update_clause = \(\s*SELECT STRING_AGG\(FORMAT\('T\.`%s` = S\.`%s`', c, c\), ', '\)\s*"
            r"FROM UNNEST\(data_cols\) AS c\s*\);",
            text,
        ), (
            "update_clause must update the payload columns (data_cols), never the key columns"
        )
        assert "UNNEST(ARRAY_CONCAT(key_cols, data_cols))" in text, (
            "INSERT column/value lists must cover key_cols + data_cols together"
        )


class TestRequiredPlaceholdersArePresent:
    """If any of these placeholders is renamed/removed in the SQL without a matching
    change in _render_merge_sql (or vice versa), the substitution silently no-ops and
    the literal placeholder text ships into BigQuery as invalid SQL. Pinning their
    presence here catches that drift at test time instead of at DAG-run time.
    """

    def test_all_five_placeholders_appear_at_least_once(self):
        text = _sql_text()
        for placeholder in (
            "__STAGING_TABLE__",
            "__BRONZE_TABLE__",
            "__STAGING_DATASET__",
            "__ENTITY__",
            "__KEY_COLS__",
        ):
            assert placeholder in text, f"{placeholder} missing from merge_ga4.sql"


class TestSettingsMergeKeyColumnsMatchesTheRenderedSql:
    """Cross-checks ga4_settings.merge_key_columns/period_merge_key_columns against the
    actual DECLARE line they render into -- the GA4 analogue of
    tests/sql/test_solidgate_merge_sql.py's QUALIFY-vs-ON drift check, adapted to the
    fact that GA4's key is parameterized rather than duplicated in the SQL text.
    """

    def test_per_day_report_key_renders_into_the_declare_line(self):
        spec = {"dimensions": ["sessionSourceMedium"]}
        key_cols = ga4_settings.merge_key_columns(spec)

        rendered = _sql_text().replace("__KEY_COLS__", _render_key_cols(key_cols))

        assert (
            "DECLARE key_cols ARRAY<STRING> DEFAULT "
            "['property_id', 'date', 'sessionSourceMedium'];"
        ) in rendered

    def test_no_dimension_report_key_renders_into_the_declare_line(self):
        spec = {}
        key_cols = ga4_settings.merge_key_columns(spec)

        rendered = _sql_text().replace("__KEY_COLS__", _render_key_cols(key_cols))

        assert (
            "DECLARE key_cols ARRAY<STRING> DEFAULT ['property_id', 'date'];"
            in rendered
        )

    def test_period_totals_report_key_renders_into_the_declare_line(self):
        key_cols = ga4_settings.period_merge_key_columns()

        rendered = _sql_text().replace("__KEY_COLS__", _render_key_cols(key_cols))

        assert (
            "DECLARE key_cols ARRAY<STRING> DEFAULT "
            "['property_id', 'period_start_date', 'period_end_date'];"
        ) in rendered

    def test_placeholder_is_fully_consumed_after_substitution(self):
        # If merge_key_columns() or the render formula ever produced a value that
        # didn't fully replace the placeholder (e.g. a typo'd placeholder name), this
        # would catch the leftover literal "__KEY_COLS__" surviving into the SQL.
        key_cols = ga4_settings.merge_key_columns(
            {"dimensions": ["sessionSourceMedium"]}
        )
        rendered = _sql_text().replace("__KEY_COLS__", _render_key_cols(key_cols))
        assert "__KEY_COLS__" not in rendered


class TestEmptyStagingGuard:
    def test_zero_row_staging_is_skipped_without_error(self):
        text = _sql_text()
        assert "IF staging_rows = 0 THEN" in text
        assert re.search(
            r"SELECT FORMAT\('ga4 bronze merge skipped \(empty staging\): %s', '__ENTITY__'\)",
            text,
        )

    def test_merge_only_executes_in_the_else_branch(self):
        text = _sql_text()
        if_index = text.index("IF staging_rows = 0 THEN")
        else_index = text.index("ELSE", if_index)
        end_if_index = text.index("END IF;", else_index)
        merge_section = text[else_index:end_if_index]
        assert "EXECUTE IMMEDIATE merge_sql;" in merge_section


class TestNoDedupBeforeMerge:
    """Documents a suspected real gap (include/sql/ga4/merge_ga4.sql): unlike every
    Solidgate merge_*.sql, this script does not deduplicate staging rows (no QUALIFY
    ROW_NUMBER() step) before the MERGE. The USING clause reads the staging table
    directly. If staging were ever loaded with two rows sharing the same key (e.g. an
    overlapping-window backfill, or two runs' release files landing in the same load),
    BigQuery's MERGE raises at execution time instead of silently keeping one row the
    way Solidgate's dedup would. This test pins the CURRENT (undeduped) shape of the
    USING clause; it is not asserting that shape is safe for every possible caller.
    """

    def test_using_clause_reads_the_staging_table_directly_with_no_qualify_subquery(
        self,
    ):
        text = _sql_text()
        assert "QUALIFY" not in text
        assert "USING `__STAGING_TABLE__` AS S" in text
