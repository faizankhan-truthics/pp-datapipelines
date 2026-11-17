"""Unit tests for include/config/external_setting.py.

build_schema_fields() turns the header row of a manually-maintained reference CSV into
an all-STRING BigQuery schema for dags/external/external_files_to_bronze_dag.py. Header
cells are typed by hand in a spreadsheet, so they come with spaces, punctuation, leading
digits, blanks and duplicates -- each of which BigQuery would reject as a column name,
failing the whole load. Covers sanitize_column_name()'s rules, de-duplication, and the
has_csv_files() short-circuit gate.
"""

from __future__ import annotations

import re

import pytest

from include.config.external_setting import (
    build_schema_fields,
    has_csv_files,
    sanitize_column_name,
)

BQ_COLUMN_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class TestSanitizeColumnName:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("country_code", "country_code"),
            ("Country Code", "Country_Code"),
            ("  padded  ", "padded"),
            ("Revenue (USD)", "Revenue_USD"),
            ("a--b..c", "a_b_c"),
            ("__leading_and_trailing__", "leading_and_trailing"),
            ("2024 total", "_2024_total"),
            ("﻿id", "id"),
            ("Prix €", "Prix"),
        ],
    )
    def test_cleans_into_a_valid_bigquery_name(self, raw, expected):
        assert sanitize_column_name(raw, 0) == expected

    @pytest.mark.parametrize("raw", ["", "   ", "%%%", "€"])
    def test_blank_or_all_invalid_falls_back_to_positional_name(self, raw):
        assert sanitize_column_name(raw, 3) == "column_3"


class TestBuildSchemaFields:
    def test_every_column_is_a_nullable_string(self):
        fields = build_schema_fields(["id", "Amount", "Date"])

        assert fields == [
            {"name": "id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Amount", "type": "STRING", "mode": "NULLABLE"},
            {"name": "Date", "type": "STRING", "mode": "NULLABLE"},
        ]

    def test_duplicate_names_get_numbered_suffixes(self):
        names = [
            f["name"] for f in build_schema_fields(["Name", "Name", "Na-me", "Name"])
        ]

        assert names == ["Name", "Name_1", "Na_me", "Name_2"]

    def test_names_colliding_only_after_sanitising_are_deduplicated(self):
        names = [
            f["name"]
            for f in build_schema_fields(
                ["Total Revenue", "Total-Revenue", "Total_Revenue"]
            )
        ]

        assert names == ["Total_Revenue", "Total_Revenue_1", "Total_Revenue_2"]

    def test_messy_real_world_header_yields_valid_names(self):
        header = ["", "ID", "1st Seen", "e-mail", "e mail", "", "%"]

        names = [f["name"] for f in build_schema_fields(header)]

        assert len(names) == len(header)
        assert all(BQ_COLUMN_NAME.match(n) for n in names), names
        assert len(set(names)) == len(names), names

    def test_empty_header_gives_empty_schema(self):
        assert build_schema_fields([]) == []


class TestHasCsvFiles:
    def test_false_when_no_files(self):
        assert has_csv_files([]) is False

    def test_true_when_files_found(self):
        assert has_csv_files([{"source_objects": ["a.csv"]}]) is True
