"""Unit tests for include/config/ga4_settings.py.

Scoped to the ticket's categories -- account/property fallback logic, and MERGE
key/dedup correctness -- plus the GCS path helpers (kept for parity with
tests/include/config/test_solidgate_settings.py's own ``landing_object``/
``release_object`` tests). Covers:
  * ``property_slug``/``landing_object``/``release_object``/``_zone_object`` -- the
    GCS path-shape helpers (the GA4 equivalent of Solidgate's ``landing_object``/
    ``release_object``).
  * ``merge_key_columns``/``period_merge_key_columns`` -- the bronze MERGE upsert key.
    ``tests/sql/test_ga4_merge_sql.py`` cross-checks these against the actual rendered
    SQL, the same drift class that bugfix/solidgate-merge-sql guarded against.

(Avro schema field-ordering and table-name-builder tests were dropped as out of the
ticket's scope -- ``build_release_schema``/``build_period_schema`` are still imported
here only because the kept merge-key tests use them to assert the key is a prefix of
the schema's field list.)
"""

from __future__ import annotations

from include.config import ga4_settings
from include.config.ga4_settings import (
    build_period_schema,
    build_release_schema,
    landing_object,
    merge_key_columns,
    period_merge_key_columns,
    property_slug,
    release_object,
)


class TestPropertySlug:
    def test_wraps_property_id_in_analytics_prefix(self):
        assert property_slug("518695982") == "analytics_518695982"

    def test_works_with_non_numeric_ids_too(self):
        assert property_slug("abc-123") == "analytics_abc-123"


class TestGcsPathHelpers:
    def test_zone_object_path_shape(self):
        assert (
            ga4_settings._zone_object(
                "landing",
                "analytics_123",
                "overview_daily",
                "2026-08-18",
                "overview_daily_2026-08-18.csv",
            )
            == "landing/ga4/analytics_123/overview_daily/2026-08-18/overview_daily_2026-08-18.csv"
        )

    def test_landing_object_uses_the_landing_prefix(self):
        assert landing_object(
            "analytics_123",
            "overview_daily",
            "2026-08-18",
            "overview_daily_2026-08-18.csv",
        ) == (
            "landing/ga4/analytics_123/overview_daily/2026-08-18/overview_daily_2026-08-18.csv"
        )

    def test_release_object_uses_the_release_prefix(self):
        assert release_object(
            "analytics_123",
            "overview_daily",
            "2026-08-18",
            "overview_daily_2026-08-18.avro",
        ) == (
            "release/ga4/analytics_123/overview_daily/2026-08-18/overview_daily_2026-08-18.avro"
        )

    def test_landing_and_release_agree_on_everything_but_the_zone(self):
        args = ("analytics_999", "session_source_medium", "2026-01-05", "f.avro")
        landing = landing_object(*args)
        release = release_object(*args)
        assert landing.replace("landing/", "", 1) == release.replace("release/", "", 1)


class TestMergeKeyColumns:
    def test_no_dimensions_key_is_just_property_and_date(self):
        assert merge_key_columns({}) == ["property_id", "date"]

    def test_dimensions_are_appended_after_property_and_date(self):
        spec = {"dimensions": ["sessionSourceMedium"]}
        assert merge_key_columns(spec) == ["property_id", "date", "sessionSourceMedium"]

    def test_multiple_dimensions_preserve_spec_order(self):
        spec = {"dimensions": ["dimB", "dimA"]}
        assert merge_key_columns(spec) == ["property_id", "date", "dimB", "dimA"]

    def test_key_columns_are_a_prefix_of_the_release_schema_dimension_block(self):
        # The upsert key must never include a metric/derived column -- only the grain
        # (property_id, date) plus the report's own dimensions.
        spec = {"dimensions": ["sessionSourceMedium"], "metrics": ["sessions"]}
        schema_names = [f["name"] for f in build_release_schema(spec)["fields"]]
        key = merge_key_columns(spec)
        assert schema_names[: len(key)] == key


class TestPeriodMergeKeyColumns:
    def test_fixed_window_grain_key(self):
        assert period_merge_key_columns() == [
            "property_id",
            "period_start_date",
            "period_end_date",
        ]

    def test_is_a_prefix_of_the_period_schema_fields(self):
        schema_names = [
            f["name"]
            for f in build_period_schema({"metrics": ["activeUsers"]})["fields"]
        ]
        key = period_merge_key_columns()
        assert schema_names[: len(key)] == key


class TestReservedColumnCollisionIsNotGuardedAgainst:
    """Documents a suspected real gap (include/config/ga4_settings.py:80-88): neither
    ``build_release_schema`` nor ``merge_key_columns`` validates that a report's
    ``dimensions`` list avoids the fixed grain/metadata column names ("date",
    "property_id", "_elt_insert_timestamp", "_elt_update_timestamp", "_gcs_file_path").
    Every ``_REPORTS`` entry in dags/ga4/ga4_daily_source_snapshots.py currently avoids
    this by convention/comment only ("The `date` dimension is added automatically; do
    NOT list it here."). If a future report spec ever lists "date" as one of its own
    dimensions, this currently produces a schema with a duplicate Avro field name and a
    duplicate entry in the merge key -- neither function raises. This test pins the
    CURRENT (unguarded) behavior; it is not asserting that behavior is desirable.
    """

    def test_dimension_named_date_produces_a_duplicate_named_avro_field(self):
        schema = build_release_schema({"dimensions": ["date"]})
        names = [f["name"] for f in schema["fields"]]
        assert names.count("date") == 2

    def test_dimension_named_date_produces_a_duplicate_entry_in_the_merge_key(self):
        assert merge_key_columns({"dimensions": ["date"]}) == [
            "property_id",
            "date",
            "date",
        ]
