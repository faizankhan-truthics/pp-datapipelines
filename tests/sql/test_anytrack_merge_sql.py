"""Regression tests for include/sql/anytrack/merge_events_all_events.sql.

Unlike Solidgate/Stripe/ChargebackHit, AnyTrack has no merchant/account column to
scope the merge by -- each product gets its own bronze table
(``anytrack_<product>_events``, see dags/anytrack/events_dag.py), so cross-product
collisions aren't possible at the SQL level the way cross-account collisions are for
the other sources. The invariant that matters here instead is the file's own header
comment: ``clickId`` is deliberately NOT the merge key (it's reused across multiple
events in a customer's attribution chain), so ``transactionId`` must be the key on
both sides of the QUALIFY/ON dedup, and the latest event per transaction (by
eventTimestamp) must be the one that survives -- except events with no transactionId
at all, which have no dedup key and must always pass through untouched instead of
being dropped or collapsed together.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

SQL_PATH = (
    Path(__file__).parents[2]
    / "include"
    / "sql"
    / "anytrack"
    / "merge_events_all_events.sql"
)


def _text() -> str:
    return SQL_PATH.read_text(encoding="utf-8")


class TestMergeJoinKey:
    def test_on_clause_joins_on_transaction_id(self):
        assert re.search(r"ON\s+T\.transaction_id\s*=\s*S\.transaction_id", _text())

    def test_click_id_is_not_used_as_the_merge_key(self):
        # Pins the file's own documented reasoning: clickId is reused across multiple
        # events for the same customer's attribution chain, so it can't be a
        # per-event key. If someone "simplifies" the ON clause to click_id later,
        # this must fail loudly.
        assert not re.search(r"ON\s+T\.click_id\s*=\s*S\.click_id", _text())

    def test_qualify_partitions_on_transaction_id_json_path(self):
        text = _text()
        match = re.search(
            r"QUALIFY\s+JSON_VALUE\(jp,\s*'\$\.transactionId'\)\s+IS NULL\s*"
            r"OR\s+ROW_NUMBER\(\)\s+OVER\s*\(\s*PARTITION BY\s+JSON_VALUE\(jp,\s*'\$\.transactionId'\)",
            text,
        )
        assert match, (
            "QUALIFY dedup must partition by the same transactionId the ON clause joins on"
        )

    def test_qualify_keeps_the_latest_event_by_event_timestamp(self):
        # The header comment says this keeps "the latest state per transaction" --
        # verify the ORDER BY actually points at eventTimestamp DESC (not, say,
        # insert order), immediately followed by `= 1` to keep exactly one row.
        text = _text()
        assert re.search(
            r"PARTITION BY\s+JSON_VALUE\(jp,\s*'\$\.transactionId'\)\s*"
            r"ORDER BY\s+SAFE_CAST\(JSON_VALUE\(jp,\s*'\$\.eventTimestamp'\)\s+AS\s+INT64\)\s+DESC"
            r"\s*\)\s*=\s*1",
            text,
        )

    def test_events_without_a_transaction_id_bypass_the_dedup_partition(self):
        # There's no reliable key to dedup a null-transactionId event against, so
        # every one of them must pass the QUALIFY untouched rather than being
        # dropped (the old merge_events.sql behavior) or collapsed into a single
        # row per batch (what a naive PARTITION BY NULL would otherwise do, since
        # BigQuery groups every NULL into the same partition).
        assert re.search(
            r"QUALIFY\s+JSON_VALUE\(jp,\s*'\$\.transactionId'\)\s+IS NULL\s*\n\s*OR\s+ROW_NUMBER\(\)",
            _text(),
        ), (
            "events with no transactionId must bypass the ROW_NUMBER() dedup partition entirely, "
            "not be filtered out or collapsed together"
        )


class TestProductScopedTable:
    def test_target_table_name_is_parameterized_by_product(self):
        assert (
            "`{{ params.bq_project }}.{{ params.bq_bronze_dataset }}.anytrack_{{ params.product }}_events`"
            in _text()
        )

    def test_staging_source_table_name_is_parameterized_by_product(self):
        assert (
            "`{{ params.bq_project }}.{{ params.bq_staging_dataset }}.anytrack_{{ params.product }}_events`"
            in _text()
        )

    def test_create_table_if_not_exists_targets_the_same_product_scoped_table(self):
        text = _text()
        create_match = re.search(r"CREATE TABLE IF NOT EXISTS\s+`([^`]+)`", text)
        merge_match = re.search(r"MERGE\s+`([^`]+)`\s+T\b", text)
        assert create_match and merge_match
        assert create_match.group(1) == merge_match.group(1), (
            "CREATE TABLE and MERGE must target the exact same product-scoped table name"
        )

    def test_transaction_id_column_is_nullable(self):
        # Unlike the old merge_events.sql, transaction_id must NOT be NOT NULL here --
        # not every event carries one.
        text = _text()
        assert "transaction_id STRING NOT NULL" not in text
        assert "transaction_id STRING," in text


class TestRequiredPlaceholders:
    def test_all_five_placeholders_appear_at_least_once(self):
        text = _text()
        for placeholder in (
            "{{ params.bq_project }}",
            "{{ params.bq_staging_dataset }}",
            "{{ params.bq_bronze_dataset }}",
            "{{ params.product }}",
            "{{ params.source }}",
        ):
            assert placeholder in text, f"missing required placeholder: {placeholder}"


class TestKnownGapFieldsAreCaptured:
    """externalClickId/referrer/linkId/linkURL/linklabel -- confirmed present in real
    live payloads (2026-09 investigation) but not captured by any column until now."""

    @pytest.mark.parametrize(
        "json_path,column",
        [
            ("$.externalClickId", "external_click_id"),
            ("$.referrer", "referrer"),
            ("$.linkId", "link_id"),
            ("$.linkURL", "link_url"),
            ("$.linklabel", "link_label"),
        ],
    )
    def test_field_is_extracted_into_its_own_column(self, json_path, column):
        text = _text()
        assert f"{column} STRING" in text, f"{column} must be declared in CREATE TABLE"
        assert re.search(
            rf"JSON_VALUE\(jp,\s*'{re.escape(json_path)}'\)\s+AS\s+{column}\b", text
        ), f"{column} must be extracted from {json_path}"
        assert f"T.{column} = S.{column}" in text, f"{column} must be updated on match"
        # INSERT's column/value lists wrap across lines, so a column name can be
        # preceded by a newline+indent instead of a literal ", " -- \s* covers both.
        assert re.search(rf",\s*{column}\s*,", text), (
            f"{column} must appear in the INSERT column list"
        )
        assert re.search(rf"S\.{column}\b", text), (
            f"{column} must appear in the INSERT VALUES list"
        )


class TestRawPayloadSafetyNet:
    def test_raw_payload_column_is_native_json_type(self):
        assert "_raw_payload JSON" in _text()

    def test_raw_payload_stores_the_entire_parsed_object_not_a_sub_path(self):
        # The whole point is a permanent safety net against fields no column
        # covers -- it must be the full `jp`, not JSON_QUERY(jp, '$.somePath')
        # extracting only part of it.
        assert re.search(r"\bjp\s+AS\s+_raw_payload\b", _text()), (
            "_raw_payload must be assigned the entire parsed payload (jp), not a "
            "JSON_QUERY/JSON_VALUE extraction of part of it"
        )

    def test_raw_payload_is_carried_through_update_and_insert(self):
        text = _text()
        assert "T._raw_payload = S._raw_payload" in text
        assert "S._raw_payload" in text


class TestSourceMarker:
    def test_source_column_is_nullable(self):
        # Not NOT NULL: BigQuery won't allow ADD COLUMN ... NOT NULL on a
        # table that already has rows, so a freshly-created table (via
        # CREATE TABLE) and an already-existing one (via the ALTER TABLE
        # below) must end up with the same nullability, or schemas drift
        # apart depending on which path happened to provision a given table.
        text = _text()
        assert "_source STRING NOT NULL" not in text
        assert "_source STRING," in text

    def test_source_is_populated_from_the_source_param(self):
        assert re.search(r"'\{\{\s*params\.source\s*\}\}'\s+AS\s+_source\b", _text())


class TestSelfHealingSchema:
    """CREATE TABLE IF NOT EXISTS only provisions a brand-new table -- it's a
    no-op for every product's table today, since they all already exist. Without
    an ALTER TABLE, adding a column here silently breaks the MERGE for every
    existing table until someone remembers to migrate each environment by hand
    (this happened once already, 2026-09). ADD COLUMN IF NOT EXISTS is what
    makes that automatic and safe to run on every single execution."""

    def test_alter_table_targets_the_same_product_scoped_table(self):
        text = _text()
        alter_match = re.search(r"ALTER TABLE\s+`([^`]+)`", text)
        merge_match = re.search(r"MERGE\s+`([^`]+)`\s+T\b", text)
        assert alter_match and merge_match
        assert alter_match.group(1) == merge_match.group(1), (
            "ALTER TABLE must target the exact same product-scoped table as MERGE"
        )

    def test_alter_table_runs_before_the_merge(self):
        text = _text()
        alter_pos = text.find("ALTER TABLE")
        merge_pos = text.find("MERGE `")
        assert alter_pos != -1 and merge_pos != -1
        assert alter_pos < merge_pos, (
            "the schema migration must run before the MERGE that depends on it"
        )

    @pytest.mark.parametrize(
        "column,column_type",
        [
            ("external_click_id", "STRING"),
            ("referrer", "STRING"),
            ("link_id", "STRING"),
            ("link_url", "STRING"),
            ("link_label", "STRING"),
            ("_raw_payload", "JSON"),
            ("_source", "STRING"),
        ],
    )
    def test_every_new_column_is_added_idempotently(self, column, column_type):
        # Every column introduced alongside this ALTER TABLE must actually be
        # in it -- otherwise a table that predates one of them breaks the same
        # way iqcenter's did, just for whichever column got left out.
        assert re.search(
            rf"ADD COLUMN IF NOT EXISTS\s+{re.escape(column)}\s+{column_type}\b",
            _text(),
        ), f"{column} must be added via idempotent ADD COLUMN IF NOT EXISTS"

    def test_source_is_carried_through_update_and_insert(self):
        text = _text()
        assert "T._source = S._source" in text
        assert "S._source" in text
