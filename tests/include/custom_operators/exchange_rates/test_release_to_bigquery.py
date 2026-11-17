"""Unit tests for ReleaseToBigQueryOperator
(include/custom_operators/exchange_rates/release_to_bigquery.py).

Generic staging-load + MERGE operator shared by every exchange-rates pipeline.
BigQueryHook/Client are faked below -- no real GCP calls -- but real
``google.cloud.bigquery`` model classes (SchemaField/Dataset/Table/
TimePartitioning) are used directly, since those are plain data objects with
no network behavior of their own.

The highest-value thing pinned here is the dynamically-built MERGE SQL: the
ON clause, UPDATE SET list, and INSERT column/value lists are all assembled
by hand from ``schema``/``natural_key_fields`` in the operator, which is
exactly the kind of string-building logic that silently breaks (e.g. a key
column accidentally included in the UPDATE SET, or INSERT columns/values
drifting out of order relative to each other) without a test pinning it.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

import include.custom_operators.exchange_rates.release_to_bigquery as release_to_bigquery_module
from include.custom_operators.exchange_rates.release_to_bigquery import (
    ReleaseToBigQueryOperator,
)


class _FakeLoadJob:
    def __init__(self, output_rows=5):
        self.output_rows = output_rows

    def result(self):
        return None


class _FakeQueryJob:
    def __init__(self, sql, num_dml_affected_rows=3):
        self.sql = sql
        self.num_dml_affected_rows = num_dml_affected_rows

    def result(self):
        return None


class _FakeBigQueryClient:
    """Stands in for google.cloud.bigquery.Client: tracks every call instead
    of touching real BigQuery, and raises NotFound for get_table() unless the
    table ref was pre-seeded as "existing"."""

    def __init__(self, existing_tables=()):
        self._existing_tables = set(existing_tables)
        self.created_datasets = []
        self.created_tables = []
        self.load_calls = []
        self.queries = []

    def get_table(self, table_ref):
        if table_ref not in self._existing_tables:
            raise NotFound(table_ref)
        return SimpleNamespace(table_ref=table_ref)

    def create_dataset(self, dataset, exists_ok=False):
        self.created_datasets.append((dataset, exists_ok))

    def create_table(self, table):
        self.created_tables.append(table)

    def load_table_from_uri(self, uri, table_ref, job_config=None):
        self.load_calls.append(
            {"uri": uri, "table_ref": table_ref, "job_config": job_config}
        )
        return _FakeLoadJob()

    def query(self, sql):
        self.queries.append(sql)
        return _FakeQueryJob(sql)


def _patch_bigquery_hook(monkeypatch, client: _FakeBigQueryClient):
    monkeypatch.setattr(
        release_to_bigquery_module,
        "BigQueryHook",
        lambda **kwargs: SimpleNamespace(get_client=lambda project_id: client),
    )


_SCHEMA = [
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("currency", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("close", "FLOAT64", mode="REQUIRED"),
    bigquery.SchemaField("source", "STRING"),
]


def _operator(**overrides) -> ReleaseToBigQueryOperator:
    kwargs = dict(
        task_id="release_to_bigquery",
        gcs_bucket="test-bucket",
        release_task_id="landing_to_release",
        bq_project="proj",
        bq_staging_dataset="staging",
        bq_bronze_dataset="bronze",
        bq_table="daily_fx",
        schema=_SCHEMA,
        natural_key_fields=["currency", "date"],
    )
    kwargs.update(overrides)
    return ReleaseToBigQueryOperator(**kwargs)


def _context(release_path="release/fx_bronze/daily_snapshot/2026-01-01.ndjson"):
    return {"ti": SimpleNamespace(xcom_pull=lambda task_ids: release_path)}


class TestNoReleasePath:
    def test_raises_runtime_error_when_upstream_produced_no_release_path(
        self, monkeypatch
    ):
        client = _FakeBigQueryClient()
        _patch_bigquery_hook(monkeypatch, client)
        op = _operator()

        with pytest.raises(RuntimeError, match="landing_to_release"):
            op.execute(_context(release_path=None))

        # Must fail before ever touching BigQuery.
        assert client.created_datasets == []
        assert client.load_calls == []


class TestEnsureTargetTable:
    def test_creates_table_with_partitioning_when_it_does_not_exist(self, monkeypatch):
        client = _FakeBigQueryClient(existing_tables=())
        _patch_bigquery_hook(monkeypatch, client)
        op = _operator(partition_field="date", partition_type="DAY")

        op.execute(_context())

        assert len(client.created_tables) == 1
        created = client.created_tables[0]
        assert created.schema == _SCHEMA
        assert created.time_partitioning.field == "date"
        assert created.time_partitioning.type_ == bigquery.TimePartitioningType.DAY

    def test_does_not_recreate_a_table_that_already_exists(self, monkeypatch):
        target_ref = "proj.bronze.daily_fx"
        client = _FakeBigQueryClient(existing_tables={target_ref})
        _patch_bigquery_hook(monkeypatch, client)
        op = _operator()

        op.execute(_context())

        assert client.created_tables == []

    def test_applies_clustering_fields_when_given(self, monkeypatch):
        client = _FakeBigQueryClient()
        _patch_bigquery_hook(monkeypatch, client)
        op = _operator(clustering_fields=["currency"])

        op.execute(_context())

        assert client.created_tables[0].clustering_fields == ["currency"]

    def test_default_partition_type_is_a_real_bigquery_time_partitioning_type(
        self, monkeypatch
    ):
        """Regression test: the default used to be 'QUARTER', which
        bigquery.TimePartitioningType does not define (only DAY/HOUR/MONTH/YEAR exist) --
        any caller relying on the default would crash with AttributeError the first time
        the target table didn't already exist. No real caller hit this because
        fx_bronze_daily_snapshot_dag.py always passes partition_type explicitly, but the
        default itself must stay valid."""
        client = _FakeBigQueryClient()
        _patch_bigquery_hook(monkeypatch, client)
        op = (
            _operator()
        )  # deliberately not passing partition_type -- exercises the default

        op.execute(_context())

        assert (
            client.created_tables[0].time_partitioning.type_
            == bigquery.TimePartitioningType.DAY
        )


class TestDatasetCreation:
    def test_both_datasets_are_created_idempotently(self, monkeypatch):
        client = _FakeBigQueryClient(existing_tables={"proj.bronze.daily_fx"})
        _patch_bigquery_hook(monkeypatch, client)
        op = _operator()

        op.execute(_context())

        assert len(client.created_datasets) == 2
        # exists_ok=True everywhere -- a rerun must never fail because the dataset
        # from a prior run is already there.
        assert all(exists_ok is True for _ds, exists_ok in client.created_datasets)


class TestLoadAndMerge:
    def test_loads_the_release_uri_into_the_staging_table_with_write_truncate(
        self, monkeypatch
    ):
        client = _FakeBigQueryClient(existing_tables={"proj.bronze.daily_fx"})
        _patch_bigquery_hook(monkeypatch, client)
        op = _operator()

        op.execute(
            _context(release_path="release/fx_bronze/daily_snapshot/2026-03-01.ndjson")
        )

        assert len(client.load_calls) == 1
        load_call = client.load_calls[0]
        assert (
            load_call["uri"]
            == "gs://test-bucket/release/fx_bronze/daily_snapshot/2026-03-01.ndjson"
        )
        assert load_call["table_ref"] == "proj.staging.daily_fx"
        assert (
            load_call["job_config"].write_disposition
            == bigquery.WriteDisposition.WRITE_TRUNCATE
        )
        assert (
            load_call["job_config"].source_format
            == bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )

    def test_merge_on_clause_ands_together_every_natural_key_field(self, monkeypatch):
        client = _FakeBigQueryClient(existing_tables={"proj.bronze.daily_fx"})
        _patch_bigquery_hook(monkeypatch, client)
        op = _operator(natural_key_fields=["currency", "date"])

        op.execute(_context())

        sql = client.queries[0]
        assert "ON T.currency = S.currency AND T.date = S.date" in sql

    def test_update_set_excludes_the_natural_key_columns(self, monkeypatch):
        client = _FakeBigQueryClient(existing_tables={"proj.bronze.daily_fx"})
        _patch_bigquery_hook(monkeypatch, client)
        op = _operator(natural_key_fields=["currency", "date"])

        op.execute(_context())

        sql = client.queries[0]
        update_clause = sql.split("UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
        assert "currency" not in update_clause
        assert "date" not in update_clause
        assert "close = S.close" in update_clause
        assert "source = S.source" in update_clause

    def test_insert_columns_and_values_cover_every_schema_field_in_the_same_order(
        self, monkeypatch
    ):
        client = _FakeBigQueryClient(existing_tables={"proj.bronze.daily_fx"})
        _patch_bigquery_hook(monkeypatch, client)
        op = _operator()

        op.execute(_context())

        sql = client.queries[0]
        insert_cols = sql.split("INSERT (")[1].split(")")[0]
        insert_vals = sql.split("VALUES (")[1].split(")")[0]
        assert [c.strip() for c in insert_cols.split(",")] == [
            "date",
            "currency",
            "close",
            "source",
        ]
        assert [v.strip() for v in insert_vals.split(",")] == [
            "S.date",
            "S.currency",
            "S.close",
            "S.source",
        ]

    def test_merge_targets_and_sources_the_correct_fully_qualified_tables(
        self, monkeypatch
    ):
        client = _FakeBigQueryClient(existing_tables={"proj.bronze.daily_fx"})
        _patch_bigquery_hook(monkeypatch, client)
        op = _operator()

        op.execute(_context())

        sql = client.queries[0]
        assert "MERGE `proj.bronze.daily_fx` T" in sql
        assert "USING `proj.staging.daily_fx` S" in sql
