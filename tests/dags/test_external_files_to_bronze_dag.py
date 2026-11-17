"""DAG-level tests for dags/external/external_files_to_bronze_dag.py
(dag_id `gcs_external_to_bigquery_bronze`).

get_csv_files() is the only real logic in the DAG: it lists reference CSVs under
GCS_PREFIX, reads each header, and builds the kwargs that load_to_bronze is
expand_kwargs()-mapped over. Covers:
  * non-CSV objects being skipped and table names derived from the file name.
  * header -> schema going through build_schema_fields (its own rules are covered in
    tests/include/config/test_external_setting.py).
  * every key get_csv_files() emits being an argument GCSToBigQueryOperator accepts --
    a typo there only fails at runtime when the mapped task expands.
  * the short-circuit gate sitting between discovery and the load.
"""

from __future__ import annotations

import inspect

import pytest
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

import dags.external.external_files_to_bronze_dag as external


@pytest.fixture(scope="module")
def dag():
    return external.gcs_external_to_bigquery_bronze()


@pytest.fixture
def get_csv_files(dag, monkeypatch):
    def _run(objects: dict[str, bytes]):
        calls = []

        class FakeGCSHook:
            def __init__(self, gcp_conn_id):
                calls.append(("init", gcp_conn_id))

            def list(self, bucket_name, prefix):
                calls.append(("list", bucket_name, prefix))
                return list(objects)

            def download(self, bucket_name, object_name):
                return objects[object_name]

        monkeypatch.setattr(external, "GCSHook", FakeGCSHook)
        return dag.get_task("get_csv_files").python_callable(), calls

    return _run


class TestGetCsvFiles:
    def test_maps_each_csv_to_its_own_bronze_table(self, get_csv_files):
        result, calls = get_csv_files(
            {
                "external/reference/Country_Codes.csv": b"Country Code,Country Name\nGB,United Kingdom\n",
                "external/reference/readme.txt": b"ignore me",
                "external/reference/sub/fx_map.CSV": b"a,b\n1,2\n",
            }
        )

        assert calls[:2] == [
            ("init", external.GCP_CONN_ID),
            ("list", external.GCS_BUCKET, external.GCS_PREFIX),
        ]
        assert result == [
            {
                "source_objects": ["external/reference/Country_Codes.csv"],
                "destination_project_dataset_table": (
                    f"{external.BQ_PROJECT}.{external.BQ_BRONZE_DATASET}.country_codes"
                ),
                "schema_fields": [
                    {"name": "Country_Code", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "Country_Name", "type": "STRING", "mode": "NULLABLE"},
                ],
            },
            {
                "source_objects": ["external/reference/sub/fx_map.CSV"],
                "destination_project_dataset_table": (
                    f"{external.BQ_PROJECT}.{external.BQ_BRONZE_DATASET}.fx_map"
                ),
                "schema_fields": [
                    {"name": "a", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "b", "type": "STRING", "mode": "NULLABLE"},
                ],
            },
        ]

    def test_quoted_header_cells_with_commas_stay_one_column(self, get_csv_files):
        result, _ = get_csv_files(
            {"external/reference/t.csv": b'"Name, Full",Age\nx,1\n'}
        )

        assert [f["name"] for f in result[0]["schema_fields"]] == ["Name_Full", "Age"]

    def test_bom_on_the_first_header_is_not_kept_in_the_column_name(
        self, get_csv_files
    ):
        result, _ = get_csv_files(
            {"external/reference/t.csv": "﻿id,name\n1,a\n".encode("utf-8")}
        )

        assert [f["name"] for f in result[0]["schema_fields"]] == ["id", "name"]

    def test_no_csv_files_returns_empty_list(self, get_csv_files):
        result, _ = get_csv_files({"external/reference/readme.txt": b"x"})

        assert result == []

    def test_emitted_keys_are_valid_gcs_to_bigquery_arguments(self, get_csv_files):
        result, _ = get_csv_files({"external/reference/t.csv": b"a\n1\n"})
        accepted = set(inspect.signature(GCSToBigQueryOperator.__init__).parameters)

        assert set(result[0]) <= accepted


class TestWiring:
    def test_dag_definition(self, dag):
        assert dag.dag_id == "gcs_external_to_bigquery_bronze"
        assert dag.schedule is None
        assert dag.max_active_runs == 1

    def test_load_is_gated_by_the_short_circuit(self, dag):
        assert dag.get_task("check_csv_files").upstream_task_ids == {"get_csv_files"}
        assert dag.get_task("load_to_bronze").upstream_task_ids == {
            "get_csv_files",
            "check_csv_files",
        }

    def test_load_truncates_and_skips_the_header_row(self, dag):
        partial = dag.get_task("load_to_bronze").partial_kwargs

        assert partial["write_disposition"] == "WRITE_TRUNCATE"
        assert partial["skip_leading_rows"] == 1
        assert partial["autodetect"] is False
        assert partial["source_format"] == "CSV"
