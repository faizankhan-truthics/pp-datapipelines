"""Unit tests for include/custom_operators/anytrack/backfill_utils.py.

The manual CSV backfill path: CSV export(s) in GCS -> one webhook-shaped JSON per row
(csv_mapper.map_row) -> chunked Release Avro files under the `backfill_events`
namespace. Covers:
  * list_csv_files() scoping to the given prefix and only picking up .csv blobs.
  * the "nothing to do" branches (no CSVs, header-only CSVs) returning record_count 0
    and uploading nothing -- the DAG's check_records skips the BigQuery load on that.
  * chunking across CHUNK_SIZE boundaries and across multiple source CSVs, with
    per-row provenance (_gcs_file_path) surviving the chunk boundaries.
  * release paths landing under backfill_events/, never the daily pipeline's
    events/ namespace for the same ds.
  * the BOM in real exports being stripped via Blob.open(encoding="utf-8-sig") --
    otherwise every row's clientId silently comes through as None.
  * raw_data being exactly csv_mapper.map_row()'s output (the click-id shape fixed in
    8e1f8d1 is pinned in test_csv_mapper.py; this checks it reaches the Avro intact).
"""

from __future__ import annotations

import io
import json
from types import SimpleNamespace

import fastavro
import pytest

import include.custom_operators.anytrack.backfill_utils as backfill_utils
from include.custom_operators.anytrack.backfill_utils import (
    backfill_csv_to_release,
    list_csv_files,
)
from include.custom_operators.anytrack.csv_mapper import map_row

DS = "2026-09-01"


class FakeBlob:
    def __init__(self, name: str, data: bytes = b""):
        self.name = name
        self._data = data
        self.uploaded_bytes: bytes | None = None
        self.open_calls: list[tuple] = []

    def open(self, mode: str = "r", encoding: str | None = None):
        self.open_calls.append((mode, encoding))
        return io.TextIOWrapper(io.BytesIO(self._data), encoding=encoding, newline="")

    def upload_from_filename(self, path):
        # Read at upload time: the temp file is the code's, not ours to keep relying on.
        with open(path, "rb") as fh:
            self.uploaded_bytes = fh.read()


class FakeBucket:
    def __init__(self, blobs: list[FakeBlob] | None = None):
        self._blobs = {b.name: b for b in blobs or []}
        self.list_prefixes: list[str] = []

    def list_blobs(self, prefix: str):
        self.list_prefixes.append(prefix)
        return [b for name, b in self._blobs.items() if name.startswith(prefix)]

    def blob(self, path: str) -> FakeBlob:
        return self._blobs.setdefault(path, FakeBlob(path))

    def uploaded(self) -> dict[str, FakeBlob]:
        return {
            name: b for name, b in self._blobs.items() if b.uploaded_bytes is not None
        }


def _patch_gcs(monkeypatch, bucket: FakeBucket, seen_bucket_names: list | None = None):
    def _bucket(name):
        if seen_bucket_names is not None:
            seen_bucket_names.append(name)
        return bucket

    fake_client = SimpleNamespace(bucket=_bucket)
    monkeypatch.setattr(
        backfill_utils,
        "GCSHook",
        lambda **kwargs: SimpleNamespace(get_conn=lambda: fake_client),
    )


def _csv(header: list[str], rows: list[list[str]], bom: bool = False) -> bytes:
    lines = [",".join(header)] + [",".join(r) for r in rows]
    text = "\n".join(lines) + "\n"
    return (("﻿" if bom else "") + text).encode("utf-8")


def _records(blob: FakeBlob) -> list[dict]:
    return list(fastavro.reader(io.BytesIO(blob.uploaded_bytes)))


def _run(product="iqcenter", csv_blob_names=(), bucket_name="test-bucket"):
    return backfill_csv_to_release(
        product=product,
        csv_blob_names=list(csv_blob_names),
        ds=DS,
        gcs_bucket=bucket_name,
        gcp_conn_id="test-conn",
    )


class TestListCsvFiles:
    def test_returns_only_csv_blobs_under_the_prefix(self, monkeypatch):
        bucket = FakeBucket(
            [
                FakeBlob("external/anytrack/iqcenter/2026-09-01/a.csv"),
                FakeBlob("external/anytrack/iqcenter/2026-09-01/B.CSV"),
                FakeBlob("external/anytrack/iqcenter/2026-09-01/notes.txt"),
                FakeBlob("external/anytrack/iqcenter/2026-09-01/a.csv.bak"),
                FakeBlob("external/anytrack/tracelo/2026-09-01/other.csv"),
            ]
        )
        seen = []
        _patch_gcs(monkeypatch, bucket, seen)

        result = list_csv_files(
            file_path="external/anytrack/iqcenter/2026-09-01/",
            gcs_bucket="test-bucket",
            gcp_conn_id="c",
        )

        assert result == [
            "external/anytrack/iqcenter/2026-09-01/a.csv",
            "external/anytrack/iqcenter/2026-09-01/B.CSV",
        ]
        assert bucket.list_prefixes == ["external/anytrack/iqcenter/2026-09-01/"]
        assert seen == ["test-bucket"]

    def test_empty_prefix_returns_empty_list(self, monkeypatch):
        _patch_gcs(monkeypatch, FakeBucket())

        assert (
            list_csv_files(file_path="nothing/here/", gcs_bucket="b", gcp_conn_id="c")
            == []
        )


class TestNothingToRelease:
    def test_no_csv_files_returns_zero_and_uploads_nothing(self, monkeypatch):
        bucket = FakeBucket()
        _patch_gcs(monkeypatch, bucket)

        assert _run(csv_blob_names=[]) == {"release_paths": [], "record_count": 0}
        assert bucket.uploaded() == {}

    def test_header_only_csv_returns_zero_and_uploads_nothing(self, monkeypatch):
        bucket = FakeBucket(
            [FakeBlob("in/a.csv", _csv(["clientId", "transactionId"], []))]
        )
        _patch_gcs(monkeypatch, bucket)

        assert _run(csv_blob_names=["in/a.csv"]) == {
            "release_paths": [],
            "record_count": 0,
        }
        assert bucket.uploaded() == {}


class TestRelease:
    def test_single_chunk_lands_under_the_backfill_namespace(self, monkeypatch):
        bucket = FakeBucket(
            [
                FakeBlob(
                    "in/a.csv",
                    _csv(["clientId", "transactionId"], [["c1", "t1"], ["c2", "t2"]]),
                )
            ]
        )
        _patch_gcs(monkeypatch, bucket)

        result = _run(product="tracelo", csv_blob_names=["in/a.csv"])

        assert result == {
            "release_paths": [
                f"release/anytrack/tracelo/backfill_events/{DS}/tracelo_backfill_events_{DS}_part0.avro"
            ],
            "record_count": 2,
        }

    def test_release_paths_never_collide_with_the_daily_events_namespace(
        self, monkeypatch
    ):
        bucket = FakeBucket([FakeBlob("in/a.csv", _csv(["transactionId"], [["t1"]]))])
        _patch_gcs(monkeypatch, bucket)

        result = _run(product="iqcenter", csv_blob_names=["in/a.csv"])

        for path in result["release_paths"]:
            assert not path.startswith("release/anytrack/iqcenter/events/")

    def test_rows_are_split_into_chunk_size_files_across_multiple_csvs(
        self, monkeypatch
    ):
        monkeypatch.setattr(backfill_utils, "CHUNK_SIZE", 2)
        bucket = FakeBucket(
            [
                FakeBlob("in/a.csv", _csv(["transactionId"], [["t1"], ["t2"], ["t3"]])),
                FakeBlob("in/b.csv", _csv(["transactionId"], [["t4"], ["t5"]])),
            ]
        )
        _patch_gcs(monkeypatch, bucket)

        result = _run(csv_blob_names=["in/a.csv", "in/b.csv"])

        assert result["record_count"] == 5
        assert [p.rsplit("_", 1)[1] for p in result["release_paths"]] == [
            "part0.avro",
            "part1.avro",
            "part2.avro",
        ]
        per_file = [_records(bucket.blob(p)) for p in result["release_paths"]]
        assert [len(r) for r in per_file] == [2, 2, 1]
        # Chunk 1 straddles the a.csv -> b.csv boundary: provenance must follow each row.
        assert [
            (json.loads(r["raw_data"])["transactionId"], r["_gcs_file_path"])
            for r in per_file[1]
        ] == [
            ("t3", "gs://test-bucket/in/a.csv"),
            ("t4", "gs://test-bucket/in/b.csv"),
        ]

    def test_release_row_shape_and_raw_data_matches_csv_mapper(self, monkeypatch):
        header = ["clientId", "transactionId", "eventTime", "gclid"]
        values = ["c1", "t1", "2026-08-19T10:15:30.000Z", "g-1"]
        bucket = FakeBucket([FakeBlob("in/a.csv", _csv(header, [values]))])
        _patch_gcs(monkeypatch, bucket)

        result = _run(product="infochecker", csv_blob_names=["in/a.csv"])

        [record] = _records(bucket.blob(result["release_paths"][0]))
        assert record["_elt_product"] == "infochecker"
        assert record["_elt_endpoint"] == "backfill_events"
        assert record["_gcs_file_path"] == "gs://test-bucket/in/a.csv"
        assert record["_elt_insert_timestamp"] == record["_elt_update_timestamp"]
        raw = json.loads(record["raw_data"])
        assert raw == map_row(dict(zip(header, values)))  # noqa: B905
        assert raw["externalClientIds"] == {}
        assert raw["mainAttribution"]["gclid"] == "g-1"

    def test_bom_in_the_export_does_not_null_out_client_id(self, monkeypatch):
        blob = FakeBlob(
            "in/a.csv", _csv(["clientId", "transactionId"], [["c1", "t1"]], bom=True)
        )
        bucket = FakeBucket([blob])
        _patch_gcs(monkeypatch, bucket)

        result = _run(csv_blob_names=["in/a.csv"])

        [record] = _records(bucket.blob(result["release_paths"][0]))
        assert json.loads(record["raw_data"])["clientId"] == "c1"
        assert blob.open_calls == [("r", "utf-8-sig")]

    def test_csv_is_streamed_not_downloaded_whole(self, monkeypatch):
        # The module docstring's memory contract: Blob.open(), never download_as_bytes().
        blob = FakeBlob("in/a.csv", _csv(["transactionId"], [["t1"]]))
        blob.download_as_bytes = lambda: pytest.fail(
            "CSV must be streamed via Blob.open()"
        )
        _patch_gcs(monkeypatch, FakeBucket([blob]))

        assert _run(csv_blob_names=["in/a.csv"])["record_count"] == 1

    def test_quoted_fields_with_commas_and_newlines_survive(self, monkeypatch):
        data = 'transactionId,location\nt1,"https://x.com/?a=1,b=2\nline2"\n'.encode(
            "utf-8"
        )
        bucket = FakeBucket([FakeBlob("in/a.csv", data)])
        _patch_gcs(monkeypatch, bucket)

        result = _run(csv_blob_names=["in/a.csv"])

        [record] = _records(bucket.blob(result["release_paths"][0]))
        assert result["record_count"] == 1
        assert (
            json.loads(record["raw_data"])["location"]
            == "https://x.com/?a=1,b=2\nline2"
        )
