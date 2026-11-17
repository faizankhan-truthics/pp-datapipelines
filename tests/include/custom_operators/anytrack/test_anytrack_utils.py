"""Unit tests for landing_to_release (include/custom_operators/anytrack/utils.py).

Unlike Solidgate/Stripe/etc., the AnyTrack webhook receiver (cloud_runs/webhook-service/)
already writes one raw JSON file per event straight to GCS landing in real time -- this
module's only job is step 2: batch a product's full `ds` landing folder into one Release
Avro file. Covers:
  * the "no landing files this window" branch, which must return record_count == 0
    and skip writing/uploading a Release Avro entirely (there's nothing analogous to
    Solidgate's "land raw pages, skip release" split here -- there's no landing step
    to (re)do at all).
  * the Release Avro's row shape, since it's assembled by hand per record rather than
    coming straight off an API response like every other source.
  * that ``landing_prefix`` scopes ``list_blobs`` to exactly this product/ds, so two
    products' events sitting in the same bucket never bleed into each other's release.
"""

from __future__ import annotations

from types import SimpleNamespace

import fastavro

import include.custom_operators.anytrack.utils as anytrack_utils
from include.custom_operators.anytrack.utils import landing_to_release


class FakeBlob:
    def __init__(self, name: str, data: bytes):
        self.name = name
        self._data = data
        self.uploaded_from_filename = None

    def download_as_bytes(self) -> bytes:
        return self._data

    def upload_from_filename(self, path):
        self.uploaded_from_filename = path


class FakeBucket:
    def __init__(self, landing_blobs: list[FakeBlob]):
        self._landing_blobs = landing_blobs
        self._release_blobs: dict[str, FakeBlob] = {}

    def list_blobs(self, prefix: str):
        return [b for b in self._landing_blobs if b.name.startswith(prefix)]

    def blob(self, path: str) -> FakeBlob:
        return self._release_blobs.setdefault(path, FakeBlob(path, b""))


def _patch_gcs(monkeypatch, bucket: FakeBucket):
    fake_client = SimpleNamespace(bucket=lambda name: bucket)
    monkeypatch.setattr(
        anytrack_utils,
        "GCSHook",
        lambda **kwargs: SimpleNamespace(get_conn=lambda: fake_client),
    )


def _read_avro_records(path: str) -> list[dict]:
    with open(path, "rb") as fh:
        return list(fastavro.reader(fh))


def test_no_landing_files_returns_zero_record_count_and_no_release(monkeypatch):
    bucket = FakeBucket(landing_blobs=[])
    _patch_gcs(monkeypatch, bucket)

    result = landing_to_release(
        product="iqcenter",
        ds="2026-08-19",
        gcs_bucket="test-bucket",
        gcp_conn_id="test-conn",
    )

    assert result == {"release_paths": [], "record_count": 0}
    assert bucket._release_blobs == {}  # nothing was ever uploaded


def test_records_present_returns_release_path_and_record_count(monkeypatch):
    bucket = FakeBucket(
        landing_blobs=[
            FakeBlob(
                "landing/anytrack/iqcenter/events/2026-08-19/evt1.json",
                b'{"transactionId": "t1"}',
            ),
            FakeBlob(
                "landing/anytrack/iqcenter/events/2026-08-19/evt2.json",
                b'{"transactionId": "t2"}',
            ),
        ]
    )
    _patch_gcs(monkeypatch, bucket)

    result = landing_to_release(
        product="iqcenter",
        ds="2026-08-19",
        gcs_bucket="test-bucket",
        gcp_conn_id="test-conn",
    )

    assert result["record_count"] == 2
    assert result["release_paths"] == [
        "release/anytrack/iqcenter/events/2026-08-19/iqcenter_events_2026-08-19_part0.avro"
    ]
    release_blob = bucket.blob(result["release_paths"][0])
    assert release_blob.uploaded_from_filename is not None


def test_release_row_shape_matches_avro_schema(monkeypatch):
    bucket = FakeBucket(
        landing_blobs=[
            FakeBlob(
                "landing/anytrack/infochecker/events/2026-08-19/evt1.json",
                b'{"transactionId": "t1"}',
            ),
        ]
    )
    _patch_gcs(monkeypatch, bucket)

    result = landing_to_release(
        product="infochecker",
        ds="2026-08-19",
        gcs_bucket="test-bucket",
        gcp_conn_id="test-conn",
    )

    release_blob = bucket.blob(result["release_paths"][0])
    records = _read_avro_records(release_blob.uploaded_from_filename)

    assert len(records) == 1
    record = records[0]
    assert record["_elt_product"] == "infochecker"
    assert record["_elt_endpoint"] == "events"
    assert record["_gcs_file_path"] == (
        "gs://test-bucket/landing/anytrack/infochecker/events/2026-08-19/evt1.json"
    )
    assert record["raw_data"] == '{"transactionId": "t1"}'
    # insert/update timestamps are stamped identically on a first release (no prior
    # bronze row exists yet to preserve an original insert time for).
    assert record["_elt_insert_timestamp"] == record["_elt_update_timestamp"]


def test_only_blobs_under_this_products_ds_prefix_are_included(monkeypatch):
    bucket = FakeBucket(
        landing_blobs=[
            FakeBlob(
                "landing/anytrack/iqcenter/events/2026-08-19/evt1.json",
                b'{"transactionId": "t1"}',
            ),
            # Same day, different product -- must not be swept into iqcenter's release.
            FakeBlob(
                "landing/anytrack/infochecker/events/2026-08-19/evt2.json",
                b'{"transactionId": "t2"}',
            ),
            # Same product, different day -- must not be swept into today's release.
            FakeBlob(
                "landing/anytrack/iqcenter/events/2026-08-18/evt3.json",
                b'{"transactionId": "t3"}',
            ),
        ]
    )
    _patch_gcs(monkeypatch, bucket)

    result = landing_to_release(
        product="iqcenter",
        ds="2026-08-19",
        gcs_bucket="test-bucket",
        gcp_conn_id="test-conn",
    )

    assert result["record_count"] == 1
    release_blob = bucket.blob(result["release_paths"][0])
    records = _read_avro_records(release_blob.uploaded_from_filename)
    assert records[0]["raw_data"] == '{"transactionId": "t1"}'
