"""AnyTrack CSV backfill -> Release.

One-off manual path for loading historical AnyTrack events that predate the
live webhook receiver. Unlike the daily pipeline, there's no Landing step
here -- the CSV(s) dropped under `external/anytrack/<product>/<drop_ds>/`
already serve as the raw source. This reads each CSV, transforms every row
into the same JSON shape as a live webhook payload, and writes Release Avro
files to a namespace (`backfill_events`) that never collides with the daily
pipeline's `events` release files for the same ds -- see BACKFILL_ENTITY /
backfill_release_object() in anytrack_settings.py.

The resulting Avro loads into the *same* staging/bronze tables as the live
pipeline, so backfilled and live rows get deduped together via
merge_events_all_events.sql's transactionId key.

CSV rows are read via Blob.open() (a streaming file-like reader), not
download_as_bytes() -- a full CSV export can be large, so this avoids ever
holding the whole file in memory. Rows are written out in chunks of
CHUNK_SIZE per Release Avro file, same reasoning as the live pipeline's
utils.py: bounded memory/file size regardless of how large the export is.
"""

from __future__ import annotations

import csv
import itertools
import json
import logging
import tempfile
from datetime import datetime, timezone

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from fastavro import writer as avro_writer

from include.config.anytrack_settings import (
    AVRO_SCHEMA,
    BACKFILL_ENTITY,
    GCP_CONN_ID,
    GCS_BUCKET,
    backfill_release_object,
)
from include.custom_operators.anytrack.csv_mapper import (
    map_row as _csv_row_to_webhook_json,
)

logger = logging.getLogger(__name__)

CHUNK_SIZE = 50000


def _chunked_iter(iterable, size: int):
    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, size))
        if not chunk:
            return
        yield chunk


def list_csv_files(
    *,
    file_path: str,
    gcs_bucket: str = GCS_BUCKET,
    gcp_conn_id: str = GCP_CONN_ID,
) -> list[str]:
    """List CSV blobs under the given GCS prefix (file_path param)."""
    client = GCSHook(gcp_conn_id=gcp_conn_id).get_conn()
    bucket = client.bucket(gcs_bucket)
    blobs = [
        blob.name
        for blob in bucket.list_blobs(prefix=file_path)
        if blob.name.lower().endswith(".csv")
    ]
    logger.info("Found %d CSV file(s) under %s", len(blobs), file_path)
    return blobs


def _iter_csv_rows(bucket, blob_name: str):
    """Stream one CSV's rows without loading the whole file into memory.

    Yields (blob_name, row) so provenance survives chunking across multiple
    source CSVs. utf-8-sig strips a leading BOM if present (these exports
    have one) -- without it, the first header comes through as
    '﻿clientId' and every row's clientId lookup silently returns None.
    """
    blob = bucket.blob(blob_name)
    with blob.open("r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield blob_name, row


def backfill_csv_to_release(
    *,
    product: str,
    csv_blob_names: list[str],
    ds: str,
    gcs_bucket: str = GCS_BUCKET,
    gcp_conn_id: str = GCP_CONN_ID,
) -> dict:
    """Read the given CSV files, transform every row, write chunked Release
    Avro files (CHUNK_SIZE rows each).

    Returns {"release_paths": list[str], "record_count": int}.
    """
    client = GCSHook(gcp_conn_id=gcp_conn_id).get_conn()
    bucket = client.bucket(gcs_bucket)

    if not csv_blob_names:
        logger.info("No CSV files to process for %s", product)
        return {"release_paths": [], "record_count": 0}

    now = datetime.now(timezone.utc).isoformat()

    def all_rows():
        for blob_name in csv_blob_names:
            yield from _iter_csv_rows(bucket, blob_name)

    release_paths = []
    total_records = 0

    for chunk_index, row_chunk in enumerate(_chunked_iter(all_rows(), CHUNK_SIZE)):
        filename = f"{product}_{BACKFILL_ENTITY}_{ds}_part{chunk_index}.avro"
        release_path = backfill_release_object(product, ds, filename)
        release_uri = f"gs://{gcs_bucket}/{release_path}"

        def release_rows(chunk=row_chunk):
            for blob_name, row in chunk:
                webhook_json = _csv_row_to_webhook_json(row)
                yield {
                    "_elt_product": product,
                    "_elt_endpoint": BACKFILL_ENTITY,
                    "_elt_insert_timestamp": now,
                    "_elt_update_timestamp": now,
                    "_gcs_file_path": f"gs://{gcs_bucket}/{blob_name}",
                    "raw_data": json.dumps(webhook_json),
                }

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as tmp:
            tmp_path = tmp.name
            avro_writer(tmp, AVRO_SCHEMA, release_rows())

        release_blob = bucket.blob(release_path)
        release_blob.upload_from_filename(tmp_path)
        release_paths.append(release_path)
        total_records += len(row_chunk)

        logger.info(
            "Backfill-released chunk %d (%d records) for %s -> %s",
            chunk_index,
            len(row_chunk),
            product,
            release_uri,
        )

    if total_records == 0:
        logger.info("CSV files under %s produced no rows", csv_blob_names)
        return {"release_paths": [], "record_count": 0}

    logger.info(
        "Backfill-released %d records total for %s across %d file(s)",
        total_records,
        product,
        len(release_paths),
    )
    return {"release_paths": release_paths, "record_count": total_records}
