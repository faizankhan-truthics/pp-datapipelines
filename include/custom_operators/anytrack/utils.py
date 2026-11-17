"""AnyTrack Landing -> Release.

The Cloud Run webhook receiver (cloud_runs/webhook-service/) already writes one
raw JSON file per event straight to GCS landing in real time. This module's job
starts one step later: batch a product's full `ds` landing folder into Release
Avro files (ELT metadata + raw_data JSON text), matching the Solidgate/Stripe
Release shape -- see include/config/anytrack_settings.py.

Landing files are written in chunks of CHUNK_SIZE per Release Avro file, rather
than one Avro file for the whole day, so a single run's memory/file size stays
bounded even if a day's event count grows well past current volume. Multiple
Avro paths are fine downstream -- GCSToBigQueryOperator's source_objects
already accepts a list.
"""

from __future__ import annotations

import logging
import tempfile
from datetime import datetime, timezone

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from fastavro import writer as avro_writer

from include.config.anytrack_settings import (
    AVRO_SCHEMA,
    ENTITY,
    GCP_CONN_ID,
    GCS_BUCKET,
    landing_prefix,
    release_object,
)

logger = logging.getLogger(__name__)

CHUNK_SIZE = 5000


def _chunked(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def landing_to_release(
    *,
    product: str,
    ds: str,
    gcs_bucket: str = GCS_BUCKET,
    gcp_conn_id: str = GCP_CONN_ID,
) -> dict:
    """Convert one product's full `ds` landing folder into chunked Release
    Avro files (CHUNK_SIZE records each).

    Returns {"release_paths": list[str], "record_count": int}. record_count
    == 0 means no landing files existed for this product/ds -- the caller
    should skip the staging/bronze steps for it (same AirflowSkipException
    pattern used by every other source in this repo).
    """
    client = GCSHook(gcp_conn_id=gcp_conn_id).get_conn()
    bucket = client.bucket(gcs_bucket)

    prefix = landing_prefix(product, ds)
    blobs = list(bucket.list_blobs(prefix=prefix))

    if not blobs:
        logger.info("No landing files for %s/%s at %s", product, ds, prefix)
        return {"release_paths": [], "record_count": 0}

    now = datetime.now(timezone.utc).isoformat()
    release_paths = []

    for chunk_index, blob_chunk in enumerate(_chunked(blobs, CHUNK_SIZE)):
        filename = f"{product}_{ENTITY}_{ds}_part{chunk_index}.avro"
        release_path = release_object(product, ds, filename)
        release_uri = f"gs://{gcs_bucket}/{release_path}"

        def release_rows(chunk=blob_chunk):
            for blob in chunk:
                raw_bytes = blob.download_as_bytes()
                yield {
                    "_elt_product": product,
                    "_elt_endpoint": ENTITY,
                    "_elt_insert_timestamp": now,
                    "_elt_update_timestamp": now,
                    "_gcs_file_path": f"gs://{gcs_bucket}/{blob.name}",
                    "raw_data": raw_bytes.decode("utf-8"),
                }

        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as tmp:
            tmp_path = tmp.name
            avro_writer(tmp, AVRO_SCHEMA, release_rows())

        release_blob = bucket.blob(release_path)
        release_blob.upload_from_filename(tmp_path)
        release_paths.append(release_path)

        logger.info(
            "Released chunk %d (%d records) for %s/%s -> %s",
            chunk_index,
            len(blob_chunk),
            product,
            ds,
            release_uri,
        )

    logger.info(
        "Released %d records total for %s/%s across %d file(s)",
        len(blobs),
        product,
        ds,
        len(release_paths),
    )
    return {"release_paths": release_paths, "record_count": len(blobs)}
