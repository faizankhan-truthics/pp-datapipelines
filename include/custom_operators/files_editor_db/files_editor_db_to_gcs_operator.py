"""Files Editor DB (RDS Postgres, reached via Starburst) land+release helper.

Each of the 5 billing_* tables (see TABLES in files_editor_db_settings) is landed as
a 1:1 raw mirror -- no joins, no business logic. Joins/classification live downstream
in dbt silver models. One Avro release file per BATCH of rows (not one file for the
whole table -- some of these tables return 700k+ rows for even a two-week window,
which OOM'd the task when buffered as a single Python list/Avro blob). Each file
holds the same 5-column Release shape every other source uses (ELT metadata +
raw_data JSON text), so the bronze MERGE step shreds it the same way
merge_events_all_events.sql / merge_card_orders.sql do. Mirrors Solidgate's
one-file-per-page pattern, just batched by row count instead of by API page.

``SELECT *`` on purpose, no column allowlist here -- a new Postgres column shows up
automatically in raw_data without a code change in this module. The bronze MERGE SQL
is where columns get named/typed; anything not explicitly named here still survives
via that table's `_raw_payload` safety-net column.

Upload work (serialize + 2 GCS writes per batch) happens in background threads, not
inline in the fetch loop -- confirmed 2026-10-07: Trino's coordinator abandons a
query if the client goes quiet for `query.client.timeout` (5 min) between result
requests. Trino buffers far more rows per internal HTTP page than our batch_size, so
several fetchmany() calls can be served from an already-buffered page with no new
network request; if upload time for those batches adds up past 5 minutes before the
next real fetch, the coordinator has already discarded the query by the time we ask
for more (ABANDONED_QUERY). Running uploads in a thread pool keeps the main loop
calling fetchmany() back-to-back, so Trino is kept continuously fed regardless of
how long any one batch's GCS upload takes.

The query itself is also chunked into weekly sub-windows, not run once for the whole
[date_from, date_to) range -- confirmed 2026-10-08: Starburst Galaxy's spooled
results protocol hands out presigned S3 URLs valid for only ~30 minutes
(X-Amz-Expires=1799). A single query covering a large date range can take well over
30 minutes to fully fetch+upload (hundreds of batches), so early result segments
expire (AccessDenied: Request has expired) before the fetch loop gets to them. Each
weekly sub-window is a fresh `cursor.execute()` -- a new query_id and a fresh 30
minutes -- same mitigation Solidgate/Stripe already use elsewhere in this repo
(splitting a date range into days/hours) to avoid exactly this class of
long-running-request problem, just at week granularity here since this source's
volume comfortably fits that window with margin.
"""

from __future__ import annotations

import io
import json
import logging
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, as_completed, wait
from datetime import datetime, timedelta, timezone

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.trino.hooks.trino import TrinoHook
from fastavro import writer as avro_writer

from include.config.files_editor_db_settings import (
    AVRO_SCHEMA,
    GCP_CONN_ID,
    GCS_BUCKET,
    PRODUCT,
    SOURCE,
    TRINO_CATALOG,
    TRINO_CONN_ID,
    TRINO_SCHEMA,
)

logger = logging.getLogger(__name__)

# Rows per landing/release file. Bounds peak memory to ~this many rows' worth of
# JSON/Avro in flight at once per in-progress upload, regardless of how many rows the
# whole window returns.
_BATCH_SIZE = 25_000

# Background upload workers -- bounds how many batches' worth of rows can be resident
# in memory at once (worst case ~_UPLOAD_WORKERS * _BATCH_SIZE rows) while keeping the
# main fetch loop decoupled from GCS upload latency.
_UPLOAD_WORKERS = 4

# Days per query chunk -- keeps each individual Trino query's total fetch+upload
# time comfortably under Starburst's ~30 minute presigned URL expiry. See module
# docstring.
_CHUNK_DAYS = 7

_DT_FORMAT = "%Y-%m-%d %H:%M:%S"


def _json_default(value):
    """Datetimes/Decimals aren't natively JSON-serializable -- stringify them, same
    as every other source's raw_data (a lossless text snapshot of the row)."""
    return str(value)


def _split_into_weeks(
    date_from: str, date_to: str, chunk_days: int
) -> list[tuple[str, str]]:
    """Split [date_from, date_to) into whole chunk_days-sized windows, UTC.

    Mirrors Solidgate's _split_into_days -- same rationale (keep each individual
    request small/bounded), just configurable chunk size instead of a fixed day.
    """
    start_dt = datetime.strptime(date_from, _DT_FORMAT)
    end_dt = datetime.strptime(date_to, _DT_FORMAT)

    chunks = []
    current = start_dt
    while current < end_dt:
        chunk_end = min(current + timedelta(days=chunk_days), end_dt)
        chunks.append((current.strftime(_DT_FORMAT), chunk_end.strftime(_DT_FORMAT)))
        current = chunk_end
    return chunks


def land_and_release_table_to_gcs(
    *,
    table: str,
    date_from: str,
    date_to: str,
    ds: str,
    updated_at_col: str = "updated_at",
    trino_conn_id: str = TRINO_CONN_ID,
    catalog: str = TRINO_CATALOG,
    schema: str = TRINO_SCHEMA,
    gcs_bucket: str = GCS_BUCKET,
    gcp_conn_id: str = GCP_CONN_ID,
    batch_size: int = _BATCH_SIZE,
    upload_workers: int = _UPLOAD_WORKERS,
    chunk_days: int = _CHUNK_DAYS,
) -> dict:
    """Pull every row from ``table`` whose ``updated_at`` falls in
    ``[date_from, date_to)``, via Starburst, and land+release it as one Avro file
    per ``batch_size`` rows -- ``cursor.fetchmany()`` in a loop, not
    ``fetchall()``, so Trino's own paged HTTP results are consumed and written out
    incrementally instead of all getting buffered into one Python list first. Each
    batch's upload is handed off to a background thread so the fetch loop itself
    never blocks on GCS I/O -- see module docstring for why that matters.

    ``[date_from, date_to)`` is further split into ``chunk_days``-sized sub-windows,
    each run as its own fresh query -- see module docstring for why (Starburst's
    presigned result URLs expire after ~30 minutes; one query per week keeps every
    individual query's total fetch+upload time well under that).

    Soft-deleted rows (deleted_at IS NOT NULL) are landed same as any other row --
    this function has no opinion on deletion state, that's a silver-layer concern.
    """
    hook = TrinoHook(trino_conn_id=trino_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    landing_dir = f"landing/{SOURCE}/{PRODUCT}/{table}/{ds}"
    release_dir = f"release/{SOURCE}/{PRODUCT}/{table}/{ds}"

    client = GCSHook(gcp_conn_id=gcp_conn_id).get_conn()
    bucket = client.bucket(gcs_bucket)

    # Wipe stale files from a prior run of this same ds before writing -- otherwise a
    # rerun with fewer rows leaves orphaned files from the earlier, larger run.
    for blob in list(bucket.list_blobs(prefix=f"{landing_dir}/")):
        blob.delete()
    for blob in list(bucket.list_blobs(prefix=f"{release_dir}/")):
        blob.delete()

    now = datetime.now(timezone.utc).isoformat()

    def upload_batch(batch_idx: int, rows: list, columns: list) -> int:
        # columns passed explicitly (not closed over) -- each weekly chunk issues
        # its own cursor.execute(), so a closure over an outer `columns` variable
        # would risk a late-running worker thread picking up the NEXT chunk's
        # column list instead of the one its rows actually came from.
        file_stem = f"{table}_{ds.replace('-', '')}_{batch_idx:05d}"
        landing_path = f"{landing_dir}/{file_stem}.json"
        release_path = f"{release_dir}/{file_stem}.avro"
        release_uri = f"gs://{gcs_bucket}/{release_path}"

        records = []
        landing_lines = []
        for row in rows:
            row_dict = dict(zip(columns, row))  # noqa: B905
            raw_json = json.dumps(row_dict, default=_json_default)
            landing_lines.append(raw_json)
            records.append(
                {
                    "_elt_table": table,
                    "_elt_insert_timestamp": now,
                    "_elt_update_timestamp": now,
                    "_gcs_file_path": release_uri,
                    "raw_data": raw_json,
                }
            )

        # Landing: the lossless raw drop (newline-delimited JSON, one row per line) --
        # same role as every other source's Landing zone, kept even though
        # Release/Avro is what actually gets loaded into staging.
        bucket.blob(landing_path).upload_from_string(
            "\n".join(landing_lines), content_type="application/json"
        )

        avro_buffer = io.BytesIO()
        avro_writer(avro_buffer, AVRO_SCHEMA, records)
        bucket.blob(release_path).upload_from_string(
            avro_buffer.getvalue(), content_type="avro/binary"
        )

        logger.info(
            "[%s] batch %d: %d row(s) -> %s",
            table,
            batch_idx,
            len(records),
            release_uri,
        )
        return len(records)

    # Caps how many fetched-but-not-yet-uploaded batches can exist at once. Without
    # this, the fetch loop (fast -- Trino can hand us 25K rows in a few seconds) races
    # far ahead of the upload workers (slow -- 10-70s per batch for JSON+Avro+2 GCS
    # writes), piling up an unbounded, ever-growing backlog of in-memory batches
    # waiting for a free worker -- confirmed 2026-10-08: OOM'd after ~20 batches
    # actually finished uploading while the fetch loop had silently raced ahead by
    # 150+ batches (millions of queued rows) in the same time. A small multiple of
    # upload_workers keeps the fetch loop paced to roughly match upload throughput.
    max_in_flight = upload_workers * 2

    chunks = _split_into_weeks(date_from, date_to, chunk_days)
    total_records = 0
    batch_idx = 0

    with ThreadPoolExecutor(max_workers=upload_workers) as executor:
        pending = set()

        for chunk_from, chunk_to in chunks:
            query = (
                f'SELECT * FROM "{catalog}"."{schema}"."{table}" '
                f"WHERE {updated_at_col} >= TIMESTAMP '{chunk_from}' "
                f"AND {updated_at_col} < TIMESTAMP '{chunk_to}'"
            )
            logger.info(
                "[%s] querying Starburst (%s -> %s): %s",
                table,
                chunk_from,
                chunk_to,
                query,
            )
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]

            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                batch_idx += 1
                pending.add(executor.submit(upload_batch, batch_idx, rows, columns))

                if len(pending) >= max_in_flight:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        total_records += future.result()

        for future in as_completed(pending):
            total_records += future.result()

    logger.info(
        "[%s] done: %d chunk(s), %d batch(es), %d record(s) total for %s -> %s",
        table,
        len(chunks),
        batch_idx,
        total_records,
        date_from,
        date_to,
    )
    return {
        "table": table,
        "record_count": total_records,
        "release_glob": f"{release_dir}/*.avro",
    }
