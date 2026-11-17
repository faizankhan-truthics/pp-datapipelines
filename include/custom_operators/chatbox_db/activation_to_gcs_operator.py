"""Chatbox DB (RDS Postgres, reached via Starburst) activation-metric land+release
helper.

Unlike files_editor_db's per-table ``SELECT *`` mirrors, this runs one fixed
aggregate query against Starburst -- each user's first-ever successful AI response
timestamp ("Bot"/"ready" chatmessage rows, confirmed the only such combination that
means a completed response) -- and lands only that result.

Chunked into 30-day sub-windows, same reason as files_editor_db's weekly chunking:
Starburst's presigned result URLs expire after ~30 min. 30 days (not 7) is from
real timing tests (2026-11-09): a week and a month both took ~4-6 min wall time,
cost dominated by connector round-trip overhead rather than data volume, so a
narrower chunk bought nothing but more round trips. Each chunk's GROUP BY only
covers that chunk -- correctness is restored by the bronze MERGE, which
re-aggregates (MIN + GROUP BY) across every row before matching. See
merge_ai_response_activation.sql.
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

from include.config.chatbox_db_settings import (
    ACTIVATION_TABLE,
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

_BATCH_SIZE = 25_000
_UPLOAD_WORKERS = 4

# Keeps each query's fetch+upload time under Starburst's ~30 min presigned URL
# expiry. 30 (not 7) per the 2026-11-09 timing tests -- see module docstring.
_CHUNK_DAYS = 30

_DT_FORMAT = "%Y-%m-%d %H:%M:%S"

_ACTIVATION_QUERY_TEMPLATE = """
SELECT
    cr.userid AS account_id,
    MIN(cm.createdat) AS first_successful_ai_response_at
FROM "{catalog}"."{schema}"."chatmessage" cm
JOIN "{catalog}"."{schema}"."chatroom" cr ON cr.id = cm.chatid
WHERE cm.sentby = 'Bot'
  AND cm.status = 'ready'
  AND cm.createdat >= TIMESTAMP '{chunk_from}'
  AND cm.createdat < TIMESTAMP '{chunk_to}'
GROUP BY cr.userid
"""


def _json_default(value):
    """Datetimes/Decimals aren't JSON-serializable -- stringify them."""
    return str(value)


def _split_into_chunks(
    date_from: str, date_to: str, chunk_days: int
) -> list[tuple[str, str]]:
    """Split [date_from, date_to) into whole chunk_days-sized windows, UTC."""
    start_dt = datetime.strptime(date_from, _DT_FORMAT)
    end_dt = datetime.strptime(date_to, _DT_FORMAT)

    chunks = []
    current = start_dt
    while current < end_dt:
        chunk_end = min(current + timedelta(days=chunk_days), end_dt)
        chunks.append((current.strftime(_DT_FORMAT), chunk_end.strftime(_DT_FORMAT)))
        current = chunk_end
    return chunks


def land_and_release_activation_to_gcs(
    *,
    date_from: str,
    date_to: str,
    ds: str,
    trino_conn_id: str = TRINO_CONN_ID,
    catalog: str = TRINO_CATALOG,
    schema: str = TRINO_SCHEMA,
    gcs_bucket: str = GCS_BUCKET,
    gcp_conn_id: str = GCP_CONN_ID,
    batch_size: int = _BATCH_SIZE,
    upload_workers: int = _UPLOAD_WORKERS,
    chunk_days: int = _CHUNK_DAYS,
) -> dict:
    """Run the per-chunk first-successful-AI-response aggregate against Starburst
    and land+release the result as one Avro file per ``batch_size`` rows. See
    module docstring for the chunking/correctness rationale."""
    table = ACTIVATION_TABLE
    hook = TrinoHook(trino_conn_id=trino_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    landing_dir = f"landing/{SOURCE}/{PRODUCT}/{table}/{ds}"
    release_dir = f"release/{SOURCE}/{PRODUCT}/{table}/{ds}"

    client = GCSHook(gcp_conn_id=gcp_conn_id).get_conn()
    bucket = client.bucket(gcs_bucket)

    for blob in list(bucket.list_blobs(prefix=f"{landing_dir}/")):
        blob.delete()
    for blob in list(bucket.list_blobs(prefix=f"{release_dir}/")):
        blob.delete()

    now = datetime.now(timezone.utc).isoformat()

    def upload_batch(batch_idx: int, rows: list, columns: list) -> int:
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

    max_in_flight = upload_workers * 2

    chunks = _split_into_chunks(date_from, date_to, chunk_days)
    total_records = 0
    batch_idx = 0

    with ThreadPoolExecutor(max_workers=upload_workers) as executor:
        pending = set()

        for chunk_from, chunk_to in chunks:
            query = _ACTIVATION_QUERY_TEMPLATE.format(
                catalog=catalog,
                schema=schema,
                chunk_from=chunk_from,
                chunk_to=chunk_to,
            )
            logger.info(
                "[%s] querying Starburst (%s -> %s)",
                table,
                chunk_from,
                chunk_to,
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
