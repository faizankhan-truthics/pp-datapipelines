"""Solidgate land+release helpers -- fetch a Solidgate entity and write it into the GCS
landing/release zones.

  * ``land_and_release_to_gcs`` — paginated JSON entities (card orders, apm orders, ...).
  * ``csv_landing_to_release_avro`` — shreds a landed CSV into the standard Release Avro
    shape; used standalone by ``report_land_and_release_to_gcs`` below.
  * ``report_land_and_release_to_gcs`` — report-based entities (financial_entries), whose
    hook generates a CSV report per window (generate -> poll -> presigned S3 URL) rather
    than paginating.
"""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from fastavro import writer as avro_writer

from include.config.solidgate_settings import (
    AVRO_SCHEMA,
    GCP_CONN_ID,
    GCS_BUCKET,
    GCS_UPLOAD_CHUNK,
)

if TYPE_CHECKING:
    from include.custom_hooks.solidgate.base import SolidgateBaseHook

logger = logging.getLogger(__name__)

_DAY_MAX_WORKERS = 5


def _clean_prefix(bucket, prefix: str) -> None:
    """Delete any objects already sitting under this account+day directory."""
    stale_blobs = list(bucket.list_blobs(prefix=prefix))
    if stale_blobs:
        bucket.delete_blobs(stale_blobs)


def _parse_dt(value: str) -> datetime:
    value = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unrecognized date format: {value!r}")


def _split_into_days(date_from: str, date_to: str) -> list[dict]:
    """Split ``[date_from, date_to)`` into whole calendar days, UTC."""
    date_from_dt = _parse_dt(date_from)
    date_to_dt = _parse_dt(date_to)

    days = []
    current_day = date_from_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    while current_day < date_to_dt:
        next_day = current_day + timedelta(days=1)
        day_start = max(current_day, date_from_dt)
        day_end = min(next_day, date_to_dt)
        days.append(
            {
                "date_from": day_start.strftime("%Y-%m-%d %H:%M:%S"),
                "date_to": day_end.strftime("%Y-%m-%d %H:%M:%S"),
                "ds_str": current_day.strftime("%Y-%m-%d"),
            }
        )
        current_day = next_day
    return days


def _whole_window(date_from: str, date_to: str) -> dict:
    """The ``split_by_day=False`` single-window equivalent of ``_split_into_days``.

    Normalizes through ``_parse_dt`` the same way, so a bare-date param (no time
    component) still reaches the API as ``%Y-%m-%d %H:%M:%S`` instead of raw --
    Solidgate silently returns zero records for a malformed date_from/date_to.
    """
    date_from_dt = _parse_dt(date_from)
    date_to_dt = _parse_dt(date_to)
    return {
        "date_from": date_from_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "date_to": date_to_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "ds_str": date_from_dt.strftime("%Y-%m-%d"),
    }


def land_and_release_to_gcs(
    *,
    hook: "SolidgateBaseHook",
    date_from: str,
    date_to: str,
    entity: str,
    account_name: str,
    gcs_bucket: str = GCS_BUCKET,
    gcp_conn_id: str = GCP_CONN_ID,
    filter: str = "updated_at",
    max_workers: int = _DAY_MAX_WORKERS,
    split_by_day: bool = True,
) -> dict:
    """Land + release a Solidgate entity, one Avro file per API page per account per day.

    Mirrors StripeToGCSOperator: the ``[date_from, date_to)`` window is split into
    whole days, each landed under its own ``{entity}/{account_name}/{ds_str}``
    folder (one JSON landing file + one Avro release file per page) so a rerun only
    touches the days it recomputes and BigQuery loads via one wildcard per day
    instead of one exact URI per page. Multi-day windows (backfills) fan the days
    out across a thread pool; a single-day (daily) run stays on one thread -- the
    shared Solidgate rate limiter keeps concurrent requests within limits either way.

    ``split_by_day=False`` sends the whole ``[date_from, date_to)`` window as a
    single API request instead of splitting it into per-day requests. Landed/released
    under the window start's ``ds_str``.
    """
    days_to_process = (
        _split_into_days(date_from, date_to)
        if split_by_day
        else [_whole_window(date_from, date_to)]
    )
    account_slug = account_name.lower()

    logger.info(
        "[%s] %s: processing %d day(s) across up to %d thread(s)",
        account_name,
        entity,
        len(days_to_process),
        max_workers,
    )

    client = GCSHook(gcp_conn_id=gcp_conn_id).get_conn()
    bucket = client.bucket(gcs_bucket)

    def fetch_day(day_meta: dict) -> dict:
        ds_str = day_meta["ds_str"]
        landing_dir = f"landing/solidgate/{entity}/{account_slug}/{ds_str}"
        release_dir = f"release/solidgate/{entity}/{account_slug}/{ds_str}"

        # Wipe stale files from a prior run before writing -- otherwise a rerun with
        # fewer pages leaves orphaned files that the BigQuery load would double-count.
        _clean_prefix(bucket, f"{landing_dir}/")
        _clean_prefix(bucket, f"{release_dir}/")

        now = datetime.now(timezone.utc).isoformat()
        page_idx = 0
        total_records = 0
        release_paths = []

        for payload in hook.iter_raw_pages(
            day_meta["date_from"], day_meta["date_to"], filter=filter
        ):
            page_idx += 1
            file_stem = f"{account_slug}__{entity}_{page_idx:05d}"

            landing_path = f"{landing_dir}/{file_stem}.json"
            bucket.blob(landing_path).upload_from_string(
                json.dumps(payload), content_type="application/json"
            )

            release_path = f"{release_dir}/{file_stem}.avro"
            release_uri = f"gs://{gcs_bucket}/{release_path}"

            page_records = []
            for record in hook._extract_records(payload):
                if not isinstance(record, dict):
                    continue
                page_records.append(
                    {
                        "_elt_account_name": account_name,
                        "_elt_endpoint": entity,
                        "_elt_insert_timestamp": now,
                        "_elt_update_timestamp": now,
                        "_gcs_file_path": release_uri,
                        "raw_data": json.dumps(record),
                    }
                )

            # In-memory Avro, no temp file -- pages are bounded by the hook's page limit.
            avro_buffer = io.BytesIO()
            avro_writer(avro_buffer, AVRO_SCHEMA, page_records)
            bucket.blob(release_path).upload_from_string(
                avro_buffer.getvalue(), content_type="avro/binary"
            )

            release_paths.append(release_path)
            total_records += len(page_records)

        logger.info(
            "[%s] day %s: %d page(s), %d record(s) -> %s/*",
            account_name,
            ds_str,
            page_idx,
            total_records,
            release_dir,
        )
        return {
            "ds": ds_str,
            "pages": page_idx,
            "records": total_records,
            "release_paths": release_paths,
        }

    workers = max_workers if len(days_to_process) > 1 else 1
    day_results = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(fetch_day, day) for day in days_to_process]
        for future in as_completed(futures):
            day_results.append(future.result())

    day_results.sort(key=lambda r: r["ds"])
    total_pages = sum(r["pages"] for r in day_results)
    total_records = sum(r["records"] for r in day_results)
    release_paths = [path for r in day_results for path in r["release_paths"]]

    logger.info(
        "[%s] %s: done. %d day(s), %d page(s), %d record(s) total.",
        account_name,
        entity,
        len(day_results),
        total_pages,
        total_records,
    )

    return {
        "landing_pages_processed": total_pages,
        "record_count": total_records,
        "release_paths": release_paths,
    }


def csv_landing_to_release_avro(
    *,
    landing_path: str,
    release_path: str,
    entity: str,
    account_name: str,
    gcs_bucket: str = GCS_BUCKET,
    gcp_conn_id: str = GCP_CONN_ID,
) -> dict:
    """Read a landed CSV from GCS and write the Release Avro beside it.

    Each CSV row becomes one Avro record in the standard Release shape (the 5 ELT
    metadata columns + ``raw_data`` = the row as a JSON object), so financial_entries
    carries the same ``_elt_account_name``/``_elt_endpoint`` lineage as every other
    Solidgate table. Streamed both ways (``csv.DictReader`` over the GCS read, records
    fed to ``fastavro.writer``), so memory stays flat. The Release Avro is ALWAYS
    written — even for an empty report (0 data rows) — so the release path is
    deterministic and the staging load never references a missing object.

    Returns ``{release_path, record_count}``.
    """
    client = GCSHook(gcp_conn_id=gcp_conn_id).get_conn()
    bucket = client.bucket(gcs_bucket)
    release_uri = f"gs://{gcs_bucket}/{release_path}"
    now = datetime.now(timezone.utc).isoformat()

    landing_blob = bucket.blob(landing_path)
    stats = {"records": 0}

    def rows():
        if not landing_blob.exists():
            logger.warning(
                "landing CSV missing, writing empty Release: %s", landing_path
            )
            return
        with landing_blob.open("r") as fh:  # text mode -> csv handles quoted newlines
            for row in csv.DictReader(fh):
                stats["records"] += 1
                yield {
                    "_elt_account_name": account_name,
                    "_elt_endpoint": entity,
                    "_elt_insert_timestamp": now,
                    "_elt_update_timestamp": now,
                    "_gcs_file_path": release_uri,
                    "raw_data": json.dumps(row),
                }

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as tmp:
            tmp_path = tmp.name
            avro_writer(tmp, AVRO_SCHEMA, rows())
        release_blob = bucket.blob(release_path)
        release_blob.chunk_size = GCS_UPLOAD_CHUNK
        release_blob.upload_from_filename(tmp_path)
        logger.info("released %d records -> %s", stats["records"], release_uri)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)

    return {"release_path": release_path, "record_count": stats["records"]}


def report_land_and_release_to_gcs(
    *,
    hook,
    date_from: str,
    date_to: str,
    entity: str,
    account_name: str,
    gcs_bucket: str = GCS_BUCKET,
    gcp_conn_id: str = GCP_CONN_ID,
    filter: str = "created_at",
    max_workers: int = _DAY_MAX_WORKERS,
    split_by_day: bool = True,
) -> dict:
    """Land + release a report-based Solidgate entity (financial_entries), one day at a time.

    Unlike ``land_and_release_to_gcs`` (paginated JSON), a report entity generates one CSV
    per requested window via ``hook.resolve_report_url`` (generate -> poll -> presigned S3
    URL), so there's no natural per-page split. Splitting ``[date_from, date_to)`` into
    whole days here instead -- one report request per day, landed under its own
    ``{entity}/{account_name}/{ds_str}`` folder -- gives financial_entries the same rerun
    and BigQuery-wildcard-per-day properties as the other Solidgate entities. Multi-day
    windows (backfills) fan the days out across a thread pool; a single-day (daily) run
    stays on one thread.

    ``split_by_day=False`` sends the whole ``[date_from, date_to)`` window as a single
    report request instead of splitting it into per-day requests. Landed/released under
    the window start's ``ds_str``.
    """
    days_to_process = (
        _split_into_days(date_from, date_to)
        if split_by_day
        else [_whole_window(date_from, date_to)]
    )
    account_slug = account_name.lower()

    logger.info(
        "[%s] %s: processing %d day(s) across up to %d thread(s)",
        account_name,
        entity,
        len(days_to_process),
        max_workers,
    )

    client = GCSHook(gcp_conn_id=gcp_conn_id).get_conn()
    bucket = client.bucket(gcs_bucket)

    def fetch_day(day_meta: dict) -> dict:
        import requests

        ds_str = day_meta["ds_str"]
        file_stem = f"{account_slug}__{entity}"
        landing_path = (
            f"landing/solidgate/{entity}/{account_slug}/{ds_str}/{file_stem}.csv"
        )
        release_path = (
            f"release/solidgate/{entity}/{account_slug}/{ds_str}/{file_stem}.avro"
        )

        report_url = hook.resolve_report_url(
            day_meta["date_from"], day_meta["date_to"], filter=filter
        )

        blob = bucket.blob(landing_path)
        with requests.get(report_url, stream=True, timeout=300) as resp:
            resp.raise_for_status()
            resp.raw.decode_content = True
            blob.upload_from_file(resp.raw, content_type="text/csv")

        release = csv_landing_to_release_avro(
            landing_path=landing_path,
            release_path=release_path,
            entity=entity,
            account_name=account_name,
            gcs_bucket=gcs_bucket,
            gcp_conn_id=gcp_conn_id,
        )
        logger.info(
            "[%s] day %s: %d record(s) -> %s",
            account_name,
            ds_str,
            release["record_count"],
            release_path,
        )
        return {
            "ds": ds_str,
            "records": release["record_count"],
            "release_path": release_path,
        }

    workers = max_workers if len(days_to_process) > 1 else 1
    day_results = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(fetch_day, day) for day in days_to_process]
        for future in as_completed(futures):
            day_results.append(future.result())

    day_results.sort(key=lambda r: r["ds"])
    total_records = sum(r["records"] for r in day_results)
    release_paths = [r["release_path"] for r in day_results]

    logger.info(
        "[%s] %s: done. %d day(s), %d record(s) total.",
        account_name,
        entity,
        len(day_results),
        total_records,
    )

    return {
        "record_count": total_records,
        "release_paths": release_paths,
    }
