"""Microsoft Ads land+release: submit/poll/download a Reporting API report, then land it
into the GCS landing/release zones.

Microsoft's async report flow (SubmitGenerateReport -> PollGenerateReport -> download URL)
is the same shape as Solidgate's financial_entries report flow
(include/custom_operators/solidgate/solidgate_to_gcs_operator.py::report_land_and_release_to_gcs),
just over REST/JSON instead of a signed request. One deliberate difference: Microsoft always
ZIP-compresses report downloads, and unzipping requires seekable access (a ZIP's central
directory sits at the end of the file), which a one-way stream straight to GCS can't give
us -- so the ZIP download and CSV extraction both land on a local temp file first (deleted
immediately after), rather than ever buffering the whole response in memory the way a naive
`requests.get(...).content` would. Reports are server-aggregated (small output regardless of
window size -- a day's campaign performance is a handful of rows, not a raw row dump), so
this is mostly defense-in-depth rather than a response to a known-large payload; it keeps
this pipeline's memory profile flat and consistent with the rest of this repo regardless of
how large any single account's report ever grows.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import shutil
import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any

import requests
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from fastavro import writer as avro_writer

from include.config.microsoft_ads_settings import (
    AVRO_SCHEMA,
    GCP_CONN_ID,
    GCS_BUCKET,
    GCS_UPLOAD_CHUNK,
    REPORT_TIME_ZONE,
)

if TYPE_CHECKING:
    from include.custom_hooks.microsoft_ads.base import MicrosoftAdsHook

logger = logging.getLogger(__name__)

_DAY_MAX_WORKERS = 3  # conservative: Reporting API has its own documented concurrent-
# request throttle (error 207, ConcurrentRequestOverLimit) -- see microsoft_ads_settings.py.


def _parse_dt(value: str) -> datetime:
    value = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unrecognized date format: {value!r}")


def _split_into_days(date_from: str, date_to: str) -> list[dict]:
    """Split [date_from, date_to) into whole calendar days, UTC."""
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
    date_from_dt = _parse_dt(date_from)
    date_to_dt = _parse_dt(date_to)
    return {
        "date_from": date_from_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "date_to": date_to_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "ds_str": date_from_dt.strftime("%Y-%m-%d"),
    }


def _clean_prefix(bucket, prefix: str) -> None:
    stale_blobs = list(bucket.list_blobs(prefix=prefix))
    if stale_blobs:
        bucket.delete_blobs(stale_blobs)


def _date_component(dt: datetime) -> dict[str, int]:
    return {"Day": dt.day, "Month": dt.month, "Year": dt.year}


def build_report_request(
    *,
    report_type: str,
    columns: list[str],
    account_id: str,
    date_from: str,
    date_to: str,
    aggregation: str = "Daily",
) -> dict[str, Any]:
    """Build the JSON ReportRequest body for SubmitGenerateReport.

    date_from/date_to are [inclusive, exclusive) datetime strings (same convention as
    resolve_window elsewhere in this repo); Microsoft's CustomDateRangeEnd is the last
    *included* day, so the exclusive date_to is converted to an inclusive end date by
    stepping back one second (not a full day) -- a full-day step only lands on the
    correct calendar day when date_to is exactly midnight. The scheduled path always
    passes midnight-aligned bounds (resolve_window truncates to start_of("day")), but a
    manual backfill's date_to param isn't guaranteed to be -- e.g. date_to="...-26
    12:00:00" minus one day would wrongly exclude the 26th entirely; minus one second
    correctly lands on the 26th (or the 25th, for a clean midnight date_to).
    """
    start_dt = _parse_dt(date_from)
    end_dt = _parse_dt(date_to) - timedelta(seconds=1)
    return {
        "Type": report_type,
        "Format": "Csv",
        "Aggregation": aggregation,
        # Only affects certain fields' string shape (e.g. Hourly's TimePeriod becomes
        # "yyyy-mm-dd|hour" instead of the messier 1.0 default) -- harmless to always set.
        "FormatVersion": "2.0",
        "ExcludeColumnHeaders": False,
        # Strip Microsoft's report-header/footer metadata lines (report name, date range,
        # copyright, etc.) so the downloaded CSV is just a header row + data rows --
        # avoids having to parse/skip non-tabular lines before csv.DictReader.
        "ExcludeReportHeader": True,
        "ExcludeReportFooter": True,
        "ReturnOnlyCompleteData": False,
        "Columns": columns,
        "Scope": {"AccountIds": [int(account_id)]},
        "Time": {
            "CustomDateRangeStart": _date_component(start_dt),
            "CustomDateRangeEnd": _date_component(end_dt),
            "ReportTimeZone": REPORT_TIME_ZONE,
        },
    }


def _download_report_csv(download_url: str, csv_tmp_path: str) -> None:
    """Stream the ZIP download to a local temp file, then extract its single CSV member to
    csv_tmp_path -- never buffers the whole ZIP or CSV in memory at once, regardless of
    report size. The ZIP itself is deleted before this returns.
    """
    zip_tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as zip_tmp:
            zip_tmp_path = zip_tmp.name
            with requests.get(download_url, stream=True, timeout=300) as resp:
                resp.raise_for_status()
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    zip_tmp.write(chunk)

        with zipfile.ZipFile(zip_tmp_path) as zf:
            names = zf.namelist()
            if not names:
                raise RuntimeError("report ZIP was empty")
            with zf.open(names[0]) as src, open(csv_tmp_path, "wb") as dst:
                shutil.copyfileobj(src, dst)
    finally:
        if zip_tmp_path and os.path.exists(zip_tmp_path):
            os.remove(zip_tmp_path)


def _csv_file_to_release_avro(
    *,
    csv_path: str,
    release_path: str,
    entity: str,
    account_name: str,
    account_id: str,
    aggregation_type: str,
    gcs_bucket: str,
    gcp_conn_id: str,
    bucket,
) -> dict:
    """Shred a local CSV file into the standard Release Avro shape (ELT metadata +
    raw_data JSON).

    Reads via csv.DictReader directly off the file handle (not an in-memory string), fed
    straight to fastavro -- memory stays flat regardless of report size.
    """
    now = datetime.now(timezone.utc).isoformat()
    release_uri = f"gs://{gcs_bucket}/{release_path}"
    stats = {"records": 0}

    def rows(text_stream):
        for row in csv.DictReader(text_stream):
            stats["records"] += 1
            yield {
                "_elt_account_id": account_id,
                "_elt_endpoint": entity,
                "_elt_insert_timestamp": now,
                "_elt_update_timestamp": now,
                "_gcs_file_path": release_uri,
                "aggregation_type": aggregation_type,
                "raw_data": json.dumps(row),
            }

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(suffix=".avro", delete=False) as tmp:
            tmp_path = tmp.name
            with open(csv_path, encoding="utf-8-sig", newline="") as text_stream:
                avro_writer(tmp, AVRO_SCHEMA, rows(text_stream))
        release_blob = bucket.blob(release_path)
        release_blob.chunk_size = GCS_UPLOAD_CHUNK
        release_blob.upload_from_filename(tmp_path)
        logger.info(
            "[%s] released %d record(s) -> %s",
            account_name,
            stats["records"],
            release_uri,
        )
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)

    return {"release_path": release_path, "record_count": stats["records"]}


def report_land_and_release_to_gcs(
    *,
    hook: "MicrosoftAdsHook",
    report_type: str,
    columns: list[str],
    date_from: str,
    date_to: str,
    entity: str,
    account_name: str,
    account_id: str,
    aggregation: str = "Daily",
    gcs_bucket: str = GCS_BUCKET,
    gcp_conn_id: str = GCP_CONN_ID,
    max_workers: int = _DAY_MAX_WORKERS,
    split_by_day: bool = True,
) -> dict:
    """Land + release a Microsoft Ads performance report, one day at a time by default --
    matches Solidgate's convention (include/custom_operators/solidgate/solidgate_to_gcs_operator.py),
    so each day's file lands under that day's own date folder (including today's), rather
    than the whole window's start date. Reports are server-aggregated and small either way,
    so splitting by day just means one cheap request per day instead of one for the whole
    window -- split_by_day=False is available to force a single whole-window request if
    ever needed (e.g. troubleshooting).
    """
    days_to_process = (
        _split_into_days(date_from, date_to)
        if split_by_day
        else [_whole_window(date_from, date_to)]
    )

    logger.info(
        "[%s] %s: processing %d window(s) across up to %d thread(s)",
        account_name,
        entity,
        len(days_to_process),
        max_workers,
    )

    client = GCSHook(gcp_conn_id=gcp_conn_id).get_conn()
    bucket = client.bucket(gcs_bucket)

    def fetch_window(window_meta: dict) -> dict:
        ds_str = window_meta["ds_str"]
        file_stem = f"{account_name}__{entity}"
        landing_path = (
            f"landing/microsoft_ads/{entity}/{account_name}/{ds_str}/{file_stem}.csv"
        )
        release_path = (
            f"release/microsoft_ads/{entity}/{account_name}/{ds_str}/{file_stem}.avro"
        )

        _clean_prefix(
            bucket, f"landing/microsoft_ads/{entity}/{account_name}/{ds_str}/"
        )
        _clean_prefix(
            bucket, f"release/microsoft_ads/{entity}/{account_name}/{ds_str}/"
        )

        report_request = build_report_request(
            report_type=report_type,
            columns=columns,
            account_id=account_id,
            date_from=window_meta["date_from"],
            date_to=window_meta["date_to"],
            aggregation=aggregation,
        )
        download_url = hook.resolve_report_download_url(report_request)

        if download_url is None:
            # Report completed successfully but had zero rows -- no file to download.
            # Nothing is written to landing/release for this window; callers (and
            # get_release_paths) already tolerate an all-empty result via AirflowSkipException.
            logger.info(
                "[%s] window %s: 0 records (empty report, no file)",
                account_name,
                ds_str,
            )
            return {"ds": ds_str, "records": 0, "release_path": None}

        csv_tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as csv_tmp:
                csv_tmp_path = csv_tmp.name
            _download_report_csv(download_url, csv_tmp_path)

            bucket.blob(landing_path).upload_from_filename(
                csv_tmp_path, content_type="text/csv"
            )

            release = _csv_file_to_release_avro(
                csv_path=csv_tmp_path,
                release_path=release_path,
                entity=entity,
                account_name=account_name,
                account_id=account_id,
                aggregation_type=aggregation.lower(),
                gcs_bucket=gcs_bucket,
                gcp_conn_id=gcp_conn_id,
                bucket=bucket,
            )
        finally:
            if csv_tmp_path and os.path.exists(csv_tmp_path):
                os.remove(csv_tmp_path)
        logger.info(
            "[%s] window %s: %d record(s) -> %s",
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
    results = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(fetch_window, w) for w in days_to_process]
        for future in as_completed(futures):
            results.append(future.result())

    results.sort(key=lambda r: r["ds"])
    total_records = sum(r["records"] for r in results)
    release_paths = [r["release_path"] for r in results if r["release_path"]]

    logger.info(
        "[%s] %s: done. %d window(s), %d record(s) total.",
        account_name,
        entity,
        len(results),
        total_records,
    )

    return {
        "record_count": total_records,
        "release_paths": release_paths,
    }
