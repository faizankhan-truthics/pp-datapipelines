"""Pull one chunk of AnyTrack Conversion API events into GCS landing.

Property agnostic: the caller passes the property id, the product and a UTC date
chunk. Memory stays flat however large the property is -- events are read one
page (<= 100) at a time and flushed to NDJSON shard files as soon as
`shard_events` of them are buffered.

Landing layout (one folder per UTC day, one line per event):

    landing/anytrack/<product>/capi_events/<utc day>/<chunk start>_<seq>.ndjson
    {"_gcs_file_path": "<this object>", "raw_data": "<the API event, as JSON text>"}

That is the same two-column shape the webhook landing files use, so the staging
load is identical. Re-running a chunk first deletes its own earlier shards, so a
retry never leaves stale files behind.
"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import date, datetime, time, timezone
from time import monotonic
from typing import Protocol

from airflow.providers.google.cloud.hooks.gcs import GCSHook

from include.config.anytrack_settings import (
    CAPI_EXCLUDED_EVENT_NAMES,
    CAPI_LOG_EVERY_PAGES,
    CAPI_SHARD_EVENTS,
    GCP_CONN_ID,
    GCS_BUCKET,
    capi_landing_prefix,
)
from include.custom_hooks.anytrack.capi_hook import (
    MAX_EVENT_NAMES,
    AnytrackCapiHook,
    parse_event_time,
    utc_range_to_api_range,
)

logger = logging.getLogger(__name__)


class Sink(Protocol):
    def delete_prefix(self, prefix: str) -> None: ...
    def upload(self, object_name: str, data: str) -> None: ...


class GcsSink:
    def __init__(self, bucket: str = GCS_BUCKET, conn_id: str = GCP_CONN_ID):
        self._bucket = bucket
        self._hook = GCSHook(gcp_conn_id=conn_id)

    def delete_prefix(self, prefix: str) -> None:
        bucket = self._hook.get_conn().bucket(self._bucket)
        for blob in bucket.list_blobs(prefix=prefix):
            blob.delete()

    def upload(self, object_name: str, data: str) -> None:
        self._hook.upload(
            bucket_name=self._bucket,
            object_name=object_name,
            data=data,
            mime_type="application/x-ndjson",
        )


def _name_batches(names: list[str]) -> list[list[str]]:
    return [
        names[i : i + MAX_EVENT_NAMES] for i in range(0, len(names), MAX_EVENT_NAMES)
    ]


def overall_progress_pct(
    done_days: int, chunk_days: int, window_days: int, chunk_fraction: float
) -> float:
    """Percent of the whole requested window that is done.

    `done_days` days belong to chunks finished before this one, and
    `chunk_fraction` (0..1) of this chunk's `chunk_days` is read so far.
    """
    if window_days <= 0:
        return 100.0
    fraction = min(max(chunk_fraction, 0.0), 1.0)
    pct = (done_days + chunk_days * fraction) / window_days * 100
    return round(min(pct, 100.0), 1)


def chunk_fraction_read(
    reached: datetime | None,
    lo: datetime,
    hi: datetime,
    batch_index: int,
    batch_count: int,
) -> float:
    """How much of a chunk is read, from the time of the oldest event read so far.

    The API returns newest first, so the range between `hi` and `reached` is
    already read. When the event names are pulled in several batches (more than
    50 names) each batch walks the whole chunk again, so the batch index counts.
    """
    if reached is None:
        batch_fraction = 0.0
    else:
        batch_fraction = min(max((hi - reached) / (hi - lo), 0.0), 1.0)
    return (batch_index + batch_fraction) / max(batch_count, 1)


def pull_chunk_to_landing(
    hook: AnytrackCapiHook,
    sink: Sink,
    *,
    product: str,
    property_id: str,
    chunk_start: date,
    chunk_end: date,
    shard_events: int = CAPI_SHARD_EVENTS,
    window_start: date | None = None,
    window_end: date | None = None,
    chunk_index: int = 1,
    chunk_total: int = 1,
    log_every_pages: int = CAPI_LOG_EVERY_PAGES,
) -> dict:
    """Pull events with UTC time in [chunk_start, chunk_end) and land them.

    Returns a summary: the UTC days that received files (for the staging load),
    event and file counts, the event names pulled and the number of events the
    padded API range returned outside the chunk (dropped; the neighbouring
    chunk lands them).

    Progress is logged so a run can be followed in the task log: one line when the
    chunk starts, one every `log_every_pages` pages and one when it ends, each with
    the percent of the whole window (`window_start`..`window_end`, default: this
    chunk) that is done.
    """
    api_from, api_to = utc_range_to_api_range(chunk_start, chunk_end)
    names = [
        n
        for n in hook.list_event_names(property_id, api_from, api_to)
        if n not in CAPI_EXCLUDED_EVENT_NAMES
    ]
    logger.info("%s %s..%s event names: %s", product, chunk_start, chunk_end, names)

    lo = datetime.combine(chunk_start, time.min, tzinfo=timezone.utc)
    hi = datetime.combine(chunk_end, time.min, tzinfo=timezone.utc)
    tag = chunk_start.isoformat()

    window_start = window_start or chunk_start
    window_end = window_end or chunk_end
    window_days = (window_end - window_start).days
    chunk_days = (chunk_end - chunk_start).days
    done_days = (chunk_start - window_start).days
    batches = _name_batches(names)
    started = monotonic()
    label = f"[capi {product}] chunk {chunk_index}/{chunk_total}"

    def progress(reached: datetime | None, batch_index: int) -> tuple[float, float]:
        fraction = chunk_fraction_read(reached, lo, hi, batch_index, len(batches))
        return (
            round(fraction * 100, 1),
            overall_progress_pct(done_days, chunk_days, window_days, fraction),
        )

    logger.info(
        "%s %s..%s starting: overall %.1f%% of %s day(s) done before this chunk",
        label,
        chunk_start,
        chunk_end,
        overall_progress_pct(done_days, chunk_days, window_days, 0.0),
        window_days,
    )

    # A retry rewrites this chunk's shards; clear the earlier ones first.
    day = chunk_start
    while day < chunk_end:
        sink.delete_prefix(f"{capi_landing_prefix(product, day.isoformat())}{tag}_")
        day = date.fromordinal(day.toordinal() + 1)

    buffers: dict[str, list[str]] = defaultdict(list)
    buffered = 0
    seq = 0
    days_written: set[str] = set()
    events = files = out_of_window = 0

    def flush() -> None:
        nonlocal buffered, seq, files
        for day_str, lines in buffers.items():
            if not lines:
                continue
            object_name = (
                f"{capi_landing_prefix(product, day_str)}{tag}_{seq:05d}.ndjson"
            )
            payload = "\n".join(
                json.dumps({"_gcs_file_path": object_name, "raw_data": line})
                for line in lines
            )
            sink.upload(object_name, payload + "\n")
            days_written.add(day_str)
            files += 1
            seq += 1
        buffers.clear()
        buffered = 0

    pages = 0
    reached = None
    for batch_index, batch in enumerate(batches):
        for page in hook.iter_conversion_pages(property_id, api_from, api_to, batch):
            pages += 1
            for event in page:
                when = parse_event_time(event["time"])
                reached = when  # newest first: the oldest event read so far
                if not (lo <= when < hi):
                    out_of_window += 1
                    continue
                buffers[when.date().isoformat()].append(json.dumps(event))
                buffered += 1
                events += 1
                if buffered >= shard_events:
                    flush()
            if pages % log_every_pages == 0:
                chunk_pct, overall_pct = progress(reached, batch_index)
                logger.info(
                    "%s overall %.1f%% | this chunk %.1f%% | read down to %s | "
                    "pages %s | events %s",
                    label,
                    overall_pct,
                    chunk_pct,
                    reached.isoformat() if reached else "-",
                    pages,
                    events,
                )
    flush()
    logger.info(
        "%s done in %.0fs: %s event(s) in %s file(s), %s page(s). Overall %.1f%%",
        label,
        monotonic() - started,
        events,
        files,
        pages,
        overall_progress_pct(done_days, chunk_days, window_days, 1.0),
    )

    return {
        "chunk_start": tag,
        "chunk_end": chunk_end.isoformat(),
        "days_written": sorted(days_written),
        "events": events,
        "files": files,
        "event_names": names,
        "out_of_window": out_of_window,
    }
