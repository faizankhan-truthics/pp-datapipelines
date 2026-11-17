"""Client for AnyTrack's Conversion API (reports endpoints).

    POST /v1/properties/{id}/reports/events        daily event counts (cheap)
    POST /v1/properties/{id}/reports/conversions   the events themselves

Limits (AnyTrack docs): 60 requests/minute, 1 MB response, 65 s timeout, a
31-day range per request, `limit` max 100 and `eventNames` max 50. `limit` and
`cursor` go in the request BODY. Results are newest first and pages are not
snapshot-consistent, so a caller must expect the odd duplicate or gap at page
borders and dedupe on the event `id` downstream.

`from`/`to` are dates in the PROPERTY's timezone, while an event's `time` is
UTC. `utc_range_to_api_range` pads a UTC range to cover both.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from datetime import date, datetime, timedelta, timezone

import requests
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.hooks.base import BaseHook

from include.utils.rate_limiter import get_rate_limiter

logger = logging.getLogger(__name__)

BASE_URL = "https://api.anytrack.io/v1"
MAX_RANGE_DAYS = 31
MAX_EVENT_NAMES = 50
# Safe defaults from AnyTrack's documented limits. AnyTrack can raise both for an
# account (the DAG passes the raised values from anytrack_settings.py).
PAGE_LIMIT = 100
REQUESTS_PER_MINUTE = 60
RATE_SAFETY = 0.95  # stay a little under the limit
MAX_RESPONSE_BYTES = 1024 * 1024  # the API's 1 MB response cap
RESPONSE_WARN_FRACTION = 0.8
REQUEST_TIMEOUT_S = 70
MAX_RETRIES = 5
_RETRY_STATUS = frozenset({500, 502, 503, 504})
MAX_RETRY_AFTER_S = 120
# Backstop for a pagination bug: a 29-day chunk of the largest property is about
# 26,000 pages of 200.
MAX_PAGES = 100_000


def _retry_after_seconds(value: str | None) -> int:
    """Seconds to wait from a Retry-After header; the HTTP-date form falls back to 5."""
    try:
        return min(max(int(value), 1), MAX_RETRY_AFTER_S)
    except (TypeError, ValueError):
        return 5


def utc_range_to_api_range(utc_start: date, utc_end: date) -> tuple[str, str]:
    """Property-timezone dates (`from`, `to`) whose range covers the UTC days in
    [utc_start, utc_end).

    The property's local day starts before or after UTC midnight, so one day is
    added on each side; the caller then keeps only events whose UTC `time` falls
    in the range. Fixed padding also covers DST changes. The padded range must
    fit the API's 31-day cap. The range is inclusive of both dates, so a chunk of
    n days asks for n + 2 dates, which limits a chunk to 29 days.
    """
    api_from = utc_start - timedelta(days=1)
    api_to = utc_end  # the last UTC day is utc_end - 1, plus one day of padding
    if (api_to - api_from).days + 1 > MAX_RANGE_DAYS:
        raise ValueError(
            f"{api_from}..{api_to} exceeds the API's {MAX_RANGE_DAYS}-day range; "
            "use a chunk of at most 29 days"
        )
    return api_from.isoformat(), api_to.isoformat()


def split_date_range(
    date_from: date, date_to: date, chunk_days: int
) -> list[tuple[date, date]]:
    """Split [date_from, date_to) into consecutive [start, end) chunks."""
    if chunk_days < 1:
        raise ValueError(f"chunk_days must be >= 1, got {chunk_days}")
    if date_to <= date_from:
        raise ValueError(f"date_to {date_to} must be after date_from {date_from}")
    chunks = []
    start = date_from
    while start < date_to:
        end = min(start + timedelta(days=chunk_days), date_to)
        chunks.append((start, end))
        start = end
    return chunks


def parse_event_time(value: str) -> datetime:
    """API `time` (ISO 8601 UTC, e.g. 2026-11-02T14:03:11.000Z) -> aware datetime."""
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _data(path: str, resp: dict) -> list[dict]:
    """The page's events. The API answers an empty window with `"data": []`, so a
    missing or non-list `data` is an error envelope or a changed schema: fail instead
    of landing fewer events and still passing."""
    data = resp.get("data") if isinstance(resp, dict) else None
    if not isinstance(data, list):
        raise AirflowFailException(
            f"AnyTrack CAPI {path}: a 200 response without a `data` list: "
            f"{str(resp)[:300]}"
        )
    return data


class AnytrackCapiHook(BaseHook):
    def __init__(
        self,
        api_key: str,
        requests_per_minute: int = REQUESTS_PER_MINUTE,
        page_limit: int = PAGE_LIMIT,
    ):
        super().__init__()
        if not api_key:
            raise AirflowException("AnyTrack API key is empty")
        if requests_per_minute < 1 or page_limit < 1:
            raise AirflowException(
                "requests_per_minute and page_limit must be positive, got "
                f"{requests_per_minute} and {page_limit}"
            )
        self._api_key = api_key
        self._page_limit = page_limit
        self.max_response_bytes = 0
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            }
        )
        # One bucket per process; the DAG's 1-slot pool keeps it to one task. The
        # limit is per ACCOUNT, shared by every property, so never run two at once.
        self._limiter = get_rate_limiter(
            "anytrack_capi", requests_per_minute / 60 * RATE_SAFETY
        )

    def _post(self, path: str, body: dict) -> dict:
        for attempt in range(MAX_RETRIES):
            self._limiter.acquire()
            try:
                resp = self._session.post(
                    f"{BASE_URL}{path}", json=body, timeout=REQUEST_TIMEOUT_S
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                # A network blip must not lose the whole chunk: the task would
                # restart from page 1 and re-read every page at the rate limit.
                reason, wait = type(exc).__name__, 2**attempt
            else:
                if resp.status_code == 200:
                    self._track_size(path, len(resp.content))
                    try:
                        return resp.json()
                    except ValueError:
                        reason, wait = "a 200 with an invalid JSON body", 2**attempt
                elif resp.status_code == 429:
                    reason = "HTTP 429"
                    wait = _retry_after_seconds(resp.headers.get("Retry-After"))
                elif resp.status_code in _RETRY_STATUS:
                    reason, wait = f"HTTP {resp.status_code}", 2**attempt
                else:
                    # 400/401/403/404/413 and the like will not change on a retry,
                    # and a retry would restart the chunk from page 1.
                    raise AirflowFailException(
                        f"AnyTrack CAPI {path} returned {resp.status_code}: "
                        f"{resp.text[:300]}"
                    )
            logger.warning(
                "AnyTrack CAPI %s: %s, retrying in %ss (attempt %s/%s)",
                path,
                reason,
                wait,
                attempt + 1,
                MAX_RETRIES,
            )
            time.sleep(wait)
        raise AirflowException(
            f"AnyTrack CAPI {path}: gave up after {MAX_RETRIES} tries"
        )

    def _track_size(self, path: str, size: int) -> None:
        """Track the largest response; warn before it nears the API's 1 MB cap."""
        if size > self.max_response_bytes:
            self.max_response_bytes = size
            if size > MAX_RESPONSE_BYTES * RESPONSE_WARN_FRACTION:
                logger.warning(
                    "AnyTrack CAPI %s: a response was %.0f KB, close to the %.0f KB "
                    "cap. Lower ANYTRACK_CAPI_PAGE_LIMIT if requests start failing.",
                    path,
                    size / 1024,
                    MAX_RESPONSE_BYTES / 1024,
                )

    def _paginate(self, path: str, body: dict, limit: int) -> Iterator[dict]:
        cursor = None
        seen_cursors: set[str] = set()
        for _ in range(MAX_PAGES):
            page_body = {**body, "limit": limit}
            if cursor:
                page_body["cursor"] = cursor
            resp = self._post(path, page_body)
            yield resp
            cursor = resp.get("nextCursor")
            if not cursor:
                return
            if cursor in seen_cursors:
                raise AirflowException(
                    f"AnyTrack CAPI {path}: the API returned a cursor it had already "
                    "returned, stopping instead of looping forever"
                )
            seen_cursors.add(cursor)
        raise AirflowException(
            f"AnyTrack CAPI {path}: more than {MAX_PAGES} pages; use a smaller chunk"
        )

    def list_event_names(
        self, property_id: str, date_from: str, date_to: str
    ) -> list[str]:
        """Distinct event names the property fired in the range (daily counts report)."""
        names: set[str] = set()
        path_events = f"/properties/{property_id}/reports/events"
        for resp in self._paginate(
            path_events,
            {"from": date_from, "to": date_to},
            PAGE_LIMIT,  # the daily counts report keeps the documented page size
        ):
            names.update(row["eventName"] for row in _data(path_events, resp))
        return sorted(names)

    def iter_conversion_pages(
        self,
        property_id: str,
        date_from: str,
        date_to: str,
        event_names: list[str],
    ) -> Iterator[list[dict]]:
        """Yield one page (<= 100 events) at a time, so memory stays flat."""
        if not event_names:
            return
        if len(event_names) > MAX_EVENT_NAMES:
            raise AirflowException(
                f"{len(event_names)} event names exceeds the API cap of {MAX_EVENT_NAMES}"
            )
        body = {"from": date_from, "to": date_to, "eventNames": event_names}
        path = f"/properties/{property_id}/reports/conversions"
        for resp in self._paginate(path, body, self._page_limit):
            yield _data(path, resp)
