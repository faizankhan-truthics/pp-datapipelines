"""ZendeskBaseHook -- Basic-auth requests to the Zendesk Support API.

Auth: HTTP Basic with username ``{email}/token`` and the API token as the
password (Zendesk retired bare API-key auth years ago; every token is tied
to the agent/admin account it was generated under).

Pagination comes in two flavors depending on the endpoint (subclasses pick
one via the mixins below):
  * Cursor-based (tickets, users) -- ``after_cursor`` / ``end_of_stream``.
  * Time-based (ticket_events)    -- ``next_page`` (a ready-to-call absolute
    URL) / ``end_of_stream``.
"""

from __future__ import annotations

import random
import time
from typing import Any, Iterator

import requests
from airflow.hooks.base import BaseHook

from include.config.zendesk_settings import RATE_LIMIT_PER_SECOND, SOURCE, TIMEOUT
from include.monitoring.api_request_monitor import record_api_request
from include.utils.rate_limiter import get_rate_limiter

_RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
_MAX_ATTEMPTS = 8
_BASE_DELAY = 1.0
_MAX_DELAY = 60.0


class ZendeskBaseHook(BaseHook):
    hook_name = "Zendesk"

    _PATH: str = ""
    _RECORDS_KEY: str = ""

    def __init__(self, *, subdomain: str, email: str, api_token: str) -> None:
        super().__init__()
        self.subdomain = subdomain
        self.base_url = f"https://{subdomain}.zendesk.com/api/v2"
        self.session = requests.Session()
        self.session.auth = (f"{email}/token", api_token)

    @staticmethod
    def extract_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
        raise NotImplementedError

    def _first_page_params(self, resume_token: str | int | None) -> dict[str, Any]:
        raise NotImplementedError

    def _next_page(self, payload: dict[str, Any]) -> tuple[Any, bool]:
        """Return (params_or_url_for_next_call, is_done)."""
        raise NotImplementedError

    def next_resume_token(self, last_payload: dict[str, Any] | None) -> str | None:
        raise NotImplementedError

    def _get(self, params_or_url: Any) -> dict[str, Any]:
        if isinstance(params_or_url, str):
            url, params = params_or_url, None
        else:
            url, params = f"{self.base_url}{self._PATH}", params_or_url

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            get_rate_limiter("zendesk", RATE_LIMIT_PER_SECOND).acquire()
            started = time.monotonic()
            try:
                resp = self.session.get(url, params=params, timeout=TIMEOUT)
            except requests.exceptions.RequestException as exc:
                self._record_request(
                    url, status_code=None, attempt=attempt, started=started
                )
                self.log.warning(
                    "GET %s network error (attempt %d/%d): %s",
                    url,
                    attempt,
                    _MAX_ATTEMPTS,
                    exc,
                )
                if attempt == _MAX_ATTEMPTS:
                    raise
            else:
                self._record_request(
                    url, status_code=resp.status_code, attempt=attempt, started=started
                )
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code not in _RETRY_STATUS or attempt == _MAX_ATTEMPTS:
                    resp.raise_for_status()
                if resp.status_code == 429:
                    # Zendesk always returns Retry-After (seconds) on rate limit.
                    retry_after = float(resp.headers.get("Retry-After", _BASE_DELAY))
                    self.log.warning(
                        "GET %s rate limited; sleeping %.1fs (Retry-After)",
                        url,
                        retry_after,
                    )
                    time.sleep(retry_after)
                    continue
                self.log.warning(
                    "GET %s transient HTTP %s (attempt %d/%d); retrying",
                    url,
                    resp.status_code,
                    attempt,
                    _MAX_ATTEMPTS,
                )

            delay = min(_BASE_DELAY * (2 ** (attempt - 1)), _MAX_DELAY)
            time.sleep(random.uniform(0.5 * delay, delay))

        raise RuntimeError(f"GET {url} failed after {_MAX_ATTEMPTS} attempts")

    def _record_request(
        self, url: str, *, status_code: int | None, attempt: int, started: float
    ) -> None:
        try:
            record_api_request(
                source=SOURCE,
                account_name=self.subdomain,
                method="GET",
                url=url,
                status_code=status_code,
                attempt=attempt,
                latency_ms=int((time.monotonic() - started) * 1000),
            )
        except Exception:
            self.log.warning(
                "failed to record API request for monitoring", exc_info=True
            )

    def iter_raw_pages(
        self, resume_token: str | int | None, *, max_pages: int | None = None
    ) -> Iterator[dict[str, Any]]:
        """Yield the full raw payload dict for each page, following pagination."""
        params: Any = self._first_page_params(resume_token)
        page = 0
        while True:
            page += 1
            payload = self._get(params)
            n = len(self.extract_records(payload))
            self.log.info("%s page %d: %d records", self._PATH, page, n)
            yield payload
            params, done = self._next_page(payload)
            if done or (max_pages and page >= max_pages):
                break


class _CursorPaginationMixin:
    """tickets, users -- after_cursor / end_of_stream.

    A saved checkpoint (`get_saved_cursor`) is always a Zendesk-issued opaque
    cursor *string* (Airflow Variables round-trip as strings). A
    `date_from` test run passes an *int* Unix timestamp instead --
    those must start a fresh export (`start_time=`), not be sent as `cursor=`,
    or Zendesk 400s ("cursor=1786320000" is not a valid cursor token).
    """

    def _first_page_params(self, resume_token):
        if resume_token is None:
            return {"start_time": 1}
        if isinstance(resume_token, int):
            return {"start_time": resume_token}
        return {"cursor": resume_token}

    def _next_page(self, payload):
        if payload.get("end_of_stream"):
            return None, True
        cursor = payload.get("after_cursor")
        if not cursor:
            return None, True
        return {"cursor": cursor}, False

    def next_resume_token(self, last_payload):
        return last_payload.get("after_cursor") if last_payload else None


class _TimeBasedPaginationMixin:
    """ticket_events -- next_page (absolute URL) / end_of_stream."""

    def _first_page_params(self, resume_token):
        return {"start_time": resume_token or 1}

    def _next_page(self, payload):
        if payload.get("end_of_stream"):
            return None, True
        next_page = payload.get("next_page")
        if not next_page:
            return None, True
        return next_page, False

    def next_resume_token(self, last_payload):
        return last_payload.get("end_time") if last_payload else None
