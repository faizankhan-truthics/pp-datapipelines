"""GrowthBookHook -- Bearer-auth requests to the GrowthBook REST API.

Every endpoint the pipeline reads shares the same offset pagination: ``limit`` / ``offset`` in, ``hasMore`` / ``nextOffset``
out, with the records under an endpoint-specific key.
"""

from __future__ import annotations

import random
import time
from typing import Any, Iterator

import requests
from airflow.hooks.base import BaseHook
from airflow.sdk.exceptions import AirflowFailException

from include.config.growthbook_settings import (
    GROWTHBOOK_API_HOST,
    PAGE_SIZE,
    RATE_LIMIT_PER_SECOND,
    SOURCE,
    TIMEOUT,
)
from include.monitoring.api_request_monitor import record_api_request
from include.utils.rate_limiter import get_rate_limiter

_RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
_AUTH_STATUS = frozenset({401, 403})
_MAX_ATTEMPTS = 8
_BASE_DELAY = 1.0
_MAX_DELAY = 60.0


class GrowthBookHook(BaseHook):
    hook_name = "GrowthBook"

    def __init__(
        self,
        *,
        api_key: str,
        path: str,
        records_key: str,
        api_host: str = GROWTHBOOK_API_HOST,
        params: dict[str, Any] | None = None,
    ) -> None:
        super().__init__()
        self.path = path
        self.records_key = records_key
        self.params = dict(params or {})
        self.url = f"{api_host.rstrip('/')}{path}"
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Bearer {api_key}"

    def extract_records(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        return payload.get(self.records_key) or []

    def _get(self, params: dict[str, Any]) -> dict[str, Any]:
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            get_rate_limiter("growthbook", RATE_LIMIT_PER_SECOND).acquire()
            started = time.monotonic()
            try:
                resp = self.session.get(self.url, params=params, timeout=TIMEOUT)
            except requests.exceptions.RequestException as exc:
                self._record_request(status_code=None, attempt=attempt, started=started)
                self.log.warning(
                    "GET %s network error (attempt %d/%d): %s",
                    self.path,
                    attempt,
                    _MAX_ATTEMPTS,
                    exc,
                )
                if attempt == _MAX_ATTEMPTS:
                    raise
            else:
                self._record_request(
                    status_code=resp.status_code, attempt=attempt, started=started
                )
                if resp.status_code == 200:
                    return resp.json()
                if resp.status_code in _AUTH_STATUS:
                    # a retry can't fix a rejected key, so fail the task now instead of after 3 x 5 minutes
                    raise AirflowFailException(
                        f"GrowthBook rejected the API key (HTTP {resp.status_code}) on GET {self.path}: "
                        f"{self._error_message(resp)}. Check the GROWTHBOOK_API_KEY Variable."
                    )
                if resp.status_code not in _RETRY_STATUS or attempt == _MAX_ATTEMPTS:
                    resp.raise_for_status()
                self.log.warning(
                    "GET %s HTTP %s (attempt %d/%d); retrying",
                    self.path,
                    resp.status_code,
                    attempt,
                    _MAX_ATTEMPTS,
                )

            delay = min(_BASE_DELAY * (2 ** (attempt - 1)), _MAX_DELAY)
            time.sleep(random.uniform(0.5 * delay, delay))

        raise RuntimeError(f"GET {self.path} failed after {_MAX_ATTEMPTS} attempts")

    @staticmethod
    def _error_message(resp: requests.Response) -> str:
        try:
            return str(resp.json().get("message") or resp.reason or "no message")[:200]
        except Exception:
            return str(resp.reason or "no message")[:200]

    def _record_request(
        self, *, status_code: int | None, attempt: int, started: float
    ) -> None:
        try:
            record_api_request(
                source=SOURCE,
                account_name=SOURCE,
                method="GET",
                url=self.url,
                status_code=status_code,
                attempt=attempt,
                latency_ms=int((time.monotonic() - started) * 1000),
            )
        except Exception:
            self.log.warning(
                "failed to record API request for monitoring", exc_info=True
            )

    def iter_raw_pages(
        self, *, max_pages: int | None = None
    ) -> Iterator[dict[str, Any]]:
        """Yield each raw page payload, following ``nextOffset`` until ``hasMore`` is false.

        Fails instead of returning a partial snapshot: a snapshot that stopped early would look
        like success and quietly drop every record after the cut.
        """
        offset = 0
        page = 0
        while True:
            page += 1
            payload = self._get({**self.params, "limit": PAGE_SIZE, "offset": offset})
            self.log.info(
                "%s page %d: %d records",
                self.path,
                page,
                len(self.extract_records(payload)),
            )
            yield payload
            if not payload.get("hasMore"):
                break
            next_offset = payload.get("nextOffset")
            if next_offset is None:
                raise AirflowFailException(
                    f"GET {self.path}: hasMore is true but nextOffset is missing after page {page}; "
                    "refusing to land an incomplete snapshot"
                )
            if max_pages and page >= max_pages:
                raise AirflowFailException(
                    f"GET {self.path}: still more records after {page} pages (cap {max_pages}); "
                    "refusing to land a truncated snapshot"
                )
            offset = next_offset
