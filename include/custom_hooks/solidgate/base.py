"""SolidgateBaseHook — HMAC-signed requests to the Solidgate Reports API.

Signing (Solidgate has no token step — every request is signed):
    header ``merchant``  = public key
    header ``signature`` = base64( hex( HMAC_SHA512(pk + body + pk, secret) ) )
The signed string must be byte-identical to the body sent, so we serialize once and
send that exact string via ``data=``.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import random
import time
from typing import Any, Iterator

import requests
from airflow.hooks.base import BaseHook

from include.config.solidgate_settings import (
    BASE_URL,
    RATE_LIMIT_PER_SECOND,
    SOLIDGATE_PUBLIC_KEY_ENV,
    SOLIDGATE_SECRET_KEY_ENV,
    SOURCE,
    TIMEOUT,
)
from include.monitoring.api_request_monitor import record_api_request
from include.utils.rate_limiter import get_rate_limiter

_RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
_MAX_ATTEMPTS = 5
_BASE_DELAY = 1.0  # seconds; exponential backoff, capped at _MAX_DELAY
_MAX_DELAY = 30.0


class SolidgateBaseHook(BaseHook):
    hook_name = "Solidgate"

    _PATH: str = ""

    _PAGE_LIMIT: int = 2000

    _MAX_PAGES: int = 2000

    @staticmethod
    def _extract_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
        """Pull the list of records out of one page's payload. Subclasses override."""
        raise NotImplementedError

    @staticmethod
    def _next_cursor(payload: dict[str, Any]) -> str | None:
        meta = payload.get("metadata") or payload.get("body", {}).get("metadata", {})
        return (meta or {}).get("next_page_iterator") or None

    def iter_raw_pages(
        self,
        date_from: str,
        date_to: str,
        *,
        filter: str = "updated_at",
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield the full raw payload dict for each page, following pagination.

        Lossless: each yielded payload is exactly what the API returned (the records
        plus the API's own metadata/envelope), which is what the immutable Landing
        layer captures. Generator — one page in memory at a time, nothing accumulated.
        ``limit`` defaults to the hook's ``_PAGE_LIMIT``.
        """
        if not self._PATH:
            raise ValueError(f"{type(self).__name__} must define _PATH")
        limit = limit or self._PAGE_LIMIT
        cursor: str | None = None
        page = 0
        total = 0
        while True:
            page += 1
            body_obj: dict[str, Any] = {
                "filter": filter,
                "date_from": date_from,
                "date_to": date_to,
                "limit": limit,
            }
            if cursor:
                body_obj["next_page_iterator"] = cursor
            resp = self.signed_request("POST", self._PATH, body=json.dumps(body_obj))
            resp.raise_for_status()
            payload = resp.json()
            n = len(self._extract_records(payload))
            total += n
            self.log.info("page %d: %d records (total %d)", page, n, total)
            yield payload
            cursor = self._next_cursor(payload)
            if not cursor:
                break
            if page >= self._MAX_PAGES:
                raise RuntimeError(
                    f"{type(self).__name__}.iter_raw_pages exceeded _MAX_PAGES="
                    f"{self._MAX_PAGES} (date_from={date_from}, date_to={date_to}); "
                    "pagination cursor may be stuck"
                )

    def iter_records(
        self,
        date_from: str,
        date_to: str,
        *,
        filter: str = "updated_at",
        limit: int | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield each page's extracted records (list), built on ``iter_raw_pages``.

        Same flat-memory guarantee: one page at a time. Use this when you only need
        the records; use ``iter_raw_pages`` when you also need the raw envelope.
        """
        for payload in self.iter_raw_pages(
            date_from, date_to, filter=filter, limit=limit
        ):
            yield self._extract_records(payload)

    def __init__(
        self,
        *,
        base_url: str = BASE_URL,
        timeout: float | tuple[float, float] = TIMEOUT,
        public_key_env: str | None = None,
        secret_key_env: str | None = None,
    ) -> None:
        super().__init__()
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.public_key_env = public_key_env or SOLIDGATE_PUBLIC_KEY_ENV
        self.secret_key_env = secret_key_env or SOLIDGATE_SECRET_KEY_ENV
        self.account_name = self.public_key_env.replace("SOLIDGATE_PUBLIC_KEY_", "", 1)
        self._public_key: str | None = None
        self._secret_key: bytes | None = None

    def _load_credentials(self) -> None:
        if self._public_key is None:
            public_key = os.environ.get(self.public_key_env)
            secret_key = os.environ.get(self.secret_key_env)
            if not public_key or not secret_key:
                raise ValueError(
                    f"Missing Solidgate credentials: set "
                    f"{self.public_key_env} and {self.secret_key_env}"
                )
            self._public_key = public_key
            self._secret_key = secret_key.encode("utf-8")

    def _sign(self, body: str) -> str:
        self._load_credentials()
        pk = self._public_key.encode("utf-8")
        digest_hex = hmac.new(
            self._secret_key, pk + body.encode("utf-8") + pk, hashlib.sha512
        ).hexdigest()
        return base64.b64encode(digest_hex.encode("utf-8")).decode("utf-8")

    def _headers(self, body: str, *, content_type: bool = False) -> dict[str, str]:
        self._load_credentials()
        headers = {"merchant": self._public_key, "signature": self._sign(body)}
        if content_type:
            headers["Content-Type"] = "application/json"
        return headers

    def signed_request(
        self,
        method: str,
        path_or_url: str,
        *,
        body: str = "",
        allow_redirects: bool = True,
    ) -> requests.Response:
        url = (
            path_or_url
            if path_or_url.startswith("http")
            else f"{self.base_url}/{path_or_url.lstrip('/')}"
        )
        headers = self._headers(body, content_type=bool(body))

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            get_rate_limiter("solidgate", RATE_LIMIT_PER_SECOND).acquire()
            started = time.monotonic()
            try:
                self.log.debug(
                    "%s %s %s merchant=%s signature=<redacted> (attempt %d/%d)",
                    method,
                    url,
                    body,
                    headers.get("merchant"),
                    attempt,
                    _MAX_ATTEMPTS,
                )
                resp = requests.request(
                    method,
                    url,
                    data=body or None,
                    headers=headers,
                    timeout=self.timeout,
                    allow_redirects=allow_redirects,
                )
            except requests.exceptions.RequestException as exc:
                self._record_request(
                    method, url, status_code=None, attempt=attempt, started=started
                )
                self.log.warning(
                    "%s %s network error (attempt %d/%d): %s",
                    method,
                    url,
                    attempt,
                    _MAX_ATTEMPTS,
                    exc,
                )
                if attempt == _MAX_ATTEMPTS:
                    raise
            else:
                self._record_request(
                    method,
                    url,
                    status_code=resp.status_code,
                    attempt=attempt,
                    started=started,
                )
                self.log.info("%s %s -> HTTP %s", method, url, resp.status_code)
                if resp.status_code not in _RETRY_STATUS or attempt == _MAX_ATTEMPTS:
                    return resp
                self.log.warning(
                    "%s %s transient HTTP %s (attempt %d/%d); retrying",
                    method,
                    url,
                    resp.status_code,
                    attempt,
                    _MAX_ATTEMPTS,
                )

            delay = min(_BASE_DELAY * (2 ** (attempt - 1)), _MAX_DELAY)
            time.sleep(random.uniform(0.5 * delay, delay))

        raise RuntimeError(f"{method} {url} failed after {_MAX_ATTEMPTS} attempts")

    def _record_request(
        self,
        method: str,
        url: str,
        *,
        status_code: int | None,
        attempt: int,
        started: float,
    ) -> None:
        try:
            record_api_request(
                source=SOURCE,
                account_name=self.account_name,
                method=method,
                url=url,
                status_code=status_code,
                attempt=attempt,
                latency_ms=int((time.monotonic() - started) * 1000),
            )
        except Exception:
            self.log.warning(
                "failed to record API request for monitoring", exc_info=True
            )
