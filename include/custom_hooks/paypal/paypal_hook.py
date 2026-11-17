"""OAuth-authenticated requests to PayPal's sandbox Transaction Search API."""

from __future__ import annotations

import os
import random
import time
from typing import Iterator

import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from include.config.paypal_settings import (
    ACCOUNT_NAME,
    BASE_URL,
    CLIENT_ID_ENV,
    CLIENT_SECRET_ENV,
    ENDPOINT,
    PAGE_SIZE,
    RATE_LIMIT_PER_SECOND,
)
from include.utils.rate_limiter import get_rate_limiter

_RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
_MAX_ATTEMPTS = 5
_MAX_PAGES = 10000


class PaypalHook(BaseHook):
    """Fetch transaction pages from the PayPal sandbox reporting API."""

    def __init__(
        self,
        *,
        client_id: str | None = None,
        client_secret: str | None = None,
    ) -> None:
        super().__init__()
        self.client_id = client_id or os.environ.get(CLIENT_ID_ENV)
        self.client_secret = client_secret or os.environ.get(CLIENT_SECRET_ENV)
        if not self.client_id or not self.client_secret:
            raise AirflowException(
                f"Missing PayPal sandbox credentials: set {CLIENT_ID_ENV} and "
                f"{CLIENT_SECRET_ENV}"
            )
        self.session = requests.Session()
        self.base_url = BASE_URL
        self._access_token: str | None = None
        self._token_expires_at = 0.0

    def _request(self, method: str, url: str, **kwargs) -> requests.Response:
        for attempt in range(1, _MAX_ATTEMPTS + 1):
            get_rate_limiter("paypal", RATE_LIMIT_PER_SECOND).acquire()
            try:
                response = self.session.request(method, url, timeout=30, **kwargs)
            except requests.exceptions.RequestException as exc:
                if attempt == _MAX_ATTEMPTS:
                    raise AirflowException(
                        f"PayPal request failed after {_MAX_ATTEMPTS} attempts "
                        f"for account {ACCOUNT_NAME}: {exc}"
                    ) from exc
                self.log.warning(
                    "PayPal network error on attempt %d/%d; retrying",
                    attempt,
                    _MAX_ATTEMPTS,
                )
            else:
                if 200 <= response.status_code < 300:
                    return response
                if response.status_code not in _RETRY_STATUS:
                    raise AirflowException(
                        f"PayPal returned HTTP {response.status_code} for account "
                        f"{ACCOUNT_NAME}: {response.text[:500]}"
                    )
                if attempt == _MAX_ATTEMPTS:
                    raise AirflowException(
                        f"PayPal request failed after {_MAX_ATTEMPTS} attempts "
                        f"(HTTP {response.status_code}) for account {ACCOUNT_NAME}: "
                        f"{response.text[:500]}"
                    )
                self.log.warning(
                    "PayPal transient HTTP %d on attempt %d/%d; retrying",
                    response.status_code,
                    attempt,
                    _MAX_ATTEMPTS,
                )

            delay = min(2 ** (attempt - 1), 30)
            time.sleep(random.uniform(delay * 0.5, delay))

        raise AirflowException("PayPal request exhausted retries without a response")

    def _get_access_token(self) -> str:
        if self._access_token and time.monotonic() < self._token_expires_at:
            return self._access_token

        response = self._request(
            "POST",
            f"{self.base_url}/v1/oauth2/token",
            auth=(self.client_id, self.client_secret),
            data={"grant_type": "client_credentials"},
            headers={"Accept": "application/json"},
        )
        try:
            payload = response.json()
            token = payload["access_token"]
            expires_in = int(payload.get("expires_in", 300))
        except (ValueError, KeyError, TypeError) as exc:
            raise AirflowException(
                "PayPal OAuth response did not contain a valid access token"
            ) from exc

        self._access_token = token
        self._token_expires_at = time.monotonic() + max(expires_in - 60, 0)
        return token

    def get_raw_pages(self, start_date: str, end_date: str) -> Iterator[dict]:
        """Yield raw transaction-search responses, one API page at a time."""
        page = 1
        total_pages = 1

        while page <= total_pages:
            response = self._request(
                "GET",
                f"{self.base_url}/v1/reporting/{ENDPOINT}",
                headers={
                    "Authorization": f"Bearer {self._get_access_token()}",
                    "Accept": "application/json",
                },
                params={
                    "start_date": start_date,
                    "end_date": end_date,
                    "fields": "all",
                    "page_size": PAGE_SIZE,
                    "page": page,
                    "balance_affecting_records_only": "N",
                },
            )
            try:
                payload = response.json()
            except ValueError as exc:
                raise AirflowException(
                    "PayPal transactions response was not valid JSON"
                ) from exc

            records = payload.get("transaction_details")
            if not isinstance(records, list):
                raise AirflowException(
                    "PayPal transactions response is missing the "
                    "'transaction_details' list"
                )

            try:
                total_pages = max(int(payload.get("total_pages", page)), page)
            except (TypeError, ValueError) as exc:
                raise AirflowException(
                    "PayPal transactions response contained an invalid total_pages value"
                ) from exc
            if total_pages > _MAX_PAGES:
                raise AirflowException(
                    f"PayPal transactions response reported {total_pages} pages, "
                    f"exceeding the safety limit of {_MAX_PAGES}"
                )

            yield payload
            page += 1
