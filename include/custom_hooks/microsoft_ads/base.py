"""MicrosoftAdsHook -- REST calls to the Microsoft Advertising Reporting API.

Auth: Google OAuth (this account is Google-linked, not Microsoft/Entra -- see the
IdentityProvider header below) + a developer token, both account-wide. Only
CustomerAccountId varies per account. Every SOAP header element has a documented 1:1
REST header equivalent (Authorization: Bearer, DeveloperToken, CustomerId,
CustomerAccountId, IdentityProvider) -- see
https://learn.microsoft.com/en-us/advertising/customer-management-service/getuser?view=bingads-13
(REST zone) for the mapping this hook follows.
"""

from __future__ import annotations

import json
import os
import random
import time
from typing import Any

import requests
from airflow.hooks.base import BaseHook

from include.config.microsoft_ads_settings import (
    GOOGLE_TOKEN_URL,
    RATE_LIMIT_PER_SECOND,
    SOURCE,
    TIMEOUT,
    reporting_base_url,
)
from include.monitoring.api_request_monitor import record_api_request
from include.utils.rate_limiter import get_rate_limiter

_RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
_MAX_ATTEMPTS = 5
_BASE_DELAY = 1.0
_MAX_DELAY = 30.0

# Access tokens are short-lived (~1hr); refresh a little early to avoid a token expiring
# mid-request.
_TOKEN_REFRESH_SKEW_SECONDS = 120


class MicrosoftAdsHook(BaseHook):
    hook_name = "MicrosoftAds"

    def __init__(self, *, account_id_env: str, account_name: str) -> None:
        super().__init__()
        self.account_id_env = account_id_env
        self.account_name = account_name
        self._access_token: str | None = None
        self._access_token_expires_at: float = 0.0

    # -- credentials, read inside the task body, never at DAG-parse time --

    def _client_id(self) -> str:
        return os.environ["MSADS_CLIENT_ID"]

    def _client_secret(self) -> str:
        return os.environ["MSADS_CLIENT_SECRET"]

    def _refresh_token(self) -> str:
        return os.environ["MSADS_REFRESH_TOKEN"]

    def _developer_token(self) -> str:
        return os.environ["MSADS_DEVELOPER_TOKEN"]

    def _customer_id(self) -> str:
        return os.environ["MSADS_CUSTOMER_ID"]

    def _customer_account_id(self) -> str:
        return os.environ[self.account_id_env]

    # -- OAuth access token, lazily refreshed --

    def _get_access_token(self) -> str:
        now = time.monotonic()
        if self._access_token and now < self._access_token_expires_at:
            return self._access_token

        resp = requests.post(
            GOOGLE_TOKEN_URL,
            data={
                "client_id": self._client_id(),
                "client_secret": self._client_secret(),
                "refresh_token": self._refresh_token(),
                "grant_type": "refresh_token",
            },
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        payload = resp.json()
        self._access_token = payload["access_token"]
        expires_in = int(payload.get("expires_in", 3600))
        self._access_token_expires_at = now + expires_in - _TOKEN_REFRESH_SKEW_SECONDS
        return self._access_token

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._get_access_token()}",
            "DeveloperToken": self._developer_token(),
            "CustomerId": self._customer_id(),
            "CustomerAccountId": self._customer_account_id(),
            "IdentityProvider": "Google",
            "Content-Type": "application/json",
        }

    # -- HTTP, with the same retry/backoff/monitoring shape used by Stripe/Solidgate --

    def rest_request(
        self, method: str, url: str, *, json_body: dict | None = None
    ) -> requests.Response:
        body = json.dumps(json_body) if json_body is not None else None

        for attempt in range(1, _MAX_ATTEMPTS + 1):
            get_rate_limiter("microsoft_ads", RATE_LIMIT_PER_SECOND).acquire()
            started = time.monotonic()
            try:
                self.log.debug(
                    "%s %s (attempt %d/%d)", method, url, attempt, _MAX_ATTEMPTS
                )
                resp = requests.request(
                    method,
                    url,
                    data=body,
                    headers=self._headers(),
                    timeout=TIMEOUT,
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

    # -- SubmitGenerateReport / PollGenerateReport --

    def submit_report(self, report_request: dict[str, Any]) -> str:
        url = f"{reporting_base_url()}/Reporting/v13/GenerateReport/Submit"
        resp = self.rest_request(
            "POST", url, json_body={"ReportRequest": report_request}
        )
        if not resp.ok:
            # requests' own HTTPError message never includes the response body, and
            # Microsoft's error detail (invalid column, bad combination, etc.) only
            # exists there -- log it before raising so the real cause is visible.
            self.log.error(
                "GenerateReport/Submit rejected (HTTP %s): %s",
                resp.status_code,
                resp.text,
            )
        resp.raise_for_status()
        report_request_id = resp.json()["ReportRequestId"]
        self.log.info("submitted report, ReportRequestId=%s", report_request_id)
        return report_request_id

    def poll_report(
        self, report_request_id: str, *, max_attempts: int = 30, wait_seconds: int = 15
    ) -> str | None:
        """Poll until Status == Success; return the ReportDownloadUrl, or None if the
        report completed successfully but produced no data (no file is generated for an
        empty result -- Status is still "Success" with a null ReportDownloadUrl)."""
        url = f"{reporting_base_url()}/Reporting/v13/GenerateReport/Poll"
        for attempt in range(1, max_attempts + 1):
            resp = self.rest_request(
                "POST", url, json_body={"ReportRequestId": report_request_id}
            )
            resp.raise_for_status()
            status_obj = resp.json()["ReportRequestStatus"]
            status = status_obj.get("Status")
            self.log.info("poll %d/%d -> %s", attempt, max_attempts, status)
            if status == "Success":
                download_url = status_obj.get("ReportDownloadUrl")
                if not download_url:
                    self.log.info(
                        "report succeeded with no data (empty result, no file generated)"
                    )
                    return None
                return download_url
            if status == "Error":
                raise RuntimeError(f"report generation failed: {status_obj}")
            time.sleep(wait_seconds)
        raise TimeoutError(
            f"report {report_request_id} not ready after {max_attempts} polls"
        )

    def resolve_report_download_url(self, report_request: dict[str, Any]) -> str | None:
        """Submit + poll; return the ZIP download URL, or None if there's no data."""
        report_request_id = self.submit_report(report_request)
        return self.poll_report(report_request_id)
