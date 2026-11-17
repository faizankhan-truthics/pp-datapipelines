"""StripeHook -- rate-limited, retrying requests to the Stripe REST API."""

import os
import random
import time
from datetime import datetime, timezone

import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from include.config.stripe_settings import ENDPOINT_EXTRA_PARAMS, RATE_LIMIT_PER_SECOND
from include.utils.rate_limiter import get_rate_limiter

_RETRY_STATUS = frozenset({429, 500, 502, 503, 504})


class StripeHook(BaseHook):
    def __init__(self, api_key_env: str, account_id: str):
        super().__init__()
        self.account_id = account_id
        self.api_key_env = api_key_env
        api_key = os.environ.get(api_key_env)
        if not api_key:
            raise AirflowException(
                f"Missing Stripe API key: environment variable '{api_key_env}' is not set"
            )
        self.api_key = api_key
        self.base_url = "https://api.stripe.com/v1"
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {self.api_key}"})

    def _make_request_with_retry(
        self, url: str, params: dict, max_retries: int = 5
    ) -> dict:
        base_delay, max_delay = 1.0, 30.0
        for attempt in range(max_retries):
            get_rate_limiter("stripe", RATE_LIMIT_PER_SECOND).acquire()
            try:
                response = self.session.get(url, params=params, timeout=30)
                if response.status_code == 200:
                    return response.json()
            except requests.exceptions.RequestException as e:
                self.log.warning(f"Network exception: {str(e)}. Retrying...")
                if attempt == max_retries - 1:
                    raise AirflowException(
                        f"Stripe API request failed after {max_retries} attempts (network error). "
                        f"Account: {self.account_id}: {e}"
                    ) from e
            else:
                if response.status_code not in _RETRY_STATUS:
                    # Non-retryable (e.g. 400/401/403/404)
                    raise AirflowException(
                        f"Stripe API returned non-retryable HTTP {response.status_code} for "
                        f"account {self.account_id}: {response.text}"
                    )
                self.log.warning(f"Transient error {response.status_code}. Retrying...")
                if attempt == max_retries - 1:
                    raise AirflowException(
                        f"Stripe API request failed after {max_retries} attempts (last status "
                        f"{response.status_code}). Account: {self.account_id}: {response.text}"
                    )
            delay = min(base_delay * (2**attempt), max_delay)
            time.sleep(random.uniform(0.5 * delay, delay))

    def get_raw_pages(self, endpoint: str, start_ts: int, end_ts: int):
        """Yields raw API page payloads sequentially to prevent memory bloating."""
        extra_params = ENDPOINT_EXTRA_PARAMS.get(endpoint, {})
        if endpoint == "radar_early_fraud_warnings":
            # Endpoint has a "/" in the Stripe API path; use underscore form elsewhere (e.g. table names).
            endpoint = "radar/early_fraud_warnings"
        url = f"{self.base_url}/{endpoint}"
        window_start = datetime.fromtimestamp(start_ts, timezone.utc)
        window_end = datetime.fromtimestamp(end_ts, timezone.utc)
        self.log.info(
            f"[{self.account_id}] Fetching '{endpoint}' for window "
            f"[{window_start:%Y-%m-%d %H:%M}, {window_end:%Y-%m-%d %H:%M}) UTC."
        )

        if endpoint == "balance":
            yield self._make_request_with_retry(url, params={})
            return

        has_more = True
        starting_after = None
        page_idx = 0

        while has_more:
            params = {
                "limit": 100,
                "created[gte]": start_ts,
                "created[lt]": end_ts,
                **extra_params,
            }
            if starting_after:
                params["starting_after"] = starting_after

            payload = self._make_request_with_retry(url, params)
            page_idx += 1
            yield payload

            data_batch = payload.get("data", [])
            has_more = payload.get("has_more", False)
            self.log.debug(
                f"[{self.account_id}] '{endpoint}' page {page_idx}: {len(data_batch)} records, "
                f"has_more={has_more}."
            )
            if data_batch and has_more:
                starting_after = data_batch[-1]["id"]
            else:
                has_more = False
