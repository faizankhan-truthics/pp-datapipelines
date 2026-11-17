import base64
import hashlib
import hmac
import json
import os
import random
import time
from datetime import datetime, timezone

import requests
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook

from include.config.chargebackhit_settings import (
    DATE_FILTER_FIELDS,
    PAGINATION_STYLE,
    RATE_LIMIT_PER_SECOND,
)
from include.utils.rate_limiter import get_rate_limiter


def generate_signature(data: str, public_key: str, secret_key: str) -> str:
    encrypto_data = (public_key + data + public_key).encode("utf-8")
    sign = hmac.new(
        secret_key.encode("utf-8"), encrypto_data, hashlib.sha512
    ).hexdigest()
    return base64.b64encode(sign.encode("utf-8")).decode("utf-8")


_RETRY_STATUS = frozenset({429, 500, 502, 503, 504})


class ChargebackhitHook(BaseHook):
    def __init__(self, account_name: str, public_key_env: str, secret_key_env: str):
        super().__init__()
        self.account_name = account_name
        self.public_key_env = public_key_env
        self.secret_key_env = secret_key_env
        public_key = os.environ.get(public_key_env)
        secret_key = os.environ.get(secret_key_env)
        if not public_key or not secret_key:
            raise AirflowException(
                f"Missing Chargebackhit credentials for account '{account_name}': "
                f"environment variables '{public_key_env}' and '{secret_key_env}' are required"
            )
        self.public_key = public_key
        self.secret_key = secret_key
        self.base_url = "https://api.chargebackhit.com/api/v2"
        self.session = requests.Session()

    def _make_request_with_retry(
        self, url: str, body: dict, max_retries: int = 5
    ) -> dict:
        base_delay, max_delay = 1.0, 30.0
        body_json = json.dumps(body, separators=(",", ":"))

        headers = {
            "Content-Type": "application/json",
            "public_key": self.public_key,
            "signature": generate_signature(
                body_json, self.public_key, self.secret_key
            ),
        }

        for attempt in range(max_retries):
            get_rate_limiter("chargebackhit", RATE_LIMIT_PER_SECOND).acquire()
            try:
                response = self.session.post(
                    url, data=body_json, headers=headers, timeout=30
                )
                if response.status_code == 200:
                    return response.json()
            except requests.exceptions.RequestException as e:
                self.log.warning(f"Network exception: {str(e)}. Retrying...")
                if attempt == max_retries - 1:
                    raise AirflowException(
                        f"Chargebackhit API request failed after {max_retries} attempts (network "
                        f"error). Account: {self.account_name}: {e}"
                    ) from e
            else:
                if response.status_code not in _RETRY_STATUS:
                    # Non-retryable (e.g. 400/401/403/404)
                    raise AirflowException(
                        f"Chargebackhit API returned non-retryable HTTP {response.status_code} "
                        f"for account {self.account_name}: {response.text}"
                    )
                self.log.warning(f"Transient error {response.status_code}. Retrying...")
                if attempt == max_retries - 1:
                    raise AirflowException(
                        f"Chargebackhit API request failed after {max_retries} attempts (last "
                        f"status {response.status_code}). Account: {self.account_name}: {response.text}"
                    )
            delay = min(base_delay * (2**attempt), max_delay)
            time.sleep(random.uniform(0.5 * delay, delay))

    def get_raw_pages(self, endpoint: str, start_ts: int, end_ts: int):
        """Yields raw API page payloads sequentially to prevent memory bloating."""
        url = f"{self.base_url}/{endpoint}/list"
        style = PAGINATION_STYLE[endpoint]

        date_from = datetime.fromtimestamp(start_ts, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        date_to = datetime.fromtimestamp(end_ts, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        from_field, to_field = DATE_FILTER_FIELDS[endpoint]
        body_filter = {from_field: date_from, to_field: date_to}

        limit = 100
        offset = 0
        page_token = None

        while True:
            pagination = {"limit": limit}
            if style == "offset":
                pagination["offset"] = offset
            else:
                if page_token:
                    pagination["page_token"] = page_token

            payload = self._make_request_with_retry(
                url, {"filter": body_filter, "pagination": pagination}
            )

            data = payload.get("data", [])
            if not data:
                break

            yield data  # unwrapped list of items for this page

            if style == "offset":
                total_count = payload.get("pagination", {}).get("total_count", 0)
                offset += limit
                if offset >= total_count:
                    break
            else:
                page_token = payload.get("pagination", {}).get("next_page_token")
                if not page_token:
                    break
