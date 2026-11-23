from __future__ import annotations

from types import SimpleNamespace

import pytest
from airflow.exceptions import AirflowException

import include.custom_hooks.paypal.paypal_hook as paypal_hook_module
from include.custom_hooks.paypal.paypal_hook import PaypalHook


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code
        self.text = ""

    def json(self):
        return self.payload


def _disable_waits(monkeypatch):
    monkeypatch.setattr(paypal_hook_module.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(
        paypal_hook_module,
        "get_rate_limiter",
        lambda *_args: SimpleNamespace(acquire=lambda: None),
    )


def test_get_raw_pages_authenticates_and_follows_page_count(monkeypatch):
    _disable_waits(monkeypatch)
    hook = PaypalHook(client_id="sandbox-id", client_secret="sandbox-secret")
    requests = []
    responses = iter(
        [
            FakeResponse({"access_token": "access-token", "expires_in": 3600}),
            FakeResponse(
                {
                    "total_pages": 2,
                    "transaction_details": [
                        {"transaction_info": {"transaction_id": "TX-1"}}
                    ],
                }
            ),
            FakeResponse(
                {
                    "total_pages": 2,
                    "transaction_details": [
                        {"transaction_info": {"transaction_id": "TX-2"}}
                    ],
                }
            ),
        ]
    )

    def fake_request(method, url, **kwargs):
        requests.append((method, url, kwargs))
        return next(responses)

    monkeypatch.setattr(hook.session, "request", fake_request)

    pages = list(
        hook.get_raw_pages(
            "2026-01-01T00:00:00.000Z",
            "2026-01-01T23:59:59.999Z",
        )
    )

    assert len(pages) == 2
    assert requests[0][0:2] == (
        "POST",
        "https://api-m.sandbox.paypal.com/v1/oauth2/token",
    )
    assert requests[0][2]["auth"] == ("sandbox-id", "sandbox-secret")
    assert requests[1][2]["headers"]["Authorization"] == "Bearer access-token"
    assert requests[1][2]["params"]["page"] == 1
    assert requests[1][2]["params"]["balance_affecting_records_only"] == "N"
    assert requests[2][2]["params"]["page"] == 2
    assert all(
        call[1].startswith("https://api-m.sandbox.paypal.com/") for call in requests
    )


def test_missing_sandbox_credentials_fail_explicitly(monkeypatch):
    monkeypatch.delenv("PAYPAL_SANDBOX_CLIENT_ID", raising=False)
    monkeypatch.delenv("PAYPAL_SANDBOX_CLIENT_SECRET", raising=False)

    with pytest.raises(AirflowException, match="Missing PayPal sandbox credentials"):
        PaypalHook()
