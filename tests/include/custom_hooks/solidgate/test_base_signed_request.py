"""Unit tests for SolidgateBaseHook auth + retry logic (include/custom_hooks/solidgate/base.py).

Solidgate has no token step: every request is signed with
``base64(hex(HMAC_SHA512(pk + body + pk, secret)))``. The signed string must be
byte-identical to the body actually sent, so a change to the signing formula breaks
every Solidgate account silently (requests just come back 401). These tests pin the
formula with a golden value and cover the retry/backoff behavior around it.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import requests

from include.custom_hooks.solidgate.card_orders import CardOrdersHook


def _hook(
    monkeypatch, *, public="test_public_key", secret="test_secret_key"
) -> CardOrdersHook:
    monkeypatch.setenv("TEST_PUBLIC", public)
    monkeypatch.setenv("TEST_SECRET", secret)
    return CardOrdersHook(public_key_env="TEST_PUBLIC", secret_key_env="TEST_SECRET")


class TestSigning:
    def test_signature_matches_known_golden_value(self, monkeypatch):
        hook = _hook(monkeypatch)
        body = '{"filter":"updated_at"}'

        signature = hook._sign(body)

        assert signature == (
            "Y2I5YTNkNzQ0Y2U3M2UyN2FiYTlmNDJlMGNkMjZhM2M3YTE0ZTE4M2E1NjRkYTRhOWI4ODFlMTI1ZWUy"
            "ZDM0Yzg2YjExM2MwZDYzNDFmNjlkYTgzZTQwNGYyNjVkNzAzYjhhYmNkNTQ2YzJkYWIxMTdlNTBiZTIy"
            "YmVhMWQyMDA="
        )

    def test_different_body_changes_the_signature(self, monkeypatch):
        hook = _hook(monkeypatch)
        assert hook._sign("body-a") != hook._sign("body-b")

    def test_headers_include_merchant_and_signature(self, monkeypatch):
        hook = _hook(monkeypatch, public="pk_123")
        headers = hook._headers("body", content_type=True)

        assert headers["merchant"] == "pk_123"
        assert headers["signature"] == hook._sign("body")
        assert headers["Content-Type"] == "application/json"

    def test_missing_credentials_raises(self, monkeypatch):
        hook = CardOrdersHook(
            public_key_env="MISSING_PUBLIC", secret_key_env="MISSING_SECRET"
        )
        monkeypatch.delenv("MISSING_PUBLIC", raising=False)
        monkeypatch.delenv("MISSING_SECRET", raising=False)

        with pytest.raises(ValueError, match="Missing Solidgate credentials"):
            hook._sign("body")


class _FakeHttpResponse:
    def __init__(self, status_code: int):
        self.status_code = status_code

    def raise_for_status(self):
        pass


@pytest.fixture
def no_op_infra(monkeypatch):
    """Bypass the rate limiter, sleep, and BigQuery telemetry so retry tests run instantly."""
    import include.custom_hooks.solidgate.base as base_module

    monkeypatch.setattr(
        base_module,
        "get_rate_limiter",
        lambda *a, **k: SimpleNamespace(acquire=lambda: None),
    )
    monkeypatch.setattr(base_module.time, "sleep", lambda *_: None)
    monkeypatch.setattr(base_module, "record_api_request", lambda **kwargs: None)
    return base_module


class TestSignedRequestRetries:
    def test_succeeds_immediately_on_2xx(self, monkeypatch, no_op_infra):
        hook = _hook(monkeypatch)
        calls = []

        def fake_request(method, url, **kwargs):
            calls.append((method, url))
            return _FakeHttpResponse(200)

        monkeypatch.setattr(requests, "request", fake_request)

        resp = hook.signed_request("POST", "/card-orders", body="{}")

        assert resp.status_code == 200
        assert len(calls) == 1

    def test_non_retryable_status_returns_without_retrying(
        self, monkeypatch, no_op_infra
    ):
        hook = _hook(monkeypatch)
        calls = []

        def fake_request(method, url, **kwargs):
            calls.append((method, url))
            return _FakeHttpResponse(400)

        monkeypatch.setattr(requests, "request", fake_request)

        resp = hook.signed_request("POST", "/card-orders", body="{}")

        assert resp.status_code == 400
        assert len(calls) == 1

    def test_transient_status_is_retried_then_succeeds(self, monkeypatch, no_op_infra):
        hook = _hook(monkeypatch)
        statuses = iter([503, 503, 200])

        monkeypatch.setattr(
            requests,
            "request",
            lambda method, url, **kwargs: _FakeHttpResponse(next(statuses)),
        )

        resp = hook.signed_request("POST", "/card-orders", body="{}")

        assert resp.status_code == 200

    def test_persistent_transient_status_returns_last_response_without_raising(
        self, monkeypatch, no_op_infra
    ):
        hook = _hook(monkeypatch)
        monkeypatch.setattr(
            requests, "request", lambda method, url, **kwargs: _FakeHttpResponse(503)
        )

        resp = hook.signed_request("POST", "/card-orders", body="{}")

        # After exhausting all attempts the last (still-failing) response is returned,
        # not raised -- callers use resp.raise_for_status() themselves.
        assert resp.status_code == 503

    def test_network_error_is_retried_then_raises_after_max_attempts(
        self, monkeypatch, no_op_infra
    ):
        hook = _hook(monkeypatch)
        attempts = {"n": 0}

        def fake_request(method, url, **kwargs):
            attempts["n"] += 1
            raise requests.exceptions.ConnectionError("boom")

        monkeypatch.setattr(requests, "request", fake_request)

        with pytest.raises(requests.exceptions.ConnectionError):
            hook.signed_request("POST", "/card-orders", body="{}")

        assert attempts["n"] == 5  # _MAX_ATTEMPTS
