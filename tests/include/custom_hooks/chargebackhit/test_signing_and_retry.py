"""Unit tests for ChargebackhitHook auth + retry logic
(include/custom_hooks/chargebackhit/chargebackhit_hook.py).

ChargebackHit, like Solidgate, signs every request with
``base64(hex(HMAC_SHA512(public_key + body + public_key, secret_key)))`` -- a change to
this formula breaks every account silently (requests just come back 401). These tests
pin the formula with a golden value.

They also cover ``_make_request_with_retry``'s status-code branching: a genuinely
non-retryable status (400/401/403/404) fails fast on the first attempt instead of being
retried like a transient one, and a persistent transient status or network error raises
``AirflowException`` naming the account after exhausting retries.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
import requests
from airflow.exceptions import AirflowException

import include.custom_hooks.chargebackhit.chargebackhit_hook as chargebackhit_hook
from include.custom_hooks.chargebackhit.chargebackhit_hook import (
    ChargebackhitHook,
    generate_signature,
)


def _hook(monkeypatch) -> ChargebackhitHook:
    monkeypatch.setenv("CHARGEBACKHIT_ACME_API_PUBLIC_KEY", "pk_123")
    monkeypatch.setenv("CHARGEBACKHIT_ACME_API_SECRET_KEY", "sk_456")
    return ChargebackhitHook(
        account_name="acme",
        public_key_env="CHARGEBACKHIT_ACME_API_PUBLIC_KEY",
        secret_key_env="CHARGEBACKHIT_ACME_API_SECRET_KEY",
    )


class TestGenerateSignature:
    def test_signature_matches_known_golden_value(self):
        signature = generate_signature('{"filter":"updated_at"}', "pk_123", "sk_456")

        assert signature == (
            "ZTdmZWYxNTI3OTY4OGViZTU1YmI5NDRiNGU0NzQyYjMyNDdiYTlmZGVkODM4MjU5ZTAxZjY2MDUx"
            "OGQyNGY0NmYyZGNmMTE4NmNiMDc0NjY4ODhkMzdhNjhkZTY4MzBkMmI1N2Q3ZDI3NzhjNGI4ZDFk"
            "NzdkZTg2ZTVhNGI4ZTc="
        )

    def test_different_body_changes_the_signature(self):
        assert generate_signature("body-a", "pk", "sk") != generate_signature(
            "body-b", "pk", "sk"
        )

    def test_different_secret_changes_the_signature(self):
        assert generate_signature("body", "pk", "sk-1") != generate_signature(
            "body", "pk", "sk-2"
        )

    def test_public_key_is_folded_into_the_signed_payload_not_just_the_header(self):
        # public_key wraps the body (pk + body + pk); changing it changes the signature
        # even though the header sends `public_key` and `signature` as separate fields.
        assert generate_signature("body", "pk-1", "sk") != generate_signature(
            "body", "pk-2", "sk"
        )


class _FakeHttpResponse:
    def __init__(self, status_code: int, payload: dict | None = None, text: str = ""):
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self):
        return self._payload


@pytest.fixture
def no_sleep(monkeypatch):
    """Bypass the rate limiter and real backoff sleeps so retry tests run instantly."""
    monkeypatch.setattr(
        chargebackhit_hook,
        "get_rate_limiter",
        lambda *a, **k: SimpleNamespace(acquire=lambda: None),
    )
    monkeypatch.setattr(chargebackhit_hook.time, "sleep", lambda *_: None)


class TestRateLimiting:
    def test_every_attempt_including_retries_acquires_the_shared_limiter(
        self, no_sleep, monkeypatch
    ):
        hook = _hook(monkeypatch)
        acquired = []
        monkeypatch.setattr(
            chargebackhit_hook,
            "get_rate_limiter",
            lambda name, rate: SimpleNamespace(
                acquire=lambda: acquired.append((name, rate))
            ),
        )
        responses = iter([_FakeHttpResponse(429), _FakeHttpResponse(200, {"data": []})])
        monkeypatch.setattr(hook.session, "post", lambda *a, **k: next(responses))

        hook._make_request_with_retry(
            "https://api.example.com/alerts/list", {"filter": {}}
        )

        assert (
            acquired
            == [("chargebackhit", chargebackhit_hook.RATE_LIMIT_PER_SECOND)] * 2
        )


class TestMakeRequestWithRetrySuccess:
    def test_succeeds_immediately_on_200(self, no_sleep, monkeypatch):
        hook = _hook(monkeypatch)
        calls = []

        def fake_post(url, data=None, headers=None, timeout=None):
            calls.append((url, data, headers))
            return _FakeHttpResponse(200, {"data": [{"id": "1"}]})

        monkeypatch.setattr(hook.session, "post", fake_post)

        result = hook._make_request_with_retry(
            "https://api.example.com/alerts/list", {"filter": {}}
        )

        assert result == {"data": [{"id": "1"}]}
        assert len(calls) == 1

    def test_request_body_is_signed_with_the_public_key_and_secret_key(
        self, no_sleep, monkeypatch
    ):
        hook = _hook(monkeypatch)
        captured = {}

        def fake_post(url, data=None, headers=None, timeout=None):
            captured["data"] = data
            captured["headers"] = headers
            return _FakeHttpResponse(200, {"data": []})

        monkeypatch.setattr(hook.session, "post", fake_post)

        hook._make_request_with_retry(
            "https://api.example.com/alerts/list", {"filter": {"a": 1}}
        )

        assert captured["headers"]["public_key"] == "pk_123"
        assert captured["headers"]["signature"] == generate_signature(
            captured["data"], "pk_123", "sk_456"
        )
        assert json.loads(captured["data"]) == {"filter": {"a": 1}}


class TestMakeRequestWithRetryTransientErrors:
    def test_transient_status_is_retried_then_succeeds(self, no_sleep, monkeypatch):
        hook = _hook(monkeypatch)
        statuses = iter([503, 429, 200])

        def fake_post(url, data=None, headers=None, timeout=None):
            code = next(statuses)
            return _FakeHttpResponse(
                code, {"data": [{"id": "ok"}]} if code == 200 else {}
            )

        monkeypatch.setattr(hook.session, "post", fake_post)

        result = hook._make_request_with_retry(
            "https://api.example.com/alerts/list", {}, max_retries=5
        )

        assert result == {"data": [{"id": "ok"}]}

    def test_persistent_transient_status_raises_after_max_retries(
        self, no_sleep, monkeypatch
    ):
        hook = _hook(monkeypatch)
        calls = {"n": 0}

        def fake_post(url, data=None, headers=None, timeout=None):
            calls["n"] += 1
            return _FakeHttpResponse(503, {})

        monkeypatch.setattr(hook.session, "post", fake_post)

        with pytest.raises(AirflowException, match="acme"):
            hook._make_request_with_retry(
                "https://api.example.com/alerts/list", {}, max_retries=3
            )

        assert calls["n"] == 3

    def test_network_exception_is_retried_then_raises_after_max_retries(
        self, no_sleep, monkeypatch
    ):
        hook = _hook(monkeypatch)
        calls = {"n": 0}

        def fake_post(url, data=None, headers=None, timeout=None):
            calls["n"] += 1
            raise requests.exceptions.ConnectionError("boom")

        monkeypatch.setattr(hook.session, "post", fake_post)

        with pytest.raises(AirflowException, match="network error"):
            hook._make_request_with_retry(
                "https://api.example.com/alerts/list", {}, max_retries=3
            )

        assert calls["n"] == 3


class TestMakeRequestWithRetryNonRetryableStatus:
    def test_non_retryable_status_fails_fast_without_retrying(
        self, no_sleep, monkeypatch
    ):
        """A permanent 400 fails on the first attempt with the status and body
        surfaced in the exception, instead of being retried like a transient error."""
        hook = _hook(monkeypatch)
        calls = {"n": 0}

        def fake_post(url, data=None, headers=None, timeout=None):
            calls["n"] += 1
            return _FakeHttpResponse(400, text="Bad Request")

        monkeypatch.setattr(hook.session, "post", fake_post)

        with pytest.raises(AirflowException, match="400"):
            hook._make_request_with_retry(
                "https://api.example.com/alerts/list", {}, max_retries=3
            )

        assert calls["n"] == 1  # fails fast, no wasted retries on a permanent error
