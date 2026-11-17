"""Unit tests for ZendeskBaseHook auth + retry logic (include/custom_hooks/zendesk/base.py).

Auth is HTTP Basic with ``{email}/token`` as the username and the API token as the
password -- Zendesk retired bare API-key auth, so a wrong username shape (e.g.
missing the ``/token`` suffix) authenticates as a normal password login and fails
with a confusing 401 rather than an obvious auth-shape error. These tests pin the
exact auth tuple built in ``__init__``.

They also cover ``_get``'s retry/backoff: transient 5xx retried with exponential
backoff, 429 honoring the server's ``Retry-After`` header instead of the backoff
schedule, and persistent network errors raised after ``_MAX_ATTEMPTS``.

One test (``TestNonRetryableTwoxxStatusIsMishandled``) pins a real bug found while
writing this suite: ``_get`` only special-cases exactly HTTP 200 as success. Any
other 2xx/3xx response (e.g. 201/204, which some REST APIs use, though Zendesk's
documented incremental-export endpoints return 200) falls into the "not a retryable
status" branch, calls ``resp.raise_for_status()`` -- which does NOT raise for a
non-error status -- and then, since nothing returns or raises, falls through to the
backoff-and-retry tail and is retried anyway. If every attempt returns that same
non-200 success status, the loop exhausts ``_MAX_ATTEMPTS`` and raises
``RuntimeError("... failed after N attempts")`` even though the server never once
returned an error. This test documents the current (buggy) behavior; see the test
suite's final report for the file:line citation. Not fixed here per instructions --
tests only.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import requests

from include.custom_hooks.zendesk.tickets import TicketsHook


def _hook(subdomain="acme", email="bot@acme.com", token="tok123"):
    return TicketsHook(subdomain=subdomain, email=email, api_token=token)


class TestAuth:
    def test_session_auth_is_email_slash_token_and_the_api_token(self):
        hook = _hook(email="agent@acme.com", token="secrettoken")
        assert hook.session.auth == ("agent@acme.com/token", "secrettoken")

    def test_base_url_is_scoped_to_the_subdomain(self):
        hook = _hook(subdomain="widgetco")
        assert hook.base_url == "https://widgetco.zendesk.com/api/v2"


class _FakeHttpResponse:
    def __init__(
        self, status_code: int, *, headers: dict | None = None, body: dict | None = None
    ):
        self.status_code = status_code
        self.headers = headers or {}
        self._body = body or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(f"{self.status_code} error")

    def json(self):
        return self._body


@pytest.fixture
def no_op_infra(monkeypatch):
    """Bypass the rate limiter, sleep, and BigQuery telemetry so retry tests run instantly."""
    import include.custom_hooks.zendesk.base as base_module

    monkeypatch.setattr(
        base_module,
        "get_rate_limiter",
        lambda *a, **k: SimpleNamespace(acquire=lambda: None),
    )
    monkeypatch.setattr(base_module.time, "sleep", lambda *_: None)
    monkeypatch.setattr(base_module, "record_api_request", lambda **kwargs: None)
    return base_module


class TestGetRetries:
    def test_succeeds_immediately_on_200(self, monkeypatch, no_op_infra):
        hook = _hook()
        calls = []

        def fake_get(url, params=None, timeout=None):
            calls.append((url, params))
            return _FakeHttpResponse(200, body={"tickets": []})

        monkeypatch.setattr(hook.session, "get", fake_get)

        result = hook._get({"start_time": 1})

        assert result == {"tickets": []}
        assert len(calls) == 1

    def test_non_retryable_4xx_raises_immediately_without_retrying(
        self, monkeypatch, no_op_infra
    ):
        hook = _hook()
        calls = []

        def fake_get(url, params=None, timeout=None):
            calls.append(1)
            return _FakeHttpResponse(404)

        monkeypatch.setattr(hook.session, "get", fake_get)

        with pytest.raises(requests.exceptions.HTTPError):
            hook._get({"start_time": 1})

        assert len(calls) == 1

    def test_transient_5xx_is_retried_then_succeeds(self, monkeypatch, no_op_infra):
        hook = _hook()
        statuses = iter([503, 502, 200])

        def fake_get(url, params=None, timeout=None):
            status = next(statuses)
            return _FakeHttpResponse(status, body={"tickets": ["ok"]})

        monkeypatch.setattr(hook.session, "get", fake_get)

        result = hook._get({"start_time": 1})

        assert result == {"tickets": ["ok"]}

    def test_persistent_5xx_raises_http_error_on_the_final_attempt(
        self, monkeypatch, no_op_infra
    ):
        # Unlike SolidgateBaseHook.signed_request (which returns the last failing
        # response and leaves raise_for_status() to the caller), Zendesk's _get
        # raises internally once attempts are exhausted.
        hook = _hook()
        monkeypatch.setattr(
            hook.session,
            "get",
            lambda url, params=None, timeout=None: _FakeHttpResponse(503),
        )

        with pytest.raises(requests.exceptions.HTTPError):
            hook._get({"start_time": 1})

    def test_429_sleeps_the_server_supplied_retry_after_and_retries(
        self, monkeypatch, no_op_infra
    ):
        hook = _hook()
        sleep_calls = []
        monkeypatch.setattr(no_op_infra.time, "sleep", lambda s: sleep_calls.append(s))
        responses = [
            _FakeHttpResponse(429, headers={"Retry-After": "7"}),
            _FakeHttpResponse(200, body={"tickets": []}),
        ]
        it = iter(responses)
        monkeypatch.setattr(
            hook.session, "get", lambda url, params=None, timeout=None: next(it)
        )

        result = hook._get({"start_time": 1})

        assert result == {"tickets": []}
        # The 429 branch sleeps Retry-After directly (7.0s), not the exponential
        # backoff schedule.
        assert 7.0 in sleep_calls

    def test_network_error_is_retried_then_raises_after_max_attempts(
        self, monkeypatch, no_op_infra
    ):
        hook = _hook()
        attempts = {"n": 0}

        def fake_get(url, params=None, timeout=None):
            attempts["n"] += 1
            raise requests.exceptions.ConnectionError("boom")

        monkeypatch.setattr(hook.session, "get", fake_get)

        with pytest.raises(requests.exceptions.ConnectionError):
            hook._get({"start_time": 1})

        assert attempts["n"] == 8  # _MAX_ATTEMPTS

    def test_explicit_url_form_is_called_without_params(self, monkeypatch, no_op_infra):
        # _next_page for the time-based mixin returns a ready-to-call absolute URL;
        # _get must call it directly rather than re-prefixing base_url + _PATH.
        hook = _hook()
        calls = []

        def fake_get(url, params=None, timeout=None):
            calls.append((url, params))
            return _FakeHttpResponse(200, body={"ticket_events": []})

        monkeypatch.setattr(hook.session, "get", fake_get)

        hook._get(
            "https://acme.zendesk.com/api/v2/incremental/ticket_events.json?cursor=abc"
        )

        assert calls == [
            (
                "https://acme.zendesk.com/api/v2/incremental/ticket_events.json?cursor=abc",
                None,
            )
        ]


class TestNonRetryableTwoxxStatusIsMishandled:
    def test_persistent_201_is_retried_needlessly_and_eventually_raises_runtimeerror(
        self, monkeypatch, no_op_infra
    ):
        """Characterizes a real bug: `_get` treats any status != 200 as
        needing either a raise or a retry, so a legitimate 201 response is neither
        returned nor raised -- it's retried until attempts are exhausted, then
        RuntimeError is raised even though the server never returned an error.
        """
        hook = _hook()
        monkeypatch.setattr(
            hook.session,
            "get",
            lambda url, params=None, timeout=None: _FakeHttpResponse(
                201, body={"tickets": []}
            ),
        )

        with pytest.raises(RuntimeError, match="failed after 8 attempts"):
            hook._get({"start_time": 1})
