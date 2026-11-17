"""Unit tests for StripeHook (include/custom_hooks/stripe/stripe_hook.py).

``get_raw_pages`` drives every Stripe ingest DAG, so a cursor bug here silently truncates
or infinite-loops every entity. It has two special cases worth pinning explicitly:
``endpoint == "balance"`` short-circuits to a single non-paginated page, and
``endpoint == "radar_early_fraud_warnings"`` gets its URL path rewritten to
``radar/early_fraud_warnings`` (Stripe's API needs the "/", table/config names don't).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import requests
from airflow.exceptions import AirflowException

import include.custom_hooks.stripe.stripe_hook as stripe_hook_module
from include.custom_hooks.stripe.stripe_hook import StripeHook


def _hook(monkeypatch, **overrides) -> StripeHook:
    api_key_env = overrides.get("api_key_env", "STRIPE_API_KEY_TEST")
    monkeypatch.setenv(api_key_env, overrides.get("api_key", "sk_test_123"))
    return StripeHook(
        api_key_env=api_key_env,
        account_id=overrides.get("account_id", "acct_123"),
    )


class _FakeResponse:
    def __init__(self, status_code: int, json_data=None, text: str = ""):
        self.status_code = status_code
        self._json_data = json_data
        self.text = text

    def json(self):
        return self._json_data


@pytest.fixture
def no_op_infra(monkeypatch):
    """Bypass the rate limiter and real sleeping so retry tests run instantly."""
    monkeypatch.setattr(
        stripe_hook_module,
        "get_rate_limiter",
        lambda *a, **k: SimpleNamespace(acquire=lambda: None),
    )
    monkeypatch.setattr(stripe_hook_module.time, "sleep", lambda *_: None)


class TestMakeRequestWithRetry:
    def test_returns_json_immediately_on_200(self, monkeypatch, no_op_infra):
        hook = _hook(monkeypatch)
        calls = []

        def fake_get(url, params=None, timeout=None):
            calls.append(url)
            return _FakeResponse(200, {"ok": True})

        monkeypatch.setattr(hook.session, "get", fake_get)

        result = hook._make_request_with_retry("https://api.stripe.com/v1/charges", {})

        assert result == {"ok": True}
        assert len(calls) == 1

    def test_transient_status_is_retried_then_succeeds(self, monkeypatch, no_op_infra):
        hook = _hook(monkeypatch)
        statuses = iter([503, 502, 200])

        def fake_get(url, params=None, timeout=None):
            code = next(statuses)
            return _FakeResponse(code, {"data": []} if code == 200 else None)

        monkeypatch.setattr(hook.session, "get", fake_get)

        result = hook._make_request_with_retry(
            "https://api.stripe.com/v1/charges", {}, max_retries=5
        )

        assert result == {"data": []}

    def test_persistent_transient_status_exhausts_retries_then_raises(
        self, monkeypatch, no_op_infra
    ):
        """Fix for bug (1) from the module docstring: this used to return None silently."""
        hook = _hook(monkeypatch, account_id="acct_555")
        calls = []

        def fake_get(url, params=None, timeout=None):
            calls.append(url)
            return _FakeResponse(503, text="Service Unavailable")

        monkeypatch.setattr(hook.session, "get", fake_get)

        with pytest.raises(AirflowException, match="acct_555"):
            hook._make_request_with_retry(
                "https://api.stripe.com/v1/charges", {}, max_retries=3
            )

        assert len(calls) == 3

    def test_network_exception_is_retried_then_raises_airflow_exception_naming_the_account(
        self, monkeypatch, no_op_infra
    ):
        hook = _hook(monkeypatch, account_id="acct_777")
        attempts = {"n": 0}

        def fake_get(url, params=None, timeout=None):
            attempts["n"] += 1
            raise requests.exceptions.ConnectionError("boom")

        monkeypatch.setattr(hook.session, "get", fake_get)

        with pytest.raises(AirflowException, match="acct_777"):
            hook._make_request_with_retry(
                "https://api.stripe.com/v1/charges", {}, max_retries=3
            )

        assert attempts["n"] == 3

    def test_non_retryable_status_fails_fast_without_retrying(
        self, monkeypatch, no_op_infra
    ):
        """It fails on the first attempt with the status and
        body surfaced in the exception."""
        hook = _hook(monkeypatch)
        calls = []

        def fake_get(url, params=None, timeout=None):
            calls.append(url)
            return _FakeResponse(400, text="Bad Request: unknown parameter")

        monkeypatch.setattr(hook.session, "get", fake_get)

        with pytest.raises(AirflowException, match="400"):
            hook._make_request_with_retry(
                "https://api.stripe.com/v1/charges", {}, max_retries=3
            )

        assert len(calls) == 1  # fails fast, no wasted retries on a permanent error


class TestGetRawPagesSpecialCases:
    def test_balance_endpoint_yields_exactly_one_page_and_does_not_paginate(
        self, monkeypatch
    ):
        hook = _hook(monkeypatch)
        calls = []

        def fake_request(url, params):
            calls.append((url, params))
            return {"available": [], "pending": []}

        monkeypatch.setattr(hook, "_make_request_with_retry", fake_request)

        pages = list(hook.get_raw_pages("balance", 1_700_000_000, 1_700_086_400))

        assert pages == [{"available": [], "pending": []}]
        assert calls == [(f"{hook.base_url}/balance", {})]

    def test_radar_early_fraud_warnings_rewrites_the_url_path_to_use_a_slash(
        self, monkeypatch
    ):
        hook = _hook(monkeypatch)
        calls = []

        def fake_request(url, params):
            calls.append(url)
            return {"data": [], "has_more": False}

        monkeypatch.setattr(hook, "_make_request_with_retry", fake_request)

        list(
            hook.get_raw_pages(
                "radar_early_fraud_warnings", 1_700_000_000, 1_700_086_400
            )
        )

        assert calls == [f"{hook.base_url}/radar/early_fraud_warnings"]


class TestGetRawPagesCursorPagination:
    def test_follows_has_more_and_starting_after_cursor_across_pages(self, monkeypatch):
        hook = _hook(monkeypatch)
        pages = [
            {"data": [{"id": "ch_1"}, {"id": "ch_2"}], "has_more": True},
            {"data": [{"id": "ch_3"}], "has_more": False},
        ]
        requests_made = []

        def fake_request(url, params):
            requests_made.append(params)
            return pages[len(requests_made) - 1]

        monkeypatch.setattr(hook, "_make_request_with_retry", fake_request)

        result = list(hook.get_raw_pages("charges", 1_700_000_000, 1_700_086_400))

        assert result == pages
        assert len(requests_made) == 2
        assert "starting_after" not in requests_made[0]
        assert requests_made[1]["starting_after"] == "ch_2"
        # date window + page size are threaded through on every request
        assert requests_made[0]["created[gte]"] == 1_700_000_000
        assert requests_made[0]["created[lt]"] == 1_700_086_400
        assert requests_made[0]["limit"] == 100

    def test_has_more_true_with_empty_data_batch_stops_without_looping_or_crashing(
        self, monkeypatch
    ):
        """A payload that (incorrectly) claims has_more=True with an empty data batch must
        not infinite-loop, and must not crash trying to read data_batch[-1] for a cursor."""
        hook = _hook(monkeypatch)
        calls = {"n": 0}

        def fake_request(url, params):
            calls["n"] += 1
            return {"data": [], "has_more": True}

        monkeypatch.setattr(hook, "_make_request_with_retry", fake_request)

        pages = list(hook.get_raw_pages("charges", 1_700_000_000, 1_700_086_400))

        assert pages == [{"data": [], "has_more": True}]
        assert calls["n"] == 1

    def test_extra_params_for_the_endpoint_are_merged_into_every_page_request(
        self, monkeypatch
    ):
        hook = _hook(monkeypatch)
        captured = []

        def fake_request(url, params):
            captured.append(dict(params))
            return {"data": [], "has_more": False}

        monkeypatch.setattr(hook, "_make_request_with_retry", fake_request)

        list(hook.get_raw_pages("subscriptions", 1_700_000_000, 1_700_086_400))

        # ENDPOINT_EXTRA_PARAMS["subscriptions"] = {"status": "all"} (include/config/stripe_settings.py)
        assert captured[0]["status"] == "all"

    def test_events_endpoint_pulls_every_event_type_without_a_type_filter(
        self, monkeypatch
    ):
        hook = _hook(monkeypatch)
        calls = []
        pages = iter(
            [
                {"data": [{"id": "evt_1"}], "has_more": True},
                {"data": [{"id": "evt_2"}], "has_more": False},
            ]
        )

        def fake_request(url, params):
            calls.append((url, dict(params)))
            return next(pages)

        monkeypatch.setattr(hook, "_make_request_with_retry", fake_request)

        list(hook.get_raw_pages("events", 1_700_000_000, 1_700_086_400))

        url, params = calls[0]
        assert url == f"{hook.base_url}/events"
        # No ENDPOINT_EXTRA_PARAMS["events"]: every event type lands in bronze, dbt filters.
        assert all("type" not in p for _, p in calls)
        assert params["created[gte]"] == 1_700_000_000
        assert params["created[lt]"] == 1_700_086_400
        assert calls[1][1]["starting_after"] == "evt_1"

    def test_no_extra_params_for_an_endpoint_not_in_the_map(self, monkeypatch):
        hook = _hook(monkeypatch)
        captured = []

        def fake_request(url, params):
            captured.append(dict(params))
            return {"data": [], "has_more": False}

        monkeypatch.setattr(hook, "_make_request_with_retry", fake_request)

        list(hook.get_raw_pages("charges", 1_700_000_000, 1_700_086_400))

        assert "status" not in captured[0]


class TestGetRawPagesPropagatesFailures:
    """_make_request_with_retry now always raises AirflowException on failure instead of
    ever returning None, so get_raw_pages can no longer surface a persistent failure as
    a stray AttributeError (paginated endpoints) or a silent "null" record (balance)."""

    def test_persistent_transient_error_raises_instead_of_returning_none(
        self, monkeypatch
    ):
        hook = _hook(monkeypatch)

        def always_fails(url, params):
            raise AirflowException(
                "Stripe API request failed after 5 attempts (last status 503)."
            )

        monkeypatch.setattr(hook, "_make_request_with_retry", always_fails)

        with pytest.raises(AirflowException):
            list(hook.get_raw_pages("charges", 1_700_000_000, 1_700_086_400))

    def test_persistent_transient_error_on_balance_endpoint_also_raises(
        self, monkeypatch
    ):
        hook = _hook(monkeypatch)

        def always_fails(url, params):
            raise AirflowException(
                "Stripe API request failed after 5 attempts (last status 503)."
            )

        monkeypatch.setattr(hook, "_make_request_with_retry", always_fails)

        with pytest.raises(AirflowException):
            list(hook.get_raw_pages("balance", 1_700_000_000, 1_700_086_400))
