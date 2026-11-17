"""Unit tests for GrowthBookHook (include/custom_hooks/growthbook/base.py).

``iter_raw_pages`` drives every GrowthBook extract, so it must follow ``nextOffset`` until
``hasMore`` is false, never spin on a malformed page, and respect ``max_pages``. ``_get``
must retry rate limits and transient errors but fail fast on anything else (a bad key).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
import requests
from airflow.sdk.exceptions import AirflowFailException

import include.custom_hooks.growthbook.base as gb_base
from include.custom_hooks.growthbook.base import GrowthBookHook


@pytest.fixture(autouse=True)
def _no_side_effects(monkeypatch):
    monkeypatch.setattr(
        gb_base,
        "get_rate_limiter",
        lambda name, rate: SimpleNamespace(acquire=lambda: None),
    )
    monkeypatch.setattr(gb_base, "record_api_request", lambda **kwargs: None)
    monkeypatch.setattr(gb_base.time, "sleep", lambda seconds: None)


def _hook(**kwargs):
    return GrowthBookHook(
        api_key="secret_test",
        path="/v2/features",
        records_key="features",
        api_host="https://api.example.test/api",
        **kwargs,
    )


def _page(records, *, has_more, next_offset):
    return {"features": records, "hasMore": has_more, "nextOffset": next_offset}


class TestRequestSetup:
    def test_bearer_auth_header(self):
        assert _hook().session.headers["Authorization"] == "Bearer secret_test"

    def test_url_joins_host_and_path(self):
        assert _hook().url == "https://api.example.test/api/v2/features"

    def test_extra_params_ride_along_with_the_paging_params(self, monkeypatch):
        hook = _hook(params={"archived": "true"})
        calls = []
        monkeypatch.setattr(
            hook,
            "_get",
            lambda params: (
                calls.append(params) or _page([], has_more=False, next_offset=None)
            ),
        )

        list(hook.iter_raw_pages())

        assert calls == [{"archived": "true", "limit": 100, "offset": 0}]

    def test_extract_records_reads_the_endpoint_key(self):
        assert _hook().extract_records({"features": [{"id": "flag_1"}]}) == [
            {"id": "flag_1"}
        ]

    def test_extract_records_handles_missing_or_null_key(self):
        assert _hook().extract_records({}) == []
        assert _hook().extract_records({"features": None}) == []


class TestIterRawPages:
    def _run(self, monkeypatch, pages, **kwargs):
        hook = _hook()
        calls = []

        def fake_get(params):
            calls.append(params)
            return pages[len(calls) - 1]

        monkeypatch.setattr(hook, "_get", fake_get)
        return list(hook.iter_raw_pages(**kwargs)), calls

    def test_follows_next_offset_until_has_more_is_false(self, monkeypatch):
        pages = [
            _page([{"id": "a"}], has_more=True, next_offset=100),
            _page([{"id": "b"}], has_more=True, next_offset=200),
            _page([{"id": "c"}], has_more=False, next_offset=None),
        ]
        yielded, calls = self._run(monkeypatch, pages)

        assert yielded == pages
        assert [c["offset"] for c in calls] == [0, 100, 200]
        assert all(c["limit"] == 100 for c in calls)

    def test_single_page(self, monkeypatch):
        pages = [_page([], has_more=False, next_offset=None)]
        yielded, calls = self._run(monkeypatch, pages)

        assert yielded == pages
        assert len(calls) == 1

    def test_has_more_without_next_offset_fails_instead_of_returning_a_partial_snapshot(
        self, monkeypatch
    ):
        pages = [_page([{"id": "a"}], has_more=True, next_offset=None)]

        with pytest.raises(AirflowFailException, match="nextOffset is missing"):
            self._run(monkeypatch, pages)

    def test_max_pages_fails_when_more_records_remain(self, monkeypatch):
        pages = [
            _page([{"id": str(i)}], has_more=True, next_offset=(i + 1) * 100)
            for i in range(5)
        ]
        calls = []

        hook = _hook()
        monkeypatch.setattr(
            hook, "_get", lambda params: calls.append(params) or pages[len(calls) - 1]
        )

        with pytest.raises(AirflowFailException, match="truncated snapshot"):
            list(hook.iter_raw_pages(max_pages=2))
        assert len(calls) == 2

    def test_max_pages_is_fine_when_the_last_page_is_reached(self, monkeypatch):
        pages = [
            _page([{"id": "a"}], has_more=True, next_offset=100),
            _page([{"id": "b"}], has_more=False, next_offset=None),
        ]
        yielded, calls = self._run(monkeypatch, pages, max_pages=2)

        assert yielded == pages


class TestGetRetries:
    def _responses(self, monkeypatch, hook, responses):
        seq = iter(responses)

        def fake_session_get(url, params=None, timeout=None):
            item = next(seq)
            if isinstance(item, Exception):
                raise item
            return item

        monkeypatch.setattr(hook.session, "get", fake_session_get)

    @staticmethod
    def _resp(status, payload=None):
        resp = requests.Response()
        resp.status_code = status
        resp._content = b'{"ok": true}' if payload is None else payload
        return resp

    def test_retries_rate_limit_then_succeeds(self, monkeypatch):
        hook = _hook()
        self._responses(
            monkeypatch, hook, [self._resp(429), self._resp(503), self._resp(200)]
        )

        assert hook._get({"limit": 100, "offset": 0}) == {"ok": True}

    def test_retries_network_errors(self, monkeypatch):
        hook = _hook()
        self._responses(
            monkeypatch,
            hook,
            [requests.exceptions.ConnectionError("boom"), self._resp(200)],
        )

        assert hook._get({"limit": 100, "offset": 0}) == {"ok": True}

    @pytest.mark.parametrize("status", [401, 403])
    def test_auth_errors_fail_the_task_without_retrying(self, monkeypatch, status):
        # AirflowFailException skips Airflow's task retries too: a rejected key can't fix itself
        hook = _hook()
        calls = []
        resp = self._resp(status, b'{"message": "This API key has expired."}')
        monkeypatch.setattr(
            hook.session,
            "get",
            lambda url, params=None, timeout=None: calls.append(1) or resp,
        )

        with pytest.raises(
            AirflowFailException, match="This API key has expired"
        ) as exc:
            hook._get({"limit": 100, "offset": 0})

        assert len(calls) == 1
        assert "secret_test" not in str(exc.value)

    def test_other_client_errors_still_raise_http_error(self, monkeypatch):
        hook = _hook()
        self._responses(
            monkeypatch, hook, [self._resp(404, b'{"message": "not found"}')]
        )

        with pytest.raises(requests.exceptions.HTTPError):
            hook._get({"limit": 100, "offset": 0})

    def test_gives_up_after_max_attempts(self, monkeypatch):
        hook = _hook()
        self._responses(monkeypatch, hook, [self._resp(429)] * gb_base._MAX_ATTEMPTS)

        with pytest.raises(requests.exceptions.HTTPError):
            hook._get({"limit": 100, "offset": 0})
