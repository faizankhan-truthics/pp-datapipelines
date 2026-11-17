"""Unit tests for SolidgateBaseHook's pagination (include/custom_hooks/solidgate/base.py).

``iter_raw_pages``/``iter_records`` drive every Solidgate ingest DAG, so a cursor bug
here silently truncates or infinite-loops every entity. Exercised through
``CardOrdersHook``, a thin concrete subclass, with ``signed_request`` stubbed out so
no network/credentials/rate-limiter setup is needed.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from include.custom_hooks.solidgate.card_orders import CardOrdersHook


def _hook() -> CardOrdersHook:
    return CardOrdersHook(public_key_env="TEST_PUBLIC", secret_key_env="TEST_SECRET")


def _fake_response(payload: dict) -> SimpleNamespace:
    return SimpleNamespace(raise_for_status=lambda: None, json=lambda: payload)


class TestNextCursor:
    def test_top_level_metadata(self):
        payload = {"metadata": {"next_page_iterator": "cursor-1"}}
        assert CardOrdersHook._next_cursor(payload) == "cursor-1"

    def test_nested_body_metadata_fallback(self):
        payload = {"body": {"metadata": {"next_page_iterator": "cursor-2"}}}
        assert CardOrdersHook._next_cursor(payload) == "cursor-2"

    def test_missing_metadata_returns_none(self):
        assert CardOrdersHook._next_cursor({}) is None

    def test_explicit_null_cursor_returns_none(self):
        payload = {"metadata": {"next_page_iterator": None}}
        assert CardOrdersHook._next_cursor(payload) is None


class TestIterRawPagesCursorChaining:
    def test_follows_cursor_until_it_stops_and_sends_it_on_each_next_request(
        self, monkeypatch
    ):
        pages = [
            {"orders": [{"order_id": "o1"}], "metadata": {"next_page_iterator": "c1"}},
            {"orders": [{"order_id": "o2"}], "metadata": {"next_page_iterator": "c2"}},
            {"orders": [{"order_id": "o3"}], "metadata": {"next_page_iterator": None}},
        ]
        requests_made = []

        def fake_signed_request(method, path, *, body=""):
            requests_made.append((method, path, body))
            return _fake_response(pages[len(requests_made) - 1])

        hook = _hook()
        monkeypatch.setattr(hook, "signed_request", fake_signed_request)

        results = list(
            hook.iter_raw_pages("2026-01-01 00:00:00", "2026-01-02 00:00:00")
        )

        assert results == pages
        assert len(requests_made) == 3

        import json

        first_body = json.loads(requests_made[0][2])
        second_body = json.loads(requests_made[1][2])
        third_body = json.loads(requests_made[2][2])

        assert "next_page_iterator" not in first_body
        assert second_body["next_page_iterator"] == "c1"
        assert third_body["next_page_iterator"] == "c2"

    def test_iter_records_yields_extracted_records_per_page(self, monkeypatch):
        pages = [
            {
                "orders": [{"order_id": "o1"}, {"order_id": "o2"}],
                "metadata": {"next_page_iterator": "c1"},
            },
            {"orders": [{"order_id": "o3"}], "metadata": {}},
        ]
        call_count = {"n": 0}

        def fake_signed_request(method, path, *, body=""):
            page = pages[call_count["n"]]
            call_count["n"] += 1
            return _fake_response(page)

        hook = _hook()
        monkeypatch.setattr(hook, "signed_request", fake_signed_request)

        pages_of_records = list(
            hook.iter_records("2026-01-01 00:00:00", "2026-01-02 00:00:00")
        )

        assert pages_of_records == [
            [{"order_id": "o1"}, {"order_id": "o2"}],
            [{"order_id": "o3"}],
        ]


class TestMaxPagesSafetyCap:
    def test_stuck_cursor_raises_after_max_pages_instead_of_looping_forever(
        self, monkeypatch
    ):
        hook = _hook()
        monkeypatch.setattr(hook, "_MAX_PAGES", 3)

        def fake_signed_request(method, path, *, body=""):
            # Cursor never terminates -> would loop forever without the safety cap.
            return _fake_response(
                {
                    "orders": [{"order_id": "o"}],
                    "metadata": {"next_page_iterator": "stuck"},
                }
            )

        monkeypatch.setattr(hook, "signed_request", fake_signed_request)

        with pytest.raises(RuntimeError, match="exceeded _MAX_PAGES"):
            list(hook.iter_raw_pages("2026-01-01 00:00:00", "2026-01-02 00:00:00"))

    def test_missing_path_raises_before_any_request(self, monkeypatch):
        hook = _hook()
        hook._PATH = ""
        monkeypatch.setattr(
            hook,
            "signed_request",
            lambda *a, **k: pytest.fail("signed_request should not be called"),
        )

        with pytest.raises(ValueError, match="_PATH"):
            list(hook.iter_raw_pages("2026-01-01 00:00:00", "2026-01-02 00:00:00"))
