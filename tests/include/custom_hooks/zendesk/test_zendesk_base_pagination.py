"""Unit tests for ZendeskBaseHook's pagination (include/custom_hooks/zendesk/base.py).

Zendesk's incremental-export endpoints use two different pagination shapes, picked
per-entity via a mixin:
  * ``_CursorPaginationMixin``     (tickets, users)         -- opaque ``cursor`` string
    chained via ``after_cursor`` / ``end_of_stream``.
  * ``_TimeBasedPaginationMixin``  (ticket_comments/events)  -- a ready-to-call
    absolute ``next_page`` URL / ``end_of_stream``.

``iter_raw_pages`` drives every Zendesk ingest DAG, so a mistake in either mixin's
cursor-chaining silently truncates or infinite-loops an entity's export -- the same
class of bug already fixed once in Solidgate (bugfix/solidgate-api-pagination-safety).
Unlike ``SolidgateBaseHook``, ``ZendeskBaseHook.iter_raw_pages`` has no built-in
default page cap of its own -- callers must pass ``max_pages`` explicitly (the
operator always does, via ``max_pages_for``); these tests pin that ``max_pages``
stops the loop, and separately document that a stuck cursor with no ``max_pages``
supplied would loop forever with no safety net (unlike Solidgate's hard-coded
``_MAX_PAGES``).
"""

from __future__ import annotations

from include.custom_hooks.zendesk.base import (
    _CursorPaginationMixin,
    _TimeBasedPaginationMixin,
)
from include.custom_hooks.zendesk.ticket_comments import TicketCommentsHook
from include.custom_hooks.zendesk.tickets import TicketsHook
from include.custom_hooks.zendesk.users import UsersHook


def _hook(cls=TicketsHook):
    return cls(subdomain="acme", email="bot@acme.com", api_token="tok")


class TestCursorPaginationMixinFirstPageParams:
    def test_none_resume_token_starts_from_the_beginning_of_time(self):
        assert _CursorPaginationMixin()._first_page_params(None) == {"start_time": 1}

    def test_int_resume_token_is_sent_as_start_time_not_cursor(self):
        # A `date_from` test run passes an int Unix timestamp; Zendesk 400s
        # if that int is sent as `cursor=` instead of `start_time=`.
        assert _CursorPaginationMixin()._first_page_params(1786320000) == {
            "start_time": 1786320000
        }

    def test_string_resume_token_is_sent_as_cursor(self):
        assert _CursorPaginationMixin()._first_page_params("abc123") == {
            "cursor": "abc123"
        }


class TestCursorPaginationMixinNextPage:
    def test_end_of_stream_stops_pagination(self):
        params, done = _CursorPaginationMixin()._next_page(
            {"end_of_stream": True, "after_cursor": "c1"}
        )
        assert (params, done) == (None, True)

    def test_after_cursor_present_continues_with_cursor_param(self):
        params, done = _CursorPaginationMixin()._next_page(
            {"end_of_stream": False, "after_cursor": "c1"}
        )
        assert (params, done) == ({"cursor": "c1"}, False)

    def test_missing_after_cursor_stops_even_if_end_of_stream_is_false(self):
        # Defensive: a malformed/empty payload without after_cursor must not spin
        # forever sending an empty cursor.
        params, done = _CursorPaginationMixin()._next_page({"end_of_stream": False})
        assert (params, done) == (None, True)

    def test_next_resume_token_is_the_last_after_cursor(self):
        assert (
            _CursorPaginationMixin().next_resume_token({"after_cursor": "c9"}) == "c9"
        )

    def test_next_resume_token_handles_no_payload(self):
        assert _CursorPaginationMixin().next_resume_token(None) is None


class TestTimeBasedPaginationMixinFirstPageParams:
    def test_none_resume_token_starts_from_the_beginning_of_time(self):
        assert _TimeBasedPaginationMixin()._first_page_params(None) == {"start_time": 1}

    def test_given_resume_token_is_used_verbatim(self):
        assert _TimeBasedPaginationMixin()._first_page_params(1700000000) == {
            "start_time": 1700000000
        }


class TestTimeBasedPaginationMixinNextPage:
    def test_end_of_stream_stops_pagination(self):
        params, done = _TimeBasedPaginationMixin()._next_page(
            {"end_of_stream": True, "next_page": "https://x"}
        )
        assert (params, done) == (None, True)

    def test_next_page_url_is_passed_through_verbatim(self):
        params, done = _TimeBasedPaginationMixin()._next_page(
            {
                "end_of_stream": False,
                "next_page": "https://acme.zendesk.com/api/v2/x?cursor=1",
            }
        )
        assert (params, done) == ("https://acme.zendesk.com/api/v2/x?cursor=1", False)

    def test_missing_next_page_stops(self):
        params, done = _TimeBasedPaginationMixin()._next_page({"end_of_stream": False})
        assert (params, done) == (None, True)

    def test_next_resume_token_is_end_time_not_next_page_url(self):
        # ticket_events' resumable checkpoint is the numeric end_time, not the URL.
        assert (
            _TimeBasedPaginationMixin().next_resume_token(
                {"end_time": 1700003600, "next_page": "https://x"}
            )
            == 1700003600
        )

    def test_next_resume_token_handles_no_payload(self):
        assert _TimeBasedPaginationMixin().next_resume_token(None) is None


class TestIterRawPagesCursorChaining:
    def test_follows_cursor_until_end_of_stream_and_sends_it_on_each_next_call(
        self, monkeypatch
    ):
        pages = [
            {"tickets": [{"id": 1}], "after_cursor": "c1", "end_of_stream": False},
            {"tickets": [{"id": 2}], "after_cursor": "c2", "end_of_stream": False},
            {"tickets": [{"id": 3}], "after_cursor": "c3", "end_of_stream": True},
        ]
        calls: list[dict] = []

        def fake_get(params_or_url):
            calls.append(params_or_url)
            return pages[len(calls) - 1]

        hook = _hook(TicketsHook)
        monkeypatch.setattr(hook, "_get", fake_get)

        results = list(hook.iter_raw_pages(None))

        assert results == pages
        assert calls == [{"start_time": 1}, {"cursor": "c1"}, {"cursor": "c2"}]

    def test_resume_token_string_is_sent_as_cursor_on_the_first_call(self, monkeypatch):
        calls: list[dict] = []

        def fake_get(params_or_url):
            calls.append(params_or_url)
            return {"tickets": [], "end_of_stream": True}

        hook = _hook(UsersHook)
        monkeypatch.setattr(hook, "_get", fake_get)

        list(hook.iter_raw_pages("saved-cursor-xyz"))

        assert calls == [{"cursor": "saved-cursor-xyz"}]


class TestIterRawPagesTimeBasedChaining:
    def test_follows_next_page_url_until_end_of_stream(self, monkeypatch):
        pages = [
            {
                "ticket_events": [{"id": 1}],
                "next_page": "https://acme.zendesk.com/x?p=2",
                "end_of_stream": False,
            },
            {
                "ticket_events": [{"id": 2}],
                "next_page": "https://acme.zendesk.com/x?p=3",
                "end_of_stream": False,
            },
            {
                "ticket_events": [{"id": 3}],
                "end_of_stream": True,
                "end_time": 1700009999,
            },
        ]
        calls: list = []

        def fake_get(params_or_url):
            calls.append(params_or_url)
            return pages[len(calls) - 1]

        hook = _hook(TicketCommentsHook)
        monkeypatch.setattr(hook, "_get", fake_get)

        results = list(hook.iter_raw_pages(None))

        assert results == pages
        assert calls[0] == {
            "start_time": 1,
            "include": "comment_events",
            "per_page": 100,
        }
        assert calls[1] == "https://acme.zendesk.com/x?p=2"
        assert calls[2] == "https://acme.zendesk.com/x?p=3"


class TestMaxPagesCap:
    def test_max_pages_stops_the_loop_without_raising(self, monkeypatch):
        """Unlike SolidgateBaseHook, ZendeskBaseHook has no default safety cap of its
        own -- ``max_pages`` is an opt-in parameter the caller must supply (the
        operator always does via ``max_pages_for``). When supplied, a stuck cursor
        that never reaches end_of_stream is truncated silently (no exception) once
        ``max_pages`` is hit -- a real behavioral difference from Solidgate, which
        raises RuntimeError on a stuck cursor instead of truncating quietly.
        """
        hook = _hook(TicketsHook)

        def fake_get(params_or_url):
            # Cursor never terminates -> would loop forever without max_pages.
            return {
                "tickets": [{"id": 1}],
                "after_cursor": "stuck",
                "end_of_stream": False,
            }

        monkeypatch.setattr(hook, "_get", fake_get)

        results = list(hook.iter_raw_pages(None, max_pages=3))

        assert len(results) == 3

    def test_no_max_pages_supplied_does_not_cap_iteration(self, monkeypatch):
        """Documents the unbounded-by-default behavior: calling iter_raw_pages
        directly without max_pages relies entirely on the caller to bound it.
        Here we bound the fake data itself (not max_pages) to prove no implicit cap
        kicks in before end_of_stream.
        """
        pages = [
            {"tickets": [{"id": i}], "after_cursor": f"c{i}", "end_of_stream": False}
            for i in range(1, 11)
        ]
        pages[-1] = {"tickets": [{"id": 10}], "end_of_stream": True}
        calls = []

        def fake_get(params_or_url):
            calls.append(params_or_url)
            return pages[len(calls) - 1]

        hook = _hook(TicketsHook)
        monkeypatch.setattr(hook, "_get", fake_get)

        results = list(hook.iter_raw_pages(None))

        assert len(results) == 10


class TestIterRawPagesUsesEntityRecordsKeyForLogging:
    def test_extract_records_is_called_per_page_via_the_hook_subclass(
        self, monkeypatch
    ):
        # extract_records is exercised through iter_raw_pages' own logging call --
        # a hook that raises here would break every page, not just extraction callers.
        hook = _hook(UsersHook)
        monkeypatch.setattr(
            hook,
            "_get",
            lambda params_or_url: {"users": [{"id": 1}], "end_of_stream": True},
        )

        results = list(hook.iter_raw_pages(None))

        assert results == [{"users": [{"id": 1}], "end_of_stream": True}]
