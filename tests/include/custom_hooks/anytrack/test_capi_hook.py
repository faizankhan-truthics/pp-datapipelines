"""Tests for include/custom_hooks/anytrack/capi_hook.py (AnyTrack Conversion API client)."""

from __future__ import annotations

from datetime import date

import pytest
import requests
from airflow.exceptions import AirflowException, AirflowFailException

import include.custom_hooks.anytrack.capi_hook as capi_hook
from include.custom_hooks.anytrack.capi_hook import (
    AnytrackCapiHook,
    parse_event_time,
    split_date_range,
    utc_range_to_api_range,
)


class FakeResponse:
    def __init__(
        self, status=200, payload=None, headers=None, text="", content_size=100
    ):
        self.status_code = status
        self._payload = payload or {}
        self.headers = headers or {}
        self.text = text
        self.content = b"x" * content_size

    def json(self):
        if isinstance(self._payload, Exception):
            raise self._payload
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.headers = {}

    def post(self, url, json, timeout):
        self.calls.append({"url": url, "json": json, "timeout": timeout})
        nxt = self.responses.pop(0)
        if isinstance(nxt, Exception):
            raise nxt
        return nxt


@pytest.fixture
def make_hook(monkeypatch):
    class NoLimit:
        def acquire(self):
            pass

    sleeps = []
    limiter_calls = []

    def fake_limiter(name, rate):
        limiter_calls.append((name, rate))
        return NoLimit()

    monkeypatch.setattr(capi_hook, "get_rate_limiter", fake_limiter)
    monkeypatch.setattr(capi_hook.time, "sleep", sleeps.append)

    def _make(responses, **hook_kwargs):
        hook = AnytrackCapiHook(api_key="atk_test", **hook_kwargs)
        hook._session = FakeSession(responses)
        hook.limiter_calls = limiter_calls
        return hook, sleeps

    return _make


class TestRanges:
    def test_utc_range_is_padded_one_day_each_side(self):
        # UTC days 2026-11-02 and 2026-11-03 -> API dates 11-01 .. 11-04
        assert utc_range_to_api_range(date(2026, 11, 2), date(2026, 11, 4)) == (
            "2026-11-01",
            "2026-11-04",
        )

    def test_range_over_the_api_cap_is_rejected(self):
        with pytest.raises(ValueError, match="31-day"):
            utc_range_to_api_range(date(2026, 10, 1), date(2026, 11, 2))

    def test_twenty_nine_day_chunk_fits_the_cap(self):
        # 29 days + one day of padding each side = 31 inclusive dates
        assert utc_range_to_api_range(date(2026, 10, 1), date(2026, 10, 30)) == (
            "2026-09-30",
            "2026-10-30",
        )

    def test_thirty_day_chunk_is_over_the_cap_once_both_ends_are_counted(self):
        # 30 days + padding = 32 inclusive dates (Sep 30 .. Oct 31)
        with pytest.raises(ValueError, match="29 days"):
            utc_range_to_api_range(date(2026, 10, 1), date(2026, 10, 31))

    def test_split_date_range_is_contiguous_and_exclusive_at_the_end(self):
        chunks = split_date_range(date(2026, 10, 1), date(2026, 10, 16), 7)
        assert chunks == [
            (date(2026, 10, 1), date(2026, 10, 8)),
            (date(2026, 10, 8), date(2026, 10, 15)),
            (date(2026, 10, 15), date(2026, 10, 16)),
        ]

    @pytest.mark.parametrize(
        "start,end,days",
        [
            (date(2026, 10, 2), date(2026, 10, 1), 7),
            (date(2026, 10, 1), date(2026, 10, 2), 0),
        ],
    )
    def test_split_date_range_rejects_bad_input(self, start, end, days):
        with pytest.raises(ValueError):
            split_date_range(start, end, days)

    def test_parse_event_time_returns_utc(self):
        parsed = parse_event_time("2026-11-02T14:03:11.000Z")
        assert parsed.isoformat() == "2026-11-02T14:03:11+00:00"


class TestRequests:
    def test_limit_and_cursor_go_in_the_body_and_pages_are_followed(self, make_hook):
        hook, _ = make_hook(
            [
                FakeResponse(payload={"data": [{"id": "a"}], "nextCursor": "c1"}),
                FakeResponse(payload={"data": [{"id": "b"}]}),
            ]
        )
        pages = list(
            hook.iter_conversion_pages("P1", "2026-11-01", "2026-11-03", ["Lead"])
        )
        assert pages == [[{"id": "a"}], [{"id": "b"}]]
        first, second = hook._session.calls
        assert first["url"].endswith("/properties/P1/reports/conversions")
        assert first["json"] == {
            "from": "2026-11-01",
            "to": "2026-11-03",
            "eventNames": ["Lead"],
            "limit": 100,
        }
        assert second["json"]["cursor"] == "c1"
        assert "cursor" not in first["json"]

    def test_no_event_names_means_no_request(self, make_hook):
        hook, _ = make_hook([])
        assert list(hook.iter_conversion_pages("P1", "a", "b", [])) == []
        assert hook._session.calls == []

    def test_more_than_fifty_names_is_refused(self, make_hook):
        hook, _ = make_hook([])
        with pytest.raises(AirflowException, match="50"):
            list(
                hook.iter_conversion_pages("P1", "a", "b", [f"e{i}" for i in range(51)])
            )

    def test_429_waits_retry_after_then_succeeds(self, make_hook):
        hook, sleeps = make_hook(
            [
                FakeResponse(status=429, headers={"Retry-After": "7"}),
                FakeResponse(payload={"data": []}),
            ]
        )
        assert list(hook.iter_conversion_pages("P1", "a", "b", ["Lead"])) == [[]]
        assert sleeps == [7]

    def test_server_error_backs_off_then_succeeds(self, make_hook):
        hook, sleeps = make_hook(
            [FakeResponse(status=503), FakeResponse(payload={"data": []})]
        )
        list(hook.iter_conversion_pages("P1", "a", "b", ["Lead"]))
        assert sleeps == [1]

    def test_client_error_is_not_retried(self, make_hook):
        hook, sleeps = make_hook(
            [FakeResponse(status=403, text="api_access_unavailable")]
        )
        # AirflowFailException: the task must not retry (a retry restarts at page 1)
        with pytest.raises(AirflowFailException, match="403"):
            list(hook.iter_conversion_pages("P1", "a", "b", ["Lead"]))
        assert sleeps == []

    @pytest.mark.parametrize(
        "payload", [{}, {"error": "boom"}, {"data": None}, {"data": {"a": 1}}]
    )
    def test_a_200_without_a_data_list_fails_the_task(self, make_hook, payload):
        hook, _ = make_hook([FakeResponse(payload=payload)])
        with pytest.raises(AirflowFailException, match="data"):
            list(hook.iter_conversion_pages("P1", "a", "b", ["Lead"]))

    def test_a_200_without_a_data_list_fails_the_event_name_listing(self, make_hook):
        hook, _ = make_hook([FakeResponse(payload={"error": "boom"})])
        with pytest.raises(AirflowFailException, match="data"):
            hook.list_event_names("P1", "a", "b")

    def test_an_empty_data_list_is_a_valid_empty_window(self, make_hook):
        hook, _ = make_hook([FakeResponse(payload={"data": [], "nextCursor": None})])
        assert list(hook.iter_conversion_pages("P1", "a", "b", ["Lead"])) == [[]]

    def test_gives_up_after_max_retries(self, make_hook):
        hook, _ = make_hook([FakeResponse(status=500)] * capi_hook.MAX_RETRIES)
        with pytest.raises(AirflowException, match="gave up"):
            list(hook.iter_conversion_pages("P1", "a", "b", ["Lead"]))

    @pytest.mark.parametrize(
        "error", [requests.ConnectionError("reset"), requests.ReadTimeout("slow")]
    )
    def test_network_errors_are_retried_not_fatal(self, make_hook, error):
        hook, sleeps = make_hook([error, FakeResponse(payload={"data": []})])
        assert list(hook.iter_conversion_pages("P1", "a", "b", ["Lead"])) == [[]]
        assert sleeps == [1]

    def test_persistent_network_errors_give_up(self, make_hook):
        hook, _ = make_hook([requests.ConnectionError("down")] * capi_hook.MAX_RETRIES)
        with pytest.raises(AirflowException, match="gave up"):
            list(hook.iter_conversion_pages("P1", "a", "b", ["Lead"]))

    def test_invalid_json_on_a_200_is_retried(self, make_hook):
        hook, sleeps = make_hook(
            [
                FakeResponse(payload=ValueError("bad body")),
                FakeResponse(payload={"data": []}),
            ]
        )
        assert list(hook.iter_conversion_pages("P1", "a", "b", ["Lead"])) == [[]]
        assert sleeps == [1]

    @pytest.mark.parametrize(
        "header,expected",
        [
            ("7", 7),
            ("Wed, 21 Oct 2026 07:28:00 GMT", 5),  # HTTP-date form
            (None, 5),
            ("", 5),
            ("0", 1),
            ("99999", capi_hook.MAX_RETRY_AFTER_S),
        ],
    )
    def test_retry_after_is_parsed_safely_and_capped(self, header, expected):
        assert capi_hook._retry_after_seconds(header) == expected

    def test_a_repeated_cursor_stops_the_pull(self, make_hook):
        hook, _ = make_hook(
            [
                FakeResponse(payload={"data": [], "nextCursor": "c1"}),
                FakeResponse(payload={"data": [], "nextCursor": "c1"}),
            ]
        )
        with pytest.raises(AirflowException, match="already returned"):
            list(hook.iter_conversion_pages("P1", "a", "b", ["Lead"]))

    def test_page_count_is_capped(self, make_hook, monkeypatch):
        monkeypatch.setattr(capi_hook, "MAX_PAGES", 2)
        hook, _ = make_hook(
            [
                FakeResponse(payload={"data": [], "nextCursor": "c1"}),
                FakeResponse(payload={"data": [], "nextCursor": "c2"}),
            ]
        )
        with pytest.raises(AirflowException, match="more than 2 pages"):
            list(hook.iter_conversion_pages("P1", "a", "b", ["Lead"]))

    def test_list_event_names_is_distinct_and_sorted(self, make_hook):
        hook, _ = make_hook(
            [
                FakeResponse(
                    payload={
                        "data": [
                            {"date": "2026-11-01", "eventName": "PageView", "count": 9},
                            {"date": "2026-11-01", "eventName": "Lead", "count": 1},
                        ],
                        "nextCursor": "c",
                    }
                ),
                FakeResponse(
                    payload={
                        "data": [
                            {"date": "2026-11-02", "eventName": "Lead", "count": 2}
                        ]
                    }
                ),
            ]
        )
        assert hook.list_event_names("P1", "a", "b") == ["Lead", "PageView"]

    def test_empty_api_key_is_rejected(self):
        with pytest.raises(AirflowException):
            AnytrackCapiHook(api_key="")


class TestRaisedLimits:
    def test_defaults_are_the_documented_limits(self, make_hook):
        hook, _ = make_hook([FakeResponse(payload={"data": []})])
        list(hook.iter_conversion_pages("P1", "a", "b", ["Lead"]))
        assert hook._session.calls[0]["json"]["limit"] == 100
        ((_, rate),) = hook.limiter_calls
        assert rate == pytest.approx(60 / 60 * 0.95)

    def test_raised_page_size_applies_to_conversions(self, make_hook):
        hook, _ = make_hook(
            [FakeResponse(payload={"data": []})],
            page_limit=200,
            requests_per_minute=120,
        )
        list(hook.iter_conversion_pages("P1", "a", "b", ["Lead"]))
        assert hook._session.calls[0]["json"]["limit"] == 200

    def test_the_daily_counts_report_keeps_the_documented_page_size(self, make_hook):
        hook, _ = make_hook([FakeResponse(payload={"data": []})], page_limit=200)
        hook.list_event_names("P1", "a", "b")
        assert hook._session.calls[0]["json"]["limit"] == 100

    def test_raised_request_rate_is_used_with_a_safety_margin(self, make_hook):
        hook, _ = make_hook([], requests_per_minute=120)
        ((_, rate),) = hook.limiter_calls
        assert rate == pytest.approx(2 * 0.95)
        assert rate < 2.0  # under 120 requests per minute

    @pytest.mark.parametrize("kwargs", [{"page_limit": 0}, {"requests_per_minute": 0}])
    def test_non_positive_limits_are_rejected(self, kwargs):
        with pytest.raises(AirflowException):
            AnytrackCapiHook(api_key="atk_test", **kwargs)

    def test_largest_response_is_tracked_and_a_near_cap_response_warns(
        self, make_hook, caplog
    ):
        hook, _ = make_hook(
            [
                FakeResponse(
                    payload={"data": [], "nextCursor": "c"}, content_size=300_000
                ),
                FakeResponse(payload={"data": []}, content_size=900_000),
            ]
        )
        with caplog.at_level("WARNING"):
            list(hook.iter_conversion_pages("P1", "a", "b", ["Lead"]))
        assert hook.max_response_bytes == 900_000
        assert "close to the 1024 KB cap" in caplog.text
