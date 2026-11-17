"""Unit tests for ChargebackhitHook.get_raw_pages
(include/custom_hooks/chargebackhit/chargebackhit_hook.py).

Unlike Solidgate (cursor-only pagination for every entity), ChargebackHit supports two
different pagination styles keyed by endpoint in
``include/config/chargebackhit_settings.PAGINATION_STYLE``: "offset" (currently used by
the only active endpoint, "alerts") and "token" (reserved for "disputes", which is
commented out of ``ENDPOINTS`` but still present in ``PAGINATION_STYLE`` and therefore
still reachable through ``get_raw_pages`` directly). A cursor/offset bug here would
either truncate a day's alerts or loop forever, so both styles are exercised even though
only one is currently wired into the DAG.

Note: ``get_raw_pages`` yields the *unwrapped* ``data`` list for each page (not the raw
page envelope with its ``pagination``/``metadata`` block) -- unlike Solidgate's
``iter_raw_pages``, which yields the full raw envelope so Landing stays a lossless copy
of what the API actually returned. That means Chargebackhit's Landing objects (built by
the operator from these yields) do not preserve the API's pagination metadata. This is
not exercised as a "bug" here since it doesn't break current functionality, but it is a
design asymmetry worth flagging (see final report).
"""

from __future__ import annotations

import pytest

from include.custom_hooks.chargebackhit.chargebackhit_hook import ChargebackhitHook


def _hook(monkeypatch) -> ChargebackhitHook:
    monkeypatch.setenv("CHARGEBACKHIT_ACME_API_PUBLIC_KEY", "pk")
    monkeypatch.setenv("CHARGEBACKHIT_ACME_API_SECRET_KEY", "sk")
    return ChargebackhitHook(
        account_name="acme",
        public_key_env="CHARGEBACKHIT_ACME_API_PUBLIC_KEY",
        secret_key_env="CHARGEBACKHIT_ACME_API_SECRET_KEY",
    )


class TestOffsetPagination:
    def test_pages_until_offset_reaches_total_count(self, monkeypatch):
        # `offset += limit` (the fixed page size, 100) -- NOT `offset += len(data)` -- so
        # a second request only happens while offset (a multiple of 100) is still below
        # total_count. A realistic full first page (100 records) is needed here: with a
        # short first page (as an earlier version of this test used), offset jumps straight
        # past a small total_count and pagination stops after just one request. See
        # test_short_page_before_total_count_silently_drops_the_remaining_records below for
        # the real bug that asymmetry causes.
        hook = _hook(monkeypatch)
        page_1_data = [{"id": str(i)} for i in range(100)]
        page_2_data = [{"id": "100"}, {"id": "101"}, {"id": "102"}]
        responses = [
            {"data": page_1_data, "pagination": {"total_count": 103}},
            {"data": page_2_data, "pagination": {"total_count": 103}},
        ]
        requests_made = []

        def fake_request(url, body, max_retries=5):
            requests_made.append(body)
            return responses[len(requests_made) - 1]

        monkeypatch.setattr(hook, "_make_request_with_retry", fake_request)

        pages = list(hook.get_raw_pages("alerts", 1_700_000_000, 1_700_086_400))

        assert pages == [page_1_data, page_2_data]
        assert len(requests_made) == 2
        # offset is 0 on the first request, then advances by `limit` (100) each time.
        assert requests_made[0]["pagination"]["offset"] == 0
        assert requests_made[1]["pagination"]["offset"] == 100

    def test_short_page_before_total_count_silently_drops_the_remaining_records(
        self, monkeypatch
    ):
        """Real bug, pinned not fixed: offset advances by the fixed `limit` (100) rather
        than by the number of records actually returned. If the API ever returns a
        "short" page (fewer than `limit` records) before the true total_count is reached
        -- e.g. a server-side page-size cap, or a flaky partial response -- the next
        request's offset still jumps by a full 100, silently skipping every record between
        `offset + len(data)` and `offset + limit`. Here a 2-record first page against a
        total_count of 150 causes pagination to stop after only 2 requests (offset 0, then
        100) instead of continuing until the real total is covered -- with no error, no
        warning, just missing data.
        """
        hook = _hook(monkeypatch)
        responses = [
            {"data": [{"id": "1"}, {"id": "2"}], "pagination": {"total_count": 150}},
            {"data": [{"id": "101"}], "pagination": {"total_count": 150}},
        ]
        requests_made = []

        def fake_request(url, body, max_retries=5):
            requests_made.append(body)
            return responses[len(requests_made) - 1]

        monkeypatch.setattr(hook, "_make_request_with_retry", fake_request)

        pages = list(hook.get_raw_pages("alerts", 1_700_000_000, 1_700_086_400))

        # Only 3 of the "true" 150 records are ever fetched, and pagination stops
        # without any indication that ~147 records were silently missed.
        assert pages == [[{"id": "1"}, {"id": "2"}], [{"id": "101"}]]
        assert len(requests_made) == 2
        assert requests_made[1]["pagination"]["offset"] == 100

    def test_stops_immediately_when_first_page_has_no_data(self, monkeypatch):
        hook = _hook(monkeypatch)
        requests_made = []

        def fake_request(url, body, max_retries=5):
            requests_made.append(body)
            return {"data": [], "pagination": {"total_count": 0}}

        monkeypatch.setattr(hook, "_make_request_with_retry", fake_request)

        pages = list(hook.get_raw_pages("alerts", 1_700_000_000, 1_700_086_400))

        assert pages == []
        assert len(requests_made) == 1

    def test_missing_total_count_defaults_to_zero_and_stops_after_one_page(
        self, monkeypatch
    ):
        # `payload.get("pagination", {}).get("total_count", 0)` defaults to 0 when the
        # API omits it -- offset (100) >= 0 is always true, so pagination must stop
        # after exactly one page rather than looping or raising a KeyError.
        hook = _hook(monkeypatch)
        requests_made = []

        def fake_request(url, body, max_retries=5):
            requests_made.append(body)
            return {"data": [{"id": "1"}]}

        monkeypatch.setattr(hook, "_make_request_with_retry", fake_request)

        pages = list(hook.get_raw_pages("alerts", 1_700_000_000, 1_700_086_400))

        assert pages == [[{"id": "1"}]]
        assert len(requests_made) == 1

    def test_date_window_is_formatted_as_utc_iso8601_and_sent_as_created_at_filter(
        self, monkeypatch
    ):
        hook = _hook(monkeypatch)
        captured = {}

        def fake_request(url, body, max_retries=5):
            captured["url"] = url
            captured["body"] = body
            return {"data": [], "pagination": {"total_count": 0}}

        monkeypatch.setattr(hook, "_make_request_with_retry", fake_request)

        # 2026-01-01T00:00:00Z .. 2026-01-02T00:00:00Z
        list(hook.get_raw_pages("alerts", 1767225600, 1767312000))

        assert captured["url"] == "https://api.chargebackhit.com/api/v2/alerts/list"
        assert captured["body"]["filter"] == {
            "created_at_from": "2026-01-01T00:00:00Z",
            "created_at_to": "2026-01-02T00:00:00Z",
        }


class TestTokenPagination:
    """The "disputes" endpoint (PAGINATION_STYLE["disputes"] == "token") is currently
    commented out of ENDPOINTS, but get_raw_pages is still reachable with it directly --
    this pins the token-chaining behavior so it doesn't silently rot before disputes is
    re-enabled.
    """

    def test_follows_page_token_until_it_is_absent(self, monkeypatch):
        hook = _hook(monkeypatch)
        responses = [
            {"data": [{"id": "d1"}], "pagination": {"next_page_token": "tok-1"}},
            {"data": [{"id": "d2"}], "pagination": {"next_page_token": None}},
        ]
        requests_made = []

        def fake_request(url, body, max_retries=5):
            requests_made.append(body)
            return responses[len(requests_made) - 1]

        monkeypatch.setattr(hook, "_make_request_with_retry", fake_request)

        pages = list(hook.get_raw_pages("disputes", 1_700_000_000, 1_700_086_400))

        assert pages == [[{"id": "d1"}], [{"id": "d2"}]]
        assert "page_token" not in requests_made[0]["pagination"]
        assert requests_made[1]["pagination"]["page_token"] == "tok-1"

    def test_date_window_is_sent_as_dispute_date_filter(self, monkeypatch):
        hook = _hook(monkeypatch)
        captured = {}

        def fake_request(url, body, max_retries=5):
            captured["body"] = body
            return {"data": []}

        monkeypatch.setattr(hook, "_make_request_with_retry", fake_request)

        # 2026-01-01T00:00:00Z .. 2026-01-02T00:00:00Z
        list(hook.get_raw_pages("disputes", 1767225600, 1767312000))

        assert captured["body"]["filter"] == {
            "dispute_date_from": "2026-01-01T00:00:00Z",
            "dispute_date_to": "2026-01-02T00:00:00Z",
        }

    def test_missing_pagination_block_stops_after_one_page(self, monkeypatch):
        hook = _hook(monkeypatch)
        requests_made = []

        def fake_request(url, body, max_retries=5):
            requests_made.append(body)
            return {"data": [{"id": "d1"}]}

        monkeypatch.setattr(hook, "_make_request_with_retry", fake_request)

        pages = list(hook.get_raw_pages("disputes", 1_700_000_000, 1_700_086_400))

        assert pages == [[{"id": "d1"}]]
        assert len(requests_made) == 1


class TestUnknownEndpoint:
    def test_endpoint_not_in_pagination_style_raises_keyerror(self, monkeypatch):
        hook = _hook(monkeypatch)
        monkeypatch.setattr(
            hook,
            "_make_request_with_retry",
            lambda *a, **k: pytest.fail("should not be called"),
        )

        with pytest.raises(KeyError):
            list(
                hook.get_raw_pages("not_a_real_endpoint", 1_700_000_000, 1_700_086_400)
            )
