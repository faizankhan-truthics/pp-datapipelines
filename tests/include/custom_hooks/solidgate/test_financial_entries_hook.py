"""Unit tests for FinancialEntriesHook (include/custom_hooks/solidgate/financial_entries.py).

financial_entries is the only Solidgate entity that is NOT paginated JSON: it's an async
CSV report (POST generate -> poll GET until a redirect -> presigned S3 URL), so none of
the shared base.py pagination tests reach it. It is also the revenue/fee ledger that the
net-revenue and refund-rate gold models are built on, so a silent failure here (e.g.
treating a "still processing" response as fatal, or returning the wrong URL) means a
missing or wrong day of financials rather than a loud task failure.

``signed_request`` is stubbed so no credentials, rate limiter, or network are needed;
the HMAC signing and retry behavior it wraps are covered in test_base_signed_request.py.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
import requests

import include.custom_hooks.solidgate.financial_entries as fe_module
from include.custom_hooks.solidgate.financial_entries import FinancialEntriesHook

BASE_URL = "https://reports.example.test/api/v1"


def _hook() -> FinancialEntriesHook:
    return FinancialEntriesHook(
        base_url=BASE_URL, public_key_env="TEST_PUBLIC", secret_key_env="TEST_SECRET"
    )


def _json_response(payload: dict) -> SimpleNamespace:
    return SimpleNamespace(raise_for_status=lambda: None, json=lambda: payload)


def _poll_response(
    status_code: int, *, location: str | None = None, text: str = ""
) -> SimpleNamespace:
    def raise_for_status():
        if status_code >= 400:
            raise requests.exceptions.HTTPError(f"HTTP {status_code}")

    return SimpleNamespace(
        status_code=status_code,
        is_redirect=location is not None,
        headers={"Location": location} if location else {},
        text=text,
        raise_for_status=raise_for_status,
    )


@pytest.fixture
def no_sleep(monkeypatch):
    sleeps = []
    monkeypatch.setattr(fe_module.time, "sleep", lambda s: sleeps.append(s))
    return sleeps


class TestCreateReport:
    def test_posts_window_and_filter_to_the_generate_endpoint(self, monkeypatch):
        hook = _hook()
        calls = []

        def fake_signed_request(method, path, *, body=""):
            calls.append((method, path, json.loads(body)))
            return _json_response({"report_url": "https://r/1"})

        monkeypatch.setattr(hook, "signed_request", fake_signed_request)

        hook._create_report("2026-01-01 00:00:00", "2026-01-02 00:00:00", "created_at")

        assert calls == [
            (
                "POST",
                "/finance/financial_entries",
                {
                    "date_from": "2026-01-01 00:00:00",
                    "date_to": "2026-01-02 00:00:00",
                    "filter": "created_at",
                },
            )
        ]

    @pytest.mark.parametrize(
        "payload",
        [{"report_url": "https://r/top"}, {"body": {"report_url": "https://r/top"}}],
        ids=["top_level", "nested_body"],
    )
    def test_returns_report_url_from_either_payload_shape(self, monkeypatch, payload):
        hook = _hook()
        monkeypatch.setattr(
            hook, "signed_request", lambda *a, **k: _json_response(payload)
        )

        assert hook._create_report("d1", "d2", "created_at") == "https://r/top"

    @pytest.mark.parametrize(
        "message",
        [
            "Report with report_id: abc-123 is processing",
            {"general": "Report with report_id: abc-123 is processing, try later"},
            "report_id:abc-123   IS PROCESSING",
        ],
        ids=["string_message", "dict_message", "no_space_and_upper_case"],
    )
    def test_still_processing_error_is_turned_into_the_download_url_instead_of_failing(
        self, monkeypatch, message
    ):
        # Re-requesting a window whose report is already being generated comes back as an
        # *error* payload. That must resume polling the existing report, not fail the task.
        hook = _hook()
        payload = {"error": {"code": "2.01", "message": message}}
        monkeypatch.setattr(
            hook, "signed_request", lambda *a, **k: _json_response(payload)
        )

        url = hook._create_report("d1", "d2", "created_at")

        assert url == f"{BASE_URL}/finance/financial_entries/report/abc-123/download"

    @pytest.mark.parametrize(
        "payload",
        [
            {},
            {"error": {"code": "1.01", "message": "Invalid date range"}},
            {"error": None},
        ],
        ids=["empty", "unrelated_error", "null_error"],
    )
    def test_payload_with_no_url_and_no_processing_report_raises(
        self, monkeypatch, payload
    ):
        hook = _hook()
        monkeypatch.setattr(
            hook, "signed_request", lambda *a, **k: _json_response(payload)
        )

        with pytest.raises(RuntimeError, match="financial_entries generate error"):
            hook._create_report("d1", "d2", "created_at")

    def test_http_error_on_generate_propagates(self, monkeypatch):
        hook = _hook()

        def raise_401():
            raise requests.exceptions.HTTPError("401 Unauthorized")

        monkeypatch.setattr(
            hook,
            "signed_request",
            lambda *a, **k: SimpleNamespace(
                raise_for_status=raise_401, json=lambda: {}
            ),
        )

        with pytest.raises(requests.exceptions.HTTPError):
            hook._create_report("d1", "d2", "created_at")


class TestPollForS3Url:
    def _stub_polls(self, monkeypatch, hook, responses):
        calls = []
        it = iter(responses)

        def fake_signed_request(method, url, *, allow_redirects=True):
            calls.append((method, url, allow_redirects))
            return next(it)

        monkeypatch.setattr(hook, "signed_request", fake_signed_request)
        return calls

    def test_waits_through_204s_then_returns_the_redirect_location(
        self, monkeypatch, no_sleep
    ):
        hook = _hook()
        calls = self._stub_polls(
            monkeypatch,
            hook,
            [
                _poll_response(204),
                _poll_response(204),
                _poll_response(302, location="https://s3/presigned.csv"),
            ],
        )

        url = hook._poll_for_s3_url("https://r/report", wait_seconds=7)

        assert url == "https://s3/presigned.csv"
        assert len(calls) == 3
        assert no_sleep == [7, 7]

    def test_polls_without_following_redirects(self, monkeypatch, no_sleep):
        # Following the redirect would download the whole CSV onto the worker instead of
        # handing the presigned URL to the streaming GCS upload.
        hook = _hook()
        calls = self._stub_polls(
            monkeypatch, hook, [_poll_response(307, location="https://s3/x")]
        )

        hook._poll_for_s3_url("https://r/report")

        assert calls == [("GET", "https://r/report", False)]

    def test_200_body_is_an_error_not_the_csv(self, monkeypatch, no_sleep):
        hook = _hook()
        self._stub_polls(
            monkeypatch, hook, [_poll_response(200, text='{"error":"bad report"}')]
        )

        with pytest.raises(RuntimeError, match="financial entries download error"):
            hook._poll_for_s3_url("https://r/report")

    @pytest.mark.parametrize("status", [404, 410])
    def test_missing_or_expired_report_raises(self, monkeypatch, no_sleep, status):
        hook = _hook()
        self._stub_polls(monkeypatch, hook, [_poll_response(status, text="gone")])

        with pytest.raises(RuntimeError, match=f"report {status}"):
            hook._poll_for_s3_url("https://r/report")

    def test_other_error_status_raises_http_error(self, monkeypatch, no_sleep):
        hook = _hook()
        self._stub_polls(monkeypatch, hook, [_poll_response(500)])

        with pytest.raises(requests.exceptions.HTTPError):
            hook._poll_for_s3_url("https://r/report")

    def test_never_ready_raises_timeout_after_max_attempts(self, monkeypatch, no_sleep):
        hook = _hook()
        calls = self._stub_polls(monkeypatch, hook, [_poll_response(204)] * 3)

        with pytest.raises(TimeoutError, match="not ready after 3 attempts"):
            hook._poll_for_s3_url("https://r/report", max_attempts=3)

        assert len(calls) == 3


class TestResolveReportUrl:
    def test_generates_then_polls_and_defaults_to_the_created_at_filter(
        self, monkeypatch, no_sleep
    ):
        hook = _hook()
        calls = []

        def fake_signed_request(method, path_or_url, *, body="", allow_redirects=True):
            calls.append((method, path_or_url))
            if method == "POST":
                assert json.loads(body)["filter"] == "created_at"
                return _json_response({"report_url": "https://r/report"})
            return _poll_response(302, location="https://s3/presigned.csv")

        monkeypatch.setattr(hook, "signed_request", fake_signed_request)

        assert (
            hook.resolve_report_url("2026-01-01 00:00:00", "2026-01-02 00:00:00")
            == "https://s3/presigned.csv"
        )
        assert calls == [
            ("POST", "/finance/financial_entries"),
            ("GET", "https://r/report"),
        ]
