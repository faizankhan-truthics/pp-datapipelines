"""Unit tests for include/config/zendesk_settings.py.

Unlike Solidgate, Zendesk has no per-account discovery logic (this pipeline talks
to a single subdomain), so there's no ``get_solidgate_accounts``-equivalent to test.
What it does have, and what has direct blast-radius on correctness, is:

  * ``max_pages_for`` -- the per-endpoint override of the pagination safety cap
    (``ticket_comments`` gets a much lower cap than tickets/users because its
    ``per_page=100`` is far smaller -- see include/custom_hooks/zendesk/ticket_comments.py).
  * ``parse_date_from`` -- turns the DAG's free-text ``date_from``
    param into a Unix timestamp, accepting either a raw epoch-seconds string or an
    ISO date/datetime (assumed UTC if it has no explicit offset).
  * ``get_saved_cursor``/``save_cursor`` -- the Airflow-Variable-backed checkpoint
    that lets a resumed run pick up mid-export instead of restarting a backfill from
    scratch; the value must always round-trip as a string even though a
    ticket_comments checkpoint is really a JSON int (``end_time``).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from include.config import zendesk_settings
from include.config.zendesk_settings import (
    _cursor_variable_name,
    get_saved_cursor,
    max_pages_for,
    parse_date_from,
    save_cursor,
)


class TestMaxPagesFor:
    def test_ticket_comments_uses_its_lower_override(self):
        assert max_pages_for("ticket_comments") == 150

    def test_tickets_and_users_use_the_default(self):
        assert max_pages_for("tickets") == 5000
        assert max_pages_for("users") == 5000

    def test_unknown_endpoint_falls_back_to_the_default(self):
        assert max_pages_for("some_future_endpoint") == 5000


class TestParseStartTimeOverride:
    def test_none_or_empty_returns_none(self):
        assert parse_date_from(None) is None
        assert parse_date_from("") is None

    def test_whitespace_only_raises_instead_of_being_treated_as_unset(self):
        # Real gap, pinned not papered over: `if not value: return None` runs BEFORE
        # `value.strip()`, so a whitespace-only string is truthy and falls through. It's
        # not a digit string either, so it reaches `datetime.fromisoformat("")` (after
        # stripping) and raises ValueError instead of being treated the same as None/""
        # -- an override param of "   " crashes the DAG rather than being ignored.
        with pytest.raises(ValueError):
            parse_date_from("   ")

    def test_digit_string_is_returned_as_an_int_epoch_verbatim(self):
        assert parse_date_from("1786320000") == 1786320000

    def test_digit_string_with_surrounding_whitespace_is_stripped(self):
        assert parse_date_from("  1786320000  ") == 1786320000

    def test_naive_iso_date_is_assumed_utc(self):
        expected = int(datetime(2026, 8, 10, tzinfo=timezone.utc).timestamp())
        assert parse_date_from("2026-08-10") == expected

    def test_naive_iso_datetime_is_assumed_utc(self):
        expected = int(datetime(2026, 8, 10, 13, 30, tzinfo=timezone.utc).timestamp())
        assert parse_date_from("2026-08-10T13:30:00") == expected

    def test_iso_datetime_with_explicit_offset_is_honored_not_overridden(self):
        # US/Pacific (no DST in this literal offset form) -07:00 in August.
        expected = int(datetime(2026, 8, 10, 13, 30, tzinfo=timezone.utc).timestamp())
        naive_utc_equivalent = "2026-08-10T06:30:00-07:00"
        assert parse_date_from(naive_utc_equivalent) == expected

    def test_invalid_non_digit_non_iso_string_raises(self):
        with pytest.raises(ValueError):
            parse_date_from("not-a-date")


class TestCursorVariableName:
    def test_upper_cases_the_endpoint_name(self):
        assert (
            _cursor_variable_name("ticket_comments") == "ZENDESK_TICKET_COMMENTS_CURSOR"
        )

    def test_is_stable_for_already_upper_input(self):
        assert _cursor_variable_name("TICKETS") == "ZENDESK_TICKETS_CURSOR"


def _patch_variable(monkeypatch, *, get_return=None, set_calls=None):
    set_calls = set_calls if set_calls is not None else []
    monkeypatch.setattr(
        zendesk_settings.Variable,
        "get",
        staticmethod(
            lambda key, default_var=None: (
                get_return if get_return is not None else default_var
            )
        ),
    )
    monkeypatch.setattr(
        zendesk_settings.Variable,
        "set",
        staticmethod(lambda key, value: set_calls.append((key, value))),
    )
    return set_calls


class TestGetSavedCursor:
    def test_reads_the_endpoint_scoped_variable(self, monkeypatch):
        monkeypatch.setattr(
            zendesk_settings.Variable,
            "get",
            staticmethod(
                lambda key, default_var=None: (
                    "saved-cursor-value"
                    if key == "ZENDESK_TICKETS_CURSOR"
                    else default_var
                )
            ),
        )

        assert get_saved_cursor("tickets") == "saved-cursor-value"

    def test_missing_variable_returns_none_not_raise(self, monkeypatch):
        monkeypatch.setattr(
            zendesk_settings.Variable,
            "get",
            staticmethod(lambda key, default_var=None: default_var),
        )

        assert get_saved_cursor("users") is None


class TestSaveCursor:
    def test_none_value_is_a_no_op(self, monkeypatch):
        set_calls = _patch_variable(monkeypatch)

        save_cursor("tickets", None)

        assert set_calls == []

    def test_string_cursor_is_saved_verbatim(self, monkeypatch):
        set_calls = _patch_variable(monkeypatch)

        save_cursor("tickets", "opaque-cursor-abc")

        assert set_calls == [("ZENDESK_TICKETS_CURSOR", "opaque-cursor-abc")]

    def test_int_cursor_is_coerced_to_a_string(self, monkeypatch):
        # ticket_comments' resume token is a raw JSON int (end_time); Airflow
        # Variables round-trip as strings regardless, so this must not blow up
        # or silently store an int Variable.set can't serialize consistently.
        set_calls = _patch_variable(monkeypatch)

        save_cursor("ticket_comments", 1786320000)

        assert set_calls == [("ZENDESK_TICKET_COMMENTS_CURSOR", "1786320000")]
        assert isinstance(set_calls[0][1], str)
