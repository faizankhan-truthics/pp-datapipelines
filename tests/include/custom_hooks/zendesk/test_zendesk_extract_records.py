"""Unit tests for each Zendesk hook's ``extract_records`` (the per-endpoint payload shape).

``TicketsHook``/``UsersHook`` are flat list lookups, but ``TicketCommentsHook`` has to
flatten Zendesk's Incremental Ticket Event Export shape: each page is a list of
*ticket* events, each of which carries a ``child_events`` list, and only the children
with ``event_type == "Comment"`` are actual comments -- the ticket_id lives on the
parent event, not the child, so it must be stitched back onto each extracted comment.
Getting any of this wrong either drops comments silently or mis-attributes them to
the wrong ticket.
"""

from __future__ import annotations

from include.custom_hooks.zendesk.ticket_comments import TicketCommentsHook
from include.custom_hooks.zendesk.tickets import TicketsHook
from include.custom_hooks.zendesk.users import UsersHook

RECORD = {"id": 1}


class TestTicketsHookExtractRecords:
    def test_returns_the_tickets_list(self):
        assert TicketsHook.extract_records({"tickets": [RECORD]}) == [RECORD]

    def test_missing_key_returns_empty_list(self):
        assert TicketsHook.extract_records({}) == []

    def test_explicit_null_returns_empty_list_not_none(self):
        assert TicketsHook.extract_records({"tickets": None}) == []


class TestUsersHookExtractRecords:
    def test_returns_the_users_list(self):
        assert UsersHook.extract_records({"users": [RECORD]}) == [RECORD]

    def test_missing_key_returns_empty_list(self):
        assert UsersHook.extract_records({}) == []

    def test_explicit_null_returns_empty_list_not_none(self):
        assert UsersHook.extract_records({"users": None}) == []


class TestTicketCommentsHookExtractRecords:
    def test_flattens_comment_child_events_and_stamps_the_parent_ticket_id(self):
        payload = {
            "ticket_events": [
                {
                    "ticket_id": 100,
                    "child_events": [
                        {"id": "c1", "event_type": "Comment", "body": "hi"},
                        {"id": "c2", "event_type": "Comment", "body": "there"},
                    ],
                }
            ]
        }

        records = TicketCommentsHook.extract_records(payload)

        assert records == [
            {"id": "c1", "event_type": "Comment", "body": "hi", "ticket_id": 100},
            {"id": "c2", "event_type": "Comment", "body": "there", "ticket_id": 100},
        ]

    def test_non_comment_child_event_types_are_skipped(self):
        payload = {
            "ticket_events": [
                {
                    "ticket_id": 100,
                    "child_events": [
                        {"id": "c1", "event_type": "Comment", "body": "hi"},
                        {"id": "n1", "event_type": "Notification"},
                        {"id": "s1", "event_type": "Change", "field_name": "status"},
                    ],
                }
            ]
        }

        records = TicketCommentsHook.extract_records(payload)

        assert [r["id"] for r in records] == ["c1"]

    def test_ticket_id_is_attributed_per_parent_event_not_globally(self):
        payload = {
            "ticket_events": [
                {
                    "ticket_id": 100,
                    "child_events": [{"id": "c1", "event_type": "Comment"}],
                },
                {
                    "ticket_id": 200,
                    "child_events": [{"id": "c2", "event_type": "Comment"}],
                },
            ]
        }

        records = TicketCommentsHook.extract_records(payload)

        assert {r["id"]: r["ticket_id"] for r in records} == {"c1": 100, "c2": 200}

    def test_missing_child_events_on_a_ticket_event_yields_no_comments_for_it(self):
        payload = {"ticket_events": [{"ticket_id": 100}]}

        assert TicketCommentsHook.extract_records(payload) == []

    def test_missing_ticket_events_key_returns_empty_list(self):
        assert TicketCommentsHook.extract_records({}) == []

    def test_explicit_null_ticket_events_returns_empty_list(self):
        assert TicketCommentsHook.extract_records({"ticket_events": None}) == []

    def test_explicit_null_child_events_returns_empty_list_for_that_event(self):
        payload = {"ticket_events": [{"ticket_id": 100, "child_events": None}]}

        assert TicketCommentsHook.extract_records(payload) == []
