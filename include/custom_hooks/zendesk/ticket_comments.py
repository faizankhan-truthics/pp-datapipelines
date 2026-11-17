"""TicketCommentsHook -- time-based Incremental Ticket Event Export.

GET /api/v2/incremental/ticket_events.json?include=comment_events

"""

from __future__ import annotations

from typing import Any

from include.custom_hooks.zendesk.base import ZendeskBaseHook, _TimeBasedPaginationMixin


class TicketCommentsHook(_TimeBasedPaginationMixin, ZendeskBaseHook):
    _PATH = "/incremental/ticket_events.json"
    _RECORDS_KEY = "ticket_events"

    def _first_page_params(self, resume_token):
        params = super()._first_page_params(resume_token)
        params["include"] = "comment_events"
        params["per_page"] = 100
        return params

    @staticmethod
    def extract_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
        comments = []
        for event in payload.get("ticket_events") or []:
            for child in event.get("child_events") or []:
                if child.get("event_type") != "Comment":
                    continue
                comments.append({**child, "ticket_id": event.get("ticket_id")})
        return comments
