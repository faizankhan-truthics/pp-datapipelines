"""TicketsHook -- cursor-based Incremental Ticket Export.

GET /api/v2/incremental/tickets/cursor.json
"""

from __future__ import annotations

from typing import Any

from include.custom_hooks.zendesk.base import ZendeskBaseHook, _CursorPaginationMixin


class TicketsHook(_CursorPaginationMixin, ZendeskBaseHook):
    _PATH = "/incremental/tickets/cursor.json"
    _RECORDS_KEY = "tickets"

    @staticmethod
    def extract_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
        return payload.get("tickets") or []
