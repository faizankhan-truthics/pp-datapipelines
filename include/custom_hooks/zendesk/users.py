"""UsersHook -- cursor-based Incremental User Export.

GET /api/v2/incremental/users/cursor.json

"""

from __future__ import annotations

from typing import Any

from include.custom_hooks.zendesk.base import ZendeskBaseHook, _CursorPaginationMixin


class UsersHook(_CursorPaginationMixin, ZendeskBaseHook):
    _PATH = "/incremental/users/cursor.json"
    _RECORDS_KEY = "users"

    @staticmethod
    def extract_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
        return payload.get("users") or []
