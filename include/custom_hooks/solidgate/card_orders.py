"""CardOrdersHook — paginated card-orders JSON.

POST /card-orders {filter, date_from, date_to, limit, next_page_iterator}
response: {"orders": [...], "metadata": {"next_page_iterator": <cursor or null>}}

Records are streamed page-by-page via the base ``iter_records`` generator.
"""

from __future__ import annotations

from typing import Any

from include.custom_hooks.solidgate.base import SolidgateBaseHook


class CardOrdersHook(SolidgateBaseHook):
    _PATH = "/card-orders"

    @staticmethod
    def _extract_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
        node = payload.get("orders")
        if node is None:
            node = payload.get("body", {}).get("orders", [])
        return node or []
