"""SubscriptionsHook — paginated subscriptions JSON.

The subscriptions endpoint returns the records as a ``{uuid: payload}`` map rather
than a list, so ``_extract_records`` unwraps the values. Records are streamed
page-by-page via the base ``iter_records`` generator.
"""

from __future__ import annotations

from typing import Any

from include.custom_hooks.solidgate.base import SolidgateBaseHook


class SubscriptionsHook(SolidgateBaseHook):
    _PATH = "/subscriptions"
    # Subscription records are large (~4 KB each), so a 2000-row page exceeds the
    # read timeout. Smaller pages return quickly; streaming keeps memory flat.
    _PAGE_LIMIT = 200

    @staticmethod
    def _extract_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
        node = payload.get("subscriptions")
        if node is None:
            node = payload.get("body", {}).get("subscriptions", [])

        # The API returns a {uuid: payload} dictionary map; unwrap to the payloads.
        if isinstance(node, dict):
            return list(node.values())

        return node or []
