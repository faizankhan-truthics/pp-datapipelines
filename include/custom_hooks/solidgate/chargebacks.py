"""ChargebacksHook — paginated card-order chargebacks JSON.

Records are streamed page-by-page via the base ``iter_records`` generator.
"""

from __future__ import annotations

from typing import Any

from include.custom_hooks.solidgate.base import SolidgateBaseHook


class ChargebacksHook(SolidgateBaseHook):
    _PATH = "/card-orders/chargebacks"

    @staticmethod
    def _extract_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
        node = payload.get("orders")
        if node is None:
            node = payload.get("body", {}).get("chargebacks", [])
        return node or []
