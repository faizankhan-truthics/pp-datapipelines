"""FraudAlertsHook — paginated card-order fraud-alerts JSON.

Records are streamed page-by-page via the base ``iter_records`` generator.
"""

from __future__ import annotations

from typing import Any

from include.custom_hooks.solidgate.base import SolidgateBaseHook


class FraudAlertsHook(SolidgateBaseHook):
    _PATH = "/card-orders/fraud-alerts"

    @staticmethod
    def _extract_records(payload: dict[str, Any]) -> list[dict[str, Any]]:
        node = payload.get("alerts")
        if node is None:
            node = payload.get("body", {}).get("fraud_alerts", [])
        return node or []
