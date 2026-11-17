"""FinancialEntriesHook — async CSV report: generate -> poll -> presigned S3 URL.

The caller streams the returned presigned URL straight into GCS (no worker download).
"""

from __future__ import annotations

import json
import re
import time
from typing import Any

from include.custom_hooks.solidgate.base import SolidgateBaseHook

_GENERATE_PATH = "/finance/financial_entries"
_PROCESSING_REPORT_ID_RE = re.compile(
    r"report_id:\s*(\S+?)\s+is processing", re.IGNORECASE
)


class FinancialEntriesHook(SolidgateBaseHook):
    def _create_report(self, date_from: str, date_to: str, filter: str) -> str:
        body = json.dumps(
            {"date_from": date_from, "date_to": date_to, "filter": filter}
        )
        resp = self.signed_request("POST", _GENERATE_PATH, body=body)
        resp.raise_for_status()
        payload: dict[str, Any] = resp.json()
        self.log.info("financial_entries generate response: %s", payload)

        report_url = payload.get("report_url") or payload.get("body", {}).get(
            "report_url"
        )
        if report_url:
            return report_url
        report_id = self._extract_processing_report_id(payload)
        if report_id:
            url = (
                f"{self.base_url}/finance/financial_entries/report/{report_id}/download"
            )
            self.log.info(
                "report processing; report_id=%s -> polling %s", report_id, url
            )
            return url
        raise RuntimeError(f"financial_entries generate error: {payload}")

    @staticmethod
    def _extract_processing_report_id(payload: dict[str, Any]) -> str | None:
        error = payload.get("error") or {}
        message = error.get("message")
        text = (
            " ".join(str(v) for v in message.values())
            if isinstance(message, dict)
            else str(message)
        )
        match = _PROCESSING_REPORT_ID_RE.search(text)
        return match.group(1) if match else None

    def _poll_for_s3_url(
        self, report_url: str, *, max_attempts: int = 20, wait_seconds: int = 15
    ) -> str:
        for attempt in range(1, max_attempts + 1):
            resp = self.signed_request("GET", report_url, allow_redirects=False)
            self.log.info(
                "poll %d/%d -> HTTP %d", attempt, max_attempts, resp.status_code
            )
            if resp.status_code == 204:
                time.sleep(wait_seconds)
                continue
            if resp.is_redirect or resp.status_code in (301, 302, 303, 307, 308):
                self.log.info("report ready; presigned S3 URL resolved")
                return resp.headers["Location"]
            if resp.status_code == 200:
                raise RuntimeError(
                    f"financial entries download error: {resp.text[:300]}"
                )
            if resp.status_code in (404, 410):
                raise RuntimeError(f"report {resp.status_code}: {resp.text[:200]}")
            resp.raise_for_status()
        raise TimeoutError(
            f"financial entries report not ready after {max_attempts} attempts"
        )

    def resolve_report_url(
        self, date_from: str, date_to: str, *, filter: str = "created_at"
    ) -> str:
        """Generate the report + poll until ready; return the presigned S3 CSV URL."""
        report_url = self._create_report(date_from, date_to, filter)
        return self._poll_for_s3_url(report_url)
