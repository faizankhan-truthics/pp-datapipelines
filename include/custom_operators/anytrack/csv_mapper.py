"""Map an AnyTrack `trackingEvents` CSV export to the full outgoing-webhook JSON shape.

Reconstructs the shape AnyTrack POSTs to our webhook receiver (see
docs/anytrack_integration.md, "Sample event payload") from a flat CSV
export, for cases where only the CSV export is available (no live webhook
payload). Used by backfill_utils.py's Landing -> Release step, and
runnable standalone for ad-hoc inspection (see `main()` below).

Fields with no reliable source in the CSV export are left null (or {} / [] where the sample
shows an empty container):
  - `assetId`                         — not a CSV column.
  - `externalClickId`                 — not a CSV column either (distinct from `clickId`,
                                         which the CSV does have) -- always null for
                                         backfilled rows, unlike live payloads.
  - `currency` (top-level and customParams) — not a CSV column at all; left null rather than
                                         hardcoded, per team decision.
  - `traits.country` (full name)      — CSV only has the ISO country code, not the full name
                                         AnyTrack sends here (e.g. "India" vs "IN").
  - `mainAttribution`/`attributions`  — eventIndex, subsidiary, restored, page and location
                                         are not exported; the rest come from CSV columns.
  - `ip`                              — not a CSV column.
  - `externalClientIds`               — always {}. Confirmed against real live payloads that
                                         AnyTrack never populates this field on outgoing
                                         webhooks; it puts the click id in mainAttribution/
                                         attributions instead (see below). An earlier version
                                         of this mapper put the CSV's click id here, which
                                         looked reasonable but didn't match what live payloads
                                         actually do -- that's now fixed.

The CSV *does* carry these, confirmed against its real column list -- mapped straight across:
  - `referrer`, `linkId`, `linkURL`   — same column names in the CSV as the live payload uses.
  - `linklabel`                       — CSV column is `linkLabel` (capital L); the live
                                         payload's own key is lowercase `linklabel`, so the
                                         output key here matches the payload, not the CSV.

The CSV carries the click ids and the campaign breakdown; `mainAttribution`/`attributions` are
built to match the live payload shape exactly:
  - `identifier` + `<click id column name>` — whichever one of the CSV's click id columns
                          (gclid, fbclid, msclkid, ...) is populated for this row, mirroring
                          how live payloads tag which network the attribution came from
                          (e.g. `"identifier": "gclid", "gclid": "..."`). A row should have
                          at most one populated (one click, one network) -- see
                          _primary_click_id().
  - `medium`, `campaign`, `campaignName`, `term`, `content`, `creative` — alongside the time
                          and source that were already mapped.

Two fields ARE derived (best-effort, per team decision):
  - `customParams.value`       — set equal to `eventValue`. Note real payloads show these can
                                  land on different scales (e.g. eventValue=26.17 vs value=2495
                                  in the same live event) — this is an approximation, not a
                                  guaranteed-correct backend value.
  - `customParams.funnel_type` — derived from `brandName`, the text before the first `|`
                                  (e.g. "SSL | Monthly plan" -> "SSL").

Standalone usage:
    python -m include.custom_operators.anytrack.csv_mapper \
        --input /path/to/trackingEvents-*.csv \
        --output /path/to/mapped_webhook_events.json
"""

import argparse
import csv
import json
from datetime import datetime, timezone

_NA_STRINGS = {"", "nan", "NaN"}

# Click id columns in the export, one per ad network. Named as AnyTrack names them so the
# JSON key matches the column and nothing has to be renamed downstream.
_CLICK_ID_COLUMNS = (
    "gclid",
    "fbclid",
    "msclkid",
    "ttclid",
    "tblci",
    "ob_click_id",
    "atclid",
)


def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped not in _NA_STRINGS else None


def _split_full_name(full_name: str | None) -> tuple[str | None, str | None]:
    """Best-effort split of the CSV's single `fullName` into first/last name."""
    full_name = _clean(full_name)
    if not full_name:
        return None, None
    parts = full_name.split(None, 1)
    return parts[0], (parts[1] if len(parts) > 1 else None)


def _event_timestamp(event_time: str | None) -> int | None:
    """CSV `eventTime` ('...Z' ISO8601) -> epoch seconds, matching AnyTrack's `eventTimestamp`."""
    event_time = _clean(event_time)
    if not event_time:
        return None
    dt = datetime.fromisoformat(event_time.replace("Z", "+00:00")).astimezone(
        timezone.utc
    )
    return int(dt.timestamp())


def _float_or_none(value: str | None) -> float | None:
    value = _clean(value)
    return float(value) if value is not None else None


def _funnel_type(brand_name: str | None) -> str | None:
    """Derive funnel_type from brandName's text before the first '|' (e.g. "SSL | Monthly
    plan" -> "SSL"). None if brandName is missing or has no '|' separator."""
    brand_name = _clean(brand_name)
    if not brand_name or "|" not in brand_name:
        return None
    return brand_name.split("|", 1)[0].strip() or None


def _primary_click_id(row: dict) -> tuple[str | None, str | None]:
    """A row should have at most one populated click-id column (one click, one ad
    network) -- mirrors how live payloads tag mainAttribution/attributions with a single
    identifier. Returns (identifier, value), or (None, None) if the row has none."""
    for col in _CLICK_ID_COLUMNS:
        value = _clean(row.get(col))
        if value:
            return col, value
    return None, None


def _build_attribution(row: dict) -> dict:
    """`mainAttribution` shape, built from the CSV columns that carry it. eventIndex,
    subsidiary, restored, page and location are not exported and stay null. identifier +
    the click id itself are included when the row has one, matching the live payload
    shape (see _primary_click_id())."""
    identifier, click_id_value = _primary_click_id(row)
    attribution = {
        "eventIndex": None,
        "time": _clean(row.get("attributionTime")),
        "subsidiary": None,
        "restored": None,
        "page": None,
        "location": None,
        "identifier": identifier,
        "source": _clean(row.get("attributionSource")),
        "medium": _clean(row.get("attributionMedium")),
        "campaign": _clean(row.get("attributionCampaign")),
        "campaignName": _clean(row.get("campaignName")),
        "term": _clean(row.get("attributionTerm")),
        "content": _clean(row.get("attributionContent")),
        "creative": _clean(row.get("attributionCreative")),
    }
    if identifier:
        attribution[identifier] = click_id_value
    return attribution


def map_row(row: dict) -> dict:
    first_name, last_name = _split_full_name(row.get("fullName"))
    country_code = _clean(row.get("country"))
    attribution = _build_attribution(row)
    event_value = _float_or_none(row.get("eventValue"))
    return {
        "assetId": None,
        "clientId": _clean(row.get("clientId")),
        "externalClientIds": {},
        "clickId": _clean(row.get("clickId")),
        # Not a CSV column -- AnyTrack's export has no equivalent, so this
        # always stays null for backfilled rows (see _source in
        # merge_events_all_events.sql).
        "externalClickId": None,
        "eventName": _clean(row.get("eventName")),
        "eventTime": _clean(row.get("eventTime")),
        "eventTimestamp": _event_timestamp(row.get("eventTime")),
        "eventValue": event_value,
        "currency": None,
        "userAgent": _clean(row.get("userAgent")),
        "location": _clean(row.get("location")),
        "referrer": _clean(row.get("referrer")),
        "linkId": _clean(row.get("linkId")),
        "linkURL": _clean(row.get("linkURL")),
        # CSV column is "linkLabel" (capital L) -- webhook payloads use
        # "linklabel" (lowercase); map_row's OUTPUT key must match the
        # webhook shape ("linklabel") since that's what the merge SQL reads.
        "linklabel": _clean(row.get("linkLabel")),
        "brandName": _clean(row.get("brandName")),
        "trackingGroupId": _clean(row.get("trackingGroupId")),
        "integrationId": _clean(row.get("integrationId")),
        "transactionId": _clean(row.get("transactionId")),
        "traits": {
            "email": _clean(row.get("email")),
            "firstName": first_name,
            "lastName": last_name,
            "country": None,
            "countryCode": country_code,
        },
        "customParams": {
            "event_name": _clean(row.get("eventName")),
            "brand_name": _clean(row.get("brandName")),
            "refid": _clean(row.get("clickId")),
            "postal_code": _clean(row.get("zipcode")),
            "value": event_value,
            "currency": None,
            "email": _clean(row.get("email")),
            "firstName": first_name,
            "lastName": last_name,
            "country": country_code,
            "transaction_id": _clean(row.get("transactionId")),
            "funnel_type": _funnel_type(row.get("brandName")),
        },
        "mainAttribution": attribution,
        "attributions": [attribution],
        "ip": None,
    }


def map_file(input_path: str) -> list[dict]:
    # utf-8-sig strips a leading BOM if present (common in Excel-exported CSVs), which
    # otherwise gets glued onto the first header ('﻿clientId'), silently breaking any
    # row.get("clientId") lookup.
    with open(input_path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return [map_row(row) for row in reader]


def write_json(rows: list[dict], output_path: str) -> None:
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", required=True, help="Path to the AnyTrack trackingEvents CSV export"
    )
    parser.add_argument(
        "--output", required=True, help="Path to write the mapped webhook-shaped JSON"
    )
    args = parser.parse_args()

    rows = map_file(args.input)
    write_json(rows, args.output)
    print(f"Mapped {len(rows)} row(s) -> {args.output}")


if __name__ == "__main__":
    main()
