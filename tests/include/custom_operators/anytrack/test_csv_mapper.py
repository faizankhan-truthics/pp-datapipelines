"""Unit tests for include/custom_operators/anytrack/csv_mapper.py.

map_row() reconstructs a live AnyTrack webhook payload from one row of the
`trackingEvents` CSV export, for the manual backfill path (backfill_utils.py). Its
output lands in the same bronze tables as live webhook rows and is read by the same
merge_events_all_events.sql, so the shape has to match a live payload exactly --
CHANGELOG v7.8.0 records a fix for exactly this (8e1f8d1: click ids were placed in
externalClientIds, where live payloads never put them). Covers:
  * the click-id placement fixed in 8e1f8d1 -- identifier + click id inside
    mainAttribution/attributions, externalClientIds always {}.
  * a contract check that every `$.` JSON path the merge SQL reads from raw_data
    exists in map_row()'s output, so a rename on either side fails here instead of
    silently loading NULL columns for every backfilled row.
  * the per-field cleaning/derivation rules the module docstring documents
    (NA strings, fullName split, eventTimestamp, funnel_type, linkLabel casing).
  * map_file()'s BOM handling -- a BOM glued onto the first header would silently
    null out every row's clientId.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from include.custom_operators.anytrack import csv_mapper
from include.custom_operators.anytrack.csv_mapper import map_file, map_row

MERGE_SQL_PATH = (
    Path(__file__).parents[4]
    / "include"
    / "sql"
    / "anytrack"
    / "merge_events_all_events.sql"
)


def _row(**overrides) -> dict:
    """A fully-populated CSV row, keyed by the export's real column names."""
    row = {
        "clientId": "client-1",
        "clickId": "click-1",
        "eventName": "Purchase",
        "eventTime": "2026-08-19T10:15:30.000Z",
        "eventValue": "26.17",
        "userAgent": "Mozilla/5.0",
        "location": "https://example.com/checkout",
        "referrer": "https://google.com/",
        "linkId": "link-1",
        "linkURL": "https://example.com/lp",
        "linkLabel": "Main LP",
        "brandName": "SSL | Monthly plan",
        "trackingGroupId": "tg-1",
        "integrationId": "int-1",
        "transactionId": "txn-1",
        "email": "a@example.com",
        "fullName": "Ada Lovelace",
        "country": "GB",
        "zipcode": "N1 9GU",
        "attributionTime": "2026-08-18T09:00:00.000Z",
        "attributionSource": "google",
        "attributionMedium": "cpc",
        "attributionCampaign": "12345",
        "campaignName": "Brand - UK",
        "attributionTerm": "infochecker",
        "attributionContent": "ad-1",
        "attributionCreative": "creative-1",
        "gclid": "",
        "fbclid": "",
        "msclkid": "",
        "ttclid": "",
        "tblci": "",
        "ob_click_id": "",
        "atclid": "",
    }
    row.update(overrides)
    return row


def _json_paths_read_by_merge_sql() -> set[str]:
    return set(
        re.findall(
            r"'\$\.([A-Za-z0-9_.]+)'", MERGE_SQL_PATH.read_text(encoding="utf-8")
        )
    )


def _has_path(payload: dict, dotted: str) -> bool:
    node = payload
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            return False
        node = node[part]
    return True


class TestClickIdPlacement:
    """Regression for 8e1f8d1 (CHANGELOG v7.8.0)."""

    def test_click_id_goes_into_main_attribution_with_its_identifier(self):
        out = map_row(_row(gclid="gclid-abc"))

        assert out["mainAttribution"]["identifier"] == "gclid"
        assert out["mainAttribution"]["gclid"] == "gclid-abc"

    def test_external_client_ids_is_always_empty_even_when_a_click_id_is_present(self):
        out = map_row(_row(gclid="gclid-abc", fbclid="fb-xyz"))

        assert out["externalClientIds"] == {}

    def test_attributions_carries_the_same_attribution_as_main_attribution(self):
        out = map_row(_row(msclkid="ms-1"))

        assert out["attributions"] == [out["mainAttribution"]]
        assert out["attributions"][0]["msclkid"] == "ms-1"

    @pytest.mark.parametrize("column", csv_mapper._CLICK_ID_COLUMNS)
    def test_every_click_id_column_is_recognised(self, column):
        out = map_row(_row(**{column: "value-1"}))

        assert out["mainAttribution"]["identifier"] == column
        assert out["mainAttribution"][column] == "value-1"

    def test_row_without_any_click_id_has_null_identifier_and_no_click_id_key(self):
        out = map_row(_row())

        assert out["mainAttribution"]["identifier"] is None
        assert not set(csv_mapper._CLICK_ID_COLUMNS) & set(out["mainAttribution"])

    def test_first_populated_click_id_column_wins_when_several_are_set(self):
        # One click, one network is the expected shape -- if an export ever has two,
        # the result must be deterministic (column order), not dict-order dependent.
        out = map_row(_row(fbclid="fb-1", gclid="g-1"))

        assert out["mainAttribution"]["identifier"] == "gclid"
        assert "fbclid" not in out["mainAttribution"]

    def test_na_click_id_values_are_treated_as_missing(self):
        out = map_row(_row(gclid="nan", fbclid="fb-1"))

        assert out["mainAttribution"]["identifier"] == "fbclid"


class TestMergeSqlContract:
    def test_merge_sql_reads_at_least_the_core_fields(self):
        # Guards the contract test below against a regex that silently matches nothing.
        paths = _json_paths_read_by_merge_sql()
        assert {"transactionId", "eventTimestamp", "mainAttribution"} <= paths

    def test_every_json_path_read_by_the_merge_sql_exists_in_mapped_output(self):
        out = map_row(_row())

        missing = sorted(
            p for p in _json_paths_read_by_merge_sql() if not _has_path(out, p)
        )

        assert not missing, (
            f"merge_events_all_events.sql reads paths map_row() never emits: {missing}"
        )

    def test_mapped_output_is_json_serialisable(self):
        # backfill_utils.py json.dumps() every row into raw_data.
        json.dumps(map_row(_row(gclid="g-1")))


class TestFieldMapping:
    def test_link_label_csv_column_maps_to_lowercase_payload_key(self):
        out = map_row(_row(linkLabel="Main LP"))

        assert out["linklabel"] == "Main LP"
        assert "linkLabel" not in out

    def test_event_time_is_converted_to_epoch_seconds(self):
        out = map_row(_row(eventTime="2026-08-19T10:15:30.000Z"))

        assert out["eventTime"] == "2026-08-19T10:15:30.000Z"
        assert out["eventTimestamp"] == 1787134530

    def test_event_value_is_a_float_and_mirrored_into_custom_params_value(self):
        out = map_row(_row(eventValue="26.17"))

        assert out["eventValue"] == 26.17
        assert out["customParams"]["value"] == 26.17

    @pytest.mark.parametrize(
        "brand_name,expected",
        [
            ("SSL | Monthly plan", "SSL"),
            ("  Trial  | Weekly", "Trial"),
            ("No separator here", None),
            ("| Leading pipe", None),
            ("", None),
        ],
    )
    def test_funnel_type_is_derived_from_brand_name(self, brand_name, expected):
        assert (
            map_row(_row(brandName=brand_name))["customParams"]["funnel_type"]
            == expected
        )

    @pytest.mark.parametrize(
        "full_name,first,last",
        [
            ("Ada Lovelace", "Ada", "Lovelace"),
            ("Ada King Lovelace", "Ada", "King Lovelace"),
            ("Ada", "Ada", None),
            ("", None, None),
        ],
    )
    def test_full_name_is_split_into_first_and_last(self, full_name, first, last):
        out = map_row(_row(fullName=full_name))

        assert (out["traits"]["firstName"], out["traits"]["lastName"]) == (first, last)
        assert (out["customParams"]["firstName"], out["customParams"]["lastName"]) == (
            first,
            last,
        )

    def test_country_code_is_mapped_and_full_country_name_left_null(self):
        out = map_row(_row(country="GB"))

        assert out["traits"]["countryCode"] == "GB"
        assert out["traits"]["country"] is None
        assert out["customParams"]["country"] == "GB"

    def test_fields_with_no_csv_source_are_always_null(self):
        out = map_row(_row())

        assert out["assetId"] is None
        assert out["externalClickId"] is None
        assert out["currency"] is None
        assert out["customParams"]["currency"] is None
        assert out["ip"] is None

    def test_attribution_campaign_fields_are_carried_across(self):
        attribution = map_row(_row())["mainAttribution"]

        assert attribution["time"] == "2026-08-18T09:00:00.000Z"
        assert attribution["source"] == "google"
        assert attribution["medium"] == "cpc"
        assert attribution["campaign"] == "12345"
        assert attribution["campaignName"] == "Brand - UK"
        assert attribution["term"] == "infochecker"
        assert attribution["content"] == "ad-1"
        assert attribution["creative"] == "creative-1"

    @pytest.mark.parametrize("na_value", ["", "   ", "nan", "NaN"])
    def test_na_strings_become_null(self, na_value):
        out = map_row(
            _row(transactionId=na_value, eventValue=na_value, eventTime=na_value)
        )

        assert out["transactionId"] is None
        assert out["customParams"]["transaction_id"] is None
        assert out["eventValue"] is None
        assert out["eventTimestamp"] is None

    def test_values_are_stripped(self):
        assert map_row(_row(clientId="  client-1  "))["clientId"] == "client-1"

    def test_missing_columns_do_not_raise(self):
        # An older/narrower export may not carry every column.
        out = map_row({"clientId": "c1", "transactionId": "t1"})

        assert out["clientId"] == "c1"
        assert out["transactionId"] == "t1"
        assert out["eventTimestamp"] is None
        assert out["mainAttribution"]["identifier"] is None


class TestMapFile:
    def test_leading_bom_is_stripped_from_the_first_header(self, tmp_path):
        csv_path = tmp_path / "trackingEvents.csv"
        csv_path.write_bytes("﻿clientId,transactionId\nc1,t1\n".encode("utf-8"))

        rows = map_file(str(csv_path))

        assert [r["clientId"] for r in rows] == ["c1"]

    def test_maps_every_row_and_main_writes_json(self, tmp_path, monkeypatch):
        csv_path = tmp_path / "in.csv"
        out_path = tmp_path / "out.json"
        csv_path.write_text(
            "clientId,transactionId,gclid\nc1,t1,g1\nc2,t2,\n", encoding="utf-8"
        )
        monkeypatch.setattr(
            "sys.argv",
            ["csv_mapper", "--input", str(csv_path), "--output", str(out_path)],
        )

        csv_mapper.main()

        written = json.loads(out_path.read_text(encoding="utf-8"))
        assert [r["transactionId"] for r in written] == ["t1", "t2"]
        assert written[0]["mainAttribution"]["gclid"] == "g1"
        assert written[1]["mainAttribution"]["identifier"] is None
