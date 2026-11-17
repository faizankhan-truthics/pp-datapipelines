"""Tests for include/utils/billing_health_sheet.py: the Google Sheet tabs and the Excel file."""

import io

from openpyxl import load_workbook

from include.utils.billing_health_sheet import (
    PAYMENT_COLUMNS,
    RETRY_COLUMNS,
    TAB_PAYMENT,
    TAB_RETRY,
    TAB_TRIAL,
    TRIAL_COLUMNS,
    _row_ranges,
    build_tabs,
    build_workbook,
    write_tabs,
)

DAILY = {
    "product_key": "giftmusic",
    "product_name": "GiftMusic",
    "entity": "Apollo",
    "attempts": 2510,
    "approval_overall": 0.322,
    "approval_renewals_all": 0.231,
    "bank_declines_pct": 0.652,
    "attempts_per_order": 1.92,
    "top_decline_reasons": "insufficient_funds 61% | do_not_honor 19%",
}

STATUS = [
    {
        "product_key": "giftmusic",
        "product_name": "GiftMusic",
        "metric": "blocked_pct",
        "value": 0.025,
        "amount_usd": 1234.5,
        "status": "red",
        "alert_now": True,
    },
    {
        "product_key": "giftmusic",
        "product_name": "GiftMusic",
        "metric": "approval_signups",
        "value": 0.30,
        "status": "green",
        "alert_now": False,
    },
]

RETRY = {
    "product_key": "giftmusic",
    "product_name": "GiftMusic",
    "entity": "Apollo",
    "cohort_start": "2026-09-23",
    "cohort_end": "2026-09-29",
    "first_charges": 400,
    "first_charge_failures": 155,
    "recovered_pct": 0.129,
    "failed_after_all_retries_pct": 0.871,
    "avg_attempts_per_failed": 7.6,
    "baseline": None,
    "target": 0.2,
    "status": "no_baseline",
    "alert_now": False,
}

COHORT = {
    "product_key": "chat_box",
    "product_name": "Chat-Box",
    "entity": "Apollo",
    "cohort_week": "2026-09-21",
    "is_final": True,
    "trial_days": 5,
    "trial_price": 0.65,
    "first_full_price": 29.99,
    "trials_started": 12668,
    "cancelled_pct": 0.073,
    "cancelled_status": "green",
    "reached_first_charge_pct": 0.924,
    "converted_first_pct": 0.378,
    "recovered_pct": 0.177,
    "total_converted_pct": 0.555,
    "converted_status": "amber",
    "failed_pct": 0.3,
    "other_pct": 0.01,
    "pending_pct": 0.0,
    "top_decline_reason": "insufficient_funds",
}

RESULTS = {"status": STATUS, "daily": [DAILY], "retry": [RETRY], "trials": [COHORT]}


def _by_header(columns, row) -> dict:
    return dict(zip([h for h, _ in columns], row, strict=True))


def test_one_tab_per_section_with_report_date_first():
    tabs = build_tabs(RESULTS, "2026-11-03")

    assert list(tabs) == [TAB_PAYMENT, TAB_RETRY, TAB_TRIAL]
    for columns, rows in tabs.values():
        assert columns[0][0] == "Report date"
        assert len(rows) == 1
        assert len(rows[0]) == len(columns)
        assert rows[0][0] == "2026-11-03"


def test_payment_row_keeps_raw_numbers_and_statuses():
    (row,) = build_tabs(RESULTS, "2026-11-03")[TAB_PAYMENT][1]
    cells = _by_header(PAYMENT_COLUMNS, row)

    assert cells["Status"] == "red"  # worst check of the product
    assert cells["Attempts"] == 2510
    assert cells["Blocked"] == 0.025
    assert cells["Blocked status"] == "red"
    assert cells["Blocked $"] == 1234.5
    assert cells["Approval sign-ups"] == 0.30
    assert cells["Duplicates"] is None  # no check today: blank cell


def test_retry_and_trial_rows():
    tabs = build_tabs(RESULTS, "2026-11-03")
    (retry,) = tabs[TAB_RETRY][1]
    (trial,) = tabs[TAB_TRIAL][1]

    assert _by_header(RETRY_COLUMNS, retry)["Failed after all retries"] == 0.871
    cells = _by_header(TRIAL_COLUMNS, trial)
    assert cells["Status"] == "amber"
    assert cells["Cohort week"] == "2026-09-21"
    assert cells["Cohort"] == "final"
    assert cells["Total converted status"] == "amber"


def test_trial_tab_has_every_cohort_with_pending_ones_marked():
    pending = {
        **COHORT,
        "cohort_week": "2026-10-26",
        "is_final": False,
        "final_on": "2026-12-05",
        "converted_status": None,
        "cancelled_status": None,
    }
    older = {**COHORT, "cohort_week": "2026-09-14"}
    no_trials = {**COHORT, "cohort_week": None, "trials_started": None}
    tabs = build_tabs(
        {**RESULTS, "trials": [pending, COHORT, older, no_trials]}, "2026-11-03"
    )

    rows = [_by_header(TRIAL_COLUMNS, r) for r in tabs[TAB_TRIAL][1]]
    assert [r["Cohort week"] for r in rows] == [
        "2026-09-14",
        "2026-09-21",
        "2026-10-26",
    ]
    last = rows[-1]
    assert last["Cohort"] == "pending"
    assert last["Final on"] == "2026-12-05"
    assert last["Status"] == last["Cancelled status"] == "pending"
    assert last["Total converted status"] == "pending"


def test_products_without_data_are_left_out():
    tabs = build_tabs(
        {
            "status": [],
            "daily": [{**DAILY, "attempts": 0}],
            "retry": [{**RETRY, "first_charge_failures": 0}],
            "trials": [{**COHORT, "cohort_week": None, "trials_started": None}],
        },
        "2026-11-03",
    )

    assert all(rows == [] for _, rows in tabs.values())


def test_row_ranges_are_contiguous_and_last_first():
    assert _row_ranges([3, 1, 2, 7, 8, 10]) == [(10, 11), (7, 9), (1, 4)]


class FakeSheets:
    """Records the Sheets v4 calls write_tabs makes; column A per tab is the stored data."""

    def __init__(self, tabs: dict[str, list[list]], column_counts: dict | None = None):
        self.tabs = tabs
        self.column_counts = column_counts or {}
        self.calls: list[tuple[str, dict]] = []

    def spreadsheets(self):
        return self

    def values(self):
        return self

    def _call(self, name, kwargs, result=None):
        self.calls.append((name, kwargs))
        return type("Req", (), {"execute": lambda _self: result})()

    def get(self, **kwargs):
        if "range" in kwargs:
            title = kwargs["range"].split("'")[1]
            return self._call("values.get", kwargs, {"values": self.tabs[title]})
        sheets = [
            {
                "properties": {
                    "title": t,
                    "sheetId": i,
                    "gridProperties": {"columnCount": self.column_counts.get(t, 26)},
                }
            }
            for i, t in enumerate(self.tabs)
        ]
        return self._call("get", kwargs, {"sheets": sheets})

    def batchUpdate(self, **kwargs):
        requests = kwargs["body"]["requests"]
        if "addSheet" in requests[0]:
            title = requests[0]["addSheet"]["properties"]["title"]
            self.tabs[title] = []
            reply = {"replies": [{"addSheet": {"properties": {"sheetId": 99}}}]}
            return self._call("addSheet", kwargs, reply)
        return self._call("batchUpdate", kwargs, {})

    def update(self, **kwargs):
        return self._call("values.update", kwargs)

    def append(self, **kwargs):
        return self._call("values.append", kwargs)


def test_write_tabs_creates_missing_tabs_and_replaces_the_days_rows():
    service = FakeSheets(
        {
            TAB_PAYMENT: [
                ["Report date"],
                ["2026-11-02"],
                ["2026-11-03"],
                ["2026-11-03"],
                ["2026-11-04"],
            ]
        }
    )

    write_tabs(service, "SHEET", build_tabs(RESULTS, "2026-11-03"), "2026-11-03")

    added = [
        k["body"]["requests"][0]["addSheet"]["properties"]["title"]
        for name, k in service.calls
        if name == "addSheet"
    ]
    assert added == [TAB_RETRY, TAB_TRIAL]

    deletes = [
        r["deleteDimension"]["range"]
        for name, k in service.calls
        if name == "batchUpdate"
        for r in k["body"]["requests"]
        if "deleteDimension" in r
    ]
    assert deletes == [
        {"sheetId": 0, "dimension": "ROWS", "startIndex": 2, "endIndex": 4}
    ]

    appends = [k for name, k in service.calls if name == "values.append"]
    assert [a["range"] for a in appends] == [
        f"'{TAB_PAYMENT}'!A1",
        f"'{TAB_RETRY}'!A1",
        f"'{TAB_TRIAL}'!A1",
    ]
    payment_values = appends[0]["body"]["values"][0]
    assert payment_values[0] == "2026-11-03"
    assert "" in payment_values and None not in payment_values  # blanks, not nulls


def test_new_tab_gets_formats_and_status_colours():
    service = FakeSheets({})

    write_tabs(service, "SHEET", build_tabs(RESULTS, "2026-11-03"), "2026-11-03")

    (format_call, *_) = [k for name, k in service.calls if name == "batchUpdate"]
    requests = format_call["body"]["requests"]
    assert any("addConditionalFormatRule" in r for r in requests)
    patterns = {
        r["repeatCell"]["cell"]["userEnteredFormat"]["numberFormat"]["pattern"]
        for r in requests
        if "numberFormat"
        in r.get("repeatCell", {}).get("cell", {}).get("userEnteredFormat", {})
    }
    assert {"0.0%", "#,##0", "$#,##0.00"} <= patterns


def test_existing_tab_gets_new_columns_and_current_formats():
    # a Trial-to-paid tab made before the currency columns: 22 columns, prices formatted as $
    service = FakeSheets(
        {
            TAB_PAYMENT: [["Report date"]],
            TAB_RETRY: [["Report date"]],
            TAB_TRIAL: [["Report date"]],
        },
        column_counts={TAB_TRIAL: len(TRIAL_COLUMNS) - 2},
    )

    write_tabs(service, "SHEET", build_tabs(RESULTS, "2026-11-03"), "2026-11-03")

    trial_sheet_id = list(service.tabs).index(TAB_TRIAL)
    requests = [
        r
        for name, k in service.calls
        if name == "batchUpdate"
        for r in k["body"]["requests"]
        if r.get("appendDimension", r.get("repeatCell", {}).get("range", {})).get(
            "sheetId"
        )
        == trial_sheet_id
    ]
    assert requests[0] == {
        "appendDimension": {
            "sheetId": trial_sheet_id,
            "dimension": "COLUMNS",
            "length": 2,
        }
    }
    headers = [h for h, _ in TRIAL_COLUMNS]
    price_formats = {
        r["repeatCell"]["range"]["startColumnIndex"]: r["repeatCell"]["cell"][
            "userEnteredFormat"
        ]["numberFormat"]["pattern"]
        for r in requests[1:]
    }
    assert price_formats[headers.index("Trial price")] == "0.00"  # no $: any currency
    assert headers[-2:] == ["Trial price currency", "Full price currency"]
    assert not any("addConditionalFormatRule" in r for r in requests)  # not duplicated


def test_workbook_has_the_three_tabs_with_formats():
    content = build_workbook(build_tabs(RESULTS, "2026-11-03"))
    workbook = load_workbook(io.BytesIO(content))

    assert workbook.sheetnames == [TAB_PAYMENT, TAB_RETRY, TAB_TRIAL]
    sheet = workbook[TAB_PAYMENT]
    headers = [c.value for c in sheet[1]]
    assert headers == [h for h, _ in PAYMENT_COLUMNS]
    blocked = sheet.cell(2, headers.index("Blocked") + 1)
    assert blocked.value == 0.025 and blocked.number_format == "0.0%"
    status = sheet.cell(2, headers.index("Blocked status") + 1)
    assert status.fill.start_color.rgb.endswith("F4CCCC")
