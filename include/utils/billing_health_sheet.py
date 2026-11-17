"""Google Sheet and Excel file for the billing health alerts: one tab per section.

The same rows as the Slack tables (include/utils/billing_health_alerts.py and
billing_health_retry_trial_alerts.py), with raw numbers instead of formatted text so the sheet
can be filtered, sorted and charted. Trial-to-paid has every cohort (one row per product x trial
x cohort week, as the ticket asks), pending ones marked "pending" until trial + 30 days has
passed; Slack shows only each trial's latest final cohort.

* build_tabs: {tab title: (columns, rows)} for Sections A, B and C. The first column of every row
  is the report date, so a tab holds the history of every day the DAG ran.
* write_tabs: writes them to the spreadsheet. Missing tabs are created (header, number formats,
  status colours); then the report date's rows are deleted and written again, so a re-run of a
  day replaces it instead of adding it twice.
* build_workbook: the same tabs (only the report date's rows) as an .xlsx file, posted to Slack.
"""

import io

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill

from include.utils.billing_health_alerts import (
    ENTITY_SHORT,
    _by_metric,
    _products_with_data,
    _worst_status,
)
from include.utils.billing_health_retry_trial_alerts import (
    _cohort_status,
    _retry_with_data,
    _sort_key,
)

TAB_PAYMENT = "Payment health"
TAB_RETRY = "Retry performance"
TAB_TRIAL = "Trial-to-paid"

# (header, format). Formats: text, int, pct, usd, num, status.
PAYMENT_COLUMNS = [
    ("Report date", "text"),
    ("Product", "text"),
    ("Entity", "text"),
    ("Status", "status"),
    ("Attempts", "int"),
    ("Approval overall", "pct"),
    ("Approval sign-ups", "pct"),
    ("Approval sign-ups status", "status"),
    ("Approval renewals (1st)", "pct"),
    ("Approval renewals status", "status"),
    ("Renewals all attempts", "pct"),
    ("Bank declines", "pct"),
    ("Blocked", "pct"),
    ("Blocked status", "status"),
    ("Blocked $", "usd"),
    ("Duplicates", "pct"),
    ("Duplicates status", "status"),
    ("Attempts / order", "num"),
    ("Retries after do-not-retry", "int"),
    ("Retries after do-not-retry status", "status"),
    ("Top 3 decline reasons (% of declines)", "text"),
]

RETRY_COLUMNS = [
    ("Report date", "text"),
    ("Product", "text"),
    ("Entity", "text"),
    ("Status", "status"),
    ("Cohort start", "text"),
    ("Cohort end", "text"),
    ("First charges", "int"),
    ("First-charge failures", "int"),
    ("Recovered by retries", "pct"),
    ("Failed after all retries", "pct"),
    ("Avg attempts per failed charge", "num"),
    ("Baseline (4-week avg)", "pct"),
    ("Target", "pct"),
]

TRIAL_COLUMNS = [
    ("Report date", "text"),
    ("Product", "text"),
    ("Entity", "text"),
    ("Status", "status"),
    ("Trial days", "int"),
    # in the currency of the last two columns (added at the end, so older rows keep their place)
    ("Trial price", "num"),
    ("Full price", "num"),
    ("Cohort week", "text"),
    ("Cohort", "text"),
    ("Final on", "text"),
    ("Trials started", "int"),
    ("Cancelled before trial end", "pct"),
    ("Cancelled status", "status"),
    ("Reached first charge", "pct"),
    ("Converted on 1st attempt", "pct"),
    ("Recovered by retries", "pct"),
    ("Total converted", "pct"),
    ("Total converted status", "status"),
    ("Failed after all retries", "pct"),
    ("Other (refund / dispute)", "pct"),
    ("Still pending", "pct"),
    ("Top decline reason at first charge", "text"),
    ("Trial price currency", "text"),
    ("Full price currency", "text"),
]

NUMBER_FORMATS = {
    "int": "#,##0",
    "pct": "0.0%",
    "usd": "$#,##0.00",
    "num": "0.00",
}
# Cell colours of the status columns, as in the Slack tables.
STATUS_COLOURS = {"red": "F4CCCC", "amber": "FFF2CC", "green": "D9EAD3"}


# ---------------------------------------------------------------------------------------------
# Rows
# ---------------------------------------------------------------------------------------------


def _metric(by_metric: dict, key: str, metric: str) -> tuple:
    row = by_metric.get((key, metric)) or {}
    return row.get("value"), row.get("status")


def payment_rows(
    status_rows: list[dict], daily_rows: list[dict], report_date: str
) -> list[list]:
    by_metric = _by_metric(status_rows)
    rows = []
    for d in _products_with_data(daily_rows):
        key = d["product_key"]
        blocked = by_metric.get((key, "blocked_pct")) or {}
        rows.append(
            [
                report_date,
                d["product_name"],
                ENTITY_SHORT.get(d["entity"], d["entity"]),
                _worst_status(key, by_metric),
                d["attempts"],
                d.get("approval_overall"),
                *_metric(by_metric, key, "approval_signups"),
                *_metric(by_metric, key, "approval_renewals_first_attempt"),
                d.get("approval_renewals_all"),
                d.get("bank_declines_pct"),
                *_metric(by_metric, key, "blocked_pct"),
                blocked.get("amount_usd"),
                *_metric(by_metric, key, "duplicate_pct"),
                d.get("attempts_per_order"),
                *_metric(by_metric, key, "retries_after_do_not_retry"),
                d.get("top_decline_reasons"),
            ]
        )
    return rows


def retry_rows(rows: list[dict], report_date: str) -> list[list]:
    return [
        [
            report_date,
            r["product_name"],
            ENTITY_SHORT.get(r["entity"], r["entity"]),
            r["status"],
            r.get("cohort_start"),
            r.get("cohort_end"),
            r["first_charges"],
            r["first_charge_failures"],
            r.get("recovered_pct"),
            r.get("failed_after_all_retries_pct"),
            r.get("avg_attempts_per_failed"),
            r.get("baseline"),
            r.get("target"),
        ]
        for r in _retry_with_data(rows)
    ]


def _trial_status(cohort: dict, status: str | None) -> str | None:
    return status if cohort.get("is_final") else "pending"


def trial_rows(rows: list[dict], report_date: str) -> list[list]:
    """Every cohort with trials, final and pending, by product, trial length and week."""
    cohorts = sorted(
        (c for c in rows if c.get("cohort_week") and c.get("trials_started")),
        key=lambda c: (_sort_key(c), str(c["cohort_week"])),
    )
    return [
        [
            report_date,
            c["product_name"],
            ENTITY_SHORT.get(c["entity"], c["entity"]),
            _trial_status(c, _cohort_status(c)),
            c["trial_days"],
            c.get("trial_price"),
            c.get("first_full_price"),
            c["cohort_week"],
            "final" if c.get("is_final") else "pending",
            c.get("final_on"),
            c["trials_started"],
            c.get("cancelled_pct"),
            _trial_status(c, c.get("cancelled_status")),
            c.get("reached_first_charge_pct"),
            c.get("converted_first_pct"),
            c.get("recovered_pct"),
            c.get("total_converted_pct"),
            _trial_status(c, c.get("converted_status")),
            c.get("failed_pct"),
            c.get("other_pct"),
            c.get("pending_pct"),
            c.get("top_decline_reason"),
            c.get("trial_price_currency"),
            c.get("first_full_price_currency"),
        ]
        for c in cohorts
    ]


def build_tabs(results: dict, report_date: str) -> dict[str, tuple[list, list[list]]]:
    """{tab title: (columns, rows)} from the evaluate task's results."""
    return {
        TAB_PAYMENT: (
            PAYMENT_COLUMNS,
            payment_rows(results["status"], results["daily"], report_date),
        ),
        TAB_RETRY: (RETRY_COLUMNS, retry_rows(results["retry"], report_date)),
        TAB_TRIAL: (TRIAL_COLUMNS, trial_rows(results["trials"], report_date)),
    }


def _cell_value(value):
    """Sheets cell value: blank for None."""
    return "" if value is None else value


# ---------------------------------------------------------------------------------------------
# Google Sheet
# ---------------------------------------------------------------------------------------------


def _rgb(hex_colour: str) -> dict:
    return {
        name: int(hex_colour[i : i + 2], 16) / 255
        for name, i in (("red", 0), ("green", 2), ("blue", 4))
    }


def _column_range(sheet_id: int, i: int) -> dict:
    return {
        "sheetId": sheet_id,
        "startRowIndex": 1,
        "startColumnIndex": i,
        "endColumnIndex": i + 1,
    }


def _number_format_requests(sheet_id: int, columns: list[tuple]) -> list[dict]:
    """Number format per column. Safe to repeat, so it is re-applied on every write and a
    changed format reaches an existing tab too."""
    return [
        {
            "repeatCell": {
                "range": _column_range(sheet_id, i),
                "cell": {
                    "userEnteredFormat": {
                        "numberFormat": {
                            "type": "NUMBER",
                            "pattern": NUMBER_FORMATS[fmt],
                        }
                    }
                },
                "fields": "userEnteredFormat.numberFormat",
            }
        }
        for i, (_, fmt) in enumerate(columns)
        if fmt in NUMBER_FORMATS
    ]


def _tab_format_requests(sheet_id: int, columns: list[tuple]) -> list[dict]:
    """Bold header, number formats and status colours for a new tab."""
    requests = [
        {
            "repeatCell": {
                "range": {"sheetId": sheet_id, "startRowIndex": 0, "endRowIndex": 1},
                "cell": {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold",
            }
        }
    ]
    requests += _number_format_requests(sheet_id, columns)
    for i, (_, fmt) in enumerate(columns):
        column = _column_range(sheet_id, i)
        if fmt == "status":
            requests.extend(
                {
                    "addConditionalFormatRule": {
                        "rule": {
                            "ranges": [column],
                            "booleanRule": {
                                "condition": {
                                    "type": "TEXT_EQ",
                                    "values": [{"userEnteredValue": status}],
                                },
                                "format": {"backgroundColor": _rgb(colour)},
                            },
                        },
                        "index": 0,
                    }
                }
                for status, colour in STATUS_COLOURS.items()
            )
    return requests


def _add_tab(service, spreadsheet_id: str, title: str, columns: list[tuple]) -> int:
    reply = (
        service.spreadsheets()
        .batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": [
                    {
                        "addSheet": {
                            "properties": {
                                "title": title,
                                "gridProperties": {
                                    "frozenRowCount": 1,
                                    "columnCount": len(columns),
                                },
                            }
                        }
                    }
                ]
            },
        )
        .execute()
    )
    sheet_id = reply["replies"][0]["addSheet"]["properties"]["sheetId"]
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": _tab_format_requests(sheet_id, columns)},
    ).execute()
    return sheet_id


def _row_ranges(indexes: list[int]) -> list[tuple[int, int]]:
    """Contiguous (start, end) ranges of row indexes, last first so deleting one does not shift
    the others."""
    ranges: list[tuple[int, int]] = []
    for i in sorted(indexes):
        if ranges and ranges[-1][1] == i:
            ranges[-1] = (ranges[-1][0], i + 1)
        else:
            ranges.append((i, i + 1))
    return ranges[::-1]


def write_tabs(
    service, spreadsheet_id: str, tabs: dict[str, tuple], report_date: str
) -> None:
    """Replace the report date's rows in each tab (creating the tab if it does not exist).

    `service` is a Sheets v4 client (GSheetsHook(...).get_conn()).
    """
    sheets = service.spreadsheets()
    meta = sheets.get(
        spreadsheetId=spreadsheet_id,
        fields="sheets.properties(sheetId,title,gridProperties.columnCount)",
    ).execute()
    existing = {s["properties"]["title"]: s["properties"] for s in meta["sheets"]}
    for title, (columns, rows) in tabs.items():
        if title not in existing:
            sheet_id = _add_tab(service, spreadsheet_id, title, columns)
        else:
            # a tab made by an older version: add the columns added since, refresh formats
            sheet_id = existing[title]["sheetId"]
            missing = len(columns) - existing[title].get("gridProperties", {}).get(
                "columnCount", len(columns)
            )
            requests = _number_format_requests(sheet_id, columns)
            if missing > 0:
                requests.insert(
                    0,
                    {
                        "appendDimension": {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "length": missing,
                        }
                    },
                )
            if requests:
                sheets.batchUpdate(
                    spreadsheetId=spreadsheet_id, body={"requests": requests}
                ).execute()

        dates = (
            sheets.values()
            .get(spreadsheetId=spreadsheet_id, range=f"'{title}'!A:A")
            .execute()
            .get("values", [])
        )
        stale = [i for i, v in enumerate(dates) if i > 0 and v and v[0] == report_date]
        if stale:
            sheets.batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={
                    "requests": [
                        {
                            "deleteDimension": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "dimension": "ROWS",
                                    "startIndex": start,
                                    "endIndex": end,
                                }
                            }
                        }
                        for start, end in _row_ranges(stale)
                    ]
                },
            ).execute()

        # Header every run, so a column added to the code shows up in an existing tab too.
        sheets.values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{title}'!A1",
            valueInputOption="RAW",
            body={"values": [[header for header, _ in columns]]},
        ).execute()
        if rows:
            sheets.values().append(
                spreadsheetId=spreadsheet_id,
                range=f"'{title}'!A1",
                valueInputOption="RAW",
                insertDataOption="INSERT_ROWS",
                body={"values": [[_cell_value(v) for v in row] for row in rows]},
            ).execute()
        print(f"{title}: replaced {len(stale)} rows of {report_date} with {len(rows)}")


def sheet_url(spreadsheet_id: str) -> str:
    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit"


# ---------------------------------------------------------------------------------------------
# Excel file
# ---------------------------------------------------------------------------------------------


def build_workbook(tabs: dict[str, tuple]) -> bytes:
    """The tabs as an .xlsx file: same columns, number formats and status colours."""
    workbook = Workbook()
    workbook.remove(workbook.active)
    fills = {
        status: PatternFill("solid", start_color=colour)
        for status, colour in STATUS_COLOURS.items()
    }
    for title, (columns, rows) in tabs.items():
        sheet = workbook.create_sheet(title)
        sheet.append([header for header, _ in columns])
        for cell in sheet[1]:
            cell.font = Font(bold=True)
        sheet.freeze_panes = "A2"
        for row in rows:
            sheet.append(row)
            for cell, (_, fmt) in zip(sheet[sheet.max_row], columns, strict=True):
                if fmt in NUMBER_FORMATS:
                    cell.number_format = NUMBER_FORMATS[fmt]
                elif fmt == "status" and cell.value in fills:
                    cell.fill = fills[cell.value]
        for i, (header, _) in enumerate(columns):
            sheet.column_dimensions[sheet.cell(1, i + 1).column_letter].width = max(
                12, min(len(header) + 2, 40)
            )
    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()
