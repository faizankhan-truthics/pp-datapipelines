"""Slack messages for the billing health alerts, Sections B (retry performance) and C (trial-to-paid).

Pure functions over the rows of include/sql/billing_health/section_b_retry.sql (one row per
product: the 7-day cohort of first-charge failures whose 30-day retry window just closed) and
section_c_trials.sql (one row per product x trial length x weekly cohort), in the same style as
Section A (include/utils/billing_health_alerts.py):

* build_retry_blocks / build_trial_blocks: ONE Block Kit message per section, a short description
  (status counts, today's reds, data notes), a one-line legend and a table block.
* build_retry_messages / build_trial_messages: the same as plain text (monospace code blocks)
  under Slack's 4,000-character limit, the fallback when Slack rejects the table block.

Section C shows each product and trial length's latest FINAL cohort (trial + 30 days passed),
since only final cohorts are coloured. A cohort's red is called out on the day it becomes final.
Summaries name products with a red; each gets a reply in the message's thread tagging
its owner (include/utils/billing_health_red_alerts.py).
"""

from include.utils.billing_health_alerts import (
    ENTITY_ORDER,
    ENTITY_SHORT,
    SLACK_TEXT_LIMIT,
    STATUS_EMOJI_NAME,
    STATUS_RANK,
    _chunks,
    _pct,
    _raw,
    _rich,
    legend_with_env,
    new_red_line,
    section_blocks,
    status_counts,
)

STATUS_EMOJI_NAME = {**STATUS_EMOJI_NAME, "no_source": "white_circle"}
STATUS_MARK = {
    "red": "!!",
    "amber": "!",
    "low_volume": "~",
    "no_baseline": "?",
    "no_source": "x",
}

RETRY_COLUMNS = [
    ("Product", "left"),
    ("Entity", "left"),
    ("Status", "center"),
    ("First charges", "right"),
    ("First-charge failures", "right"),
    ("Recovered by retries", "right"),
    ("Failed after all retries", "right"),
    ("Avg attempts per failed charge", "right"),
    ("Baseline (4-week avg)", "right"),
    ("Target", "right"),
]

RETRY_LEGEND_MRKDWN = (
    "Charges first attempted {start} – {end} that failed; their 30-day retry window has "
    "closed · Failed after all retries vs its 4-week avg: amber +5, red +10 pts · Target 20% · "
    "⚪ under 50 failures or no baseline"
)

RETRY_TABLE_COLUMNS = [
    ("Product", 19),
    ("1st chg", 8),
    ("Failures", 8),
    ("Recov", 7),
    ("Failed", 8),
    ("Att/fail", 8),
    ("Baseline", 8),
    ("Target", 6),
]

RETRY_LEGEND = (
    "!! red  ! amber  ~ under 50 failures  ? no baseline  - n/a\n"
    "Charges whose 1st attempt failed 30-36 days ago. Recov + Failed = 100%.\n"
    "Failed vs the 4-week avg (Baseline): amber +5, red +10 pts. Target 20%."
)

TRIAL_COLUMNS = [
    ("Product", "left"),
    ("Entity", "left"),
    ("Status", "center"),
    ("Trial (length · price → full price)", "left"),
    ("Cohort week", "left"),
    ("Trials started", "right"),
    ("Cancelled before trial end", "right"),
    ("Reached first charge", "right"),
    ("Converted on 1st attempt", "right"),
    ("Recovered by retries", "right"),
    ("Total converted", "right"),
    ("Failed after all retries", "right"),
    ("Other (refund / dispute)", "right"),
    ("Still pending", "right"),
    ("Top decline reason at first charge", "left"),
]

TRIAL_LEGEND_MRKDWN = (
    "Latest final cohort per trial (trial + 30 days passed) · % of trials started · "
    "Total converted vs the last 4 cohorts: amber −5, red −10 pts · Cancelled before trial "
    "end: amber +5, red +10 pts · ⚪ under 50 trials, no baseline or no source"
)

TRIAL_TABLE_COLUMNS = [
    ("Product", 19),
    ("Trial", 5),
    ("Cohort", 10),
    ("Trials", 7),
    ("Canc", 7),
    ("Reach", 6),
    ("Conv1", 6),
    ("Recov", 6),
    ("Total", 7),
    ("Failed", 6),
    ("Other", 6),
    ("Pend", 5),
]

TRIAL_LEGEND = (
    "!! red  ! amber  ~ under 50 trials  ? no baseline  x no source  - n/a\n"
    "Latest final cohort per trial. % of trials: Canc + Total + Failed + Other + Pend = 100%.\n"
    "Total vs the last 4 cohorts: amber -5, red -10 pts. Canc: amber +5, red +10 pts."
)

# ---------------------------------------------------------------------------------------------
# Shared
# ---------------------------------------------------------------------------------------------


def _sort_key(row: dict) -> tuple:
    entity = row["entity"]
    return (
        ENTITY_ORDER.index(entity) if entity in ENTITY_ORDER else len(ENTITY_ORDER),
        row["product_name"],
        row.get("trial_days") or 0,
    )


def _worst(statuses: list[str]) -> str | None:
    ranked = [s for s in statuses if s]
    return max(ranked, key=lambda s: STATUS_RANK.get(s, 0)) if ranked else None


def _status_cell(text: str | None, status: str | None) -> dict:
    """A coloured metric cell: red / amber get their colour; grey states a white circle."""
    if text is None:
        return _raw("–")
    if status in ("red", "amber"):
        return _rich(text, STATUS_EMOJI_NAME[status], bold=status == "red")
    if status in ("low_volume", "no_baseline", "no_source"):
        return _rich(text, "white_circle")
    return _raw(text)


def _status_text(text: str | None, status: str | None) -> str:
    return "-" if text is None else text + STATUS_MARK.get(status, "")


def _dash(text: str) -> str:
    return text.replace("-", "–") if text == "-" else text


def _table_block(columns: list[tuple], rows: list[list[dict]]) -> dict:
    return {
        "type": "table",
        "column_settings": [
            {"align": align, "is_wrapped": header.startswith("Top")}
            for header, align in columns
        ],
        "rows": [[_rich(h, bold=True) for h, _ in columns], *rows],
    }


def _text_table(columns: list[tuple], rows: list[tuple[str, list[str]]]) -> str:
    """Monospace table, grouped by entity: rows are (entity, cells)."""

    def line(cells: list[str]) -> str:
        (first, (_, width)), *rest = zip(cells, columns, strict=True)
        return first[:width].ljust(width) + "".join(
            f" {c.rjust(w)}" for c, (_, w) in rest
        )

    header = line([h for h, _ in columns])
    lines, entity = [header, "-" * len(header)], None
    for row_entity, cells in rows:
        if row_entity != entity:
            entity = row_entity
            lines.append(entity.upper())
        lines.append(line(cells))
    return "\n".join(lines)


def _messages(
    title: str, env: str, lines: list[str], tables: list[tuple[str, str]]
) -> list[str]:
    title = f"*{title}*" + ("" if env == "Production" else f" · {env}")
    messages = _chunks([title, *lines], SLACK_TEXT_LIMIT)
    for name, table in tables:
        for part in _chunks(table.split("\n"), SLACK_TEXT_LIMIT - 40):
            messages.append(f"{name}\n```{part}```")
            name = f"{name} (cont.)"
    return messages


# ---------------------------------------------------------------------------------------------
# Section B: retry performance
# ---------------------------------------------------------------------------------------------


def _retry_with_data(rows: list[dict]) -> list[dict]:
    return sorted((r for r in rows if r.get("first_charge_failures")), key=_sort_key)


def _retry_lines(rows: list[dict]) -> list[str]:
    """Status counts, reds, data notes."""
    with_data = _retry_with_data(rows)
    lines = [status_counts({r["product_key"]: r["status"] for r in with_data})]
    new_red = new_red_line([r["product_name"] for r in with_data if r.get("alert_now")])
    if new_red:
        lines.append(new_red)
    unlinked = sorted(r["product_name"] for r in rows if r.get("unlinked_invoices"))
    if unlinked:
        lines.append(
            "_Not measurable (Stripe invoices not linked to charges):_ "
            + ", ".join(unlinked)
        )
    no_data = sorted(
        r["product_name"]
        for r in rows
        if not r.get("first_charge_failures") and not r.get("unlinked_invoices")
    )
    if no_data:
        lines.append("_No data:_ " + ", ".join(no_data))
    return lines


def _attempts(row: dict) -> str:
    value = row.get("avg_attempts_per_failed")
    return "-" if value is None else f"{value:.1f}"


def _retry_legend(rows: list[dict]) -> str:
    start, end = (
        (rows[0]["cohort_start"], rows[0]["cohort_end"]) if rows else ("?", "?")
    )
    return RETRY_LEGEND_MRKDWN.format(start=start, end=end)


def build_retry_blocks(
    rows: list[dict], report_date: str, env: str
) -> tuple[str, list[dict]]:
    """(notification text, blocks): ONE message, description then the Section B table."""
    description = _retry_lines(rows)
    table_rows = []
    for r in _retry_with_data(rows):
        status = r["status"]
        table_rows.append(
            [
                _rich(r["product_name"], bold=True),
                _raw(ENTITY_SHORT.get(r["entity"], r["entity"])),
                _rich("", STATUS_EMOJI_NAME[status])
                if status in STATUS_EMOJI_NAME
                else _raw("–"),
                _raw(f"{int(r['first_charges']):,}"),
                _raw(f"{int(r['first_charge_failures']):,}"),
                _raw(_dash(_pct(r.get("recovered_pct")))),
                _status_cell(_pct(r.get("failed_after_all_retries_pct")), status),
                _raw(_dash(_attempts(r))),
                _raw(_dash(_pct(r.get("baseline")))),
                _raw(_pct(r.get("target"), 0)),
            ]
        )
    blocks = section_blocks(
        f"Retry performance · {report_date}",
        description,
        legend_with_env(_retry_legend(rows), env),
        _table_block(RETRY_COLUMNS, table_rows),
    )
    return f"Retry performance {report_date}: {description[0]}", blocks


def build_retry_table(rows: list[dict]) -> str:
    return _text_table(
        RETRY_TABLE_COLUMNS,
        [
            (
                r["entity"],
                [
                    r["product_name"],
                    f"{int(r['first_charges']):,}",
                    f"{int(r['first_charge_failures']):,}",
                    _pct(r.get("recovered_pct")),
                    _status_text(
                        _pct(r.get("failed_after_all_retries_pct")), r["status"]
                    ),
                    _attempts(r),
                    _pct(r.get("baseline")),
                    _pct(r.get("target"), 0),
                ],
            )
            for r in _retry_with_data(rows)
        ],
    )


def build_retry_messages(rows: list[dict], report_date: str, env: str) -> list[str]:
    return _messages(
        f"Retry performance · {report_date}",
        env,
        _retry_lines(rows),
        [("*All products*", f"{build_retry_table(rows)}\n\n{RETRY_LEGEND}")],
    )


# ---------------------------------------------------------------------------------------------
# Section C: trial-to-paid
# ---------------------------------------------------------------------------------------------


def latest_final_cohorts(rows: list[dict]) -> list[dict]:
    """Each product x trial length's latest final cohort, sorted by entity and product."""
    latest: dict[tuple, dict] = {}
    for r in rows:
        if not r.get("is_final") or not r.get("trials_started"):
            continue
        key = (r["product_key"], r["trial_days"])
        if key not in latest or r["cohort_week"] > latest[key]["cohort_week"]:
            latest[key] = r
    return sorted(latest.values(), key=_sort_key)


def _cohort_status(row: dict) -> str | None:
    return _worst([row.get("converted_status"), row.get("cancelled_status")])


def _price(value, currency: str | None) -> str:
    """$ for USD; any other currency by its code (prices are in the invoice's currency)."""
    if value is None:
        return "?"
    amount = f"{value:,.0f}" if value >= 100 else f"{value:,.2f}"
    return f"${amount}" if currency in (None, "USD") else f"{amount} {currency}"


def _trial_label(row: dict) -> str:
    return (
        f"{row['trial_days']}-day · "
        f"{_price(row.get('trial_price'), row.get('trial_price_currency'))} → "
        f"{_price(row.get('first_full_price'), row.get('first_full_price_currency'))}"
    )


def _trial_lines(rows: list[dict]) -> list[str]:
    """Status counts, reds, data notes."""
    cohorts = latest_final_cohorts(rows)
    worst: dict[str, str | None] = {}
    for c in cohorts:
        worst[c["product_key"]] = _worst(
            [worst.get(c["product_key"]), _cohort_status(c)]
        )
    lines = [status_counts(worst)]
    new_red = new_red_line(
        [
            c["product_name"]
            for c in cohorts
            if c.get("converted_alert_now") or c.get("cancelled_alert_now")
        ]
    )
    if new_red:
        lines.append(new_red)
    # Products whose cohorts are all still pending (e.g. a new launch) have no table row yet.
    shown = {c["product_key"] for c in cohorts}
    first_final: dict[str, tuple[str, str]] = {}
    for r in rows:
        if r.get("cohort_week") and r["product_key"] not in shown:
            current = first_final.get(r["product_key"])
            if current is None or str(r["final_on"]) < current[1]:
                first_final[r["product_key"]] = (r["product_name"], str(r["final_on"]))
    if first_final:
        lines.append(
            "_No final cohort yet:_ "
            + ", ".join(
                f"{name} (first on {final_on})"
                for name, final_on in sorted(first_final.values())
            )
        )
    no_trials = sorted(r["product_name"] for r in rows if not r.get("cohort_week"))
    if no_trials:
        lines.append("_No trials:_ " + ", ".join(no_trials))
    return lines


def build_trial_blocks(
    rows: list[dict], report_date: str, env: str
) -> tuple[str, list[dict]]:
    """(notification text, blocks): ONE message, description then the Section C table."""
    description = _trial_lines(rows)
    table_rows = []
    for c in latest_final_cohorts(rows):
        status = _cohort_status(c)
        table_rows.append(
            [
                _rich(c["product_name"], bold=True),
                _raw(ENTITY_SHORT.get(c["entity"], c["entity"])),
                _rich("", STATUS_EMOJI_NAME[status])
                if status in STATUS_EMOJI_NAME
                else _raw("–"),
                _raw(_trial_label(c)),
                _raw(str(c["cohort_week"])),
                _raw(f"{int(c['trials_started']):,}"),
                _status_cell(_pct(c.get("cancelled_pct")), c.get("cancelled_status")),
                _raw(_dash(_pct(c.get("reached_first_charge_pct")))),
                _raw(_dash(_pct(c.get("converted_first_pct")))),
                _raw(_dash(_pct(c.get("recovered_pct")))),
                _status_cell(
                    _pct(c.get("total_converted_pct")), c.get("converted_status")
                ),
                _raw(_dash(_pct(c.get("failed_pct")))),
                _raw(_dash(_pct(c.get("other_pct")))),
                _raw(_dash(_pct(c.get("pending_pct")))),
                _raw(c.get("top_decline_reason") or "–"),
            ]
        )
    blocks = section_blocks(
        f"Trial-to-paid · {report_date}",
        description,
        legend_with_env(TRIAL_LEGEND_MRKDWN, env),
        _table_block(TRIAL_COLUMNS, table_rows),
    )
    return f"Trial-to-paid {report_date}: {description[0]}", blocks


def build_trial_table(rows: list[dict]) -> str:
    return _text_table(
        TRIAL_TABLE_COLUMNS,
        [
            (
                c["entity"],
                [
                    c["product_name"],
                    f"{c['trial_days']}d",
                    str(c["cohort_week"]),
                    f"{int(c['trials_started']):,}",
                    _status_text(
                        _pct(c.get("cancelled_pct")), c.get("cancelled_status")
                    ),
                    _pct(c.get("reached_first_charge_pct"), 0),
                    _pct(c.get("converted_first_pct"), 0),
                    _pct(c.get("recovered_pct"), 0),
                    _status_text(
                        _pct(c.get("total_converted_pct")), c.get("converted_status")
                    ),
                    _pct(c.get("failed_pct"), 0),
                    _pct(c.get("other_pct"), 0),
                    _pct(c.get("pending_pct"), 0),
                ],
            )
            for c in latest_final_cohorts(rows)
        ],
    )


def build_trial_reason_table(rows: list[dict]) -> str:
    """Trial config and top decline reason at the first charge, per latest final cohort."""
    lines = [
        "Product             Trial (price -> full)          Top decline reason at 1st charge"
    ]
    lines.append("-" * len(lines[0]))
    for c in latest_final_cohorts(rows):
        lines.append(
            f"{c['product_name'][:19].ljust(19)} "
            f"{_trial_label(c).replace('→', '->').ljust(30)} "
            f"{c.get('top_decline_reason') or '-'}"
        )
    return "\n".join(lines)


def build_trial_messages(rows: list[dict], report_date: str, env: str) -> list[str]:
    return _messages(
        f"Trial-to-paid · {report_date}",
        env,
        _trial_lines(rows),
        [
            ("*All products*", f"{build_trial_table(rows)}\n\n{TRIAL_LEGEND}"),
            (
                "*Trial configs and first-charge declines*",
                build_trial_reason_table(rows),
            ),
        ],
    )
