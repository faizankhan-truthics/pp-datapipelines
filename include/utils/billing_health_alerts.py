"""Slack messages for the billing health alerts (Section A, payment health).

Pure functions over the rows of include/sql/billing_health/section_a_status.sql (coloured
metrics, one row per product x metric) and section_a_daily.sql (the ticket's other columns, one
row per product), so wording and routing are testable without BigQuery or Slack:

* build_summary_blocks: the whole daily alert as ONE Block Kit message. A short description
  (status counts, today's reds, data notes), a one-line legend and one table block
  with every Section A column of the ticket per product: status colour per cell, and the top 3
  decline reasons in the same row. Slack renders it as a grid that scrolls sideways when it is
  wider than the window.
* build_summary_messages: the same as plain text (monospace code blocks), split into messages
  under Slack's 4,000-character limit. Fallback when Slack rejects the table block.

The summary is the ticket's daily (amber) summary; it names today's reds without tagging
anyone: each product with a red gets a reply in the message's thread tagging its owner
(include/utils/billing_health_red_alerts.py). Reds that were already red the day before are not
called out again (the table shows them), so a product that is red every day does not page its
owner daily.
"""

STATUS_EMOJI = {"red": "🔴", "amber": "🟡", "green": "🟢"}
# Slack emoji names for the same colours, used inside table cells.
STATUS_EMOJI_NAME = {
    "red": "red_circle",
    "amber": "large_yellow_circle",
    "green": "large_green_circle",
    "low_volume": "white_circle",
    "no_baseline": "white_circle",
}
# Per-cell markers in the plain-text fallback table (emoji would break monospace alignment).
STATUS_MARK = {"red": "!!", "amber": "!", "low_volume": "~", "no_baseline": "?"}

METRIC_LABELS = {
    "approval_signups": "Approval, sign-ups",
    "approval_renewals_first_attempt": "Approval, renewals (1st attempt)",
    "blocked_pct": "Blocked by Stripe",
    "duplicate_pct": "Duplicate attempts",
    "retries_after_do_not_retry": 'Retries after "do not retry"',
}

COUNT_METRICS = {"retries_after_do_not_retry"}
ENTITY_ORDER = ["Apollo", "Strategic Outsourcing", "Unmapped"]
ENTITY_SHORT = {
    "Apollo": "Apollo",
    "Strategic Outsourcing": "SO",
    "Unmapped": "Unmapped",
}
STATUS_RANK = {"red": 3, "amber": 2, "green": 1}

SLACK_TEXT_LIMIT = 3900  # Slack splits text messages above 4,000 characters
SECTION_TEXT_LIMIT = 2900  # Block Kit section text limit is 3,000

# Block Kit table: (header, align). Ticket Section A order, plus status and decline reasons.
BLOCK_COLUMNS = [
    ("Product", "left"),
    ("Entity", "left"),
    ("Status", "center"),
    ("Attempts", "right"),
    ("Approval overall", "right"),
    ("Approval sign-ups", "right"),
    ("Approval renewals (1st)", "right"),
    ("Renewals all attempts", "right"),
    ("Bank declines", "right"),
    ("Blocked", "right"),
    ("Blocked $", "right"),
    ("Duplicates", "right"),
    ("Attempts / order", "right"),
    ("Retries after do-not-retry", "right"),
    ("Top 3 decline reasons (% of declines)", "left"),
]

LEGEND_MRKDWN = (
    "Approval vs its 7-day avg: amber −5, red −10 pts · Blocked: amber > 1%, red > 2% and "
    "+1 pt vs 7-day avg · Duplicates (same card + amount within 10 s): amber > 5%, red > 10% "
    "and +5 pts vs 7-day avg · Retries after do-not-retry: amber if any, red at 2× and +5 vs "
    "7-day avg · ⚪ under 50 attempts or no baseline"
)

# (header, width) of the plain-text fallback table.
TABLE_COLUMNS = [
    ("Product", 19),
    ("Attempts", 8),
    ("Appr all", 8),
    ("Appr SU", 8),
    ("Appr RN", 8),
    ("RN all", 6),
    ("Bank dcl", 8),
    ("Blocked", 8),
    ("Blk $", 7),
    ("Dup", 7),
    ("Att/ord", 7),
    ("DNR", 7),
]

LEGEND = (
    "!! red  ! amber  ~ under 50 attempts  ? no baseline  - n/a\n"
    "Appr SU / RN = sign-ups / renewals (1st attempt): amber -5, red -10 pts vs 7-day avg.\n"
    "Blocked: amber > 1%, red > 2% and +1 pt vs 7-day avg. Dup (same card + amount in 10 s):\n"
    "amber > 5%, red > 10% and +5 pts vs 7-day avg.\n"
    'DNR = retries after "do not retry": amber if any, red at 2x and +5 vs 7-day avg.\n'
    "Att/ord = Stripe charges per Solidgate order."
)


# ---------------------------------------------------------------------------------------------
# Values and lines shared by both formats
# ---------------------------------------------------------------------------------------------


def format_value(metric: str, value) -> str:
    if value is None:
        return "n/a"
    if metric in COUNT_METRICS:
        return f"{int(value):,}"
    return f"{value:.1%}"


def _pct(value, decimals: int = 1) -> str:
    return "-" if value is None else f"{value * 100:.{decimals}f}%"


def _metric_text(status_row: dict | None, decimals: int = 1) -> str | None:
    if not status_row or status_row.get("value") is None:
        return None
    if status_row["metric"] in COUNT_METRICS:
        return f"{int(status_row['value']):,}"
    return _pct(status_row["value"], decimals)


def _by_metric(status_rows: list[dict]) -> dict[tuple, dict]:
    return {(r["product_key"], r["metric"]): r for r in status_rows if r.get("metric")}


def _worst_status(product_key: str, by_metric: dict[tuple, dict]) -> str | None:
    statuses = [r["status"] for (k, _), r in by_metric.items() if k == product_key]
    if not statuses:
        return None
    return max(statuses, key=lambda s: STATUS_RANK.get(s, 0))


def _products_with_data(daily_rows: list[dict]) -> list[dict]:
    return sorted(
        (d for d in daily_rows if d.get("attempts")),
        key=lambda d: (
            ENTITY_ORDER.index(d["entity"])
            if d["entity"] in ENTITY_ORDER
            else len(ENTITY_ORDER),
            d["product_name"],
        ),
    )


def status_counts(worst_by_product: dict[str, str | None]) -> str:
    """'🔴 1 red · 🟡 2 amber · 🟢 14 green · ⚪ 3 n/a', counting each product by its worst check."""
    count = {
        s: sum(1 for v in worst_by_product.values() if v == s)
        for s in ("red", "amber", "green")
    }
    other = len(worst_by_product) - sum(count.values())
    text = (
        f"🔴 {count['red']} red · 🟡 {count['amber']} amber · 🟢 {count['green']} green"
    )
    return text + (f" · ⚪ {other} n/a" if other else "")


def legend_with_env(legend: str, env: str) -> str:
    """The legend line, prefixed with the environment outside Production."""
    return legend if env == "Production" else f"*{env}* · {legend}"


def section_blocks(
    header: str, description: list[str], legend: str, table: dict
) -> list[dict]:
    """Header, description, one-line legend, table: the layout of every section message."""
    blocks = [{"type": "header", "text": {"type": "plain_text", "text": header}}]
    if description:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "\n".join(description)[:SECTION_TEXT_LIMIT],
                },
            }
        )
    blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": legend}]})
    blocks.append(table)
    return blocks


def new_red_line(product_names: list[str]) -> str | None:
    """'*Red:* GiftMusic, Tracelo · details and owners in the thread'. The details go in
    one thread reply for the section (billing_health_red_alerts), so they are not repeated here."""
    names = sorted(set(product_names))
    if not names:
        return None
    return "*Red:* " + ", ".join(names) + " · details and owners in the thread"


def _summary_lines(status_rows: list[dict], daily_rows: list[dict]) -> list[str]:
    """Status counts, reds, data notes."""
    metric_rows = [r for r in status_rows if r.get("metric")]
    worst: dict[str, str | None] = {}
    for r in metric_rows:
        current = worst.get(r["product_key"])
        if current is None or STATUS_RANK.get(r["status"], 0) > STATUS_RANK.get(
            current, 0
        ):
            worst[r["product_key"]] = r["status"]
    lines = [status_counts(worst)]
    new_red = new_red_line(
        [r["product_name"] for r in metric_rows if r.get("alert_now")]
    )
    if new_red:
        lines.append(new_red)
    unmapped = [
        d["product_name"]
        for d in _products_with_data(daily_rows)
        if d["entity"] == "Unmapped"
    ]
    if unmapped:
        lines.append("_Not in the products seed yet:_ " + ", ".join(unmapped))
    no_data = sorted(d["product_name"] for d in daily_rows if not d.get("attempts"))
    if no_data:
        lines.append("_No data:_ " + ", ".join(no_data))
    return lines


# ---------------------------------------------------------------------------------------------
# Block Kit (table block)
# ---------------------------------------------------------------------------------------------


def _raw(text: str) -> dict:
    return {"type": "raw_text", "text": text}


def _rich(text: str, emoji: str | None = None, bold: bool = False) -> dict:
    elements = [{"type": "emoji", "name": emoji}] if emoji else []
    if text:
        element = {"type": "text", "text": f" {text}" if emoji else text}
        if bold:
            element["style"] = {"bold": True}
        elements.append(element)
    return {
        "type": "rich_text",
        "elements": [{"type": "rich_text_section", "elements": elements}],
    }


def _metric_cell(status_row: dict | None, decimals: int = 1) -> dict:
    """A coloured metric cell: red / amber get their colour; others show the value only."""
    text = _metric_text(status_row, decimals)
    if text is None:
        return _raw("–")
    status = status_row.get("status")
    if status in ("red", "amber"):
        return _rich(text, STATUS_EMOJI_NAME[status], bold=status == "red")
    if status in ("low_volume", "no_baseline"):
        return _rich(text, "white_circle")
    return _raw(text)


def build_table_block(status_rows: list[dict], daily_rows: list[dict]) -> dict:
    """One table: a row per product with data, every Section A column + top 3 reasons."""
    by_metric = _by_metric(status_rows)
    rows = [[_rich(h, bold=True) for h, _ in BLOCK_COLUMNS]]
    for d in _products_with_data(daily_rows):
        key = d["product_key"]
        blocked = by_metric.get((key, "blocked_pct"))
        usd = blocked.get("amount_usd") if blocked else None
        worst = _worst_status(key, by_metric)
        rows.append(
            [
                _rich(d["product_name"], bold=True),
                _raw(ENTITY_SHORT.get(d["entity"], d["entity"])),
                _rich("", STATUS_EMOJI_NAME[worst]) if worst else _raw("–"),
                _raw(f"{int(d['attempts']):,}"),
                _raw(_pct(d.get("approval_overall")).replace("-", "–")),
                _metric_cell(by_metric.get((key, "approval_signups"))),
                _metric_cell(by_metric.get((key, "approval_renewals_first_attempt"))),
                _raw(_pct(d.get("approval_renewals_all"), 0).replace("-", "–")),
                _raw(_pct(d.get("bank_declines_pct")).replace("-", "–")),
                _metric_cell(blocked, 2),
                _raw("–" if usd is None else f"${usd:,.0f}"),
                _metric_cell(by_metric.get((key, "duplicate_pct"))),
                _raw(
                    "–"
                    if d.get("attempts_per_order") is None
                    else f"{d['attempts_per_order']:.2f}"
                ),
                _metric_cell(by_metric.get((key, "retries_after_do_not_retry"))),
                _raw((d.get("top_decline_reasons") or "–").replace(" | ", "\n")),
            ]
        )
    return {
        "type": "table",
        "column_settings": [
            {"align": align, "is_wrapped": header.startswith("Top 3")}
            for header, align in BLOCK_COLUMNS
        ],
        "rows": rows,
    }


def build_summary_blocks(
    status_rows: list[dict], daily_rows: list[dict], report_date: str, env: str
) -> tuple[str, list[dict]]:
    """(notification text, blocks): ONE message, a short description and then the table."""
    description = _summary_lines(status_rows, daily_rows)
    blocks = section_blocks(
        f"Payment health · {report_date}",
        description,
        legend_with_env(LEGEND_MRKDWN, env),
        build_table_block(status_rows, daily_rows),
    )
    return f"Payment health {report_date}: {description[0]}", blocks


# ---------------------------------------------------------------------------------------------
# Plain-text fallback (monospace code blocks), split under Slack's message limit
# ---------------------------------------------------------------------------------------------


def _cell(status_row: dict | None, decimals: int = 1) -> str:
    """A coloured metric's plain-text cell: value plus its status marker."""
    text = _metric_text(status_row, decimals)
    if text is None:
        return "-"
    return text + STATUS_MARK.get(status_row.get("status"), "")


def _row_line(cells: list[str]) -> str:
    first, *rest = zip(cells, TABLE_COLUMNS, strict=True)
    text = first[0][: first[1][1]].ljust(first[1][1])
    return text + "".join(f" {c.rjust(w)}" for c, (_, w) in rest)


def build_table(status_rows: list[dict], daily_rows: list[dict]) -> str:
    """Plain-text Section A table: one row per product with data, grouped by entity."""
    by_metric = _by_metric(status_rows)
    header = _row_line([h for h, _ in TABLE_COLUMNS])
    lines = [header, "-" * len(header)]
    entity = None
    for d in _products_with_data(daily_rows):
        if d["entity"] != entity:
            entity = d["entity"]
            lines.append(entity.upper())
        key = d["product_key"]
        blocked = by_metric.get((key, "blocked_pct"))
        usd = blocked.get("amount_usd") if blocked else None
        lines.append(
            _row_line(
                [
                    d["product_name"],
                    f"{int(d['attempts']):,}",
                    _pct(d.get("approval_overall")),
                    _cell(by_metric.get((key, "approval_signups"))),
                    _cell(by_metric.get((key, "approval_renewals_first_attempt"))),
                    _pct(d.get("approval_renewals_all"), 0),
                    _pct(d.get("bank_declines_pct")),
                    _cell(blocked, 2),
                    "-" if usd is None else f"${usd:,.0f}",
                    _cell(by_metric.get((key, "duplicate_pct"))),
                    "-"
                    if d.get("attempts_per_order") is None
                    else f"{d['attempts_per_order']:.2f}",
                    _cell(by_metric.get((key, "retries_after_do_not_retry"))),
                ]
            )
        )
    return "\n".join(lines)


def build_decline_table(daily_rows: list[dict]) -> str:
    """Top 3 decline reasons per product, each as % of that product's declines."""
    width = TABLE_COLUMNS[0][1]
    lines = [f"{'Product'.ljust(width)} Top 3 decline reasons (% of declines)"]
    lines.append("-" * len(lines[0]))
    for d in _products_with_data(daily_rows):
        if d.get("top_decline_reasons"):
            lines.append(
                f"{d['product_name'][:width].ljust(width)} {d['top_decline_reasons']}"
            )
    return "\n".join(lines)


def _chunks(lines: list[str], limit: int) -> list[str]:
    """Join lines into texts of at most `limit` characters, breaking only between lines."""
    chunks, current = [], ""
    for line in lines:
        candidate = f"{current}\n{line}" if current else line
        if len(candidate) > limit and current:
            chunks.append(current)
            current = line
        else:
            current = candidate
    if current:
        chunks.append(current)
    return chunks


def build_summary_messages(
    status_rows: list[dict], daily_rows: list[dict], report_date: str, env: str
) -> list[str]:
    """The summary as plain-text messages, each under Slack's limit (code blocks never split)."""
    title = f"*Payment health · {report_date}*" + (
        "" if env == "Production" else f" · {env}"
    )
    messages = _chunks(
        [title, *_summary_lines(status_rows, daily_rows)], SLACK_TEXT_LIMIT
    )
    for title, table in (
        ("*All products*", f"{build_table(status_rows, daily_rows)}\n\n{LEGEND}"),
        ("*Top decline reasons*", build_decline_table(daily_rows)),
    ):
        for part in _chunks(table.split("\n"), SLACK_TEXT_LIMIT - 40):
            messages.append(f"{title}\n```{part}```")
            title = f"{title} (cont.)"
    return messages
