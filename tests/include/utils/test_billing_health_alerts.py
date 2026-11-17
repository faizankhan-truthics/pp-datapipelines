"""Tests for include/utils/billing_health_alerts.py: what the Slack messages say and who they tag."""

from include.utils.billing_health_alerts import (
    BLOCK_COLUMNS,
    SLACK_TEXT_LIMIT,
    TABLE_COLUMNS,
    build_summary_blocks,
    build_summary_messages,
    build_table,
    build_table_block,
    format_value,
)


def summary_text(*args, **kwargs) -> str:
    return "\n".join(build_summary_messages(*args, **kwargs))


def _daily(**overrides) -> dict:
    row = {
        "product_key": "giftmusic",
        "product_name": "GiftMusic",
        "entity": "Apollo",
        "is_stripe_direct": False,
        "attempts": 2510,
        "approval_total": 0.322,
        "approval_overall": 0.292,
        "approval_renewals_all": 0.231,
        "bank_declines_pct": 0.652,
        "attempts_per_order": 1.917,
        "top_decline_reasons": "insufficient_funds 61% | do_not_honor 19% | transaction_not_allowed 10%",
    }
    row.update(overrides)
    return row


def _row(**overrides) -> dict:
    row = {
        "report_date": "2026-10-29",
        "product_key": "giftmusic",
        "product_name": "GiftMusic",
        "entity": "Apollo",
        "payments_owner_slack_id": "U07OWNER",
        "metric": "duplicate_pct",
        "value": 0.117,
        "volume": 2775,
        "amount_usd": None,
        "ref_7d": 0.07,
        "rule": "above_limit",
        "amber": 0.05,
        "red": 0.10,
        "status": "red",
        "prev_status": "amber",
        "alert_now": True,
    }
    row.update(overrides)
    return row


def test_format_value_percent_and_count():
    assert format_value("duplicate_pct", 0.117) == "11.7%"
    assert format_value("retries_after_do_not_retry", 44.0) == "44"
    assert format_value("approval_signups", None) == "n/a"


def test_new_red_is_named_in_the_summary_without_tagging():
    # the owner is tagged in the separate red alert (billing_health_red_alerts)
    text, blocks = build_summary_blocks(
        [_row()], [_daily()], "2026-10-29", "Production"
    )
    description = blocks[1]["text"]["text"]

    assert "*Red:* GiftMusic · details and owners in the thread" in description
    assert "<@" not in description
    assert text == "Payment health 2026-10-29: 🔴 1 red · 🟡 0 amber · 🟢 0 green"


def test_environment_is_shown_outside_production_only():
    _, prod = build_summary_blocks([_row()], [_daily()], "2026-10-29", "Production")
    _, dev = build_summary_blocks([_row()], [_daily()], "2026-10-29", "Development")

    assert not prod[2]["elements"][0]["text"].startswith("*Production*")
    assert dev[2]["elements"][0]["text"].startswith("*Development* · ")


def test_red_already_red_yesterday_is_not_called_out_again():
    ongoing = _row(prev_status="red", alert_now=False)

    _, blocks = build_summary_blocks([ongoing], [_daily()], "2026-10-29", "Production")

    assert "*Red:*" not in blocks[1]["text"]["text"]


def test_summary_counts_lists_reds_ambers_and_products_without_data():
    rows = [
        _row(),
        _row(
            metric="blocked_pct",
            value=0.0165,
            amount_usd=2109.64,
            status="amber",
            alert_now=False,
        ),
        _row(
            product_key="chat_box",
            product_name="Chat-Box",
            metric="approval_signups",
            value=0.64,
            rule="below_reference",
            ref_7d=0.63,
            status="green",
            alert_now=False,
        ),
        _row(
            product_key="fitday",
            product_name="FitDay",
            metric="duplicate_pct",
            status="low_volume",
            alert_now=False,
        ),
        _row(
            product_key="safetraveler",
            product_name="SafeTraveler",
            metric=None,
            value=None,
            status=None,
            alert_now=False,
        ),
    ]

    daily = [
        _daily(),
        _daily(product_key="safetraveler", product_name="SafeTraveler", attempts=None),
    ]
    summary = summary_text(rows, daily, "2026-10-29", "Production")

    # GiftMusic is red (worst of red + amber), Chat-Box green, FitDay low volume.
    assert "🔴 1 red · 🟡 0 amber · 🟢 1 green · ⚪ 1 n/a" in summary
    assert "*Red:*" in summary
    assert "_No data:_ SafeTraveler" in summary
    assert summary.count("```") == 4  # Section A table + decline reasons table
    assert "<@" not in summary


def test_summary_when_everything_is_green():
    summary = summary_text(
        [_row(status="green", alert_now=False)],
        [_daily()],
        "2026-10-29",
        "Production",
    )

    assert "🔴 0 red · 🟡 0 amber · 🟢 1 green" in summary
    assert "*Red:*" not in summary


def test_table_has_every_ticket_column_with_status_marks():
    status = [
        _row(
            metric="approval_signups",
            value=0.688,
            rule="below_reference",
            status="green",
        ),
        _row(
            metric="approval_renewals_first_attempt",
            value=0.189,
            rule="below_reference",
            status="no_baseline",
        ),
        _row(metric="blocked_pct", value=0.0165, amount_usd=2109.64, status="amber"),
        _row(metric="duplicate_pct", value=0.0886, status="amber"),
        _row(metric="retries_after_do_not_retry", value=44.0, rule="any", status="red"),
    ]

    table = build_table(status, [_daily()]).splitlines()

    assert [h for h, _ in TABLE_COLUMNS] == [
        "Product",
        "Attempts",
        "Appr all",
        "Appr SU",
        "Appr RN",
        "RN all",
        "Bank dcl",
        "Blocked",
        "Blk $",
        "Dup",
        "Att/ord",
        "DNR",
    ]
    assert table[2] == "APOLLO"
    row = table[3]
    assert row.startswith("GiftMusic")
    for cell in (
        "2,510",
        "29.2%",
        "68.8%",
        "18.9%?",
        "23%",
        "65.2%",
        "1.65%!",
        "$2,110",
        "8.9%!",
        "1.92",
        "44!!",
    ):
        assert cell in row
    assert len(row) == len(table[0])  # columns line up


def test_unmapped_products_are_listed_under_their_own_heading():
    daily = [
        _daily(),
        _daily(product_key="new_prod", product_name="NEW_PROD", entity="Unmapped"),
    ]

    table = build_table([], daily)
    summary = summary_text([], daily, "2026-10-29", "Production")

    assert table.splitlines()[-2] == "UNMAPPED"
    assert "_Not in the products seed yet:_ NEW_PROD" in summary
    assert "NEW_PROD" in summary


def _cell_text(cell: dict) -> str:
    if cell["type"] == "raw_text":
        return cell["text"]
    return "".join(
        e.get("text", f":{e.get('name')}:") for e in cell["elements"][0]["elements"]
    )


def test_table_block_has_one_row_per_product_with_colours_and_reasons():
    status = [
        _row(
            metric="approval_signups",
            value=0.688,
            rule="below_reference",
            status="green",
        ),
        _row(metric="blocked_pct", value=0.0165, amount_usd=2109.64, status="amber"),
        _row(metric="retries_after_do_not_retry", value=44.0, rule="any", status="red"),
    ]

    table = build_table_block(status, [_daily()])

    assert table["type"] == "table"
    assert len(table["column_settings"]) == len(BLOCK_COLUMNS) <= 20
    header, row = table["rows"]
    assert [_cell_text(c) for c in header] == [h for h, _ in BLOCK_COLUMNS]
    cells = dict(zip([h for h, _ in BLOCK_COLUMNS], row, strict=True))
    assert _cell_text(cells["Product"]) == "GiftMusic"
    assert _cell_text(cells["Status"]) == ":red_circle:"  # worst check
    assert _cell_text(cells["Approval overall"]) == "29.2%"
    assert _cell_text(cells["Approval sign-ups"]) == "68.8%"  # green: value only
    assert _cell_text(cells["Blocked"]) == ":large_yellow_circle: 1.65%"
    assert _cell_text(cells["Blocked $"]) == "$2,110"
    assert _cell_text(cells["Retries after do-not-retry"]) == ":red_circle: 44"
    assert "insufficient_funds 61%" in _cell_text(
        cells["Top 3 decline reasons (% of declines)"]
    )
    for cell in row:  # Slack rejects empty text elements
        if cell["type"] == "rich_text":
            for e in cell["elements"][0]["elements"]:
                assert e["type"] != "text" or e["text"]


def test_summary_is_one_short_message_ending_with_the_table():
    text, blocks = build_summary_blocks(
        [_row()], [_daily()], "2026-10-29", "Production"
    )

    assert [b["type"] for b in blocks] == ["header", "section", "context", "table"]
    assert "🔴 1 red" in text
    assert all(len(b["text"]["text"]) <= 3000 for b in blocks if b["type"] == "section")


def test_text_fallback_never_splits_a_code_block_or_exceeds_the_limit():
    daily = [
        _daily(product_key=f"p{i}", product_name=f"Product {i:02d}") for i in range(80)
    ]

    messages = build_summary_messages([_row()], daily, "2026-10-29", "Production")

    assert len(messages) > 2
    for m in messages:
        assert len(m) <= SLACK_TEXT_LIMIT
        assert m.count("```") % 2 == 0
