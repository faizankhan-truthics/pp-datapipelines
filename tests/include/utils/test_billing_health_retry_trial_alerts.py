"""Tests for include/utils/billing_health_retry_trial_alerts.py: Sections B and C Slack messages."""

from include.utils.billing_health_retry_trial_alerts import (
    RETRY_COLUMNS,
    SLACK_TEXT_LIMIT,
    TRIAL_COLUMNS,
    build_retry_blocks,
    build_retry_messages,
    build_retry_table,
    build_trial_blocks,
    build_trial_messages,
    latest_final_cohorts,
)


def _retry(**overrides) -> dict:
    row = {
        "report_date": "2026-11-02",
        "product_key": "files_editor",
        "product_name": "Files-Editor",
        "entity": "Apollo",
        "is_stripe_direct": False,
        "payments_owner_slack_id": "U07PAY",
        "cohort_start": "2026-09-27",
        "cohort_end": "2026-10-03",
        "first_charges": 50695,
        "first_charge_failures": 24357,
        "recovered_pct": 0.213,
        "failed_after_all_retries_pct": 0.787,
        "avg_attempts_per_failed": 5.72,
        "baseline": 0.687,
        "ref_source": "4w",
        "target": 0.2,
        "status": "red",
        "prev_status": "green",
        "alert_now": True,
        "unlinked_invoices": None,
    }
    row.update(overrides)
    return row


def _cohort(**overrides) -> dict:
    row = {
        "report_date": "2026-11-02",
        "product_key": "chat_box",
        "product_name": "Chat-Box",
        "entity": "Apollo",
        "is_stripe_direct": False,
        "payments_owner_slack_id": "U07PAY",
        "product_owner_slack_id": "U07PROD",
        "cohort_week": "2026-09-21",
        "trial_days": 5,
        "trial_price": 0.65,
        "first_full_price": 29.99,
        "trials_started": 12668,
        "cancelled_pct": 0.073,
        "reached_first_charge_pct": 0.924,
        "converted_first_pct": 0.378,
        "recovered_pct": 0.177,
        "total_converted_pct": 0.555,
        "failed_pct": 0.340,
        "other_pct": 0.031,
        "pending_pct": 0.002,
        "top_decline_reason": "insufficient_funds",
        "unlinked_first_charges": 0,
        "history_coverage": 1.0,
        "is_final": True,
        "final_on": "2026-11-02",
        "converted_ref": 0.580,
        "converted_ref_source": "4 cohorts",
        "converted_status": "green",
        "cancelled_ref": 0.064,
        "cancelled_ref_source": "4 cohorts",
        "cancelled_status": "green",
        "converted_alert_now": False,
        "cancelled_alert_now": False,
    }
    row.update(overrides)
    return row


def _cell_text(cell: dict) -> str:
    if cell["type"] == "raw_text":
        return cell["text"]
    return "".join(
        e.get("text", f":{e.get('name')}:") for e in cell["elements"][0]["elements"]
    )


# --------------------------------------------------------------------------- Section B


def test_retry_new_red_is_named_without_tagging():
    # the owner is tagged in the separate red alert (billing_health_red_alerts)
    _, blocks = build_retry_blocks([_retry()], "2026-11-02", "Production")
    description = blocks[1]["text"]["text"]

    assert "*Red:* Files-Editor · details and owners in the thread" in description
    assert "<@" not in description


def test_retry_ongoing_red_is_not_called_out_again():
    _, ongoing = build_retry_blocks(
        [_retry(prev_status="red", alert_now=False)], "2026-11-02", "Production"
    )

    assert "*Red:*" not in ongoing[1]["text"]["text"]
    # the cohort dates are in the legend line
    assert "2026-09-27 – 2026-10-03" in ongoing[2]["elements"][0]["text"]


def test_retry_notes_unlinked_invoices_and_products_without_data():
    rows = [
        _retry(status="green", alert_now=False),
        _retry(
            product_key="online_qr_generator",
            product_name="Online QR Generator",
            first_charges=None,
            first_charge_failures=None,
            status="low_volume",
            alert_now=False,
            unlinked_invoices=8400,
        ),
    ]

    text = "\n".join(build_retry_messages(rows, "2026-11-02", "Production"))

    assert (
        "_Not measurable (Stripe invoices not linked to charges):_ Online QR Generator"
        in text
    )
    assert "_No data:_" not in text  # not listed twice
    assert "🔴 0 red · 🟡 0 amber · 🟢 1 green" in text


def test_retry_table_block_has_every_ticket_column_and_colours_failed():
    _, blocks = build_retry_blocks([_retry()], "2026-11-02", "Production")
    table = blocks[-1]

    assert [b["type"] for b in blocks] == ["header", "section", "context", "table"]
    assert blocks[0]["text"]["text"] == "Retry performance · 2026-11-02"
    header, row = table["rows"]
    assert [_cell_text(c) for c in header] == [h for h, _ in RETRY_COLUMNS]
    cells = dict(zip([h for h, _ in RETRY_COLUMNS], row, strict=True))
    assert _cell_text(cells["Status"]) == ":red_circle:"
    assert _cell_text(cells["First-charge failures"]) == "24,357"
    assert _cell_text(cells["Recovered by retries"]) == "21.3%"
    assert _cell_text(cells["Failed after all retries"]) == ":red_circle: 78.7%"
    assert _cell_text(cells["Avg attempts per failed charge"]) == "5.7"
    assert _cell_text(cells["Baseline (4-week avg)"]) == "68.7%"
    assert _cell_text(cells["Target"]) == "20%"


def test_retry_text_table_lines_up():
    table = build_retry_table(
        [
            _retry(),
            _retry(
                product_key="iq",
                product_name="IQ Center",
                entity="Strategic Outsourcing",
                status="amber",
            ),
        ]
    ).splitlines()

    assert table[2] == "APOLLO"
    assert "78.7%!!" in table[3]
    assert table[4] == "STRATEGIC OUTSOURCING"
    assert "78.7%!" in table[5]
    assert len(table[3]) == len(table[0])


# --------------------------------------------------------------------------- Section C


def test_latest_final_cohort_per_product_and_trial_length():
    rows = [
        _cohort(cohort_week="2026-09-14"),
        _cohort(cohort_week="2026-09-21"),
        _cohort(cohort_week="2026-09-28", is_final=False),  # pending: not shown
        _cohort(trial_days=7, cohort_week="2026-09-14"),
        _cohort(
            product_key="opengen",
            product_name="OpenGen",
            cohort_week=None,
            trial_days=None,
            trials_started=None,
            is_final=None,
        ),
    ]

    latest = latest_final_cohorts(rows)

    assert [(c["trial_days"], c["cohort_week"]) for c in latest] == [
        (5, "2026-09-21"),
        (7, "2026-09-14"),
    ]


def test_trial_new_reds_are_named_without_tagging():
    rows = [
        _cohort(
            converted_status="red", converted_alert_now=True, total_converted_pct=0.42
        ),
        _cohort(
            trial_days=7,
            cancelled_status="red",
            cancelled_alert_now=True,
            cancelled_pct=0.21,
            cancelled_ref=0.096,
        ),
    ]

    _, blocks = build_trial_blocks(rows, "2026-11-02", "Production")
    description = blocks[1]["text"]["text"]

    # one name per product, even with two red cohorts
    assert "*Red:* Chat-Box · details and owners in the thread" in description

    assert "<@" not in description


def test_trial_red_from_an_earlier_final_cohort_is_not_called_out_again():
    rows = [
        _cohort(
            converted_status="red", converted_alert_now=False, final_on="2026-10-26"
        )
    ]

    _, blocks = build_trial_blocks(rows, "2026-11-02", "Production")

    assert "*Red:*" not in blocks[1]["text"]["text"]


def test_trial_table_block_has_every_ticket_column():
    rows = [
        _cohort(converted_status="amber"),
        _cohort(
            product_key="opengen",
            product_name="OpenGen",
            cohort_week=None,
            trials_started=None,
            is_final=None,
        ),
    ]

    _, blocks = build_trial_blocks(rows, "2026-11-02", "Production")
    header, row = blocks[-1]["rows"]

    assert len(TRIAL_COLUMNS) <= 20
    assert [_cell_text(c) for c in header] == [h for h, _ in TRIAL_COLUMNS]
    cells = dict(zip([h for h, _ in TRIAL_COLUMNS], row, strict=True))
    assert _cell_text(cells["Status"]) == ":large_yellow_circle:"
    assert (
        _cell_text(cells["Trial (length · price → full price)"])
        == "5-day · $0.65 → $29.99"
    )
    assert _cell_text(cells["Trials started"]) == "12,668"
    assert _cell_text(cells["Total converted"]) == ":large_yellow_circle: 55.5%"
    assert _cell_text(cells["Cancelled before trial end"]) == "7.3%"
    assert (
        _cell_text(cells["Top decline reason at first charge"]) == "insufficient_funds"
    )
    assert "_No trials:_ OpenGen" in blocks[1]["text"]["text"]
    for cell in row:  # Slack rejects empty text elements
        if cell["type"] == "rich_text":
            for e in cell["elements"][0]["elements"]:
                assert e["type"] != "text" or e["text"]


def test_trial_prices_show_their_currency():
    rows = [
        _cohort(
            trial_price=4.99,
            trial_price_currency="EUR",
            first_full_price=4500,
            first_full_price_currency="JPY",
        )
    ]

    _, blocks = build_trial_blocks(rows, "2026-11-02", "Production")
    _, row = blocks[-1]["rows"]
    cells = dict(zip([h for h, _ in TRIAL_COLUMNS], row, strict=True))

    # $ only for USD; other currencies by their code (no conversion)
    assert (
        _cell_text(cells["Trial (length · price → full price)"])
        == "5-day · 4.99 EUR → 4,500 JPY"
    )


def test_trial_product_with_only_pending_cohorts_is_still_named():
    rows = [
        _cohort(),
        _cohort(
            product_key="giftmusic",
            product_name="GiftMusic",
            cohort_week="2026-10-05",
            is_final=False,
            final_on="2026-11-16",
        ),
        _cohort(
            product_key="giftmusic",
            product_name="GiftMusic",
            cohort_week="2026-09-28",
            is_final=False,
            final_on="2026-11-09",
        ),
    ]

    _, blocks = build_trial_blocks(rows, "2026-11-02", "Production")

    assert len(blocks[-1]["rows"]) == 2  # header + Chat-Box
    assert (
        "_No final cohort yet:_ GiftMusic (first on 2026-11-09)"
        in blocks[1]["text"]["text"]
    )


def test_trial_text_fallback_never_splits_a_code_block_or_exceeds_the_limit():
    rows = [
        _cohort(product_key=f"p{i}", product_name=f"Product {i:02d}") for i in range(80)
    ]

    messages = build_trial_messages(rows, "2026-11-02", "Production")

    assert len(messages) > 2
    for m in messages:
        assert len(m) <= SLACK_TEXT_LIMIT
        assert m.count("```") % 2 == 0
