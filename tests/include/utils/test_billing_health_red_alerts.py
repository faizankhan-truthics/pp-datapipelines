"""Tests for include/utils/billing_health_red_alerts.py: one reply per section, payments team tagged once."""

from include.utils.billing_health_red_alerts import build_red_replies

A_RED = {
    "product_key": "giftmusic",
    "product_name": "GiftMusic",
    "entity": "Apollo",
    "payments_owner_slack_id": "U07PAY",
    "metric": "retries_after_do_not_retry",
    "value": 44.0,
    "ref_7d": 13.3,
    "amount_usd": None,
    "status": "red",
    "alert_now": True,
}

B_RED = {
    "product_key": "files_editor",
    "product_name": "Files-Editor",
    "entity": "Apollo",
    "payments_owner_slack_id": "U07PAY",
    "cohort_start": "2026-09-27",
    "cohort_end": "2026-10-03",
    "first_charge_failures": 24357,
    "failed_after_all_retries_pct": 0.812,
    "baseline": 0.696,
    "target": 0.2,
    "status": "red",
    "alert_now": True,
}

C_RED = {
    "product_key": "chat_box",
    "product_name": "Chat-Box",
    "entity": "Apollo",
    "payments_owner_slack_id": "U07PAY",
    "product_owner_slack_id": "U07PROD",
    "cohort_week": "2026-09-21",
    "trial_days": 5,
    "trials_started": 12668,
    "total_converted_pct": 0.42,
    "converted_ref": 0.58,
    "cancelled_pct": 0.21,
    "cancelled_ref": 0.064,
    "converted_alert_now": True,
    "cancelled_alert_now": True,
}


def test_one_thread_reply_per_product_per_section_tagging_the_owner():
    replies = build_red_replies([A_RED], [B_RED], [C_RED])

    assert replies == {
        "payment": [
            "🔴 *GiftMusic* · 1 red\n"
            '• Retries after "do not retry": *44* '
            "(red at 2x and 5+ above its 7-day avg 13.3)\n"
            "<@U07PAY> please take a look."
        ],
        "retry": [
            "🔴 *Files-Editor* · 1 red\n"
            "• Failed after all retries: *81.2%* of 24,357 failed first charges "
            "(2026-09-27 – 2026-10-03); 4-week avg 69.6%, red above +10 pts\n"
            "<@U07PAY> please take a look."
        ],
        # conversion and cancellation both tag only the payments team, in one reply
        "trial": [
            "🔴 *Chat-Box* · 2 reds\n"
            "• 5-day trial, week of 2026-09-21: total converted *42.0%* "
            "(last 4 cohorts 58.0%, red 10+ pts below)\n"
            "• 5-day trial, week of 2026-09-21: cancelled before trial end *21.0%* "
            "(last 4 cohorts 6.4%, red 10+ pts above)\n"
            "<@U07PAY> please take a look."
        ],
    }


def test_all_products_with_reds_share_one_reply_and_one_tag():
    rows = [
        A_RED,
        {
            **A_RED,
            "metric": "blocked_pct",
            "value": 0.024,
            "ref_7d": 0.008,
            "amount_usd": 25340.0,
        },
        {**A_RED, "product_key": "tracelo", "product_name": "Tracelo"},
    ]

    (reply,) = build_red_replies(rows, [], [])["payment"]

    assert reply == (
        "🔴 *GiftMusic* · 2 reds\n"
        "• Blocked by Stripe: *2.4%* (red above 2% and 1+ pt above its 7-day avg 0.8%), "
        "$25,340 blocked\n"
        '• Retries after "do not retry": *44* (red at 2x and 5+ above its 7-day avg 13.3)\n'
        "\n"
        "🔴 *Tracelo* · 1 red\n"
        '• Retries after "do not retry": *44* (red at 2x and 5+ above its 7-day avg 13.3)\n'
        "<@U07PAY> please take a look."
    )


def test_rule_text_names_the_reference_or_the_fixed_limit_without_one():
    rows = [
        {**A_RED, "metric": "duplicate_pct", "value": 0.16, "ref_7d": 0.04},
        {**A_RED, "metric": "approval_signups", "value": 0.5, "ref_7d": 0.7}
        | {"ref_source": "baseline"},
        {**A_RED, "ref_7d": None},
    ]

    (reply,) = build_red_replies(rows, [], [])["payment"]

    assert "(red 10+ pts below its baseline 70.0%)" in reply
    assert "(red above 10% and 5+ pts above its 7-day avg 4.0%)" in reply
    assert "(red if any, no usual level yet)" in reply


def test_owners_of_different_products_are_tagged_once_in_the_one_reply():
    rows = [
        {**A_RED, "payments_owner_slack_id": "U0EUGENIA U0ALEX"},
        {
            **A_RED,
            "product_name": "Tracelo",
            "payments_owner_slack_id": "U0ALEX U0OTHER",
        },
    ]

    (reply,) = build_red_replies(rows, [], [])["payment"]

    assert reply.endswith("<@U0EUGENIA> <@U0ALEX> <@U0OTHER> please take a look.")
    assert reply.count("<@U0ALEX>") == 1


def test_a_product_without_owner_is_named_and_the_rest_still_tagged():
    rows = [A_RED, {**A_RED, "product_name": "Tracelo", "payments_owner_slack_id": ""}]

    (reply,) = build_red_replies(rows, [], [])["payment"]

    assert reply.endswith(
        "<@U07PAY> please take a look.\n"
        "_No owner set in seed_billing_health_products for: Tracelo._"
    )


def test_reds_that_are_not_new_get_no_reply():
    replies = build_red_replies(
        [{**A_RED, "alert_now": False}, {**A_RED, "metric": None, "alert_now": False}],
        [{**B_RED, "alert_now": False}],
        [{**C_RED, "converted_alert_now": False, "cancelled_alert_now": False}],
    )

    assert replies == {"payment": [], "retry": [], "trial": []}


def test_several_owners_are_all_tagged_once():
    (reply,) = build_red_replies(
        [
            {**A_RED, "payments_owner_slack_id": "U0EUGENIA U0ALEX"},
            {
                **A_RED,
                "metric": "duplicate_pct",
                "payments_owner_slack_id": "U0EUGENIA U0ALEX",
            },
        ],
        [],
        [],
    )["payment"]

    assert reply.endswith("<@U0EUGENIA> <@U0ALEX> please take a look.")


def test_product_owner_is_never_tagged():
    # cancellations before trial end too: the payments team tags the product owner by hand
    (trial,) = build_red_replies([], [], [{**C_RED, "converted_alert_now": False}])[
        "trial"
    ]

    assert trial.endswith("<@U07PAY> please take a look.")
    assert "U07PROD" not in trial


def test_nobody_is_tagged_when_tagging_is_off():
    replies = build_red_replies([A_RED], [B_RED], [C_RED], tag_owners=False)

    for (reply,) in replies.values():
        assert reply.endswith("_Owner tagging is off._")
        assert "<@" not in reply


def test_owner_missing_from_the_seed_is_not_tagged():
    (reply,) = build_red_replies([{**A_RED, "payments_owner_slack_id": None}], [], [])[
        "payment"
    ]

    assert reply.endswith(
        "_No owner set in seed_billing_health_products for: GiftMusic._"
    )
    assert "<@" not in reply
