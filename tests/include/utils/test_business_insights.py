"""Tests for include/utils/business_insights.py -- the rules and formatting behind the
weekly per-product Slack report. The numbers come from the marts; what can break here is
the story we tell about them (growth sign, biggest driver, partial-month labelling)."""

from __future__ import annotations

import json
import urllib.error
from datetime import date

import pytest
from airflow.exceptions import AirflowFailException

from include.utils import business_insights as bi


def row(
    month,
    *,
    partial=False,
    begin=1000.0,
    new=100.0,
    react=0.0,
    exp=50.0,
    contr=-10.0,
    churn=-140.0,
    end=1000.0,
    subs_begin=100,
    subs_new=10,
    subs_react=0,
    subs_churn=5,
    subs_end=105,
):
    return {
        "month_start": date(2026, month, 1),
        "data_through": date(2026, month, 28),
        "is_partial": partial,
        "beginning_mrr": begin,
        "new_business_mrr": new,
        "reactivation_mrr": react,
        "expansion_mrr": exp,
        "contraction_mrr": contr,
        "churn_mrr": churn,
        "ending_mrr": end,
        "beginning_subscribers": subs_begin,
        "new_subscribers": subs_new,
        "reactivated_subscribers": subs_react,
        "expansion_customers": 3,
        "contraction_customers": 1,
        "churned_subscribers": subs_churn,
        "ending_subscribers": subs_end,
        "beginning_subscriptions": 100,
        "ending_subscriptions": 104,
        "mrr_growth": None if begin is None else (end - begin) / begin,
        "subscriber_growth": None
        if subs_begin is None
        else (subs_end - subs_begin) / subs_begin,
        "subscription_growth": 0.04,
        "net_mrr_change": end - begin if begin is not None else None,
        "movements_mrr": new + react + exp + contr + churn,
    }


def test_money_and_pct_formatting():
    assert bi.money(1_970_000) == "$1.97M"
    assert bi.money(-12_345) == "-$12.3k"
    assert bi.money(None) == "-"
    assert bi.pct(0.0123) == "+1.2%"
    assert bi.pct(-0.05) == "-5.0%"


def test_partial_month_is_starred():
    assert bi.month_label(row(10, partial=True)) == "Oct-26*"
    assert bi.month_label(row(9)) == "Sep-26"


def test_highlights_report_growth_direction_and_drivers():
    rows = [
        row(8, end=1000),
        row(9, begin=1000, end=900, new=10, exp=0, churn=-200, contr=-5),
    ]
    text = "\n".join(bi.build_highlights(rows))

    assert "fell -10.0% MoM" in text
    assert "biggest loss: churn -$200" in text
    assert "Biggest gain: new business $10" in text


def test_highlights_flag_a_growth_sign_flip():
    rows = [row(8, begin=1000, end=1100), row(9, begin=1100, end=1000)]

    assert any("turned negative" in h for h in bi.build_highlights(rows))


def test_highlights_flag_churn_spike_against_prior_average():
    rows = [
        row(6, churn=-100),
        row(7, churn=-100),
        row(8, churn=-100),
        row(9, churn=-400),
    ]

    assert any(
        "above its prior 3-month average" in h for h in bi.build_highlights(rows)
    )
    assert not any(
        "above its prior" in h
        for h in bi.build_highlights(rows[:3] + [row(9, churn=-110)])
    )


def test_highlights_mention_month_to_date_separately_from_completed_months():
    rows = [row(8), row(9), row(10, partial=True, begin=1000, end=1100)]
    text = "\n".join(bi.build_highlights(rows))

    assert "Month to date" in text and "month not complete" in text
    assert (
        "*Sep-26*" in text
    )  # headline is the last COMPLETED month, not the partial one


def test_highlights_for_a_new_product_with_no_completed_comparison_still_report_where_it_stands():
    out = bi.build_highlights(
        [row(10, partial=True, begin=None, end=22_000.0, subs_end=19)]
    )

    assert len(out) == 1
    assert "New product: MRR is $22.0k and 19 paid subscribers as of Oct 28" in out[0]
    assert "first full month" in out[0]


def test_new_product_message_still_builds_end_to_end():
    first = row(9, begin=None, end=889.0, subs_begin=None, subs_end=19)
    payload = bi.build_main_payload(
        "GiftMusic", [first, row(10, partial=True, begin=889.0, end=22_000.0)]
    )

    texts = [b["text"]["text"] for b in payload["blocks"] if b["type"] == "section"]

    assert any("New product" in t for t in texts)
    assert "vs last month" not in "\n".join(
        texts
    )  # no completed month to compare against yet
    assert "month to date through Oct 28" in payload["blocks"][1]["elements"][0]["text"]
    assert not any("Next Steps" in t for t in texts)


def test_main_message_follows_the_management_template_order():
    payload = bi.build_main_payload("QR", [row(8), row(9)])
    blocks = payload["blocks"]
    sections = [b["text"]["text"] for b in blocks if b["type"] == "section"]

    assert payload["text"] == "Weekly Business Insights: QR"
    assert [b["type"] for b in blocks[:5]] == [
        "header",
        "context",
        "section",
        "section",
        "data_visualization",
    ]
    assert (
        blocks[0]["text"]["text"]
        == "\U0001f4ca WEEKLY BUSINESS INSIGHTS: QR \U0001f4c8"
        and blocks[0]["text"]["emoji"] is True
    )
    assert blocks[1]["elements"][0]["text"].startswith(
        "_Reporting Period: Sep 2026 (last completed month)"
    )
    assert sections[0].startswith("Hey team!")
    assert sections[1].startswith("*\U0001f4a1 The Big Picture*")
    assert sections[2].startswith("*\U0001f50d Key Takeaways & Context*")
    assert [b["type"] for b in blocks].count(
        "data_visualization"
    ) == 1  # one chart inline, as in the template's image slot
    assert all(len(t) < 3000 for t in sections)


def test_big_picture_has_four_bullets_with_arrows_and_stays_short_enough_not_to_fold():
    rows = [
        row(8),
        row(
            9,
            begin=1000,
            end=900,
            subs_begin=100,
            subs_end=80,
            subs_new=9914,
            subs_react=608,
            subs_churn=13129,
        ),
    ]
    text = bi._big_picture(rows)
    bullets = text.splitlines()[1:]

    assert len(bullets) == 4
    assert "*Ending MRR:* $900 (\U0001f53d 10.0% vs last month)" in bullets[0]
    assert "\U0001f53d 20.0% vs last month" in bullets[2]
    assert "+9,914 new, +608 reactivated, -13,129 churned" in bullets[2]
    assert "Churn -$140" in bullets[3]
    assert "\U0001f53c" in bi._big_picture(
        [row(8), row(9, begin=1000, end=1100)]
    )  # growth gets the up arrow


def test_big_picture_marks_growth_off_a_tiny_base_as_not_meaningful():
    assert "n/m vs last month" in bi._big_picture(
        [row(8), row(9, begin=10.0, end=5000.0)]
    )


def test_headline_drops_the_percentage_off_a_tiny_base():
    headline = bi.build_highlights([row(8), row(9, begin=10.0, end=5000.0)])[0]
    assert "grew to" in headline and "%" not in headline


def test_takeaways_use_the_highlights_and_next_steps_only_appear_when_a_rule_fires():
    quiet = bi.build_main_payload("p", [row(8), row(9)])
    noisy = bi.build_main_payload(
        "p",
        [
            row(8, begin=5000, end=5000),
            row(9, begin=5000, end=4000, new=100, exp=0, contr=-9, churn=-240),
        ],
    )
    quiet_sections = [
        b["text"]["text"] for b in quiet["blocks"] if b["type"] == "section"
    ]
    noisy_sections = [
        b["text"]["text"] for b in noisy["blocks"] if b["type"] == "section"
    ]

    assert not any("Next Steps" in t for t in quiet_sections)
    steps = next(t for t in noisy_sections if "Next Steps / Action Items" in t)
    assert steps.startswith(
        "*\U0001f680 Next Steps / Action Items*\n1. Data team to reconcile"
    )
    assert (
        "@" not in steps
    )  # no owners are invented: the warehouse has no team-ownership data


def test_next_steps_empty_for_a_new_product():
    assert bi.build_next_steps([row(10, partial=True, begin=None, end=22_000.0)]) == []


def test_charts_obey_slack_schema_limits():
    rows = [row(m, partial=(m == 10)) for m in range(4, 11)]
    blocks = (
        bi.build_main_payload("p", rows)["blocks"]
        + bi.build_thread_payload("p", rows)["blocks"]
    )
    charts = [b for b in blocks if b["type"] == "data_visualization"]
    assert len(charts) == 2  # Slack allows at most 2 per message

    for block in charts:
        chart = block["chart"]
        cats = chart["axis_config"]["categories"]
        assert len(block["title"]) <= 50 and 1 <= len(chart["series"]) <= 12
        assert len({s["name"] for s in chart["series"]}) == len(chart["series"])
        for s in chart["series"]:
            assert len(s["name"]) <= 20 and 1 <= len(s["data"]) <= 20
            assert [
                d["label"] for d in s["data"]
            ] == cats  # exactly one point per category, in order
        assert all(len(c) <= 20 for c in cats)
    assert "Oct-26*" in charts[0]["chart"]["axis_config"]["categories"]


def test_highlights_flag_movements_that_do_not_explain_the_net_change():
    # movements sum to -149 but MRR actually fell 1,000 (the QR dashboard's Sep-26 shape)
    rows = [
        row(8, begin=5000, end=5000),
        row(9, begin=5000, end=4000, new=100, react=0, exp=0, contr=-9, churn=-240),
    ]

    assert any("Data check" in h for h in bi.build_highlights(rows))
    assert not any(
        "Data check" in h
        for h in bi.build_highlights(
            [row(8, begin=1000, end=1000), row(9, begin=1000, end=1000)]
        )
    )


def test_group_accounts_derives_products_and_lets_overrides_claim_accounts():
    rows = [
        ("solidgate", "QR_PROD_QGA_LIVE", "qr_generator_ai", True),
        ("stripe", "OQG", "online-qr-generator.com", True),
        ("stripe", "TRACELO", "tracelo.com", True),
        ("solidgate", "PS_PROD_TRACELO_LIVE", "tracelo.com", True),
        ("adyen", "X", "ignored", True),
    ]
    overrides = {"QR": {"solidgate": ["QR_PROD_QGA_LIVE"], "stripe": ["OQG", "QCI"]}}

    groups = bi.group_accounts(rows, overrides)

    assert groups["QR"] == {"solidgate": ["QR_PROD_QGA_LIVE"], "stripe": ["OQG", "QCI"]}
    assert "qr_generator_ai" not in groups and "online-qr-generator.com" not in groups
    assert groups["tracelo.com"] == {
        "solidgate": ["PS_PROD_TRACELO_LIVE"],
        "stripe": ["TRACELO"],
    }
    assert "ignored" not in groups


def test_group_accounts_keeps_retired_accounts_of_live_products_and_drops_fully_retired_products():
    rows = [
        (
            "stripe",
            "OLD_ACCT",
            "tracelo.com",
            False,
        ),  # retired, but another account keeps the product live
        ("solidgate", "PS_PROD_TRACELO_LIVE", "tracelo.com", True),
        ("stripe", "GONE", "gone.com", False),  # nothing live -> not reported
        ("stripe", "RETIRED_QR", "qr", False),
    ]
    overrides = {"RETIRED GROUP": {"stripe": ["RETIRED_QR"]}}

    groups = bi.group_accounts(rows, overrides)

    assert groups["tracelo.com"] == {
        "solidgate": ["PS_PROD_TRACELO_LIVE"],
        "stripe": ["OLD_ACCT"],
    }
    assert "gone.com" not in groups
    assert "RETIRED GROUP" not in groups


def test_table_growth_is_not_meaningful_when_the_base_was_tiny():
    assert bi.growth_cell(2967.211) == "n/m"
    assert bi.growth_cell(-10.5) == "n/m"
    assert bi.growth_cell(3.54) == "+354.0%"
    assert bi.growth_cell(None) == "-"


# ------------------------------------------------------------------ bot-token / thread delivery


class _ApiResp:
    def __init__(self, body: dict):
        self._body = json.dumps(body).encode()

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _fake_api(monkeypatch, replies):
    """Patch urlopen; `replies` is a list of dicts (one per call) the fake Slack API returns."""
    calls = []

    def fake(req, timeout):
        calls.append(
            {
                "url": req.full_url,
                "auth": req.get_header("Authorization"),
                "body": json.loads(req.data),
            }
        )
        return _ApiResp(replies[len(calls) - 1])

    monkeypatch.setattr(bi.urllib.request, "urlopen", fake)
    return calls


def test_trend_chart_is_inline_and_movements_chart_is_in_the_thread():
    rows = [row(8), row(9)]
    main = bi.build_main_payload("p", rows)
    thread = bi.build_thread_payload("p", rows)

    assert [b["type"] for b in thread["blocks"]] == ["data_visualization", "context"]
    assert thread["blocks"][0]["title"] == "MRR movements by month (USD)"
    assert main["blocks"][4]["title"] == "Ending MRR (USD)"
    assert (
        "Churn and contraction are negatives"
        in thread["blocks"][-1]["elements"][0]["text"]
    )
    assert thread["text"] == "MRR movements: p"


def test_post_via_api_sends_bearer_token_channel_and_thread(monkeypatch):
    calls = _fake_api(monkeypatch, [{"ok": True, "ts": "1700.0001"}])

    ts = bi.post_via_api(
        "xoxb-secret", "C123", {"text": "t", "blocks": []}, thread_ts="1699.9"
    )

    body = calls[0]["body"]
    assert ts == "1700.0001"
    assert calls[0]["url"] == "https://slack.com/api/chat.postMessage"
    assert calls[0]["auth"] == "Bearer xoxb-secret"
    assert (
        body["channel"] == "C123"
        and body["thread_ts"] == "1699.9"
        and body["unfurl_links"] is False
    )


def test_post_via_api_raises_on_ok_false_even_though_http_is_200(monkeypatch):
    _fake_api(monkeypatch, [{"ok": False, "error": "not_in_channel"}])

    with pytest.raises(RuntimeError, match="not_in_channel"):
        bi.post_via_api("xoxb", "C123", {"text": "t", "blocks": []})


def test_deliver_with_token_posts_main_then_charts_in_its_thread(monkeypatch):
    calls = _fake_api(
        monkeypatch, [{"ok": True, "ts": "111.1"}, {"ok": True, "ts": "111.2"}]
    )

    bi.deliver("p", [row(8), row(9)], bot_token="xoxb", channel="C1")

    assert len(calls) == 2
    assert "thread_ts" not in calls[0]["body"]
    assert [b["type"] for b in calls[0]["body"]["blocks"]].count(
        "data_visualization"
    ) == 1  # trend chart inline
    assert calls[1]["body"]["thread_ts"] == "111.1"
    assert [b["type"] for b in calls[1]["body"]["blocks"]].count(
        "data_visualization"
    ) == 1


def test_deliver_keeps_the_main_message_when_the_thread_reply_fails(monkeypatch):
    calls = _fake_api(
        monkeypatch,
        [{"ok": True, "ts": "111.1"}, {"ok": False, "error": "invalid_blocks"}],
    )

    bi.deliver("p", [row(8), row(9)], bot_token="xoxb", channel="C1")

    assert (
        len(calls) == 2
    )  # no exception -> Airflow won't retry and double-post the main message


def test_deliver_main_failure_propagates_so_the_task_retries(monkeypatch):
    _fake_api(monkeypatch, [{"ok": False, "error": "channel_not_found"}])

    with pytest.raises(RuntimeError, match="channel_not_found"):
        bi.deliver("p", [row(8), row(9)], bot_token="xoxb", channel="C1")


def test_deliver_does_not_retry_when_slack_may_already_have_the_message(monkeypatch):
    calls = []

    def lost_response(req, timeout):
        calls.append(req)
        raise TimeoutError("read timed out")  # the post may have gone through

    monkeypatch.setattr(bi.urllib.request, "urlopen", lost_response)

    with pytest.raises(AirflowFailException, match="may already have the message"):
        bi.deliver("p", [row(8), row(9)], bot_token="xoxb", channel="C1")

    assert len(calls) == 1  # and no thread reply was attempted


def test_deliver_retries_when_slack_rate_limits_because_nothing_was_posted(monkeypatch):
    def rate_limited(req, timeout):
        raise urllib.error.HTTPError(req.full_url, 429, "Too Many Requests", {}, None)

    monkeypatch.setattr(bi.urllib.request, "urlopen", rate_limited)

    with pytest.raises(urllib.error.HTTPError):
        bi.deliver("p", [row(8), row(9)], bot_token="xoxb", channel="C1")


@pytest.mark.parametrize(
    "token, channel", [(None, None), (None, "C1"), ("xoxb", None), ("", "C1")]
)
def test_deliver_without_a_token_and_channel_raises_and_posts_nothing(
    monkeypatch, token, channel
):
    calls = _fake_api(monkeypatch, [])

    with pytest.raises(RuntimeError, match="No Slack destination"):
        bi.deliver("p", [row(8), row(9)], bot_token=token, channel=channel)

    assert calls == []


# ------------------------------------------------------------------ trimmed layout


def test_highlights_stay_within_four_bullets_even_when_every_rule_fires():
    # growth flips sign, churn spikes, movements don't explain the net change, and a partial month exists
    rows = [
        row(6, churn=-100, begin=1000, end=1100),
        row(7, churn=-100, begin=1100, end=1100),
        row(8, churn=-100, begin=1100, end=1200),
        row(9, begin=1200, end=900, new=10, exp=0, contr=-5, churn=-400),
        row(10, partial=True, begin=900, end=950),
    ]
    out = bi.build_highlights(rows)

    assert len(out) <= 4
    assert (
        sum(len(h) for h in out) < 700
    )  # Slack folds longer section text behind "Show more"
    assert (
        "turned negative" in out[0]
    )  # the flip is folded into the headline, not its own bullet
    assert "above its prior 3-month average" in out[1]
    assert any("Data check" in h for h in out) and any(
        "Month to date" in h for h in out
    )


def test_highlights_do_not_repeat_what_the_big_picture_shows():
    text = "\n".join(bi.build_highlights([row(8), row(9)]))

    assert "Paid subscribers" not in text and "reactivated" not in text


def test_thread_note_carries_the_metric_definitions():
    thread_note = bi.build_thread_payload("p", [row(8), row(9)])["blocks"][-1][
        "elements"
    ][0]["text"]

    assert "net change = End - Begin" in thread_note and "n/m" in thread_note
