# include/utils/business_insights.py
"""Rule-based weekly business insights per product, posted to Slack.

The metrics come from include/sql/business_insights/*.sql (the gold marts); everything
the Slack message says is derived from those rows by the rules below -- no free text is
generated, so every number can be traced back to a mart.
"""

import json
import urllib.error
import urllib.request
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from jinja2 import Template

CHURN_SPIKE_FACTOR = (
    1.5  # flag churn MRR above this multiple of the prior 3-month average
)


def _render(sql_file: Path, template_params: dict) -> str:
    return Template(sql_file.read_text(encoding="utf-8").strip().rstrip(";")).render(
        params=template_params
    )


def group_accounts(
    account_rows: list[tuple[str, str, str, bool]],
    overrides: dict[str, dict[str, list[str]]],
) -> dict[str, dict[str, list[str]]]:
    """(provider, account_name, product_name, is_live) rows -> {product: {"solidgate": [...], "stripe": [...]}}.

    Only live products are reported, but a live product keeps ALL its accounts so a retired account's
    history stays in its metrics. Hand-maintained `overrides` win: their accounts are removed from the
    derived products so no account is reported under two products, and an override with no live
    account is dropped like any other retired product.
    """
    live = {(prov, acct) for prov, acct, _, is_live in account_rows if is_live}
    claimed = {
        (prov, acct)
        for grp in overrides.values()
        for prov, accts in grp.items()
        for acct in accts
    }
    products = {
        name: {
            "solidgate": list(grp.get("solidgate", [])),
            "stripe": list(grp.get("stripe", [])),
        }
        for name, grp in overrides.items()
    }
    for provider, account, product, _ in account_rows:
        if (provider, account) in claimed or provider not in ("solidgate", "stripe"):
            continue
        products.setdefault(product, {"solidgate": [], "stripe": []})[provider].append(
            account
        )
    return {
        name: grp
        for name, grp in products.items()
        if any(
            (provider, account) in live
            for provider, accts in grp.items()
            for account in accts
        )
    }


def fetch_account_rows(
    *, sql_dir: Path, gcp_conn_id: str, bq_location: str, template_params: dict
) -> list[tuple[str, str, str, bool]]:
    hook = BigQueryHook(
        gcp_conn_id=gcp_conn_id, use_legacy_sql=False, location=bq_location
    )
    rows = hook.get_records(sql=_render(sql_dir / "product_list.sql", template_params))
    return [(r[0], r[1], r[2], bool(r[3])) for r in rows]


def fetch_product_metrics(
    *,
    sql_dir: Path,
    accounts: dict[str, list[str]],
    gcp_conn_id: str,
    bq_location: str,
    template_params: dict,
) -> list[dict]:
    from google.cloud import bigquery

    hook = BigQueryHook(
        gcp_conn_id=gcp_conn_id, use_legacy_sql=False, location=bq_location
    )
    job = hook.get_client(location=bq_location).query(
        _render(sql_dir / "product_monthly_metrics.sql", template_params),
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter(
                    "solidgate_accounts", "STRING", accounts.get("solidgate", [])
                ),
                bigquery.ArrayQueryParameter(
                    "stripe_accounts", "STRING", accounts.get("stripe", [])
                ),
            ]
        ),
    )
    return [dict(row.items()) for row in job.result()]


# ---------------------------------------------------------------- formatting


def money(value: float | None) -> str:
    if value is None:
        return "-"
    sign = "-" if value < 0 else ""
    v = abs(value)
    if v >= 1_000_000:
        return f"{sign}${v / 1_000_000:.2f}M"
    if v >= 1_000:
        return f"{sign}${v / 1_000:.1f}k"
    return f"{sign}${v:.0f}"


def count(value: float | None) -> str:
    return "-" if value is None else f"{round(value):,}"


def pct(value: float | None) -> str:
    return "-" if value is None else f"{value * 100:+.1f}%"


def growth_cell(value: float | None) -> str:
    """Growth for a table cell: 'n/m' (not meaningful) when the base was so small it hit 1000%+."""
    return "n/m" if value is not None and abs(value) >= 10 else pct(value)


def month_label(row: dict) -> str:
    return row["month_start"].strftime("%b-%y") + ("*" if row["is_partial"] else "")


# ---------------------------------------------------------------- rules


def _unexplained_gap(last: dict) -> float | None:
    """The part of the real MRR change the five movements don't account for, when it is material."""
    gap = last["net_mrr_change"] - last["movements_mrr"]
    if abs(gap) > 0.01 * abs(last["beginning_mrr"]) and abs(gap) > 0.1 * abs(
        last["net_mrr_change"]
    ):
        return gap
    return None


def _churn_spike_baseline(complete: list[dict], last: dict) -> tuple[float, int] | None:
    """(average, months) of prior churn when this month's churn is well above it."""
    earlier = [r["churn_mrr"] for r in complete[-4:-1]]
    if earlier:
        avg = sum(earlier) / len(earlier)
        if avg < 0 and last["churn_mrr"] < avg * CHURN_SPIKE_FACTOR:
            return avg, len(earlier)
    return None


def build_highlights(rows: list[dict]) -> list[str]:
    """Up to four plain-language takeaways, derived only by rule from the monthly rows.

    Kept short on purpose: Slack folds a section behind "Show more" past ~5 lines, which would hide the
    last bullet. Subscriber and movement figures live in the summary block, not here.
    """
    complete = [r for r in rows if not r["is_partial"] and r["mrr_growth"] is not None]
    if not complete:
        # New product: no completed month to compare against yet. Still say where it stands today.
        latest = rows[-1]
        subs = latest["ending_subscribers"]
        subs_text = f" and {count(subs)} paid subscribers" if subs else ""
        return [
            (
                f"New product: MRR is {money(latest['ending_mrr'])}{subs_text} as of {latest['data_through']:%b %d}. "
                "Month-over-month comparisons start after its first full month of history."
            )
        ]

    last = complete[-1]
    prev = complete[-2] if len(complete) > 1 else None
    out = []

    verb = "grew" if last["mrr_growth"] >= 0 else "fell"
    # Off a near-zero base the percentage is meaningless (e.g. +8587.6%), so drop it like the summary does.
    change = (
        f"{verb} {pct(last['mrr_growth'])} MoM"
        if growth_cell(last["mrr_growth"]) != "n/m"
        else verb
    )
    headline = (
        f"*{month_label(last)}* MRR {change} "
        f"to {money(last['ending_mrr'])} ({money(last['ending_mrr'] - last['beginning_mrr'])} net)."
    )
    if (
        prev
        and prev["mrr_growth"] is not None
        and (prev["mrr_growth"] >= 0) != (last["mrr_growth"] >= 0)
    ):
        headline += (
            f" Growth {'turned positive' if last['mrr_growth'] >= 0 else 'turned negative'} "
            f"(was {growth_cell(prev['mrr_growth'])} in {month_label(prev)})."
        )
    out.append(headline)

    drivers = {
        "new business": last["new_business_mrr"],
        "reactivation": last["reactivation_mrr"],
        "expansion": last["expansion_mrr"],
        "contraction": last["contraction_mrr"],
        "churn": last["churn_mrr"],
    }
    up_name, up = max(drivers.items(), key=lambda kv: kv[1])
    down_name, down = min(drivers.items(), key=lambda kv: kv[1])
    movers = []
    if up > 0:
        movers.append(f"Biggest gain: {up_name} {money(up)}")
    if down < 0:
        movers.append(f"biggest loss: {down_name} {money(down)}")
    baseline = _churn_spike_baseline(complete, last)
    spike = (
        f" Churn is above its prior {baseline[1]}-month average ({money(baseline[0])})."
        if baseline
        else ""
    )
    if movers:
        line = "; ".join(movers)
        out.append(line[0].upper() + line[1:] + "." + spike)

    gap = _unexplained_gap(last)
    if gap is not None:
        out.append(
            f":warning: Data check: movements explain only {money(last['movements_mrr'])} of the "
            f"{money(last['net_mrr_change'])} net change ({money(gap)} unaccounted for)."
        )

    partial = next((r for r in rows if r["is_partial"]), None)
    if partial and partial["beginning_mrr"] is not None:
        out.append(
            f"Month to date ({partial['data_through']:%b %d}): MRR {pct(partial['mrr_growth'])} "
            f"({money(partial['ending_mrr'] - partial['beginning_mrr'])}), month not complete."
        )
    return out


def build_next_steps(rows: list[dict]) -> list[str]:
    """Follow-ups that follow from the same rules as the highlights. Empty when nothing needs attention.

    No owners are named: this warehouse has no team-ownership data, so an @mention would be invented.
    """
    complete = [r for r in rows if not r["is_partial"] and r["mrr_growth"] is not None]
    if not complete:
        return []
    last = complete[-1]
    steps = []
    gap = _unexplained_gap(last)
    if gap is not None:
        steps.append(
            f"Data team to reconcile the {money(gap)} of MRR change the movements don't explain."
        )
    if _churn_spike_baseline(complete, last):
        steps.append(
            "Review what drove churn this month; it is above its recent average."
        )
    return steps


def _last_complete(rows: list[dict]) -> dict | None:
    done = [r for r in rows if not r["is_partial"] and r["mrr_growth"] is not None]
    return done[-1] if done else None


def _vs_last_month(value: float | None) -> str:
    """' (🔼 3.4% vs last month)'; empty when there is no prior month, 'n/m' off a near-zero base."""
    if value is None:
        return ""
    if abs(value) >= 10:
        return " (n/m vs last month)"
    return f" ({'🔼' if value >= 0 else '🔽'} {abs(value) * 100:.1f}% vs last month)"


def _big_picture(rows: list[dict]) -> str:
    """The template's 'The Big Picture' bullets: four lines, so Slack does not fold the section."""
    last = _last_complete(rows)
    base = (
        last or rows[-1]
    )  # a brand-new product has no completed month: show where it stands today

    def growth(key: str) -> float | None:
        return base[key] if last else None

    bullets = [
        f"\u2022 *Ending MRR:* {money(base['ending_mrr'])}{_vs_last_month(growth('mrr_growth'))}",
        (
            f"\u2022 *Paid subscriptions:* {count(base['ending_subscriptions'])}"
            f"{_vs_last_month(growth('subscription_growth'))}"
        ),
        (
            f"\u2022 *Paid subscribers:* {count(base['ending_subscribers'])}{_vs_last_month(growth('subscriber_growth'))}"
            f" \u2014 +{count(base['new_subscribers'])} new, +{count(base['reactivated_subscribers'])} reactivated, "
            f"-{count(base['churned_subscribers'])} churned"
        ),
        (
            f"\u2022 *MRR movements:* New {money(base['new_business_mrr'])}, Reactivation {money(base['reactivation_mrr'])}, "
            f"Expansion {money(base['expansion_mrr'])}, Contraction {money(base['contraction_mrr'])}, "
            f"Churn {money(base['churn_mrr'])}"
        ),
    ]
    return "*\U0001f4a1 The Big Picture*\n" + "\n".join(bullets)


def _reporting_period(rows: list[dict]) -> str:
    last = _last_complete(rows)
    partial = next((r for r in rows if r["is_partial"]), None)
    parts = []
    if last:
        parts.append(f"{last['month_start']:%b %Y} (last completed month)")
    if partial:
        parts.append(f"month to date through {partial['data_through']:%b %d}")
    elif not last:
        parts.append(f"data through {rows[-1]['data_through']:%b %d}")
    return (
        "_Reporting Period: "
        + " \u00b7 ".join(parts)
        + " \u00b7 * = month in progress_"
    )


def _chart_block(
    title: str,
    rows: list[dict],
    series: dict[str, str],
    *,
    chart_type: str,
    y_label: str,
) -> dict:
    """Slack data_visualization block. series maps legend name -> row key; labels must be <= 20 chars."""
    categories = [month_label(r) for r in rows]
    return {
        "type": "data_visualization",
        "title": title[:50],
        "chart": {
            "type": chart_type,
            "series": [
                {
                    "name": name[:20],
                    "data": [
                        {"label": c, "value": round(float(r[key] or 0))}
                        for c, r in zip(categories, rows, strict=True)
                    ],
                }
                for name, key in series.items()
            ],
            "axis_config": {
                "categories": categories,
                "x_label": "Month",
                "y_label": y_label,
            },
        },
    }


DEFINITIONS = (
    "Stripe + Solidgate accounts combined per product. Churn and contraction are negatives; "
    "net change = End - Begin. n/m = growth off a near-zero base. * = month in progress."
)


def _context(text: str) -> dict:
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}


def _trend_chart(rows: list[dict]) -> dict:
    return _chart_block(
        "Ending MRR (USD)",
        rows,
        {"Ending MRR": "ending_mrr"},
        chart_type="area",
        y_label="USD",
    )


def _movements_chart(rows: list[dict]) -> dict:
    return _chart_block(
        "MRR movements by month (USD)",
        rows,
        {
            "New": "new_business_mrr",
            "Reactivation": "reactivation_mrr",
            "Expansion": "expansion_mrr",
            "Contraction": "contraction_mrr",
            "Churn": "churn_mrr",
        },
        chart_type="bar",
        y_label="USD",
    )


def _section(text: str) -> dict:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def build_main_payload(product: str, rows: list[dict]) -> dict:
    """The management template: header, reporting period, big picture, trend chart, takeaways, next steps.

    The template's image slot holds a native Slack chart (no public image URL needed). The movements
    chart and the metric definitions go in the thread reply. Each section is kept short enough that
    Slack does not fold it behind "Show more".
    """
    title = f"\U0001f4ca WEEKLY BUSINESS INSIGHTS: {product} \U0001f4c8"
    blocks: list[dict] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": title[:150], "emoji": True},
        },
        _context(_reporting_period(rows)),
        _section(
            f"Hey team! Here is a quick breakdown of {product}'s performance trends and key takeaways."
        ),
        _section(_big_picture(rows)),
        _trend_chart(rows),
        _section(
            "*\U0001f50d Key Takeaways & Context*\n"
            + "\n".join(f"\u2022 {h}" for h in build_highlights(rows))
        ),
    ]
    steps = build_next_steps(rows)
    if steps:
        blocks.append(
            _section(
                "*\U0001f680 Next Steps / Action Items*\n"
                + "\n".join(f"{i}. {t}" for i, t in enumerate(steps, 1))
            )
        )
    return {"text": f"Weekly Business Insights: {product}", "blocks": blocks}


def build_thread_payload(product: str, rows: list[dict]) -> dict:
    """Thread reply: the movements chart plus the metric definitions."""
    return {
        "text": f"MRR movements: {product}",
        "blocks": [_movements_chart(rows), _context(DEFINITIONS)],
    }


SLACK_POST_URL = "https://slack.com/api/chat.postMessage"


def post_via_api(
    token: str, channel: str, payload: dict, *, thread_ts: str | None = None
) -> str:
    """chat.postMessage; returns the message ts. Slack answers HTTP 200 with ok=false on errors."""
    body = {
        "channel": channel,
        "text": payload["text"],
        "blocks": payload["blocks"],
        "unfurl_links": False,
        "unfurl_media": False,
    }
    if thread_ts:
        body["thread_ts"] = thread_ts
    req = urllib.request.Request(
        SLACK_POST_URL,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    if not data.get("ok"):
        raise RuntimeError(
            f"Slack chat.postMessage failed: {data.get('error')} {data.get('response_metadata', '')}"
        )
    if data.get("warning") or data.get("response_metadata"):
        print(
            f"Slack accepted the message with notes: warning={data.get('warning')} {data.get('response_metadata')}"
        )
    return data["ts"]


def _nothing_was_posted(err: Exception) -> bool:
    """True when the failure proves Slack did not post: it answered ok=false, or rate-limited (429)."""
    return isinstance(err, RuntimeError) or (
        isinstance(err, urllib.error.HTTPError) and err.code == 429
    )


def deliver(
    product: str, rows: list[dict], *, bot_token: str | None, channel: str | None
) -> None:
    """Post one product's report, then the movements chart and definitions in a thread under it."""
    if not (bot_token and channel):
        raise RuntimeError(
            f"No Slack destination for {product}: need a bot token and a channel ID"
        )
    try:
        ts = post_via_api(bot_token, channel, build_main_payload(product, rows))
    except Exception as err:  # noqa: BLE001 - classified below
        if _nothing_was_posted(err):
            raise  # safe for Airflow to retry
        # A timeout / connection drop / 5xx: Slack may have posted it and only the reply was lost. Slack has
        # no idempotency key, so a retry could post the report twice. Fail without retrying instead.
        raise AirflowFailException(
            f"Posting {product} failed ({err}); Slack may already have the message. "
            "Check the channel before clearing this task."
        ) from err
    print(f"Posted main message to {channel} (ts {ts}).")
    try:
        reply_ts = post_via_api(
            bot_token, channel, build_thread_payload(product, rows), thread_ts=ts
        )
        print(f"Posted charts in the thread (reply ts {reply_ts}, parent ts {ts}).")
    except Exception as err:  # noqa: BLE001 - the main message already stands; don't fail/retry the task and double-post it
        print(
            f"Charts thread reply failed ({err}); main message was posted without charts."
        )
