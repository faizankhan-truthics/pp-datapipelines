"""Red alerts for the billing health report: one thread reply per section under that section's
daily message, listing every product with a red, tagging the payments team once.

The ticket's alert rule: amber goes into the daily summary only (the section messages in
include/utils/billing_health_alerts.py and billing_health_retry_trial_alerts.py); red gets a
message tagging the payments team. A red is alerted when the SQL sets alert_now: Sections A and
B every day it is red, Section C the day the cohort becomes final. Section A's blocked, duplicate
and do-not-retry metrics are red only on a spike above the product's usual level (amber
otherwise).

Every red, cancellations before trial end included, tags only the payments team in
seed_billing_health_products.payments_owner_slack_id: one or more Slack member IDs separated by
spaces (Alex and Eugenia). They tag the product owner in the thread by hand when needed.
Tagging is switched on or off in include/config/billing_health_settings.py
(TAG_PAYMENTS_OWNERS); off, the reply is still posted and says tagging is off.

    🔴 *Tracelo* · 2 reds
    • Blocked by Stripe: *4.2%* (red above 2% and 1+ pt above its 7-day avg 2.4%), $9,120 blocked
    • Retries after "do not retry": *163* (red at 2x and 5+ above its 7-day avg 71.0)

    🔴 *Reversly* · 1 red
    • Duplicate attempts: *13.0%* (red above 10% and 5+ pts above its 7-day avg 0.2%)
    <@U...> <@U...> please take a look.
"""

from include.utils.billing_health_alerts import METRIC_LABELS, _pct, format_value

# (rule against the product's usual level, rule when it has none yet); see section_a_status.sql
A_RULES = {
    "approval_signups": ("red 10+ pts below {usual}", "red"),
    "approval_renewals_first_attempt": ("red 10+ pts below {usual}", "red"),
    "blocked_pct": ("red above 2% and 1+ pt above {usual}", "red above 2%"),
    "duplicate_pct": ("red above 10% and 5+ pts above {usual}", "red above 10%"),
    "retries_after_do_not_retry": (
        "red at 2x and 5+ above {usual}",
        "red if any, no usual level yet",
    ),
}


def _rule(r: dict) -> str:
    with_ref, without_ref = A_RULES.get(r["metric"], ("red", "red"))
    ref = r.get("ref_7d")
    if ref is None:
        return without_ref
    ref_text = (
        f"{ref:,.1f}" if r["metric"] == "retries_after_do_not_retry" else _pct(ref)
    )
    source = "its baseline" if r.get("ref_source") == "baseline" else "its 7-day avg"
    return with_ref.format(usual=f"{source} {ref_text}")


def _payment_reds(rows: list[dict]) -> list[tuple[dict, str]]:
    reds = []
    for r in sorted(
        (r for r in rows if r.get("metric") and r.get("alert_now")),
        key=lambda r: r["metric"],
    ):
        metric = r["metric"]
        rule = _rule(r)
        line = (
            f"{METRIC_LABELS.get(metric, metric)}: *{format_value(metric, r['value'])}* "
            f"({rule})"
        )
        if metric == "blocked_pct" and r.get("amount_usd") is not None:
            line += f", ${r['amount_usd']:,.0f} blocked"
        reds.append((r, line))
    return reds


def _retry_reds(rows: list[dict]) -> list[tuple[dict, str]]:
    return [
        (
            r,
            f"Failed after all retries: *{_pct(r['failed_after_all_retries_pct'])}* of "
            f"{int(r['first_charge_failures']):,} failed first charges "
            f"({r['cohort_start']} – {r['cohort_end']}); 4-week avg "
            f"{_pct(r.get('baseline'))}, red above +10 pts",
        )
        for r in rows
        if r.get("alert_now")
    ]


def _trial_reds(rows: list[dict]) -> list[tuple[dict, str]]:
    reds = []
    for r in sorted(rows, key=lambda r: r.get("trial_days") or 0):
        cohort = f"{r['trial_days']}-day trial, week of {r['cohort_week']}"
        if r.get("converted_alert_now"):
            reds.append(
                (
                    r,
                    f"{cohort}: total converted *{_pct(r['total_converted_pct'])}* "
                    f"(last 4 cohorts {_pct(r.get('converted_ref'))}, red 10+ pts below)",
                )
            )
        if r.get("cancelled_alert_now"):
            reds.append(
                (
                    r,
                    f"{cohort}: cancelled before trial end *{_pct(r['cancelled_pct'])}* "
                    f"(last 4 cohorts {_pct(r.get('cancelled_ref'))}, red 10+ pts above)",
                )
            )
    return reds


def _replies(reds: list[tuple[dict, str]], tag_owners: bool) -> list[str]:
    """One reply for the section: each product's reds as bullets under its name, and the
    payments team tagged once at the end (when `tag_owners`). Empty when nothing is new."""
    if not reds:
        return []
    by_product: dict[str, list[tuple[dict, str]]] = {}
    for red in reds:
        by_product.setdefault(red[0]["product_name"], []).append(red)
    owners, tagging_off, no_owner = [], False, []
    blocks = []
    for product, items in sorted(by_product.items()):
        product_ids = []
        for row, _ in items:
            for owner_id in (row.get("payments_owner_slack_id") or "").split():
                if owner_id not in product_ids:
                    product_ids.append(owner_id)
        if not product_ids:
            no_owner.append(product)
        elif not tag_owners:
            tagging_off = True
        else:
            owners += [o for o in product_ids if o not in owners]
        count = f"{len(items)} red{'s' if len(items) > 1 else ''}"
        blocks.append(
            "\n".join(
                [f"🔴 *{product}* · {count}"] + [f"• {line}" for _, line in items]
            )
        )
    lines = ["\n\n".join(blocks)]
    if owners:
        lines.append(" ".join(f"<@{o}>" for o in owners) + " please take a look.")
    elif tagging_off:
        lines.append("_Owner tagging is off._")
    if no_owner:
        lines.append(
            f"_No owner set in seed_billing_health_products for: {', '.join(no_owner)}._"
        )
    return ["\n".join(lines)]


def build_red_replies(
    status_rows: list[dict],
    retry_rows: list[dict],
    trial_rows: list[dict],
    tag_owners: bool = True,
) -> dict[str, list[str]]:
    """Thread replies per section ("payment", "retry", "trial"): at most one, covering every
    product with a red; empty when nothing is red. `tag_owners` tags the payments team."""
    return {
        "payment": _replies(_payment_reds(status_rows), tag_owners),
        "retry": _replies(_retry_reds(retry_rows), tag_owners),
        "trial": _replies(_trial_reds(trial_rows), tag_owners),
    }
