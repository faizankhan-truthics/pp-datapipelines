import base64
import json
import os
import urllib.request

import functions_framework
from google.cloud import bigquery

SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]

# Tiny BigQuery state table -- not a real warehouse model, just enough to remember "what's
# the highest threshold we've already alerted on, for this budget, this billing period"
# across invocations (Cloud Functions have no memory between calls otherwise). One row per
# (budget_name, cost_interval_start); the cost_interval_start in the key means a new billing
# period naturally gets a fresh row, no manual reset needed. Client is created lazily/
# module-level so it's reused (not reconnected) across warm invocations of the same instance.
BQ_PROJECT_ID = os.environ["BQ_PROJECT_ID"]
BQ_MONITORING_DATASET = os.environ["BQ_MONITORING_DATASET"]
_STATE_TABLE = f"{BQ_PROJECT_ID}.{BQ_MONITORING_DATASET}.budget_alert_state"
_bq = bigquery.Client(project=BQ_PROJECT_ID)


def _format_message(budget: dict) -> str:
    """GCP Billing Budget notification payload -> a readable Slack message.

    Only called for messages that actually crossed a threshold -- see the
    is_threshold_crossing() check in notify_slack(). Payload shape (from
    GCP's own docs): budgetDisplayName, costAmount, budgetAmount,
    budgetAmountType, alertThresholdExceeded (fraction, e.g. 0.9 for 90%),
    currencyCode, costIntervalStart.
    """
    name = budget.get("budgetDisplayName", "Unknown budget")
    cost = budget.get("costAmount", 0)
    amount = budget.get("budgetAmount", 0)
    currency = budget.get("currencyCode", "USD")
    threshold_pct = budget["alertThresholdExceeded"] * 100

    return (
        f":rotating_light: *GCP Budget Alert: {name}*\n"
        f"Threshold reached: *{threshold_pct:.0f}%*\n"
        f"Spend so far: *{cost:,.2f} {currency}* of *{amount:,.2f} {currency}* budget"
    )


def is_threshold_crossing(budget: dict) -> bool:
    """GCP publishes routine cost-sync messages to the same Pub/Sub topic
    that aren't tied to an actual threshold crossing -- confirmed 2026-09-30:
    repeated messages arrived every ~30-40 minutes with alertThresholdExceeded
    absent entirely, spend not matching any configured threshold. Only a
    message that actually carries this field represents a real threshold
    crossing worth alerting on; everything else should be silently dropped."""
    return budget.get("alertThresholdExceeded") is not None


def _budget_key(budget: dict, attributes: dict) -> tuple[str, str, str]:
    """Keyed by billingAccountId + budgetId -- both immutable identifiers carried as
    Pub/Sub message *attributes* (not the JSON data payload), confirmed against GCP's docs
    (https://cloud.google.com/billing/docs/how-to/budgets-programmatic-notifications).
    budgetDisplayName is a mutable, non-unique label -- using it as the key meant two
    budgets sharing a display name could silently share/clobber each other's dedup state,
    and renaming a budget would silently lose its dedup history. Kept for presentation
    only now, not as part of the key."""
    billing_account_id = str(attributes.get("billingAccountId", "unknown"))
    budget_id = str(attributes.get("budgetId", "unknown"))
    interval_start = str(budget.get("costIntervalStart", "unknown"))
    return billing_account_id, budget_id, interval_start


def _rounded_threshold(budget: dict) -> float:
    # Guards against any floating-point serialization drift making an identical
    # threshold compare as "different" (e.g. 0.5 vs 0.5000000001) -- 4 decimal places is
    # far more precision than a whole-percent Slack message needs.
    return round(budget["alertThresholdExceeded"], 4)


def already_notified_for_threshold(budget: dict, attributes: dict) -> bool:
    """Read-only check: True if we've already Slack-notified for this threshold (or a
    higher one) for this budget's current billing period -- i.e. this message is a
    routine GCP re-send of an already-handled threshold crossing, confirmed 2026-10-22:
    GCP republishes a message with the same alertThresholdExceeded value repeatedly
    (every ~20-40 min) as long as spend stays above it, not just once per actual crossing.

    Deliberately read-only -- see record_notified_threshold() for why recording must
    happen only after a successful Slack send, not here.
    """
    billing_account_id, budget_id, interval_start = _budget_key(budget, attributes)
    query = f"""
        SELECT last_notified_threshold
        FROM `{_STATE_TABLE}`
        WHERE billing_account_id = @billing_account_id
          AND budget_id = @budget_id
          AND cost_interval_start = @interval_start
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "billing_account_id", "STRING", billing_account_id
            ),
            bigquery.ScalarQueryParameter("budget_id", "STRING", budget_id),
            bigquery.ScalarQueryParameter("interval_start", "STRING", interval_start),
        ]
    )
    rows = list(_bq.query(query, job_config=job_config).result())
    if not rows:
        return False
    last_notified = rows[0]["last_notified_threshold"]
    return last_notified is not None and _rounded_threshold(budget) <= last_notified


def record_notified_threshold(budget: dict, attributes: dict) -> None:
    """Record this threshold as the new high-water mark. Call ONLY after the Slack
    notification has actually been sent successfully -- if this ran before the Slack
    POST and the POST then failed, the table would already say "notified" and every
    subsequent retry/re-send would silently skip re-sending it. A rare duplicate Slack
    message (if two messages race each other between the SELECT above and this MERGE) is
    an acceptable tradeoff for never silently losing a real alert.

    Uses MERGE rather than a separate exists-check + INSERT/UPDATE -- it's a single atomic
    DML statement, so two concurrent invocations can't both see "no row yet" and both try
    to INSERT (which would violate the one-row-per-key intent, though BigQuery has no
    unique-key constraint to enforce it anyway).

    The matched-update branch uses GREATEST(T, S) rather than a flat overwrite -- if a
    delayed lower-threshold message (e.g. 50%) is processed after a higher one (e.g. 90%)
    already recorded, a flat overwrite would roll the high-water mark back down, causing a
    spurious duplicate 90% alert on the next routine resend. GREATEST makes the recorded
    threshold monotonically non-decreasing regardless of processing order.
    """
    billing_account_id, budget_id, interval_start = _budget_key(budget, attributes)
    threshold = _rounded_threshold(budget)
    budget_name = str(budget.get("budgetDisplayName", "unknown"))
    query = f"""
        MERGE `{_STATE_TABLE}` T
        USING (
            SELECT @billing_account_id AS billing_account_id, @budget_id AS budget_id,
                   @interval_start AS cost_interval_start, @budget_name AS budget_name,
                   @threshold AS last_notified_threshold
        ) S
        ON T.billing_account_id = S.billing_account_id
          AND T.budget_id = S.budget_id
          AND T.cost_interval_start = S.cost_interval_start
        WHEN MATCHED THEN
          UPDATE SET last_notified_threshold = GREATEST(T.last_notified_threshold, S.last_notified_threshold),
                     budget_name = S.budget_name,
                     updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
          INSERT (billing_account_id, budget_id, cost_interval_start, budget_name, last_notified_threshold, updated_at)
          VALUES (S.billing_account_id, S.budget_id, S.cost_interval_start, S.budget_name, S.last_notified_threshold, CURRENT_TIMESTAMP())
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "billing_account_id", "STRING", billing_account_id
            ),
            bigquery.ScalarQueryParameter("budget_id", "STRING", budget_id),
            bigquery.ScalarQueryParameter("interval_start", "STRING", interval_start),
            bigquery.ScalarQueryParameter("budget_name", "STRING", budget_name),
            bigquery.ScalarQueryParameter("threshold", "FLOAT64", threshold),
        ]
    )
    _bq.query(query, job_config=job_config).result()


@functions_framework.cloud_event
def notify_slack(cloud_event) -> None:
    """Pub/Sub-triggered Cloud Function (Gen2). Entry point configured via
    --entry-point=notify_slack at deploy time. The decorator is required --
    without it, functions-framework falls back to the legacy (data, context)
    background-function signature instead of calling with one CloudEvent arg.

    Uses print() rather than the logging module for its diagnostic output --
    confirmed 2026-09-30 that logger.info() calls here weren't showing up
    anywhere in Cloud Logging (likely the runtime pre-configures its own
    root logger handler, making logging.basicConfig() a silent no-op).
    print() always lands in the request's stdout log, no such ambiguity.
    """
    message = cloud_event.data["message"]
    data = message["data"]
    attributes = message.get("attributes", {})
    budget = json.loads(base64.b64decode(data).decode("utf-8"))
    print(f"Received budget notification: {budget} (attributes: {attributes})")

    if not is_threshold_crossing(budget):
        print("Not a threshold crossing (routine sync) -- skipping Slack notification")
        return

    if already_notified_for_threshold(budget, attributes):
        print(
            f"Already notified for threshold {budget['alertThresholdExceeded']} "
            "(or higher) this billing period -- skipping duplicate Slack notification"
        )
        return

    payload = json.dumps({"text": _format_message(budget)}).encode("utf-8")
    req = urllib.request.Request(
        SLACK_WEBHOOK_URL,
        data=payload,
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req) as response:
        print(f"Slack responded with status {response.status}")

    # Only recorded once the Slack send above has actually succeeded -- see
    # record_notified_threshold()'s docstring for why.
    record_notified_threshold(budget, attributes)
