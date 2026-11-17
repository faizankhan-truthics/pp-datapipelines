# gcp-budget-slack-alert

Small Pub/Sub-triggered Cloud Function that forwards GCP Billing Budget
alert notifications to Slack. Bridges the gap between GCP Budgets (which
only natively support email) and Slack, via: Budget -> Pub/Sub topic ->
this function -> Slack Incoming Webhook.

## How it's wired up

1. A GCP Budget has "Connect a Pub/Sub topic" enabled, pointed at a topic
   (e.g. `gcp-billing-budget-alerts`).
2. Whenever a budget threshold is crossed, GCP publishes a notification
   message to that topic.
3. This function is subscribed to that topic and fires on every message.
4. It decodes the message, formats a readable summary, and POSTs it to a
   Slack Incoming Webhook URL.

## Environment variables

- `SLACK_WEBHOOK_URL` -- the Slack Incoming Webhook URL to post to. Must be
  set via Secret Manager reference at deploy time, never as a plaintext env
  var (see Deploy below).
- `BQ_PROJECT_ID` -- GCP project the state table lives in (e.g. `data-499514`).
- `BQ_MONITORING_DATASET` -- dataset the state table lives in (prod: `monitoring`,
  same dataset `include/monitoring/api_request_monitor.py` writes `api_requests`
  to).

## Deduplicating repeated threshold notifications

GCP republishes a Pub/Sub message carrying the *same* `alertThresholdExceeded`
value repeatedly (every ~20-40 min) for as long as spend stays above that
threshold -- confirmed 2026-10-22, not a one-time-per-crossing event the way
you'd expect. Without dedup this means a Slack message every ~20-40 minutes
once a budget crosses 50%, not just once.

This function tracks the highest threshold already notified on, per budget
per billing period, in a tiny BigQuery table (`budget_alert_state`) in the
existing prod monitoring dataset -- one row per
`(billing_account_id, budget_id, cost_interval_start)`. Keyed by
`billing_account_id`/`budget_id` rather than the budget's display name --
`budgetDisplayName` is a mutable, non-unique label (two budgets can share one,
and renaming a budget would otherwise silently lose its dedup history), while
`billingAccountId`/`budgetId` are immutable identifiers carried as Pub/Sub
message *attributes* (not the JSON data payload) -- see
[GCP's notification format docs](https://cloud.google.com/billing/docs/how-to/budgets-programmatic-notifications).
`budget_name` is still stored, but for display/debugging only, not as part of
the key. A new billing period naturally gets a fresh row (no manual reset
needed). The write is a single atomic `MERGE` statement using
`GREATEST(T.last_notified_threshold, S.last_notified_threshold)` on conflict
(so the recorded high-water mark can only go up, never roll back if a delayed
lower-threshold message is processed after a higher one already landed), and
is only run *after* the Slack POST succeeds, so a Slack failure never leaves
the table claiming an alert went out when it didn't. Cost is negligible:
single-row point queries on a tiny table, well within the on-demand free
tier, and far under BigQuery's per-table DML quota at this function's volume
(at most a handful of writes a day).

**Prerequisite**: create the state table (one-time) and grant the function's
service account access to query/write it and run jobs in the project. If the
table already exists from an earlier deploy (keyed by `budget_name` instead
of `billing_account_id`/`budget_id`), drop it first -- there's no
self-healing schema migration for this standalone table the way there is for
the Airflow-managed bronze tables elsewhere in this repo:

```bash
bq rm -f --table <project-id>:<monitoring-dataset>.budget_alert_state

bq query --use_legacy_sql=false --project_id=<project-id> \
'CREATE TABLE IF NOT EXISTS `<project-id>.<monitoring-dataset>.budget_alert_state` (
  billing_account_id STRING NOT NULL,
  budget_id STRING NOT NULL,
  cost_interval_start STRING NOT NULL,
  budget_name STRING,
  last_notified_threshold FLOAT64 NOT NULL,
  updated_at TIMESTAMP NOT NULL
)'

gcloud projects add-iam-policy-binding <project-id> \
  --member="serviceAccount:<function-service-account>" \
  --role="roles/bigquery.jobUser"

bq add-iam-policy-binding \
  --member="serviceAccount:<function-service-account>" \
  --role="roles/bigquery.dataEditor" \
  <project-id>:<monitoring-dataset>.budget_alert_state
```

## Deploy

```bash
gcloud functions deploy gcp-budget-slack-alert \
  --gen2 \
  --runtime=python312 \
  --region=us-central1 \
  --source=. \
  --entry-point=notify_slack \
  --trigger-topic=gcp-billing-budget-alerts \
  --set-secrets=SLACK_WEBHOOK_URL=gcp-budget-slack-webhook-url:latest \
  --set-env-vars=BQ_PROJECT_ID=<project-id>,BQ_MONITORING_DATASET=<monitoring-dataset>
```