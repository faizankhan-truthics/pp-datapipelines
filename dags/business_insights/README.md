# Business insights (weekly, per product, Slack)

DAG `product_business_insights` reads the gold marts and posts one rule-based report per product to Slack.
No AI is involved: every sentence is derived by rule from mart rows, so each number traces back to a model.
Created **paused**; schedule is Mondays 08:00 UTC (to be confirmed with the team).

## What a report contains
The main message follows the management Block Kit template: https://app.slack.com/block-kit-builder/T0A3LSAA0Q5/builder#%7B%22blocks%22:%5B%7B%22type%22:%22header%22,%22text%22:%7B%22type%22:%22plain_text%22,%22text%22:%22%F0%9F%93%8A%20WEEKLY%20BUSINESS%20INSIGHTS:%20Q4%20Metrics%20%F0%9F%93%88%22,%22emoji%22:true%7D%7D,%7B%22type%22:%22context%22,%22elements%22:%5B%7B%22type%22:%22mrkdwn%22,%22text%22:%22_Reporting%20Period:%20Oct%2026%20-%20Nov%2001_%22%7D%5D%7D,%7B%22type%22:%22section%22,%22text%22:%7B%22type%22:%22mrkdwn%22,%22text%22:%22Hey%20team!%20Here%20is%20a%20quick%20breakdown%20of%20our%20performance%20trends%20and%20key%20takeaways%20from%20this%20past%20week.%5Cn%5Cn*%F0%9F%92%A1%20The%20Big%20Picture*%5Cn%E2%80%A2%20*Metric%201:*%203.4%25%20(%F0%9F%94%BC%200.6%25%20vs%20last%20week)%5Cn%E2%80%A2%20*Metric%202:*%201,240%20signups%22%7D%7D,%7B%22type%22:%22image%22,%22title%22:%7B%22type%22:%22plain_text%22,%22text%22:%22Weekly%20Performance%20Trend%20Chart%22,%22emoji%22:true%7D,%22image_url%22:%22https://www.cdc.gov/cove/media/images/2024/09/Bar-Chart-01.png%22,%22alt_text%22:%22Business%20insights%20trend%20chart%22%7D,%7B%22type%22:%22section%22,%22text%22:%7B%22type%22:%22mrkdwn%22,%22text%22:%22*%F0%9F%94%8D%20Key%20Takeaways%20&%20Context*%5Cn%E2%80%A2%20*Observation:*%20The%20chart%20highlights%20a%20significant%20traffic%20surge%20on%20Wednesday.%5Cn%5Cn*%F0%9F%9A%80%20Next%20Steps%20/%20Action%20Items*%5Cn1.%20*%20@marketing%20*%20to%20investigate%20high-performing%20segments.%22%7D%7D%5D%7D

The **thread reply** holds the MRR-movements chart and the metric definitions.

## Metric definitions
Mirrors the Executive "Growth Rate Product Dashboard" logic.
- **MRR waterfall / customers:** `gold.{solidgate,stripe}_mrr_waterfall_daily`, summed per month over the product's accounts.
- **Ending MRR / subscriptions:** last day of the month in `gold.mrr_arr_daily`; beginning = previous month's ending.
- **Churn and contraction are already negative in the marts.** Net MRR change = ending - beginning (the dashboard's
  *Net MRR Change* column subtracts them again, which is a known sign bug on its side).
- The current month is partial (marked `*`). History is restated as Stripe subscription statuses change, so
  past months can move slightly between runs.
- Growth over 1000% shows as `n/m` (near-zero base).

## Products = sets of PSP accounts
A product is a hand-maintained set of accounts, because that is how the dashboards filter.
Edit `PRODUCT_ACCOUNT_GROUPS` in `include/config/business_insights_settings.py`. Accounts claimed there are removed
from the data-derived products (`dim_product_payment_map`), so nothing is reported twice. Only products with live MRR
are reported.

## Delivery and configuration (env vars)
There is no test/live switch: dev and prod run the same code and differ only by the env vars each deployment is
given, so point dev at a test channel and prod at the real one.

| Variable | Purpose |
|---|---|
| `BUSINESS_INSIGHTS_SLACK_BOT_TOKEN` | Bot token with `chat:write` (the bot must be in the channel). Enables threaded charts. |
| `BUSINESS_INSIGHTS_CHANNEL_ID` | Channel **ID** (not name) that every product posts to. |
| `BUSINESS_INSIGHTS_CHANNEL_ID__<PRODUCT>` | Optional per-product override, e.g. `..._CHANNEL_ID__LOCATION_TOOL`. Wins over the shared one. |

Give dev and prod **different** channel IDs, or both will post the same report to the same place.

A product with no destination (no own channel and no shared one) is skipped (logged), not failed. If Slack rejects the main post (`ok:false`) or rate-limits it, the task fails
and retries. If the call times out or drops, Slack may already have the message and has no idempotency key, so the task
fails *without* retrying (check the channel, then clear the task). If only the thread reply fails, the main message
stays and the failure is logged (no double post).
The dataset read is `AIRFLOW_VAR_BQ_GOLD_DATASET`; the DAG only reads.

## Run locally
```
astro dev pytest tests/include/utils/test_business_insights.py tests/include/config/test_business_insights_settings.py tests/dags/business_insights
```
Then trigger `product_business_insights` from the Airflow UI (it posts one report per product to the channel in your local `.env`).
