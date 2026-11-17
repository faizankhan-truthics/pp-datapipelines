{{ config(
    materialized='table',
    tags=["subscribers", "silver", "daily"],
) }}

-- One-row table of the coverage dates shared by every combined_subscribers model, so the
-- provider MAX(snapshot_date) scans run once per build instead of once per stage.
--   stripe_max_date:               Stripe paid/MRR coverage (T-1).
--   solidgate_max_date:            Solidgate paid/MRR coverage (T-2 BI safety lag).
--   solidgate_free_state_max_date: Solidgate free state, also capped by captured history.
--   max_date:                      common complete date for combined rows.

WITH source_dates AS (
  SELECT
    LEAST(
      (SELECT MAX(snapshot_date)
       FROM {{ ref('int_stripe_mrr_customer_daily') }}
       WHERE mrr_local > 0),
      DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    ) AS stripe_max_date,
    LEAST(
      (SELECT MAX(snapshot_date)
       FROM {{ ref('int_solidgate_mrr_customer_daily') }}
       WHERE mrr_local > 0),
      DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
    ) AS solidgate_max_date,
    (SELECT MAX(DATE(captured_at))
     FROM {{ ref('solidgate_subscription_history') }}) AS solidgate_history_max_date
)

SELECT
  DATE '2025-08-12' AS dashboard_start,
  stripe_max_date,
  solidgate_max_date,
  LEAST(solidgate_max_date, solidgate_history_max_date) AS solidgate_free_state_max_date,
  LEAST(stripe_max_date, solidgate_max_date) AS max_date
FROM source_dates
