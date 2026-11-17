{{ config(
    materialized='table',
    cluster_by=['source_system', 'product_name', 'period_grain'],
    tags=["subscribers", "gold", "daily"],
) }}

-- Subscribers dashboard dataset: 'combined' (Stripe + Solidgate, deduplicated at subscriber
-- level, Paid wins over Free), plus the validated provider rows from stripe_subscribers and
-- solidgate_subscribers, at day/week/month/quarter/year grain.
-- Combined rows stop at the latest complete common provider date; provider rows keep their
-- own coverage. Stock metrics are end-of-period; movements and ASP inputs are period sums.
-- "All Products" is computed independently at subscriber level; never sum product rows.
-- Rebuilt in full on every dbt_dag run: identity re-keying, 15-day run bridging and trailing
-- churn windows all change history, so an incremental build would drift from a full one.

WITH periodized_input AS (
  SELECT
    period_grain,
    CASE period_grain
      WHEN 'day' THEN d.snapshot_date
      WHEN 'week' THEN DATE_TRUNC(d.snapshot_date, WEEK(MONDAY))
      WHEN 'month' THEN DATE_TRUNC(d.snapshot_date, MONTH)
      WHEN 'quarter' THEN DATE_TRUNC(d.snapshot_date, QUARTER)
      WHEN 'year' THEN DATE_TRUNC(d.snapshot_date, YEAR)
    END AS period_start,
    d.*
  FROM {{ ref('int_combined_subscribers_daily') }} d
  CROSS JOIN UNNEST(['day', 'week', 'month', 'quarter', 'year']) AS period_grain
),

period_rollup AS (
  SELECT
    product_name,
    period_grain,
    period_start,
    MIN(snapshot_date) AS period_data_start,
    MAX(snapshot_date) AS period_end,
    ARRAY_AGG(
      STRUCT(
        paid_subscribers,
        free_subscribers,
        all_subscribers,
        total_mrr_usd,
        arpa_usd,
        customer_churn_rate_6m_avg,
        paid_subscriber_churn_rate_6m_avg,
        paid_without_mrr_today,
        missing_fx_rows,
        unresolved_price_subscriptions,
        free_all_state_available
      )
      ORDER BY snapshot_date DESC
      LIMIT 1
    )[OFFSET(0)] AS eop,
    SUM(new_subscribers) AS new_subscribers,
    SUM(reactivated_subscribers) AS reactivated_subscribers,
    SUM(churned_subscribers) AS churned_subscribers,
    SUM(net_change) AS net_change,
    SUM(asp_new_customers) AS asp_new_customers,
    SUM(new_business_mrr_usd) AS new_business_mrr_usd,
    SUM(asp_missing_mrr_users) AS asp_missing_mrr_users
  FROM periodized_input
  GROUP BY 1,2,3
),

product_status AS (
  SELECT
    product_name,
    LOGICAL_OR(product_name_is_fallback) AS product_name_is_fallback
  FROM (
    SELECT product_name, product_name_is_fallback
    FROM {{ ref('stripe_subscribers') }}
    UNION ALL
    SELECT product_name, product_name_is_fallback
    FROM {{ ref('solidgate_subscribers') }}
  )
  GROUP BY 1
)

SELECT
  'combined' AS source_system,
  r.product_name,
  r.period_grain,
  r.period_start,
  r.period_data_start,
  r.period_end,
  r.eop.paid_subscribers,
  r.eop.free_subscribers,
  r.eop.all_subscribers,
  r.new_subscribers,
  r.reactivated_subscribers,
  r.churned_subscribers,
  r.net_change,
  r.eop.total_mrr_usd,
  r.eop.arpa_usd,
  r.new_business_mrr_usd,
  CASE
    WHEN r.asp_new_customers = 0 THEN NULL
    WHEN r.asp_missing_mrr_users > 0 THEN NULL
    ELSE SAFE_DIVIDE(r.new_business_mrr_usd, r.asp_new_customers)
  END AS asp_usd,
  r.eop.customer_churn_rate_6m_avg,
  CASE
    WHEN r.eop.customer_churn_rate_6m_avg > 0
    THEN SAFE_DIVIDE(r.eop.arpa_usd, r.eop.customer_churn_rate_6m_avg)
    ELSE NULL
  END AS ltv_usd,
  r.eop.paid_subscriber_churn_rate_6m_avg,
  CASE
    WHEN r.period_grain = 'month'
     AND r.eop.paid_subscriber_churn_rate_6m_avg > 0
    THEN SAFE_DIVIDE(1, r.eop.paid_subscriber_churn_rate_6m_avg)
    ELSE NULL
  END AS acl_months,
  r.eop.paid_without_mrr_today,
  r.eop.missing_fx_rows,
  r.asp_missing_mrr_users,
  r.eop.unresolved_price_subscriptions,
  TRUE AS free_is_proxy,
  CASE
    WHEN r.product_name = 'All Products' THEN FALSE
    ELSE COALESCE(s.product_name_is_fallback, FALSE)
  END AS product_name_is_fallback,
  r.eop.free_all_state_available
FROM period_rollup r
LEFT JOIN product_status s
  ON r.product_name = s.product_name

UNION ALL

SELECT
  source_system,
  product_name,
  period_grain,
  period_start,
  period_data_start,
  period_end,
  paid_subscribers,
  free_subscribers,
  all_subscribers,
  new_subscribers,
  reactivated_subscribers,
  churned_subscribers,
  net_change,
  total_mrr_usd,
  arpa_usd,
  new_business_mrr_usd,
  asp_usd,
  customer_churn_rate_6m_avg,
  ltv_usd,
  paid_subscriber_churn_rate_6m_avg,
  acl_months,
  paid_without_mrr_today,
  missing_fx_rows,
  asp_missing_mrr_users,
  unresolved_price_subscriptions,
  free_is_proxy,
  product_name_is_fallback,
  free_all_state_available
FROM {{ ref('stripe_subscribers') }}

UNION ALL

SELECT
  source_system,
  product_name,
  period_grain,
  period_start,
  period_data_start,
  period_end,
  paid_subscribers,
  free_subscribers,
  all_subscribers,
  new_subscribers,
  reactivated_subscribers,
  churned_subscribers,
  net_change,
  total_mrr_usd,
  arpa_usd,
  new_business_mrr_usd,
  asp_usd,
  customer_churn_rate_6m_avg,
  ltv_usd,
  paid_subscriber_churn_rate_6m_avg,
  acl_months,
  paid_without_mrr_today,
  missing_fx_rows,
  asp_missing_mrr_users,
  CAST(NULL AS INT64) AS unresolved_price_subscriptions,
  free_is_proxy,
  product_name_is_fallback,
  free_subscribers IS NOT NULL AS free_all_state_available
FROM {{ ref('solidgate_subscribers') }}
