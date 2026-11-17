{{ config(
    materialized='table',
    tags=["subscribers", "silver", "daily"],
) }}

-- Combined Stripe + Solidgate paid MRR (USD) per product (and 'All Products') x day, from
-- dashboard_start to each provider's own coverage date. Same subscriber_key rules as
-- int_combined_subscribers_paid_runs. missing_fx_rows counts MRR rows without a USD rate.
-- The subscriber x day MRR is never stored: alongside the day totals, first_run_starts keeps
-- the per-subscriber MRR only for subscribers whose first paid run starts that day (ASP input).

WITH params AS (
  SELECT * FROM {{ ref('int_combined_subscribers_params') }}
),

stripe_product_map AS (
  SELECT DISTINCT
    stripe_account_id AS account_id,
    product_name
  FROM {{ ref('dim_product_payment_map') }}
  WHERE LOWER(payment_provider) = 'stripe'
    AND stripe_account_id IS NOT NULL
),

solidgate_product_map AS (
  SELECT
    solidgate_account_name AS account_name,
    COALESCE(product_name, solidgate_account_name) AS product_name
  FROM {{ ref('dim_product_payment_map') }}
  WHERE LOWER(payment_provider) = 'solidgate'
    AND solidgate_account_name IS NOT NULL
),

stripe_identity AS (
  SELECT
    source_account AS account_id,
    source_customer_id AS customer_id,
    user_key
  FROM {{ ref('int_identity_user_map') }}
  WHERE LOWER(source_system) = 'stripe'
    AND user_key IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY source_account, source_customer_id
    ORDER BY _elt_update_timestamp DESC
  ) = 1
),

solidgate_identity AS (
  SELECT DISTINCT
    source_account AS account_name,
    source_customer_id AS customer_id,
    user_key
  FROM {{ ref('int_identity_user_map') }}
  WHERE LOWER(source_system) = 'solidgate'
    AND user_key IS NOT NULL
),

provider_mrr AS (
  SELECT
    m.snapshot_date,
    COALESCE(pm.product_name, m._elt_stripe_account_id) AS product_name,
    COALESCE(
      CONCAT('user:', CAST(i.user_key AS STRING)),
      CONCAT('stripe:', m._elt_stripe_account_id, ':', m.customer)
    ) AS subscriber_key,
    SUM(m.mrr_usd) AS mrr_usd,
    COUNTIF(m.mrr_usd IS NULL) AS missing_fx_rows
  FROM {{ ref('int_stripe_mrr_customer_daily') }} m
  LEFT JOIN stripe_identity i
    ON m._elt_stripe_account_id = i.account_id
   AND m.customer = i.customer_id
  LEFT JOIN stripe_product_map pm
    ON m._elt_stripe_account_id = pm.account_id
  CROSS JOIN params p
  WHERE m.mrr_local > 0
    AND m.snapshot_date BETWEEN p.dashboard_start AND p.stripe_max_date
  GROUP BY 1,2,3

  UNION ALL

  SELECT
    m.snapshot_date,
    COALESCE(pm.product_name, m._elt_account_name) AS product_name,
    CONCAT('user:', CAST(i.user_key AS STRING)) AS subscriber_key,
    SUM(m.mrr_usd) AS mrr_usd,
    COUNTIF(m.mrr_usd IS NULL) AS missing_fx_rows
  FROM {{ ref('int_solidgate_mrr_customer_daily') }} m
  JOIN solidgate_identity i
    ON m._elt_account_name = i.account_name
   AND m.customer_account_id = i.customer_id
  LEFT JOIN solidgate_product_map pm
    ON m._elt_account_name = pm.account_name
  CROSS JOIN params p
  WHERE m.mrr_local > 0
    AND m.snapshot_date BETWEEN p.dashboard_start AND p.solidgate_max_date
  GROUP BY 1,2,3
),

product_rows AS (
  SELECT
    snapshot_date,
    product_name,
    subscriber_key,
    SUM(mrr_usd) AS mrr_usd,
    SUM(missing_fx_rows) AS missing_fx_rows
  FROM provider_mrr
  GROUP BY 1,2,3
),

scoped AS (
  SELECT * FROM product_rows

  UNION ALL

  SELECT
    snapshot_date,
    'All Products' AS product_name,
    subscriber_key,
    SUM(mrr_usd) AS mrr_usd,
    SUM(missing_fx_rows) AS missing_fx_rows
  FROM product_rows
  GROUP BY 1,2,3
),

first_runs AS (
  SELECT product_name, subscriber_key, run_start
  FROM {{ ref('int_combined_subscribers_paid_runs') }}
  WHERE run_number = 1
)

-- (date, product, subscriber) is unique in scoped, so COUNT(*) = distinct subscribers.
SELECT
  m.product_name,
  m.snapshot_date,
  COUNT(*) AS observed_mrr_users,
  SUM(m.mrr_usd) AS total_mrr_usd,
  SUM(m.missing_fx_rows) AS missing_fx_rows,
  ARRAY_AGG(
    IF(
      f.subscriber_key IS NOT NULL,
      STRUCT(m.subscriber_key, m.mrr_usd, m.missing_fx_rows),
      NULL
    )
    IGNORE NULLS
  ) AS first_run_starts
FROM scoped m
LEFT JOIN first_runs f
  ON m.product_name = f.product_name
 AND m.subscriber_key = f.subscriber_key
 AND m.snapshot_date = f.run_start
GROUP BY 1,2
