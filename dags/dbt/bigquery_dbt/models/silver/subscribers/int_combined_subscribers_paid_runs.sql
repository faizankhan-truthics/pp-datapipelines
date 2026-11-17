{{ config(
    materialized='table',
    cluster_by=['product_name', 'subscriber_key'],
    tags=["subscribers", "silver", "daily"],
) }}

-- Combined Stripe + Solidgate paid runs per product (and 'All Products') x subscriber.
-- subscriber_key is user:<user_key> when the identity map resolves the customer; unmapped
-- Stripe customers fall back to stripe:<account_id>:<customer_id>, unmapped Solidgate
-- customers are dropped. A run bridges paid-day gaps <= 15 days; churn_date = last paid + 1.
-- The paid-day set is only used to build runs, so it stays a CTE and is never materialized.

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

provider_paid_days AS (
  SELECT DISTINCT
    m.snapshot_date,
    COALESCE(pm.product_name, m._elt_stripe_account_id) AS product_name,
    COALESCE(
      CONCAT('user:', CAST(i.user_key AS STRING)),
      CONCAT('stripe:', m._elt_stripe_account_id, ':', m.customer)
    ) AS subscriber_key
  FROM {{ ref('int_stripe_mrr_customer_daily') }} m
  LEFT JOIN stripe_identity i
    ON m._elt_stripe_account_id = i.account_id
   AND m.customer = i.customer_id
  LEFT JOIN stripe_product_map pm
    ON m._elt_stripe_account_id = pm.account_id
  CROSS JOIN params p
  WHERE m.mrr_local > 0
    AND m.snapshot_date <= p.stripe_max_date

  UNION ALL

  SELECT DISTINCT
    m.snapshot_date,
    COALESCE(pm.product_name, m._elt_account_name) AS product_name,
    CONCAT('user:', CAST(i.user_key AS STRING)) AS subscriber_key
  FROM {{ ref('int_solidgate_mrr_customer_daily') }} m
  JOIN solidgate_identity i
    ON m._elt_account_name = i.account_name
   AND m.customer_account_id = i.customer_id
  LEFT JOIN solidgate_product_map pm
    ON m._elt_account_name = pm.account_name
  CROSS JOIN params p
  WHERE m.mrr_local > 0
    AND m.snapshot_date <= p.solidgate_max_date
),

product_paid_days AS (
  SELECT DISTINCT
    snapshot_date,
    product_name,
    subscriber_key
  FROM provider_paid_days
),

paid_days AS (
  SELECT snapshot_date, product_name, subscriber_key
  FROM product_paid_days

  UNION ALL

  SELECT DISTINCT
    snapshot_date,
    'All Products' AS product_name,
    subscriber_key
  FROM product_paid_days
),

sequenced AS (
  SELECT
    product_name,
    subscriber_key,
    snapshot_date,
    LAG(snapshot_date) OVER (
      PARTITION BY product_name, subscriber_key
      ORDER BY snapshot_date
    ) AS prev_paid_date
  FROM paid_days
),

numbered AS (
  SELECT
    *,
    SUM(
      CASE
        WHEN prev_paid_date IS NULL
          OR DATE_DIFF(snapshot_date, prev_paid_date, DAY) > 15
        THEN 1 ELSE 0
      END
    ) OVER (
      PARTITION BY product_name, subscriber_key
      ORDER BY snapshot_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS run_id
  FROM sequenced
),

runs_raw AS (
  SELECT
    product_name,
    subscriber_key,
    run_id,
    MIN(snapshot_date) AS run_start,
    MAX(snapshot_date) AS run_last_paid
  FROM numbered
  GROUP BY 1,2,3
)

SELECT
  product_name,
  subscriber_key,
  run_id,
  run_start,
  run_last_paid,
  ROW_NUMBER() OVER (
    PARTITION BY product_name, subscriber_key
    ORDER BY run_start
  ) AS run_number,
  MIN(run_start) OVER (
    PARTITION BY product_name, subscriber_key
  ) AS first_paid_date,
  DATE_ADD(run_last_paid, INTERVAL 1 DAY) AS churn_date
FROM runs_raw
