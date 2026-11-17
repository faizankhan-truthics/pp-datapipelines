{{ config(
    materialized='table',
    tags=["stripe", "gold", "daily"],
) }}

-- Product-aware Stripe subscriber reporting (paid / free / all subscribers, movements,
-- ARPA, ASP, LTV, ACL) at day/week/month/quarter/year grain.
-- "All Products" is computed independently at subscriber level; never sum product rows.
-- Free/All are reconstructed from retained Stripe event history and are NULL / paid-only
-- outside the window covered by every active Stripe account. Paid runs bridge gaps <= 15 days.
-- Rebuilt in full on every dbt_dag run (window/interval logic spans full history).

WITH params AS (
  SELECT
    DATE '2025-08-12' AS dashboard_start,
    LEAST(
      (
        SELECT MAX(snapshot_date)
        FROM {{ ref('int_stripe_mrr_customer_daily') }}
        WHERE mrr_local > 0
      ),
      DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    ) AS max_date
),

-- ============================================================
-- 1. PRODUCT MAPPING
-- ============================================================

product_map AS (
  SELECT DISTINCT
    stripe_account_id AS account_id,
    product_name
  FROM {{ ref('dim_product_payment_map') }}
  WHERE LOWER(payment_provider) = 'stripe'
    AND stripe_account_id IS NOT NULL
),

-- ============================================================
-- 2. IDENTITY
-- ============================================================

identity_customer AS (
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

-- ============================================================
-- 3. STRIPE COVERAGE AT GLOBAL SUBSCRIBER GRAIN
--
-- This keeps the complete MRR coverage history needed for
-- Reactivation / Churn / LTV / ACL.
-- ============================================================

coverage_customer_product_daily AS (
  SELECT
    m.snapshot_date,
    COALESCE(pm.product_name, m._elt_stripe_account_id) AS product_name,

    COALESCE(
      CONCAT('user:', CAST(i.user_key AS STRING)),
      CONCAT(
        'stripe:',
        m._elt_stripe_account_id,
        ':',
        m.customer
      )
    ) AS subscriber_key,

    i.user_key IS NULL AS is_identity_fallback,
    pm.product_name IS NULL AS product_name_is_fallback,

    MIN(m.first_revenue_date) AS first_revenue_date,
    COUNTIF(m.mrr_local > 0) AS positive_mrr_rows,
    SUM(m.mrr_usd) AS mrr_usd,
    SUM(COALESCE(m.subscriptions_live_now, 0)) AS subscriptions_live_now,
    LOGICAL_OR(COALESCE(m.has_missing_fx, FALSE)) AS has_missing_fx

  FROM {{ ref('int_stripe_mrr_customer_daily') }} m

  LEFT JOIN identity_customer i
    ON m._elt_stripe_account_id = i.account_id
   AND m.customer = i.customer_id

  LEFT JOIN product_map pm
    ON m._elt_stripe_account_id = pm.account_id

  CROSS JOIN params p

  WHERE m.snapshot_date <= p.max_date

  GROUP BY 1,2,3,4,5
),

-- Product rows + independent All Products row.
coverage_user_scoped AS (
  SELECT
    snapshot_date,
    product_name,
    subscriber_key,
    LOGICAL_OR(is_identity_fallback) AS is_identity_fallback,
    LOGICAL_OR(product_name_is_fallback) AS product_name_is_fallback,
    MIN(first_revenue_date) AS first_revenue_date,
    SUM(positive_mrr_rows) AS positive_mrr_rows,
    SUM(mrr_usd) AS mrr_usd,
    SUM(subscriptions_live_now) AS subscriptions_live_now,
    LOGICAL_OR(has_missing_fx) AS has_missing_fx
  FROM coverage_customer_product_daily
  GROUP BY 1,2,3

  UNION ALL

  SELECT
    snapshot_date,
    'All Products' AS product_name,
    subscriber_key,
    LOGICAL_OR(is_identity_fallback) AS is_identity_fallback,
    FALSE AS product_name_is_fallback,
    MIN(first_revenue_date) AS first_revenue_date,
    SUM(positive_mrr_rows) AS positive_mrr_rows,
    SUM(mrr_usd) AS mrr_usd,
    SUM(subscriptions_live_now) AS subscriptions_live_now,
    LOGICAL_OR(has_missing_fx) AS has_missing_fx
  FROM coverage_customer_product_daily
  GROUP BY 1,2,3
),

-- ============================================================
-- 4. POSITIVE MRR / PAID STOCK
-- ============================================================

paid_user_product_mrr_base AS (
  SELECT
    m.snapshot_date,
    COALESCE(pm.product_name, m._elt_stripe_account_id) AS product_name,

    COALESCE(
      CONCAT('user:', CAST(i.user_key AS STRING)),
      CONCAT(
        'stripe:',
        m._elt_stripe_account_id,
        ':',
        m.customer
      )
    ) AS subscriber_key,

    i.user_key IS NULL AS is_identity_fallback,
    pm.product_name IS NULL AS product_name_is_fallback,

    SUM(m.mrr_usd) AS mrr_usd,
    COUNTIF(m.mrr_usd IS NULL) AS missing_fx_rows

  FROM {{ ref('int_stripe_mrr_customer_daily') }} m

  LEFT JOIN identity_customer i
    ON m._elt_stripe_account_id = i.account_id
   AND m.customer = i.customer_id

  LEFT JOIN product_map pm
    ON m._elt_stripe_account_id = pm.account_id

  CROSS JOIN params p

  WHERE m.mrr_local > 0
    AND m.snapshot_date <= p.max_date

  GROUP BY 1,2,3,4,5
),

paid_user_mrr_scoped AS (
  SELECT
    snapshot_date,
    product_name,
    subscriber_key,
    LOGICAL_OR(is_identity_fallback) AS is_identity_fallback,
    LOGICAL_OR(product_name_is_fallback) AS product_name_is_fallback,
    SUM(mrr_usd) AS mrr_usd,
    SUM(missing_fx_rows) AS missing_fx_rows
  FROM paid_user_product_mrr_base
  GROUP BY 1,2,3

  UNION ALL

  SELECT
    snapshot_date,
    'All Products' AS product_name,
    subscriber_key,
    LOGICAL_OR(is_identity_fallback) AS is_identity_fallback,
    FALSE AS product_name_is_fallback,
    SUM(mrr_usd) AS mrr_usd,
    SUM(missing_fx_rows) AS missing_fx_rows
  FROM paid_user_product_mrr_base
  GROUP BY 1,2,3
),

paid_mrr_daily AS (
  SELECT
    snapshot_date,
    product_name,
    COUNT(DISTINCT subscriber_key) AS observed_mrr_users,
    SUM(mrr_usd) AS total_mrr_usd,
    SUM(missing_fx_rows) AS missing_fx_rows
  FROM paid_user_mrr_scoped
  GROUP BY 1,2
),

paid_days AS (
  SELECT DISTINCT
    product_name,
    subscriber_key,
    snapshot_date
  FROM paid_user_mrr_scoped
),

-- ============================================================
-- 5. UNIFIED PAID RUNS + SUBSCRIBER MOVEMENTS
--
-- Gap <= 15 days = same paid run.
-- Gap > 15 days  = new run / reactivation.
-- The interval between observed paid days inside the same run remains Paid.
-- The newest grace-window dates are provisional and can backfill later.
-- ============================================================

paid_sequence AS (
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

paid_flagged AS (
  SELECT
    *,
    CASE
      WHEN prev_paid_date IS NULL
        OR DATE_DIFF(snapshot_date, prev_paid_date, DAY) > 15
      THEN 1
      ELSE 0
    END AS starts_new_run
  FROM paid_sequence
),

paid_numbered AS (
  SELECT
    *,
    SUM(starts_new_run) OVER (
      PARTITION BY product_name, subscriber_key
      ORDER BY snapshot_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS run_id
  FROM paid_flagged
),

paid_runs_raw AS (
  SELECT
    product_name,
    subscriber_key,
    run_id,
    MIN(snapshot_date) AS run_start,
    MAX(snapshot_date) AS run_last_paid
  FROM paid_numbered
  GROUP BY 1,2,3
),

paid_runs AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY product_name, subscriber_key
      ORDER BY run_start
    ) AS run_number,
    DATE_ADD(run_last_paid, INTERVAL 1 DAY) AS churn_date
  FROM paid_runs_raw
),

paid_runs_clipped AS (
  SELECT
    r.product_name,
    r.subscriber_key,
    r.run_id,
    GREATEST(r.run_start, p.dashboard_start) AS interval_start,
    LEAST(r.run_last_paid, p.max_date) AS interval_end
  FROM paid_runs r
  CROSS JOIN params p
  WHERE r.run_last_paid >= p.dashboard_start
    AND r.run_start <= p.max_date
),

paid_scope_spine AS (
  SELECT
    p.product_name,
    d AS snapshot_date
  FROM (
    SELECT DISTINCT product_name
    FROM paid_user_mrr_scoped
  ) p
  CROSS JOIN params prm
  CROSS JOIN UNNEST(
    GENERATE_DATE_ARRAY(prm.dashboard_start, prm.max_date)
  ) AS d
),

paid_stock_events AS (
  SELECT
    product_name,
    interval_start AS event_date,
    1 AS delta
  FROM paid_runs_clipped

  UNION ALL

  SELECT
    product_name,
    DATE_ADD(interval_end, INTERVAL 1 DAY) AS event_date,
    -1 AS delta
  FROM paid_runs_clipped
),

paid_stock_event_daily AS (
  SELECT
    product_name,
    event_date,
    SUM(delta) AS delta
  FROM paid_stock_events
  GROUP BY 1,2
),

daily_paid_stock AS (
  SELECT
    s.product_name,
    s.snapshot_date,
    SUM(COALESCE(e.delta, 0)) OVER (
      PARTITION BY s.product_name
      ORDER BY s.snapshot_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS paid_subscribers
  FROM paid_scope_spine s
  LEFT JOIN paid_stock_event_daily e
    ON s.product_name = e.product_name
   AND s.snapshot_date = e.event_date
),

movement_events AS (
  SELECT
    product_name,
    run_start AS snapshot_date,
    subscriber_key,
    CASE
      WHEN run_number = 1 THEN 'new'
      ELSE 'reactivated'
    END AS movement_type
  FROM paid_runs

  UNION ALL

  SELECT
    product_name,
    churn_date AS snapshot_date,
    subscriber_key,
    'churned' AS movement_type
  FROM paid_runs
),

daily_movements AS (
  SELECT
    product_name,
    snapshot_date,

    COUNT(DISTINCT IF(
      movement_type = 'new',
      subscriber_key,
      NULL
    )) AS new_subscribers,

    COUNT(DISTINCT IF(
      movement_type = 'reactivated',
      subscriber_key,
      NULL
    )) AS reactivated_subscribers,

    COUNT(DISTINCT IF(
      movement_type = 'churned',
      subscriber_key,
      NULL
    )) AS churned_subscribers

  FROM movement_events
  GROUP BY 1,2
),

-- ============================================================
-- 6. ASP / NEW BUSINESS MRR
--
-- Primary source: official Stripe new_business movement MRR.
-- Fallback: validated MRR on the subscriber's first paid day when
-- a globally deduped new subscriber has no matching movement row.
-- ============================================================

new_business_user_product_day_base AS (
  SELECT
    m.snapshot_date,
    COALESCE(pm.product_name, m._elt_stripe_account_id) AS product_name,

    COALESCE(
      CONCAT('user:', CAST(i.user_key AS STRING)),
      CONCAT(
        'stripe:',
        m._elt_stripe_account_id,
        ':',
        m.customer
      )
    ) AS subscriber_key,

    SUM(m.mrr_usd) AS new_business_mrr_usd,
    COUNTIF(m.mrr_usd IS NULL) AS missing_mrr_rows

  FROM {{ ref('int_stripe_mrr_movements_daily') }} m

  LEFT JOIN identity_customer i
    ON m._elt_stripe_account_id = i.account_id
   AND m.customer = i.customer_id

  LEFT JOIN product_map pm
    ON m._elt_stripe_account_id = pm.account_id

  CROSS JOIN params p

  WHERE m.movement_type = 'new_business'
    AND m.snapshot_date <= p.max_date

  GROUP BY 1,2,3
),

new_business_user_day_scoped AS (
  SELECT
    snapshot_date,
    product_name,
    subscriber_key,
    SUM(new_business_mrr_usd) AS new_business_mrr_usd,
    SUM(missing_mrr_rows) AS missing_mrr_rows
  FROM new_business_user_product_day_base
  GROUP BY 1,2,3

  UNION ALL

  SELECT
    snapshot_date,
    'All Products' AS product_name,
    subscriber_key,
    SUM(new_business_mrr_usd) AS new_business_mrr_usd,
    SUM(missing_mrr_rows) AS missing_mrr_rows
  FROM new_business_user_product_day_base
  GROUP BY 1,2,3
),

new_lifecycle_events AS (
  SELECT
    product_name,
    run_start AS snapshot_date,
    subscriber_key
  FROM paid_runs
  WHERE run_number = 1
),

daily_asp AS (
  SELECT
    n.product_name,
    n.snapshot_date,

    COUNT(DISTINCT n.subscriber_key) AS asp_new_customers,

    SUM(
      CASE
        WHEN mv.subscriber_key IS NOT NULL
         AND COALESCE(mv.missing_mrr_rows, 0) = 0
         AND mv.new_business_mrr_usd IS NOT NULL
          THEN mv.new_business_mrr_usd
        ELSE COALESCE(pd.mrr_usd, 0)
      END
    ) AS new_business_mrr_usd,

    COUNT(DISTINCT IF(
      NOT (
        (
          mv.subscriber_key IS NOT NULL
          AND COALESCE(mv.missing_mrr_rows, 0) = 0
          AND mv.new_business_mrr_usd IS NOT NULL
        )
        OR
        (
          pd.subscriber_key IS NOT NULL
          AND pd.mrr_usd IS NOT NULL
          AND COALESCE(pd.missing_fx_rows, 0) = 0
        )
      ),
      n.subscriber_key,
      NULL
    )) AS asp_missing_mrr_users

  FROM new_lifecycle_events n

  LEFT JOIN new_business_user_day_scoped mv
    ON n.product_name = mv.product_name
   AND n.snapshot_date = mv.snapshot_date
   AND n.subscriber_key = mv.subscriber_key

  LEFT JOIN paid_user_mrr_scoped pd
    ON n.product_name = pd.product_name
   AND n.snapshot_date = pd.snapshot_date
   AND n.subscriber_key = pd.subscriber_key

  GROUP BY 1,2
),

-- ============================================================
-- 7. HISTORICAL FREE / ALL SUBSCRIPTION STATE
--
-- The Stripe history sources are event-based, not daily snapshots.
-- Reconstruct EOD subscription state inside the complete retained-event window:
--   1) event rows provide the post-event subscription/item state;
--   2) previous_attributes restores the state before a subscription's first
--      retained event when status/items changed;
--   3) current active-like subscriptions with no retained event are treated as
--      unchanged inside the retained window;
--   4) Paid wins over Free at subscriber grain, matching the existing logic.
--
-- Historical Free / All is available only when every currently active Stripe
-- account is represented in the event history. Outside that complete window,
-- Free remains NULL and All remains the available Paid state only.
-- ============================================================

latest_prices AS (
  SELECT
    _elt_stripe_account_id AS account_id,
    price_id,

    COALESCE(
      CAST(unit_amount AS BIGNUMERIC),
      SAFE_CAST(unit_amount_decimal AS BIGNUMERIC)
    ) AS price_amount

  FROM {{ ref('int_stripe_prices') }}

  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _elt_stripe_account_id, price_id
    ORDER BY _elt_update_timestamp DESC
  ) = 1
),

latest_items AS (
  SELECT
    _elt_stripe_account_id AS account_id,
    subscription_id,
    subscription_item_id,
    price_id,
    product_id,
    quantity

  FROM {{ ref('int_stripe_subscription_items') }}

  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      _elt_stripe_account_id,
      subscription_id,
      subscription_item_id
    ORDER BY _elt_update_timestamp DESC
  ) = 1
),

current_active_like_subscriptions AS (
  SELECT
    COALESCE(pm.product_name, s._elt_stripe_account_id) AS product_name,

    COALESCE(
      CONCAT('user:', CAST(i.user_key AS STRING)),
      CONCAT(
        'stripe:',
        s._elt_stripe_account_id,
        ':',
        s.customer
      )
    ) AS subscriber_key,

    i.user_key IS NULL AS is_identity_fallback,
    pm.product_name IS NULL AS product_name_is_fallback,

    s._elt_stripe_account_id AS account_id,
    s.id AS subscription_id,
    s.customer AS customer_id,
    s.status,
    s.start_date

  FROM {{ ref('int_stripe_subscriptions') }} s

  LEFT JOIN identity_customer i
    ON s._elt_stripe_account_id = i.account_id
   AND s.customer = i.customer_id

  LEFT JOIN product_map pm
    ON s._elt_stripe_account_id = pm.account_id

  WHERE s.status IN (
    'active',
    'past_due',
    'trialing'
  )
),

current_subscription_classification AS (
  SELECT
    s.product_name,
    s.subscriber_key,
    s.is_identity_fallback,
    s.product_name_is_fallback,
    s.account_id,
    s.subscription_id,
    s.customer_id,
    s.status,
    s.start_date,

    COUNT(i.subscription_item_id) AS item_count,
    COUNTIF(p.price_id IS NOT NULL) AS matched_price_items,
    COUNTIF(p.price_amount IS NOT NULL) AS known_price_amount_items,
    COUNTIF(p.price_amount > 0) AS nonzero_price_items,
    COUNTIF(p.price_amount = 0) AS zero_price_items,

    (
      COUNT(i.subscription_item_id) > 0
      AND COUNTIF(p.price_id IS NOT NULL)
          = COUNT(i.subscription_item_id)
      AND COUNTIF(p.price_amount IS NOT NULL)
          = COUNT(i.subscription_item_id)
      AND COUNTIF(p.price_amount > 0) = 0
      AND COUNTIF(p.price_amount = 0)
          = COUNT(i.subscription_item_id)
    ) AS is_explicit_free_subscription,

    (
      COUNT(i.subscription_item_id) = 0
      OR COUNTIF(p.price_id IS NOT NULL)
         < COUNT(i.subscription_item_id)
      OR COUNTIF(p.price_amount IS NOT NULL)
         < COUNT(i.subscription_item_id)
    ) AS has_unresolved_price

  FROM current_active_like_subscriptions s

  LEFT JOIN latest_items i
    ON s.account_id = i.account_id
   AND s.subscription_id = i.subscription_id

  LEFT JOIN latest_prices p
    ON i.account_id = p.account_id
   AND i.price_id = p.price_id

  GROUP BY 1,2,3,4,5,6,7,8,9
),

-- Retained event-history coverage by account.
history_account_bounds AS (
  SELECT
    _elt_stripe_account_id AS account_id,
    MIN(DATE(event_created_ts)) AS history_start_date,
    MAX(DATE(event_created_ts)) AS history_end_date
  FROM {{ ref('int_stripe_subscriptions_history') }}
  GROUP BY 1
),

current_active_accounts AS (
  SELECT DISTINCT account_id
  FROM current_active_like_subscriptions
),

history_coverage_qa AS (
  SELECT
    COUNTIF(h.account_id IS NULL) AS active_accounts_missing_history
  FROM current_active_accounts a
  LEFT JOIN history_account_bounds h
    USING (account_id)
),

-- Use only the date range covered by every currently active Stripe account.
free_history_bounds AS (
  SELECT
    MAX(h.history_start_date) AS history_start_date,
    MIN(h.history_end_date) AS history_end_date
  FROM current_active_accounts a
  JOIN history_account_bounds h
    USING (account_id)
),

history_subscription_events AS (
  SELECT
    s._elt_stripe_account_id AS account_id,
    s.event_id,
    s.event_type,
    s.event_created_ts,
    s._elt_update_timestamp,
    s.id AS subscription_id,
    s.customer AS customer_id,
    s.status,
    s.start_date,
    TO_JSON_STRING(s.previous_attributes) AS previous_attributes_json,

    COALESCE(pm.product_name, s._elt_stripe_account_id) AS product_name,

    COALESCE(
      CONCAT('user:', CAST(i.user_key AS STRING)),
      CONCAT(
        'stripe:',
        s._elt_stripe_account_id,
        ':',
        s.customer
      )
    ) AS subscriber_key,

    i.user_key IS NULL AS is_identity_fallback,
    pm.product_name IS NULL AS product_name_is_fallback

  FROM {{ ref('int_stripe_subscriptions_history') }} s

  LEFT JOIN identity_customer i
    ON s._elt_stripe_account_id = i.account_id
   AND s.customer = i.customer_id

  LEFT JOIN product_map pm
    ON s._elt_stripe_account_id = pm.account_id
),

history_event_item_classification AS (
  SELECT
    i._elt_stripe_account_id AS account_id,
    i.event_id,
    i.subscription_id,

    COUNT(i.subscription_item_id) AS item_count,
    COUNTIF(p.price_id IS NOT NULL) AS matched_price_items,
    COUNTIF(p.price_amount IS NOT NULL) AS known_price_amount_items,
    COUNTIF(p.price_amount > 0) AS nonzero_price_items,
    COUNTIF(p.price_amount = 0) AS zero_price_items

  FROM {{ ref('int_stripe_subscription_items_history') }} i

  LEFT JOIN latest_prices p
    ON i._elt_stripe_account_id = p.account_id
   AND i.price_id = p.price_id

  GROUP BY 1,2,3
),

history_post_event_state AS (
  SELECT
    e.*,

    COALESCE(ic.item_count, 0) AS item_count,
    COALESCE(ic.matched_price_items, 0) AS matched_price_items,
    COALESCE(ic.known_price_amount_items, 0) AS known_price_amount_items,
    COALESCE(ic.nonzero_price_items, 0) AS nonzero_price_items,
    COALESCE(ic.zero_price_items, 0) AS zero_price_items,

    (
      COALESCE(ic.item_count, 0) > 0
      AND COALESCE(ic.matched_price_items, 0)
          = COALESCE(ic.item_count, 0)
      AND COALESCE(ic.known_price_amount_items, 0)
          = COALESCE(ic.item_count, 0)
      AND COALESCE(ic.nonzero_price_items, 0) = 0
      AND COALESCE(ic.zero_price_items, 0)
          = COALESCE(ic.item_count, 0)
    ) AS is_explicit_free_subscription,

    (
      COALESCE(ic.item_count, 0) = 0
      OR COALESCE(ic.matched_price_items, 0)
         < COALESCE(ic.item_count, 0)
      OR COALESCE(ic.known_price_amount_items, 0)
         < COALESCE(ic.item_count, 0)
    ) AS has_unresolved_price

  FROM history_subscription_events e

  LEFT JOIN history_event_item_classification ic
    ON e.account_id = ic.account_id
   AND e.event_id = ic.event_id
   AND e.subscription_id = ic.subscription_id
),

-- First retained event for each subscription. Used only to restore the state
-- that existed before that first retained event when earlier EOD dates need it.
first_history_event AS (
  SELECT *
  FROM history_post_event_state
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY account_id, subscription_id
    ORDER BY event_created_ts, _elt_update_timestamp, event_id
  ) = 1
),

first_event_previous_items AS (
  SELECT
    f.account_id,
    f.subscription_id,

    JSON_VALUE(item_json, '$.price.id') AS price_id,

    SAFE_CAST(
      COALESCE(
        JSON_VALUE(item_json, '$.price.unit_amount'),
        JSON_VALUE(item_json, '$.price.unit_amount_decimal'),
        JSON_VALUE(item_json, '$.plan.amount'),
        JSON_VALUE(item_json, '$.plan.amount_decimal')
      )
      AS BIGNUMERIC
    ) AS price_amount

  FROM first_history_event f
  CROSS JOIN UNNEST(
    JSON_QUERY_ARRAY(f.previous_attributes_json, '$.items.data')
  ) AS item_json

  WHERE JSON_QUERY(
    f.previous_attributes_json,
    '$.items'
  ) IS NOT NULL
),

first_event_previous_item_classification AS (
  SELECT
    account_id,
    subscription_id,
    COUNT(*) AS item_count,
    COUNTIF(price_id IS NOT NULL) AS matched_price_items,
    COUNTIF(price_amount IS NOT NULL) AS known_price_amount_items,
    COUNTIF(price_amount > 0) AS nonzero_price_items,
    COUNTIF(price_amount = 0) AS zero_price_items
  FROM first_event_previous_items
  GROUP BY 1,2
),

initial_history_state AS (
  SELECT
    f.product_name,
    f.subscriber_key,
    f.is_identity_fallback,
    f.product_name_is_fallback,
    f.account_id,
    f.subscription_id,
    f.customer_id,
    f.event_type,
    f.event_created_ts,
    f.start_date,

    CASE
      WHEN JSON_VALUE(f.previous_attributes_json, '$.status') IS NOT NULL
      THEN JSON_VALUE(f.previous_attributes_json, '$.status')

      -- customer.subscription.deleted is already canceled in the event snapshot
      -- and Stripe does not expose the prior status in previous_attributes.
      -- For the pre-delete interval, treat it as active-like. This only affects
      -- zero-price Free/All reconstruction; Paid remains sourced from MRR history.
      WHEN f.event_type = 'customer.subscription.deleted'
      THEN 'active'

      ELSE f.status
    END AS status,

    -- If items changed in the first retained event, previous_attributes.items
    -- contains the prior item state. Otherwise the post-event items are also
    -- the pre-event items.
    CASE
      WHEN JSON_QUERY(f.previous_attributes_json, '$.items') IS NOT NULL
      THEN COALESCE(pi.item_count, 0)
      ELSE f.item_count
    END AS item_count,

    CASE
      WHEN JSON_QUERY(f.previous_attributes_json, '$.items') IS NOT NULL
      THEN COALESCE(pi.matched_price_items, 0)
      ELSE f.matched_price_items
    END AS matched_price_items,

    CASE
      WHEN JSON_QUERY(f.previous_attributes_json, '$.items') IS NOT NULL
      THEN COALESCE(pi.known_price_amount_items, 0)
      ELSE f.known_price_amount_items
    END AS known_price_amount_items,

    CASE
      WHEN JSON_QUERY(f.previous_attributes_json, '$.items') IS NOT NULL
      THEN COALESCE(pi.nonzero_price_items, 0)
      ELSE f.nonzero_price_items
    END AS nonzero_price_items,

    CASE
      WHEN JSON_QUERY(f.previous_attributes_json, '$.items') IS NOT NULL
      THEN COALESCE(pi.zero_price_items, 0)
      ELSE f.zero_price_items
    END AS zero_price_items

  FROM first_history_event f

  LEFT JOIN first_event_previous_item_classification pi
    ON f.account_id = pi.account_id
   AND f.subscription_id = pi.subscription_id
),

initial_history_state_classified AS (
  SELECT
    i.*,

    (
      item_count > 0
      AND matched_price_items = item_count
      AND known_price_amount_items = item_count
      AND nonzero_price_items = 0
      AND zero_price_items = item_count
    ) AS is_explicit_free_subscription,

    (
      item_count = 0
      OR matched_price_items < item_count
      OR known_price_amount_items < item_count
    ) AS has_unresolved_price

  FROM initial_history_state i
),

-- Keep only the last subscription event of each UTC day because dashboard
-- stock metrics use end-of-day state.
history_daily_last_event_state AS (
  SELECT
    h.*,
    DATE(h.event_created_ts) AS event_date
  FROM history_post_event_state h
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY account_id, subscription_id, DATE(event_created_ts)
    ORDER BY event_created_ts DESC, _elt_update_timestamp DESC, event_id DESC
  ) = 1
),

history_event_interval_base AS (
  SELECT
    h.*,
    LEAD(event_date) OVER (
      PARTITION BY account_id, subscription_id
      ORDER BY event_date
    ) AS next_event_date
  FROM history_daily_last_event_state h
),

-- Post-event state applies from that event date through the day before the
-- next event, capped to the complete retained-history window.
history_event_state_intervals AS (
  SELECT
    h.product_name,
    h.subscriber_key,
    h.is_identity_fallback,
    h.product_name_is_fallback,
    h.account_id,
    h.subscription_id,
    h.customer_id,
    h.status,
    h.is_explicit_free_subscription,
    h.has_unresolved_price,

    GREATEST(h.event_date, b.history_start_date) AS valid_start_date,

    LEAST(
      COALESCE(
        DATE_SUB(h.next_event_date, INTERVAL 1 DAY),
        b.history_end_date
      ),
      b.history_end_date
    ) AS valid_end_date

  FROM history_event_interval_base h
  CROSS JOIN free_history_bounds b

  WHERE h.event_date <= b.history_end_date
),

-- If a subscription existed before its first retained event, reconstruct the
-- state before that event from previous_attributes + the first event snapshot.
initial_history_state_intervals AS (
  SELECT
    i.product_name,
    i.subscriber_key,
    i.is_identity_fallback,
    i.product_name_is_fallback,
    i.account_id,
    i.subscription_id,
    i.customer_id,
    i.status,
    i.is_explicit_free_subscription,
    i.has_unresolved_price,

    b.history_start_date AS valid_start_date,
    DATE_SUB(DATE(i.event_created_ts), INTERVAL 1 DAY) AS valid_end_date

  FROM initial_history_state_classified i
  CROSS JOIN free_history_bounds b

  WHERE i.event_type != 'customer.subscription.created'
    AND DATE(i.event_created_ts) > b.history_start_date
),

history_seen_subscriptions AS (
  SELECT DISTINCT
    account_id,
    subscription_id
  FROM history_subscription_events
),

-- Subscriptions with no event anywhere in the retained window are unchanged
-- within that window. Use the current state as their stable state, respecting
-- the subscription start date so they are not counted before they existed.
no_event_current_state_intervals AS (
  SELECT
    c.product_name,
    c.subscriber_key,
    c.is_identity_fallback,
    c.product_name_is_fallback,
    c.account_id,
    c.subscription_id,
    c.customer_id,
    c.status,
    c.is_explicit_free_subscription,
    c.has_unresolved_price,

    GREATEST(
      b.history_start_date,
      COALESCE(c.start_date, b.history_start_date)
    ) AS valid_start_date,

    b.history_end_date AS valid_end_date

  FROM current_subscription_classification c
  CROSS JOIN free_history_bounds b

  LEFT JOIN history_seen_subscriptions h
    ON c.account_id = h.account_id
   AND c.subscription_id = h.subscription_id

  WHERE h.subscription_id IS NULL
),

historical_subscription_state_intervals AS (
  SELECT * FROM history_event_state_intervals
  WHERE valid_start_date <= valid_end_date

  UNION ALL

  SELECT * FROM initial_history_state_intervals
  WHERE valid_start_date <= valid_end_date

  UNION ALL

  SELECT * FROM no_event_current_state_intervals
  WHERE valid_start_date <= valid_end_date
),

historical_active_like_daily AS (
  SELECT
    i.product_name,
    i.subscriber_key,
    i.is_identity_fallback,
    i.product_name_is_fallback,
    i.account_id,
    i.subscription_id,
    i.customer_id,
    i.status,
    i.is_explicit_free_subscription,
    i.has_unresolved_price,
    snapshot_date

  FROM historical_subscription_state_intervals i
  CROSS JOIN UNNEST(
    GENERATE_DATE_ARRAY(i.valid_start_date, i.valid_end_date)
  ) AS snapshot_date

  WHERE i.status IN ('active', 'past_due', 'trialing')
),

-- Client Free definition: active zero-price subscription only.
-- A customer who is also Paid on the same date/scope is excluded later.
-- Trialing-only subscriptions do not qualify as Free.
historical_free_candidates_product AS (
  SELECT DISTINCT
    snapshot_date,
    product_name,
    subscriber_key
  FROM historical_active_like_daily
  WHERE is_explicit_free_subscription
    AND status = 'active'
),

historical_free_candidates_scoped AS (
  SELECT
    snapshot_date,
    product_name,
    subscriber_key
  FROM historical_free_candidates_product

  UNION ALL

  SELECT
    snapshot_date,
    'All Products' AS product_name,
    subscriber_key
  FROM historical_free_candidates_product
  GROUP BY 1,2,3
),

-- Client All definition includes Active/Past-due free subscriptions.
-- Keep Past-due free-only separate because it belongs to All but not Free.
historical_all_free_candidates_product AS (
  SELECT DISTINCT
    snapshot_date,
    product_name,
    subscriber_key
  FROM historical_active_like_daily
  WHERE is_explicit_free_subscription
    AND status IN ('active', 'past_due')
),

historical_all_free_candidates_scoped AS (
  SELECT
    snapshot_date,
    product_name,
    subscriber_key
  FROM historical_all_free_candidates_product

  UNION ALL

  SELECT
    snapshot_date,
    'All Products' AS product_name,
    subscriber_key
  FROM historical_all_free_candidates_product
  GROUP BY 1,2,3
),

historical_paid_users AS (
  SELECT DISTINCT
    snapshot_date,
    product_name,
    subscriber_key
  FROM paid_user_mrr_scoped
),

historical_subscriber_state AS (
  SELECT
    snapshot_date,
    product_name,
    subscriber_key,
    MAX(is_paid) AS is_paid,
    MAX(has_active_free_subscription) AS has_active_free_subscription,
    MAX(has_all_qualifying_free_subscription) AS has_all_qualifying_free_subscription
  FROM (
    SELECT
      snapshot_date,
      product_name,
      subscriber_key,
      1 AS is_paid,
      0 AS has_active_free_subscription,
      0 AS has_all_qualifying_free_subscription
    FROM historical_paid_users

    UNION ALL

    SELECT
      snapshot_date,
      product_name,
      subscriber_key,
      0 AS is_paid,
      1 AS has_active_free_subscription,
      0 AS has_all_qualifying_free_subscription
    FROM historical_free_candidates_scoped

    UNION ALL

    SELECT
      snapshot_date,
      product_name,
      subscriber_key,
      0 AS is_paid,
      0 AS has_active_free_subscription,
      1 AS has_all_qualifying_free_subscription
    FROM historical_all_free_candidates_scoped
  )
  GROUP BY 1,2,3
),

historical_free_stock AS (
  SELECT
    snapshot_date,
    product_name,

    COUNTIF(
      has_active_free_subscription = 1
      AND is_paid = 0
    ) AS free_subscribers,

    COUNTIF(
      has_all_qualifying_free_subscription = 1
      AND has_active_free_subscription = 0
      AND is_paid = 0
    ) AS past_due_free_only

  FROM historical_subscriber_state
  GROUP BY 1,2
),

current_free_qa AS (
  SELECT
    COUNTIF(
      has_unresolved_price
      AND status IN ('active', 'past_due')
    ) AS unresolved_price_subscriptions
  FROM current_subscription_classification
),

-- ============================================================
-- 8. PRODUCT SCOPES + DATE SPINE
-- ============================================================

product_scopes AS (
  SELECT DISTINCT product_name
  FROM paid_user_mrr_scoped
  WHERE product_name != 'All Products'

  UNION DISTINCT

  SELECT DISTINCT product_name
  FROM current_active_like_subscriptions

  UNION DISTINCT

  SELECT DISTINCT product_name
  FROM history_subscription_events

  UNION DISTINCT

  SELECT 'All Products'
),

product_scope_status AS (
  SELECT
    product_name,
    LOGICAL_OR(product_name_is_fallback) AS product_name_is_fallback
  FROM (
    SELECT
      product_name,
      product_name_is_fallback
    FROM paid_user_product_mrr_base

    UNION ALL

    SELECT
      product_name,
      product_name_is_fallback
    FROM current_active_like_subscriptions

    UNION ALL

    SELECT
      product_name,
      product_name_is_fallback
    FROM history_subscription_events
  )
  GROUP BY 1
),

date_spine AS (
  SELECT snapshot_date
  FROM params,
  UNNEST(
    GENERATE_DATE_ARRAY(
      dashboard_start,
      max_date
    )
  ) AS snapshot_date
),

scope_spine AS (
  SELECT
    p.product_name,
    d.snapshot_date
  FROM product_scopes p
  CROSS JOIN date_spine d
),

-- Central availability layer for Stripe historical Free state.
-- When event-history coverage is incomplete, return NULL rather than presenting
-- missing subscription state as zero.
available_free_state_daily AS (
  SELECT
    s.product_name,
    s.snapshot_date,

    CASE
      WHEN q.active_accounts_missing_history = 0
       AND s.snapshot_date BETWEEN b.history_start_date AND b.history_end_date
      THEN COALESCE(h.free_subscribers, 0)
      ELSE NULL
    END AS free_subscribers,

    CASE
      WHEN q.active_accounts_missing_history = 0
       AND s.snapshot_date BETWEEN b.history_start_date AND b.history_end_date
      THEN COALESCE(h.past_due_free_only, 0)
      ELSE NULL
    END AS past_due_free_only

  FROM scope_spine s
  CROSS JOIN free_history_bounds b
  CROSS JOIN history_coverage_qa q

  LEFT JOIN historical_free_stock h
    ON s.product_name = h.product_name
   AND s.snapshot_date = h.snapshot_date
),

-- ============================================================
-- 9. MONTHLY CHURN / LTV / ACL
--
-- Churn rate uses the opening Paid cohort and asks who is still Paid at
-- month end. This naturally excludes subscribed-and-churned customers and
-- churned-and-reactivated customers from the final churned cohort.
-- Current incomplete months remain NULL.
-- ============================================================

month_spine AS (
  SELECT month_start
  FROM params,
  UNNEST(
    GENERATE_DATE_ARRAY(
      DATE_ADD(
        DATE_TRUNC(dashboard_start, MONTH),
        INTERVAL 1 MONTH
      ),
      DATE_TRUNC(max_date, MONTH),
      INTERVAL 1 MONTH
    )
  ) AS month_start
),

month_bounds AS (
  SELECT
    month_start,
    DATE_SUB(month_start, INTERVAL 1 DAY) AS opening_snapshot,
    LAST_DAY(month_start) AS month_end
  FROM month_spine
),

monthly_scope_spine AS (
  SELECT
    p.product_name,
    m.month_start,
    m.opening_snapshot,
    m.month_end
  FROM product_scopes p
  CROSS JOIN month_bounds m
),

monthly_opening_users AS (
  SELECT DISTINCT
    m.product_name,
    m.month_start,
    m.month_end,
    r.subscriber_key
  FROM monthly_scope_spine m
  JOIN paid_runs r
    ON m.product_name = r.product_name
   AND m.opening_snapshot BETWEEN r.run_start AND r.run_last_paid
),

monthly_ending_users AS (
  SELECT DISTINCT
    m.product_name,
    m.month_start,
    r.subscriber_key
  FROM monthly_scope_spine m
  JOIN paid_runs r
    ON m.product_name = r.product_name
   AND m.month_end BETWEEN r.run_start AND r.run_last_paid
),

monthly_churn_counts AS (
  SELECT
    o.product_name,
    o.month_start,
    COUNT(DISTINCT o.subscriber_key) AS opening_paid_subscribers,

    CASE
      WHEN MAX(o.month_end) <= p.max_date
      THEN COUNT(DISTINCT IF(
        e.subscriber_key IS NULL,
        o.subscriber_key,
        NULL
      ))
      ELSE NULL
    END AS churned_from_start

  FROM monthly_opening_users o

  LEFT JOIN monthly_ending_users e
    ON o.product_name = e.product_name
   AND o.month_start = e.month_start
   AND o.subscriber_key = e.subscriber_key

  CROSS JOIN params p

  GROUP BY 1,2,p.max_date
),

monthly_churn_rate AS (
  SELECT
    s.product_name,
    s.month_start,

    COALESCE(c.opening_paid_subscribers, 0)
      AS opening_paid_subscribers,

    c.churned_from_start,

    SAFE_DIVIDE(
      c.churned_from_start,
      NULLIF(c.opening_paid_subscribers, 0)
    ) AS customer_churn_rate,

    SAFE_DIVIDE(
      c.churned_from_start,
      NULLIF(c.opening_paid_subscribers, 0)
    ) AS paid_subscriber_churn_rate

  FROM monthly_scope_spine s

  LEFT JOIN monthly_churn_counts c
    ON s.product_name = c.product_name
   AND s.month_start = c.month_start
),

monthly_churn_window AS (
  SELECT
    *,

    AVG(customer_churn_rate) OVER six_month_window
      AS customer_churn_rate_6m_raw,

    COUNT(customer_churn_rate) OVER six_month_window
      AS customer_churn_months_available,

    AVG(paid_subscriber_churn_rate) OVER six_month_window
      AS paid_churn_rate_6m_raw,

    COUNT(paid_subscriber_churn_rate) OVER six_month_window
      AS paid_churn_months_available

  FROM monthly_churn_rate

  WINDOW six_month_window AS (
    PARTITION BY product_name
    ORDER BY month_start
    ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
  )
),

monthly_trailing_churn AS (
  SELECT
    product_name,
    month_start,

    CASE
      WHEN customer_churn_months_available = 6
      THEN customer_churn_rate_6m_raw
      ELSE NULL
    END AS customer_churn_rate_6m_avg,

    CASE
      WHEN paid_churn_months_available = 6
      THEN paid_churn_rate_6m_raw
      ELSE NULL
    END AS paid_subscriber_churn_rate_6m_avg

  FROM monthly_churn_window
),

-- ============================================================
-- 10. DAILY METRICS
-- ============================================================

daily_core AS (
  SELECT
    s.product_name,
    s.snapshot_date,

    COALESCE(ps.paid_subscribers, 0)
      AS paid_subscribers,

    -- Free is NULL outside the complete retained Stripe event-history window.
    fs.free_subscribers,

    -- Client definition:
    -- All = Paid + active Free-only + Past-due free-only.
    -- Missing Free state contributes 0 outside the retained-history window;
    -- free_all_state_available distinguishes complete Free/All state from Paid-only.
    COALESCE(ps.paid_subscribers, 0)
      + COALESCE(fs.free_subscribers, 0)
      + COALESCE(fs.past_due_free_only, 0)
      AS all_subscribers,

    COALESCE(mv.new_subscribers, 0)
      AS new_subscribers,

    COALESCE(mv.reactivated_subscribers, 0)
      AS reactivated_subscribers,

    COALESCE(mv.churned_subscribers, 0)
      AS churned_subscribers,

    COALESCE(mv.new_subscribers, 0)
      + COALESCE(mv.reactivated_subscribers, 0)
      - COALESCE(mv.churned_subscribers, 0)
      AS net_change,

    COALESCE(mrr.total_mrr_usd, 0)
      AS total_mrr_usd,

    SAFE_DIVIDE(
      COALESCE(mrr.total_mrr_usd, 0),
      NULLIF(ps.paid_subscribers, 0)
    ) AS arpa_usd,

    COALESCE(asp.asp_new_customers, 0)
      AS asp_new_customers,

    COALESCE(asp.new_business_mrr_usd, 0)
      AS new_business_mrr_usd,

    COALESCE(asp.asp_missing_mrr_users, 0)
      AS asp_missing_mrr_users,

    CASE
      WHEN COALESCE(asp.asp_new_customers, 0) = 0
      THEN NULL

      WHEN COALESCE(asp.asp_missing_mrr_users, 0) > 0
      THEN NULL

      ELSE SAFE_DIVIDE(
        asp.new_business_mrr_usd,
        asp.asp_new_customers
      )
    END AS asp_usd,

    tc.customer_churn_rate_6m_avg,
    tc.paid_subscriber_churn_rate_6m_avg,

    CASE
      WHEN tc.customer_churn_rate_6m_avg > 0
      THEN SAFE_DIVIDE(
        SAFE_DIVIDE(
          COALESCE(mrr.total_mrr_usd, 0),
          NULLIF(ps.paid_subscribers, 0)
        ),
        tc.customer_churn_rate_6m_avg
      )
      ELSE NULL
    END AS ltv_usd,

    CASE
      WHEN tc.paid_subscriber_churn_rate_6m_avg > 0
      THEN SAFE_DIVIDE(
        1,
        tc.paid_subscriber_churn_rate_6m_avg
      )
      ELSE NULL
    END AS acl_months,

    COALESCE(ps.paid_subscribers, 0)
      - COALESCE(mrr.observed_mrr_users, 0)
      AS paid_without_mrr_today,

    COALESCE(mrr.missing_fx_rows, 0)
      AS missing_fx_rows,

    CASE
      WHEN s.snapshot_date = p.max_date
      THEN qa.unresolved_price_subscriptions
      ELSE NULL
    END AS unresolved_price_subscriptions

  FROM scope_spine s

  CROSS JOIN params p
  CROSS JOIN current_free_qa qa

  LEFT JOIN daily_paid_stock ps
    ON s.product_name = ps.product_name
   AND s.snapshot_date = ps.snapshot_date

  LEFT JOIN paid_mrr_daily mrr
    ON s.product_name = mrr.product_name
   AND s.snapshot_date = mrr.snapshot_date

  LEFT JOIN daily_movements mv
    ON s.product_name = mv.product_name
   AND s.snapshot_date = mv.snapshot_date

  LEFT JOIN daily_asp asp
    ON s.product_name = asp.product_name
   AND s.snapshot_date = asp.snapshot_date

  LEFT JOIN available_free_state_daily fs
    ON s.product_name = fs.product_name
   AND s.snapshot_date = fs.snapshot_date

  LEFT JOIN monthly_trailing_churn tc
    ON s.product_name = tc.product_name
   AND DATE_TRUNC(s.snapshot_date, MONTH) = tc.month_start
),

-- ============================================================
-- 11. PERIODIZATION
--
-- Stock metrics = End-of-period.
-- Flow metrics  = Sum within period.
-- ASP           = New Business MRR / New Customers in period.
-- ACL           = Monthly only.
-- Free          = reconstructed within the complete retained event-history window.
-- All           = Paid + reconstructed Free/Past-due-free within that window;
--                 earlier rows remain the available Paid state only.
-- ============================================================

periodized_input AS (
  SELECT
    product_name,
    'day' AS period_grain,
    snapshot_date AS period_start,
    d.* EXCEPT(product_name)
  FROM daily_core d

  UNION ALL

  SELECT
    product_name,
    'week' AS period_grain,
    DATE_TRUNC(snapshot_date, WEEK(MONDAY)) AS period_start,
    d.* EXCEPT(product_name)
  FROM daily_core d

  UNION ALL

  SELECT
    product_name,
    'month' AS period_grain,
    DATE_TRUNC(snapshot_date, MONTH) AS period_start,
    d.* EXCEPT(product_name)
  FROM daily_core d

  UNION ALL

  SELECT
    product_name,
    'quarter' AS period_grain,
    DATE_TRUNC(snapshot_date, QUARTER) AS period_start,
    d.* EXCEPT(product_name)
  FROM daily_core d

  UNION ALL

  SELECT
    product_name,
    'year' AS period_grain,
    DATE_TRUNC(snapshot_date, YEAR) AS period_start,
    d.* EXCEPT(product_name)
  FROM daily_core d
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
        snapshot_date,
        paid_subscribers,
        free_subscribers,
        all_subscribers,
        total_mrr_usd,
        arpa_usd,
        customer_churn_rate_6m_avg,
        paid_subscriber_churn_rate_6m_avg,
        ltv_usd,
        acl_months,
        paid_without_mrr_today,
        missing_fx_rows,
        unresolved_price_subscriptions
      )
      ORDER BY snapshot_date DESC
      LIMIT 1
    )[OFFSET(0)] AS eop,

    SUM(new_subscribers)
      AS new_subscribers,

    SUM(reactivated_subscribers)
      AS reactivated_subscribers,

    SUM(churned_subscribers)
      AS churned_subscribers,

    SUM(net_change)
      AS net_change,

    SUM(asp_new_customers)
      AS asp_new_customers,

    SUM(new_business_mrr_usd)
      AS new_business_mrr_usd,

    SUM(asp_missing_mrr_users)
      AS asp_missing_mrr_users

  FROM periodized_input
  GROUP BY 1,2,3
)

-- ============================================================
-- 12. FINAL OUTPUT CONTRACT
-- ============================================================

SELECT
  'stripe' AS source_system,

  r.product_name,

  r.period_grain,
  r.period_start,
  r.period_data_start,
  r.period_end,

  -- Stock metrics
  r.eop.paid_subscribers,
  r.eop.free_subscribers,
  r.eop.all_subscribers,

  -- Subscriber Movements
  r.new_subscribers,
  r.reactivated_subscribers,
  r.churned_subscribers,
  r.net_change,

  -- MRR / ARPA
  r.eop.total_mrr_usd,
  r.eop.arpa_usd,

  -- ASP
  r.new_business_mrr_usd,

  CASE
    WHEN r.asp_new_customers = 0
    THEN NULL

    WHEN r.asp_missing_mrr_users > 0
    THEN NULL

    ELSE SAFE_DIVIDE(
      r.new_business_mrr_usd,
      r.asp_new_customers
    )
  END AS asp_usd,

  -- LTV
  r.eop.customer_churn_rate_6m_avg,

  CASE
    WHEN r.eop.customer_churn_rate_6m_avg > 0
    THEN SAFE_DIVIDE(
      r.eop.arpa_usd,
      r.eop.customer_churn_rate_6m_avg
    )
    ELSE NULL
  END AS ltv_usd,

  -- ACL
  r.eop.paid_subscriber_churn_rate_6m_avg,

  CASE
    WHEN r.period_grain = 'month'
      AND r.eop.paid_subscriber_churn_rate_6m_avg > 0
    THEN SAFE_DIVIDE(
      1,
      r.eop.paid_subscriber_churn_rate_6m_avg
    )
    ELSE NULL
  END AS acl_months,

  -- QA
  r.eop.paid_without_mrr_today,
  r.eop.missing_fx_rows,
  r.asp_missing_mrr_users,
  r.eop.unresolved_price_subscriptions,

  FALSE AS free_is_proxy,

  -- FALSE for all currently mapped used Stripe accounts.
  CASE
    WHEN r.product_name = 'All Products' THEN FALSE
    ELSE COALESCE(s.product_name_is_fallback, FALSE)
  END AS product_name_is_fallback,

  -- TRUE where the period end falls inside complete reconstructed Stripe
  -- Free/All history coverage.
  r.eop.free_subscribers IS NOT NULL AS free_all_state_available

FROM period_rollup r
CROSS JOIN params p
LEFT JOIN product_scope_status s
  ON r.product_name = s.product_name

WHERE r.period_end >= p.dashboard_start

ORDER BY
  CASE
    WHEN r.product_name = 'All Products' THEN 0
    ELSE 1
  END,
  r.product_name,
  CASE r.period_grain
    WHEN 'day' THEN 1
    WHEN 'week' THEN 2
    WHEN 'month' THEN 3
    WHEN 'quarter' THEN 4
    WHEN 'year' THEN 5
  END,
  r.period_start
