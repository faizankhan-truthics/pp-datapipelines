{{ config(
    materialized='table',
    tags=["subscribers", "silver", "daily"],
) }}

-- Combined Free / past-due-free-only subscriber counts per product (and 'All Products') x day,
-- over the window covered by retained Stripe event history and Solidgate subscription history.
--   free_subscribers:   explicit-free Stripe 'active' OR Solidgate active non-trial, and not paid.
--   past_due_free_only: explicit-free Stripe 'past_due' only (no active free, not paid).
-- Paid wins over Free (combined paid runs, also matched through the Solidgate e-mail alias).
-- Stripe state per day is reconstructed from the last subscription event of each day; before a
-- subscription's first retained event, the pre-event state comes from previous_attributes.
-- A row exists only for product-days with at least one free/past-due/paid subscriber; those
-- rows override the baseline free/all values in int_combined_subscribers_daily.
-- All intervals are swept as +1/-1 events per subscriber, never exploded to subscriber x day.

WITH params_base AS (
  SELECT
    dashboard_start,
    stripe_max_date,
    solidgate_max_date,
    solidgate_free_state_max_date AS solidgate_free_end,
    max_date
  FROM {{ ref('int_combined_subscribers_params') }}
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

solidgate_identity_email AS (
  SELECT DISTINCT
    user_key,
    LOWER(TRIM(email)) AS email
  FROM {{ ref('int_identity_user_map') }}
  WHERE LOWER(source_system) = 'solidgate'
    AND user_key IS NOT NULL
    AND email IS NOT NULL
    AND TRIM(email) != ''
),

stripe_latest_prices AS (
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

stripe_latest_items AS (
  SELECT
    _elt_stripe_account_id AS account_id,
    subscription_id,
    subscription_item_id,
    price_id
  FROM {{ ref('int_stripe_subscription_items') }}
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _elt_stripe_account_id, subscription_id, subscription_item_id
    ORDER BY _elt_update_timestamp DESC
  ) = 1
),

-- ============================================================
-- 1. STRIPE CURRENT STATE (subscriptions without any retained event)
-- ============================================================

stripe_current_active_like AS (
  SELECT
    COALESCE(pm.product_name, s._elt_stripe_account_id) AS product_name,
    COALESCE(
      CONCAT('user:', CAST(i.user_key AS STRING)),
      CONCAT('stripe:', s._elt_stripe_account_id, ':', s.customer)
    ) AS subscriber_key,
    s._elt_stripe_account_id AS account_id,
    s.id AS subscription_id,
    s.status,
    s.start_date
  FROM {{ ref('int_stripe_subscriptions') }} s
  LEFT JOIN stripe_identity i
    ON s._elt_stripe_account_id = i.account_id
   AND s.customer = i.customer_id
  LEFT JOIN stripe_product_map pm
    ON s._elt_stripe_account_id = pm.account_id
  WHERE s.status IN ('active', 'past_due', 'trialing')
),

stripe_current_classification AS (
  SELECT
    s.product_name,
    s.subscriber_key,
    s.account_id,
    s.subscription_id,
    s.status,
    s.start_date,
    (
      COUNT(i.subscription_item_id) > 0
      AND COUNTIF(p.price_id IS NOT NULL) = COUNT(i.subscription_item_id)
      AND COUNTIF(p.price_amount IS NOT NULL) = COUNT(i.subscription_item_id)
      AND COUNTIF(p.price_amount > 0) = 0
      AND COUNTIF(p.price_amount = 0) = COUNT(i.subscription_item_id)
    ) AS is_explicit_free_subscription
  FROM stripe_current_active_like s
  LEFT JOIN stripe_latest_items i
    ON s.account_id = i.account_id
   AND s.subscription_id = i.subscription_id
  LEFT JOIN stripe_latest_prices p
    ON i.account_id = p.account_id
   AND i.price_id = p.price_id
  GROUP BY 1,2,3,4,5,6
),

-- ============================================================
-- 2. COVERAGE WINDOW
-- Stripe free history starts once every account with a current active-like subscription
-- has retained events, and ends at the earliest of all providers' coverage.
-- ============================================================

stripe_history_account_bounds AS (
  SELECT
    _elt_stripe_account_id AS account_id,
    MIN(DATE(event_created_ts)) AS history_start_date,
    MAX(DATE(event_created_ts)) AS history_end_date
  FROM {{ ref('int_stripe_subscriptions_history') }}
  GROUP BY 1
),

stripe_coverage AS (
  SELECT
    MAX(h.history_start_date) AS history_start_date,
    MIN(h.history_end_date) AS history_end_date,
    COUNTIF(h.account_id IS NULL) AS active_accounts_missing_history
  FROM (SELECT DISTINCT account_id FROM stripe_current_active_like) a
  LEFT JOIN stripe_history_account_bounds h
    USING (account_id)
),

params AS (
  SELECT
    b.*,
    c.history_start_date AS stripe_free_start,
    c.active_accounts_missing_history,
    LEAST(
      b.stripe_max_date,
      b.solidgate_max_date,
      c.history_end_date,
      b.solidgate_free_end
    ) AS common_free_end
  FROM params_base b
  CROSS JOIN stripe_coverage c
),

-- ============================================================
-- 3. STRIPE HISTORICAL STATE FROM RETAINED EVENTS
-- ============================================================

stripe_history_events AS (
  SELECT
    s._elt_stripe_account_id AS account_id,
    s.event_id,
    s.event_type,
    s.event_created_ts,
    s._elt_update_timestamp,
    s.id AS subscription_id,
    s.status,
    TO_JSON_STRING(s.previous_attributes) AS previous_attributes_json,
    COALESCE(pm.product_name, s._elt_stripe_account_id) AS product_name,
    COALESCE(
      CONCAT('user:', CAST(i.user_key AS STRING)),
      CONCAT('stripe:', s._elt_stripe_account_id, ':', s.customer)
    ) AS subscriber_key
  FROM {{ ref('int_stripe_subscriptions_history') }} s
  LEFT JOIN stripe_identity i
    ON s._elt_stripe_account_id = i.account_id
   AND s.customer = i.customer_id
  LEFT JOIN stripe_product_map pm
    ON s._elt_stripe_account_id = pm.account_id
),

stripe_history_event_items AS (
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
  LEFT JOIN stripe_latest_prices p
    ON i._elt_stripe_account_id = p.account_id
   AND i.price_id = p.price_id
  GROUP BY 1,2,3
),

stripe_history_post_state AS (
  SELECT
    e.*,
    COALESCE(ic.item_count, 0) AS item_count,
    COALESCE(ic.matched_price_items, 0) AS matched_price_items,
    COALESCE(ic.known_price_amount_items, 0) AS known_price_amount_items,
    COALESCE(ic.nonzero_price_items, 0) AS nonzero_price_items,
    COALESCE(ic.zero_price_items, 0) AS zero_price_items,
    (
      COALESCE(ic.item_count, 0) > 0
      AND COALESCE(ic.matched_price_items, 0) = COALESCE(ic.item_count, 0)
      AND COALESCE(ic.known_price_amount_items, 0) = COALESCE(ic.item_count, 0)
      AND COALESCE(ic.nonzero_price_items, 0) = 0
      AND COALESCE(ic.zero_price_items, 0) = COALESCE(ic.item_count, 0)
    ) AS is_explicit_free_subscription
  FROM stripe_history_events e
  LEFT JOIN stripe_history_event_items ic
    ON e.account_id = ic.account_id
   AND e.event_id = ic.event_id
   AND e.subscription_id = ic.subscription_id
),

stripe_first_history_event AS (
  SELECT *
  FROM stripe_history_post_state
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY account_id, subscription_id
    ORDER BY event_created_ts, _elt_update_timestamp, event_id
  ) = 1
),

stripe_first_previous_item_classification AS (
  SELECT
    account_id,
    subscription_id,
    COUNT(*) AS item_count,
    COUNTIF(price_id IS NOT NULL) AS matched_price_items,
    COUNTIF(price_amount IS NOT NULL) AS known_price_amount_items,
    COUNTIF(price_amount > 0) AS nonzero_price_items,
    COUNTIF(price_amount = 0) AS zero_price_items
  FROM (
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
    FROM stripe_first_history_event f
    CROSS JOIN UNNEST(
      JSON_QUERY_ARRAY(f.previous_attributes_json, '$.items.data')
    ) AS item_json
    WHERE JSON_QUERY(f.previous_attributes_json, '$.items') IS NOT NULL
  )
  GROUP BY 1,2
),

-- State before the first retained event (previous_attributes override the post-event state).
stripe_initial_state AS (
  SELECT
    f.product_name,
    f.subscriber_key,
    f.event_type,
    f.event_created_ts,
    CASE
      WHEN JSON_VALUE(f.previous_attributes_json, '$.status') IS NOT NULL
      THEN JSON_VALUE(f.previous_attributes_json, '$.status')
      WHEN f.event_type = 'customer.subscription.deleted'
      THEN 'active'
      ELSE f.status
    END AS status,
    has_prev_items,
    IF(has_prev_items, COALESCE(pi.item_count, 0), f.item_count) AS item_count,
    IF(has_prev_items, COALESCE(pi.matched_price_items, 0), f.matched_price_items) AS matched_price_items,
    IF(has_prev_items, COALESCE(pi.known_price_amount_items, 0), f.known_price_amount_items) AS known_price_amount_items,
    IF(has_prev_items, COALESCE(pi.nonzero_price_items, 0), f.nonzero_price_items) AS nonzero_price_items,
    IF(has_prev_items, COALESCE(pi.zero_price_items, 0), f.zero_price_items) AS zero_price_items
  FROM (
    SELECT
      *,
      JSON_QUERY(previous_attributes_json, '$.items') IS NOT NULL AS has_prev_items
    FROM stripe_first_history_event
  ) f
  LEFT JOIN stripe_first_previous_item_classification pi
    ON f.account_id = pi.account_id
   AND f.subscription_id = pi.subscription_id
),

stripe_daily_last_event AS (
  SELECT
    *,
    DATE(event_created_ts) AS event_date
  FROM stripe_history_post_state
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY account_id, subscription_id, DATE(event_created_ts)
    ORDER BY event_created_ts DESC, _elt_update_timestamp DESC, event_id DESC
  ) = 1
),

stripe_event_intervals AS (
  SELECT
    h.product_name,
    h.subscriber_key,
    h.status,
    h.is_explicit_free_subscription,
    GREATEST(h.event_date, p.stripe_free_start) AS valid_start_date,
    LEAST(
      COALESCE(DATE_SUB(h.next_event_date, INTERVAL 1 DAY), p.common_free_end),
      p.common_free_end
    ) AS valid_end_date
  FROM (
    SELECT
      *,
      LEAD(event_date) OVER (
        PARTITION BY account_id, subscription_id
        ORDER BY event_date
      ) AS next_event_date
    FROM stripe_daily_last_event
  ) h
  CROSS JOIN params p
  WHERE h.event_date <= p.common_free_end
),

stripe_initial_intervals AS (
  SELECT
    i.product_name,
    i.subscriber_key,
    i.status,
    (
      i.item_count > 0
      AND i.matched_price_items = i.item_count
      AND i.known_price_amount_items = i.item_count
      AND i.nonzero_price_items = 0
      AND i.zero_price_items = i.item_count
    ) AS is_explicit_free_subscription,
    p.stripe_free_start AS valid_start_date,
    DATE_SUB(DATE(i.event_created_ts), INTERVAL 1 DAY) AS valid_end_date
  FROM stripe_initial_state i
  CROSS JOIN params p
  WHERE i.event_type != 'customer.subscription.created'
    AND DATE(i.event_created_ts) > p.stripe_free_start
),

stripe_no_event_current_intervals AS (
  SELECT
    c.product_name,
    c.subscriber_key,
    c.status,
    c.is_explicit_free_subscription,
    GREATEST(
      p.stripe_free_start,
      COALESCE(c.start_date, p.stripe_free_start)
    ) AS valid_start_date,
    p.common_free_end AS valid_end_date
  FROM stripe_current_classification c
  CROSS JOIN params p
  LEFT JOIN (
    SELECT DISTINCT account_id, subscription_id
    FROM stripe_history_events
  ) h
    ON c.account_id = h.account_id
   AND c.subscription_id = h.subscription_id
  WHERE h.subscription_id IS NULL
),

-- Only explicit-free active / past_due state can contribute to Free or past-due-free-only.
stripe_free_intervals_product AS (
  SELECT product_name, subscriber_key, status, valid_start_date, valid_end_date
  FROM (
    SELECT * FROM stripe_event_intervals
    UNION ALL
    SELECT * FROM stripe_initial_intervals
    UNION ALL
    SELECT * FROM stripe_no_event_current_intervals
  )
  WHERE valid_start_date <= valid_end_date
    AND is_explicit_free_subscription
    AND status IN ('active', 'past_due')
),

stripe_free_intervals AS (
  SELECT * FROM stripe_free_intervals_product
  UNION ALL
  SELECT 'All Products', subscriber_key, status, valid_start_date, valid_end_date
  FROM stripe_free_intervals_product
),

-- ============================================================
-- 4. SOLIDGATE ACTIVE (NON-TRIAL) INTERVALS
-- ============================================================

solidgate_mrr_coverage AS (
  SELECT
    _elt_account_name AS account_name,
    MIN(snapshot_date) AS first_mrr_date
  FROM {{ ref('int_solidgate_mrr_local_subscription_daily') }}
  WHERE mrr_local > 0
  GROUP BY 1
),

solidgate_active_intervals_base AS (
  SELECT
    COALESCE(pm.product_name, h.account_name) AS product_name,
    CASE
      WHEN i.user_key IS NOT NULL
      THEN CONCAT('user:', CAST(i.user_key AS STRING))
      WHEN h.customer_email IS NOT NULL AND TRIM(h.customer_email) != ''
      THEN CONCAT('email:', LOWER(TRIM(h.customer_email)))
      ELSE NULL
    END AS subscriber_key,
    GREATEST(DATE(h.valid_from), c.first_mrr_date, p.stripe_free_start) AS interval_start,
    LEAST(
      COALESCE(DATE_SUB(DATE(h.valid_to), INTERVAL 1 DAY), p.common_free_end),
      p.common_free_end
    ) AS interval_end
  FROM {{ ref('solidgate_subscription_history') }} h
  JOIN solidgate_mrr_coverage c
    ON h.account_name = c.account_name
  LEFT JOIN solidgate_identity i
    ON h.account_name = i.account_name
   AND h.customer_id = i.customer_id
  LEFT JOIN solidgate_product_map pm
    ON h.account_name = pm.account_name
  CROSS JOIN params p
  WHERE h.status = 'active'
    AND h.trial = FALSE
    AND h.valid_from < TIMESTAMP(DATE_ADD(p.common_free_end, INTERVAL 1 DAY))
),

solidgate_active_intervals AS (
  SELECT product_name, subscriber_key, interval_start, interval_end
  FROM solidgate_active_intervals_base
  WHERE subscriber_key IS NOT NULL
    AND interval_start <= interval_end

  UNION ALL

  SELECT 'All Products', subscriber_key, interval_start, interval_end
  FROM solidgate_active_intervals_base
  WHERE subscriber_key IS NOT NULL
    AND interval_start <= interval_end
),

-- ============================================================
-- 5. PAID EXCLUSION INTERVALS (paid runs clipped to the window + e-mail aliases)
-- ============================================================

paid_intervals_direct AS (
  SELECT
    r.product_name,
    r.subscriber_key,
    GREATEST(r.run_start, p.stripe_free_start) AS interval_start,
    LEAST(r.run_last_paid, p.common_free_end) AS interval_end
  FROM {{ ref('int_combined_subscribers_paid_runs') }} r
  CROSS JOIN params p
  WHERE r.run_last_paid >= p.stripe_free_start
    AND r.run_start <= p.common_free_end
),

paid_intervals AS (
  SELECT product_name, subscriber_key, interval_start, interval_end
  FROM paid_intervals_direct
  WHERE interval_start <= interval_end

  UNION ALL

  SELECT
    p.product_name,
    CONCAT('email:', e.email) AS subscriber_key,
    p.interval_start,
    p.interval_end
  FROM paid_intervals_direct p
  JOIN solidgate_identity_email e
    ON p.subscriber_key = CONCAT('user:', CAST(e.user_key AS STRING))
  WHERE p.interval_start <= p.interval_end
),

-- ============================================================
-- 6. PER-SUBSCRIBER STATE SWEEP
-- Counters > 0 mean "in that state on the day"; overlaps are naturally de-duplicated.
-- ============================================================

subscriber_events AS (
  SELECT product_name, subscriber_key, valid_start_date AS event_date,
         IF(status = 'active', 1, 0) AS d_stripe_active_free,
         1 AS d_stripe_all_free, 0 AS d_solidgate_active, 0 AS d_paid
  FROM stripe_free_intervals
  UNION ALL
  SELECT product_name, subscriber_key, DATE_ADD(valid_end_date, INTERVAL 1 DAY),
         IF(status = 'active', -1, 0), -1, 0, 0
  FROM stripe_free_intervals
  UNION ALL
  SELECT product_name, subscriber_key, interval_start, 0, 0, 1, 0
  FROM solidgate_active_intervals
  UNION ALL
  SELECT product_name, subscriber_key, DATE_ADD(interval_end, INTERVAL 1 DAY), 0, 0, -1, 0
  FROM solidgate_active_intervals
  UNION ALL
  SELECT product_name, subscriber_key, interval_start, 0, 0, 0, 1
  FROM paid_intervals
  UNION ALL
  SELECT product_name, subscriber_key, DATE_ADD(interval_end, INTERVAL 1 DAY), 0, 0, 0, -1
  FROM paid_intervals
),

subscriber_segments AS (
  SELECT
    product_name,
    event_date AS segment_start,
    LEAD(event_date) OVER (
      PARTITION BY product_name, subscriber_key
      ORDER BY event_date
    ) AS segment_next,
    SUM(SUM(d_stripe_active_free)) OVER w AS stripe_active_free,
    SUM(SUM(d_stripe_all_free)) OVER w AS stripe_all_free,
    SUM(SUM(d_solidgate_active)) OVER w AS solidgate_active,
    SUM(SUM(d_paid)) OVER w AS paid
  FROM subscriber_events
  GROUP BY product_name, subscriber_key, event_date
  WINDOW w AS (
    PARTITION BY product_name, subscriber_key
    ORDER BY event_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )
),

segment_flags AS (
  SELECT
    product_name,
    segment_start,
    segment_next,
    IF(stripe_active_free > 0 OR stripe_all_free > 0 OR solidgate_active > 0 OR paid > 0, 1, 0) AS is_candidate,
    IF((stripe_active_free > 0 OR solidgate_active > 0) AND paid = 0, 1, 0) AS is_free,
    IF(stripe_all_free > 0 AND stripe_active_free = 0 AND solidgate_active = 0 AND paid = 0, 1, 0) AS is_past_due_free_only
  FROM subscriber_segments
  -- the last event of every subscriber closes all intervals, so segment_next is never
  -- NULL while a counter is open
  WHERE segment_next IS NOT NULL
),

-- ============================================================
-- 7. PRODUCT x DAY COUNTS
-- ============================================================

product_events AS (
  SELECT product_name, event_date,
         SUM(d_candidate) AS d_candidate,
         SUM(d_free) AS d_free,
         SUM(d_past_due_free_only) AS d_past_due_free_only
  FROM (
    SELECT product_name, segment_start AS event_date,
           is_candidate AS d_candidate, is_free AS d_free,
           is_past_due_free_only AS d_past_due_free_only
    FROM segment_flags
    WHERE is_candidate = 1
    UNION ALL
    SELECT product_name, segment_next,
           -is_candidate, -is_free, -is_past_due_free_only
    FROM segment_flags
    WHERE is_candidate = 1
  )
  GROUP BY 1,2
),

-- Only days a combined row can land on (dashboard_start..max_date) are kept.
product_day_spine AS (
  SELECT product_name, d AS event_date, 0 AS d_candidate, 0 AS d_free, 0 AS d_past_due_free_only
  FROM (SELECT DISTINCT product_name FROM product_events)
  CROSS JOIN params p
  CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(p.dashboard_start, p.max_date)) AS d
),

product_daily AS (
  SELECT
    product_name,
    event_date AS snapshot_date,
    SUM(SUM(d_candidate)) OVER w AS candidates,
    SUM(SUM(d_free)) OVER w AS free_subscribers,
    SUM(SUM(d_past_due_free_only)) OVER w AS past_due_free_only
  FROM (
    SELECT * FROM product_events
    UNION ALL
    SELECT * FROM product_day_spine
  )
  GROUP BY 1,2
  WINDOW w AS (
    PARTITION BY product_name
    ORDER BY event_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )
)

SELECT
  d.snapshot_date,
  d.product_name,
  d.free_subscribers,
  d.past_due_free_only,
  p.active_accounts_missing_history = 0
    AND d.snapshot_date BETWEEN p.stripe_free_start AND p.common_free_end
    AS free_all_state_available
FROM product_daily d
CROSS JOIN params p
WHERE d.candidates > 0
  AND d.snapshot_date BETWEEN p.dashboard_start AND p.max_date
