{{ config(
    materialized='table',
    tags=["subscribers", "silver", "daily"],
) }}

-- Combined Stripe + Solidgate subscriber metrics per product (and 'All Products') x day, from
-- dashboard_start to the common complete provider date. Feeds the 'combined' rows of
-- combined_subscribers (which only periodizes this table).
-- Free / all / free_all_state_available come from int_combined_subscribers_free_daily where it
-- has a row for the product-day; otherwise from the baseline (historical Solidgate free stock,
-- or the current combined free state on stripe_max_date). Because those fields are only ever
-- read at period end, overriding them at day grain is equivalent to overriding period rows.

WITH params AS (
  SELECT * FROM {{ ref('int_combined_subscribers_params') }}
),

paid_runs AS (
  SELECT * FROM {{ ref('int_combined_subscribers_paid_runs') }}
),

mrr_daily AS (
  SELECT * FROM {{ ref('int_combined_subscribers_mrr_daily') }}
),

-- Per-subscriber MRR on the day their first paid run starts.
first_run_mrr AS (
  SELECT m.product_name, m.snapshot_date, f.subscriber_key, f.mrr_usd, f.missing_fx_rows
  FROM mrr_daily m
  CROSS JOIN UNNEST(m.first_run_starts) AS f
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

solidgate_identity_user_email AS (
  SELECT DISTINCT
    user_key,
    LOWER(TRIM(email)) AS email
  FROM {{ ref('int_identity_user_map') }}
  WHERE LOWER(source_system) = 'solidgate'
    AND user_key IS NOT NULL
    AND email IS NOT NULL
    AND TRIM(email) != ''
),

-- ============================================================
-- 1. DAILY PAID STOCK / MOVEMENTS / MRR
-- ============================================================

product_scopes AS (
  SELECT DISTINCT product_name FROM paid_runs
  UNION DISTINCT
  SELECT DISTINCT product_name FROM {{ ref('int_combined_subscribers_solidgate_free_daily') }}
  UNION DISTINCT
  SELECT DISTINCT product_name FROM {{ ref('stripe_subscribers') }}
  UNION DISTINCT
  SELECT DISTINCT product_name FROM {{ ref('solidgate_subscribers') }}
  UNION DISTINCT
  SELECT 'All Products'
),

spine AS (
  SELECT
    ps.product_name,
    d AS snapshot_date
  FROM product_scopes ps
  CROSS JOIN params p
  CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(p.dashboard_start, p.max_date)) AS d
),

stock_event_daily AS (
  SELECT product_name, event_date, SUM(delta) AS delta
  FROM (
    SELECT r.product_name, GREATEST(r.run_start, p.dashboard_start) AS event_date, 1 AS delta
    FROM paid_runs r
    CROSS JOIN params p
    WHERE r.run_last_paid >= p.dashboard_start
      AND r.run_start <= p.max_date

    UNION ALL

    SELECT r.product_name, DATE_ADD(LEAST(r.run_last_paid, p.max_date), INTERVAL 1 DAY), -1
    FROM paid_runs r
    CROSS JOIN params p
    WHERE r.run_last_paid >= p.dashboard_start
      AND r.run_start <= p.max_date
  )
  GROUP BY 1,2
),

daily_stock AS (
  SELECT
    s.product_name,
    s.snapshot_date,
    SUM(COALESCE(e.delta, 0)) OVER (
      PARTITION BY s.product_name
      ORDER BY s.snapshot_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS paid_subscribers
  FROM spine s
  LEFT JOIN stock_event_daily e
    ON s.product_name = e.product_name
   AND s.snapshot_date = e.event_date
),

-- A subscriber has at most one run starting / churning per day, so COUNT(*) equals
-- COUNT(DISTINCT subscriber_key) here.
daily_movements AS (
  SELECT
    product_name,
    snapshot_date,
    COUNTIF(movement_type = 'new') AS new_subscribers,
    COUNTIF(movement_type = 'reactivated') AS reactivated_subscribers,
    COUNTIF(movement_type = 'churned') AS churned_subscribers
  FROM (
    SELECT
      product_name,
      run_start AS snapshot_date,
      IF(run_number = 1, 'new', 'reactivated') AS movement_type
    FROM paid_runs

    UNION ALL

    SELECT product_name, churn_date, 'churned'
    FROM paid_runs
  )
  GROUP BY 1,2
),

daily_paid AS (
  SELECT
    s.product_name,
    s.snapshot_date,
    COALESCE(ds.paid_subscribers, 0) AS paid_subscribers,
    COALESCE(mv.new_subscribers, 0) AS new_subscribers,
    COALESCE(mv.reactivated_subscribers, 0) AS reactivated_subscribers,
    COALESCE(mv.churned_subscribers, 0) AS churned_subscribers,
    COALESCE(mv.new_subscribers, 0)
      + COALESCE(mv.reactivated_subscribers, 0)
      - COALESCE(mv.churned_subscribers, 0) AS net_change,
    COALESCE(mrr.observed_mrr_users, 0) AS observed_mrr_users,
    COALESCE(mrr.total_mrr_usd, 0) AS total_mrr_usd,
    COALESCE(mrr.missing_fx_rows, 0) AS missing_fx_rows
  FROM spine s
  LEFT JOIN daily_stock ds
    ON s.product_name = ds.product_name
   AND s.snapshot_date = ds.snapshot_date
  LEFT JOIN daily_movements mv
    ON s.product_name = mv.product_name
   AND s.snapshot_date = mv.snapshot_date
  LEFT JOIN mrr_daily mrr
    ON s.product_name = mrr.product_name
   AND s.snapshot_date = mrr.snapshot_date
),

-- ============================================================
-- 2. ASP INPUTS (new-business MRR of first-run starts)
-- ============================================================

provider_nb AS (
  SELECT
    m.snapshot_date,
    COALESCE(pm.product_name, m._elt_stripe_account_id) AS product_name,
    COALESCE(
      CONCAT('user:', CAST(i.user_key AS STRING)),
      CONCAT('stripe:', m._elt_stripe_account_id, ':', m.customer)
    ) AS subscriber_key,
    SUM(m.mrr_usd) AS new_business_mrr_usd,
    COUNTIF(m.mrr_usd IS NULL) AS missing_mrr_rows
  FROM {{ ref('int_stripe_mrr_movements_daily') }} m
  LEFT JOIN stripe_identity i
    ON m._elt_stripe_account_id = i.account_id
   AND m.customer = i.customer_id
  LEFT JOIN stripe_product_map pm
    ON m._elt_stripe_account_id = pm.account_id
  CROSS JOIN params p
  WHERE m.movement_type = 'new_business'
    AND m.snapshot_date <= p.stripe_max_date
  GROUP BY 1,2,3

  UNION ALL

  SELECT
    m.snapshot_date,
    COALESCE(pm.product_name, m._elt_account_name) AS product_name,
    CONCAT('user:', CAST(i.user_key AS STRING)) AS subscriber_key,
    SUM(m.movement_mrr_usd) AS new_business_mrr_usd,
    COUNTIF(m.movement_mrr_usd IS NULL) AS missing_mrr_rows
  FROM {{ ref('int_solidgate_mrr_movements_daily') }} m
  JOIN solidgate_identity i
    ON m._elt_account_name = i.account_name
   AND m.customer_account_id = i.customer_id
  LEFT JOIN solidgate_product_map pm
    ON m._elt_account_name = pm.account_name
  CROSS JOIN params p
  WHERE m.movement_type = 'new_business'
    AND m.snapshot_date <= p.solidgate_max_date
  GROUP BY 1,2,3
),

nb_product AS (
  SELECT
    snapshot_date,
    product_name,
    subscriber_key,
    SUM(new_business_mrr_usd) AS new_business_mrr_usd,
    SUM(missing_mrr_rows) AS missing_mrr_rows
  FROM provider_nb
  GROUP BY 1,2,3
),

nb_scoped AS (
  SELECT * FROM nb_product

  UNION ALL

  SELECT
    snapshot_date,
    'All Products' AS product_name,
    subscriber_key,
    SUM(new_business_mrr_usd) AS new_business_mrr_usd,
    SUM(missing_mrr_rows) AS missing_mrr_rows
  FROM nb_product
  GROUP BY 1,2,3
),

-- (product, run_start, subscriber) is unique for first runs, and nb_scoped / first_run_mrr are
-- unique on the join keys, so plain COUNTs equal the reference COUNT(DISTINCT ...).
daily_asp AS (
  SELECT
    n.product_name,
    n.run_start AS snapshot_date,
    COUNT(*) AS asp_new_customers,
    SUM(
      CASE
        WHEN nb.subscriber_key IS NOT NULL
         AND COALESCE(nb.missing_mrr_rows, 0) = 0
         AND nb.new_business_mrr_usd IS NOT NULL
        THEN nb.new_business_mrr_usd
        ELSE COALESCE(mrr.mrr_usd, 0)
      END
    ) AS new_business_mrr_usd,
    COUNTIF(
      NOT (
        (nb.subscriber_key IS NOT NULL
         AND COALESCE(nb.missing_mrr_rows, 0) = 0
         AND nb.new_business_mrr_usd IS NOT NULL)
        OR
        (mrr.subscriber_key IS NOT NULL
         AND mrr.mrr_usd IS NOT NULL
         AND COALESCE(mrr.missing_fx_rows, 0) = 0)
      )
    ) AS asp_missing_mrr_users
  FROM paid_runs n
  LEFT JOIN nb_scoped nb
    ON n.product_name = nb.product_name
   AND n.run_start = nb.snapshot_date
   AND n.subscriber_key = nb.subscriber_key
  LEFT JOIN first_run_mrr mrr
    ON n.product_name = mrr.product_name
   AND n.run_start = mrr.snapshot_date
   AND n.subscriber_key = mrr.subscriber_key
  WHERE n.run_number = 1
  GROUP BY 1,2
),

-- ============================================================
-- 3. CURRENT COMBINED FREE STATE (baseline, used on stripe_max_date)
-- ============================================================

latest_prices AS (
  SELECT
    _elt_stripe_account_id AS account_id,
    price_id,
    COALESCE(CAST(unit_amount AS BIGNUMERIC), SAFE_CAST(unit_amount_decimal AS BIGNUMERIC)) AS price_amount
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
    price_id
  FROM {{ ref('int_stripe_subscription_items') }}
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _elt_stripe_account_id, subscription_id, subscription_item_id
    ORDER BY _elt_update_timestamp DESC
  ) = 1
),

stripe_classified AS (
  SELECT
    COALESCE(pm.product_name, s._elt_stripe_account_id) AS product_name,
    COALESCE(
      CONCAT('user:', CAST(si.user_key AS STRING)),
      CONCAT('stripe:', s._elt_stripe_account_id, ':', s.customer)
    ) AS subscriber_key,
    s.id AS subscription_id,
    s.status,
    COUNT(i.subscription_item_id) AS item_count,
    COUNTIF(p.price_id IS NOT NULL) AS matched_price_items,
    COUNTIF(p.price_amount IS NOT NULL) AS known_price_amount_items,
    COUNTIF(p.price_amount > 0) AS nonzero_price_items,
    COUNTIF(p.price_amount = 0) AS zero_price_items
  FROM {{ ref('int_stripe_subscriptions') }} s
  LEFT JOIN stripe_identity si
    ON s._elt_stripe_account_id = si.account_id
   AND s.customer = si.customer_id
  LEFT JOIN stripe_product_map pm
    ON s._elt_stripe_account_id = pm.account_id
  LEFT JOIN latest_items i
    ON s._elt_stripe_account_id = i.account_id
   AND s.id = i.subscription_id
  LEFT JOIN latest_prices p
    ON i.account_id = p.account_id
   AND i.price_id = p.price_id
  WHERE s.status IN ('active', 'past_due', 'trialing')
  GROUP BY 1,2,3,4
),

-- Stripe free QA: active / past_due subscriptions whose items cannot be fully priced.
stripe_free_qa AS (
  SELECT
    COUNTIF(
      status IN ('active', 'past_due')
      AND (
        item_count = 0
        OR matched_price_items < item_count
        OR known_price_amount_items < item_count
      )
    ) AS unresolved_price_subscriptions
  FROM (
    SELECT
      s.id AS subscription_id,
      s.status,
      COUNT(i.subscription_item_id) AS item_count,
      COUNTIF(p.price_id IS NOT NULL) AS matched_price_items,
      COUNTIF(p.price_amount IS NOT NULL) AS known_price_amount_items
    FROM {{ ref('int_stripe_subscriptions') }} s
    LEFT JOIN latest_items i
      ON s._elt_stripe_account_id = i.account_id
     AND s.id = i.subscription_id
    LEFT JOIN latest_prices p
      ON i.account_id = p.account_id
     AND i.price_id = p.price_id
    WHERE s.status IN ('active', 'past_due', 'trialing')
    GROUP BY 1,2
  )
),

stripe_free_product AS (
  SELECT DISTINCT
    product_name,
    subscriber_key,
    status = 'active' AS is_active
  FROM stripe_classified
  WHERE status IN ('active', 'past_due')
    AND item_count > 0
    AND matched_price_items = item_count
    AND known_price_amount_items = item_count
    AND nonzero_price_items = 0
    AND zero_price_items = item_count
),

solidgate_mrr_account_coverage AS (
  SELECT
    _elt_account_name AS account_name,
    MIN(snapshot_date) AS first_mrr_date
  FROM {{ ref('int_solidgate_mrr_local_subscription_daily') }}
  WHERE mrr_local > 0
  GROUP BY 1
),

solidgate_active_product AS (
  SELECT DISTINCT
    COALESCE(pm.product_name, h.account_name) AS product_name,
    CASE
      WHEN i.user_key IS NOT NULL THEN CONCAT('user:', CAST(i.user_key AS STRING))
      WHEN h.customer_email IS NOT NULL AND TRIM(h.customer_email) != ''
      THEN CONCAT('email:', LOWER(TRIM(h.customer_email)))
      ELSE NULL
    END AS subscriber_key
  FROM {{ ref('solidgate_subscription_history') }} h
  JOIN solidgate_mrr_account_coverage c
    ON h.account_name = c.account_name
  LEFT JOIN solidgate_identity i
    ON h.account_name = i.account_name
   AND h.customer_id = i.customer_id
  LEFT JOIN solidgate_product_map pm
    ON h.account_name = pm.account_name
  CROSS JOIN params d
  WHERE h.status = 'active'
    AND h.trial = FALSE
    AND d.stripe_max_date <= d.solidgate_free_state_max_date
    AND d.stripe_max_date >= c.first_mrr_date
    AND TIMESTAMP(d.stripe_max_date) >= h.valid_from
    AND (h.valid_to IS NULL OR TIMESTAMP(d.stripe_max_date) < h.valid_to)
),

current_paid AS (
  SELECT DISTINCT
    r.product_name,
    r.subscriber_key
  FROM paid_runs r
  CROSS JOIN params d
  WHERE d.stripe_max_date BETWEEN r.run_start AND r.run_last_paid
),

current_state AS (
  SELECT
    product_name,
    subscriber_key,
    MAX(is_paid) AS is_paid,
    MAX(has_active_free) AS has_active_free,
    MAX(has_all_qualifying_free) AS has_all_qualifying_free
  FROM (
    SELECT product_name, subscriber_key, 1 AS is_paid, 0 AS has_active_free, 0 AS has_all_qualifying_free
    FROM current_paid

    UNION ALL

    SELECT p.product_name, CONCAT('email:', e.email), 1, 0, 0
    FROM current_paid p
    JOIN solidgate_identity_user_email e
      ON p.subscriber_key = CONCAT('user:', CAST(e.user_key AS STRING))

    UNION ALL

    SELECT product_name, subscriber_key, 0, IF(is_active, 1, 0), 1
    FROM stripe_free_product

    UNION ALL

    SELECT 'All Products', subscriber_key, 0, IF(is_active, 1, 0), 1
    FROM stripe_free_product

    UNION ALL

    SELECT product_name, subscriber_key, 0, 1, 1
    FROM solidgate_active_product
    WHERE subscriber_key IS NOT NULL

    UNION ALL

    SELECT 'All Products', subscriber_key, 0, 1, 1
    FROM solidgate_active_product
    WHERE subscriber_key IS NOT NULL
  )
  GROUP BY 1,2
),

current_free_stock AS (
  SELECT
    product_name,
    COUNTIF(has_active_free = 1 AND is_paid = 0) AS free_subscribers,
    COUNTIF(
      has_all_qualifying_free = 1
      AND has_active_free = 0
      AND is_paid = 0
    ) AS past_due_free_only
  FROM current_state
  GROUP BY 1
),

-- ============================================================
-- 4. MONTHLY TRAILING CHURN FOR LTV / ACL
-- ============================================================

month_bounds AS (
  SELECT
    month_start,
    DATE_SUB(month_start, INTERVAL 1 DAY) AS opening_snapshot,
    LAST_DAY(month_start) AS month_end
  FROM params,
  UNNEST(
    GENERATE_DATE_ARRAY(
      DATE_ADD(DATE_TRUNC(dashboard_start, MONTH), INTERVAL 1 MONTH),
      DATE_TRUNC(max_date, MONTH),
      INTERVAL 1 MONTH
    )
  ) AS month_start
),

monthly_scope AS (
  SELECT ps.product_name, m.*
  FROM product_scopes ps
  CROSS JOIN month_bounds m
),

-- Runs of one subscriber never overlap, so each subscriber matches at most one run per date
-- and the opening / ending sets need no DISTINCT.
churn_counts AS (
  SELECT
    o.product_name,
    o.month_start,
    COUNT(*) AS opening_paid_subscribers,
    IF(
      ANY_VALUE(o.month_end) <= ANY_VALUE(p.max_date),
      COUNTIF(e.subscriber_key IS NULL),
      NULL
    ) AS churned_from_start
  FROM (
    SELECT m.product_name, m.month_start, m.month_end, r.subscriber_key
    FROM monthly_scope m
    JOIN paid_runs r
      ON m.product_name = r.product_name
     AND m.opening_snapshot BETWEEN r.run_start AND r.run_last_paid
  ) o
  LEFT JOIN (
    SELECT m.product_name, m.month_start, r.subscriber_key
    FROM monthly_scope m
    JOIN paid_runs r
      ON m.product_name = r.product_name
     AND m.month_end BETWEEN r.run_start AND r.run_last_paid
  ) e
    ON o.product_name = e.product_name
   AND o.month_start = e.month_start
   AND o.subscriber_key = e.subscriber_key
  CROSS JOIN params p
  GROUP BY 1,2
),

monthly_churn AS (
  SELECT
    s.product_name,
    s.month_start,
    IF(
      COUNT(SAFE_DIVIDE(c.churned_from_start, NULLIF(c.opening_paid_subscribers, 0))) OVER six = 6,
      AVG(SAFE_DIVIDE(c.churned_from_start, NULLIF(c.opening_paid_subscribers, 0))) OVER six,
      NULL
    ) AS churn_rate_6m_avg
  FROM monthly_scope s
  LEFT JOIN churn_counts c
    ON s.product_name = c.product_name
   AND s.month_start = c.month_start
  WINDOW six AS (
    PARTITION BY s.product_name
    ORDER BY s.month_start
    ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
  )
),

-- ============================================================
-- 5. DAILY CORE + CORRECTED FREE / ALL OVERLAY
-- ============================================================

daily_core AS (
  SELECT
    p.product_name,
    p.snapshot_date,
    p.paid_subscribers,

    CASE
      WHEN p.snapshot_date = prm.stripe_max_date
      THEN COALESCE(cf.free_subscribers, 0)
      WHEN sg.product_name IS NOT NULL
      THEN COALESCE(sg.free_subscribers, 0)
      ELSE NULL
    END AS free_subscribers,

    p.paid_subscribers
      + COALESCE(
          CASE
            WHEN p.snapshot_date = prm.stripe_max_date THEN cf.free_subscribers
            WHEN sg.product_name IS NOT NULL THEN sg.free_subscribers
            ELSE NULL
          END,
          0
        )
      + COALESCE(
          CASE
            WHEN p.snapshot_date = prm.stripe_max_date THEN cf.past_due_free_only
            ELSE NULL
          END,
          0
        ) AS all_subscribers,

    p.new_subscribers,
    p.reactivated_subscribers,
    p.churned_subscribers,
    p.net_change,
    p.total_mrr_usd,
    SAFE_DIVIDE(p.total_mrr_usd, NULLIF(p.paid_subscribers, 0)) AS arpa_usd,
    COALESCE(a.asp_new_customers, 0) AS asp_new_customers,
    COALESCE(a.new_business_mrr_usd, 0) AS new_business_mrr_usd,
    COALESCE(a.asp_missing_mrr_users, 0) AS asp_missing_mrr_users,
    c.churn_rate_6m_avg AS customer_churn_rate_6m_avg,
    c.churn_rate_6m_avg AS paid_subscriber_churn_rate_6m_avg,
    p.paid_subscribers - p.observed_mrr_users AS paid_without_mrr_today,
    p.missing_fx_rows,
    CASE
      WHEN p.snapshot_date = prm.stripe_max_date THEN qa.unresolved_price_subscriptions
      ELSE NULL
    END AS unresolved_price_subscriptions,
    (
      p.snapshot_date = prm.stripe_max_date
      AND p.snapshot_date <= prm.solidgate_free_state_max_date
    ) AS free_all_state_available
  FROM daily_paid p
  CROSS JOIN params prm
  CROSS JOIN stripe_free_qa qa
  LEFT JOIN daily_asp a
    ON p.product_name = a.product_name
   AND p.snapshot_date = a.snapshot_date
  LEFT JOIN {{ ref('int_combined_subscribers_solidgate_free_daily') }} sg
    ON p.product_name = sg.product_name
   AND p.snapshot_date = sg.snapshot_date
  LEFT JOIN current_free_stock cf
    ON p.product_name = cf.product_name
  LEFT JOIN monthly_churn c
    ON p.product_name = c.product_name
   AND DATE_TRUNC(p.snapshot_date, MONTH) = c.month_start
)

SELECT
  d.* REPLACE (
    IF(f.snapshot_date IS NOT NULL, f.free_subscribers, d.free_subscribers) AS free_subscribers,
    IF(
      f.snapshot_date IS NOT NULL,
      d.paid_subscribers + f.free_subscribers + f.past_due_free_only,
      d.all_subscribers
    ) AS all_subscribers,
    IF(f.snapshot_date IS NOT NULL, f.free_all_state_available, d.free_all_state_available)
      AS free_all_state_available
  )
FROM daily_core d
LEFT JOIN {{ ref('int_combined_subscribers_free_daily') }} f
  ON d.product_name = f.product_name
 AND d.snapshot_date = f.snapshot_date
