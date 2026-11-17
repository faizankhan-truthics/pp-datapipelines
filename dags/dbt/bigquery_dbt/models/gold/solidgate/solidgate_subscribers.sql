{{ config(
    materialized='table',
    tags=["solidgate", "gold", "daily"],
) }}

-- Product-aware Solidgate subscriber reporting (paid / free / all subscribers, movements,
-- ARPA, ASP, LTV, ACL) at day/week/month/quarter/year grain.
-- "All Products" is computed independently with global user dedup; never sum product rows.
-- Free is capped at the last captured subscription-history date; paid runs bridge gaps <= 15 days.
-- Rebuilt in full on every dbt_dag run (window/interval logic spans full history).

WITH params AS (
  SELECT
    DATE '2025-08-12' AS dashboard_start,

    -- Paid / MRR coverage can continue through the latest complete MRR day.
    LEAST(
      (
        SELECT MAX(snapshot_date)
        FROM {{ ref('int_solidgate_mrr_customer_daily') }}
        WHERE mrr_local > 0
      ),
      DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    ) AS max_date,

    -- Free requires BOTH subscription-state history and Paid exclusion data.
    -- Do not carry open history intervals beyond the last actual GCS capture.
    LEAST(
      (
        SELECT MAX(snapshot_date)
        FROM {{ ref('int_solidgate_mrr_customer_daily') }}
        WHERE mrr_local > 0
      ),
      (
        SELECT MAX(DATE(captured_at))
        FROM {{ ref('solidgate_subscription_history') }}
      ),
      DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    ) AS free_state_max_date
),

-- ============================================================
-- 1. PRODUCT MAPPING
-- ============================================================

product_map AS (
  SELECT
    solidgate_account_name AS account_name,
    COALESCE(product_name, solidgate_account_name) AS product_name,
    product_name IS NULL AS product_name_is_fallback
  FROM {{ ref('dim_product_payment_map') }}
  WHERE LOWER(payment_provider) = 'solidgate'
    AND solidgate_account_name IS NOT NULL
),

-- ============================================================
-- 2. IDENTITY
-- ============================================================

identity_customer AS (
  SELECT DISTINCT
    source_account AS account_name,
    source_customer_id AS customer_id,
    user_key
  FROM {{ ref('int_identity_user_map') }}
  WHERE LOWER(source_system) = 'solidgate'
    AND user_key IS NOT NULL
),

identity_user_email AS (
  SELECT DISTINCT
    user_key,
    LOWER(TRIM(email)) AS email
  FROM {{ ref('int_identity_user_map') }}
  WHERE LOWER(source_system) = 'solidgate'
    AND user_key IS NOT NULL
    AND email IS NOT NULL
    AND TRIM(email) != ''
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

-- ============================================================
-- 3. OBSERVED POSITIVE MRR - ACCOUNT/PRODUCT BASE
-- ============================================================

observed_user_product_mrr_base AS (
  SELECT
    c.snapshot_date,
    COALESCE(pm.product_name, c._elt_account_name) AS product_name,
    i.user_key,
    SUM(c.mrr_usd) AS mrr_usd,
    COUNTIF(c.mrr_usd IS NULL) AS missing_fx_rows
  FROM {{ ref('int_solidgate_mrr_customer_daily') }} c
  JOIN identity_customer i
    ON c._elt_account_name = i.account_name
   AND c.customer_account_id = i.customer_id
  LEFT JOIN product_map pm
    ON c._elt_account_name = pm.account_name
  WHERE c.mrr_local > 0
  GROUP BY 1,2,3
),

-- Product rows + independent All Products row.
observed_user_mrr_scoped AS (
  SELECT
    snapshot_date,
    product_name,
    user_key,
    SUM(mrr_usd) AS mrr_usd,
    SUM(missing_fx_rows) AS missing_fx_rows
  FROM observed_user_product_mrr_base
  GROUP BY 1,2,3

  UNION ALL

  SELECT
    snapshot_date,
    'All Products' AS product_name,
    user_key,
    SUM(mrr_usd) AS mrr_usd,
    SUM(missing_fx_rows) AS missing_fx_rows
  FROM observed_user_product_mrr_base
  GROUP BY 1,2,3
),

observed_mrr_daily AS (
  SELECT
    snapshot_date,
    product_name,
    COUNT(DISTINCT user_key) AS observed_mrr_users,
    SUM(mrr_usd) AS total_mrr_usd,
    SUM(missing_fx_rows) AS missing_fx_rows
  FROM observed_user_mrr_scoped
  GROUP BY 1,2
),

paid_days AS (
  SELECT DISTINCT
    product_name,
    user_key,
    snapshot_date
  FROM observed_user_mrr_scoped
),

-- ============================================================
-- 4. UNIFIED PAID RUNS
--
-- Gap <= 15 days = same paid run.
-- Gap > 15 days  = new run / reactivation.
-- Calculated independently for each Product and All Products.
-- ============================================================

paid_sequence AS (
  SELECT
    product_name,
    user_key,
    snapshot_date,
    LAG(snapshot_date) OVER (
      PARTITION BY product_name, user_key
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
      PARTITION BY product_name, user_key
      ORDER BY snapshot_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS run_id
  FROM paid_flagged
),

paid_runs_raw AS (
  SELECT
    product_name,
    user_key,
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
      PARTITION BY product_name, user_key
      ORDER BY run_start
    ) AS run_number,
    DATE_ADD(run_last_paid, INTERVAL 1 DAY) AS churn_date
  FROM paid_runs_raw
),

paid_runs_clipped AS (
  SELECT
    r.product_name,
    r.user_key,
    r.run_id,
    GREATEST(r.run_start, p.dashboard_start) AS interval_start,
    LEAST(r.run_last_paid, p.max_date) AS interval_end
  FROM paid_runs r
  CROSS JOIN params p
  WHERE r.run_last_paid >= p.dashboard_start
    AND r.run_start <= p.max_date
),

-- ============================================================
-- 5. PRODUCT SCOPES
-- ============================================================

product_scopes AS (
  SELECT DISTINCT product_name
  FROM observed_user_product_mrr_base

  UNION DISTINCT

  SELECT 'All Products'
),

scope_spine AS (
  SELECT
    p.product_name,
    d.snapshot_date
  FROM product_scopes p
  CROSS JOIN date_spine d
),

-- ============================================================
-- 6. PAID STOCK USING INTERVAL EVENTS
-- ============================================================

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
  FROM scope_spine s
  LEFT JOIN paid_stock_event_daily e
    ON s.product_name = e.product_name
   AND s.snapshot_date = e.event_date
),

-- ============================================================
-- 7. SUBSCRIBER MOVEMENTS
--
-- Historical lifecycle is reconstructed from positive-MRR coverage.
-- Gaps <= 15 days remain one paid run; gaps > 15 days start a
-- reactivation. The newest grace-window dates can backfill as later
-- coverage arrives, so recent movement values are provisional.
-- ============================================================

movement_events AS (
  SELECT
    product_name,
    run_start AS snapshot_date,
    user_key,
    CASE
      WHEN run_number = 1 THEN 'new'
      ELSE 'reactivated'
    END AS movement_type
  FROM paid_runs

  UNION ALL

  SELECT
    product_name,
    churn_date AS snapshot_date,
    user_key,
    'churned' AS movement_type
  FROM paid_runs
),

daily_movements AS (
  SELECT
    product_name,
    snapshot_date,
    COUNT(DISTINCT IF(movement_type = 'new', user_key, NULL))
      AS new_subscribers,
    COUNT(DISTINCT IF(movement_type = 'reactivated', user_key, NULL))
      AS reactivated_subscribers,
    COUNT(DISTINCT IF(movement_type = 'churned', user_key, NULL))
      AS churned_subscribers
  FROM movement_events
  GROUP BY 1,2
),

-- ============================================================
-- 8. SOLIDGATE ACCOUNT MRR COVERAGE
-- ============================================================

mrr_account_coverage AS (
  SELECT
    _elt_account_name AS account_name,
    MIN(snapshot_date) AS first_mrr_date,
    MAX(snapshot_date) AS last_mrr_date
  FROM {{ ref('int_solidgate_mrr_local_subscription_daily') }}
  WHERE mrr_local > 0
  GROUP BY 1
),

-- ============================================================
-- 9. ACTIVE NON-TRIAL SUBSCRIPTION IDENTITY INTERVALS
-- ============================================================

active_identity_intervals_base AS (
  SELECT
    COALESCE(pm.product_name, h.account_name) AS product_name,

    CASE
      WHEN i.user_key IS NOT NULL
      THEN CONCAT('user:', CAST(i.user_key AS STRING))

      WHEN h.customer_email IS NOT NULL
        AND TRIM(h.customer_email) != ''
      THEN CONCAT('email:', LOWER(TRIM(h.customer_email)))

      ELSE NULL
    END AS subscriber_identity,

    GREATEST(
      DATE(h.valid_from),
      c.first_mrr_date,
      p.dashboard_start
    ) AS interval_start,

    LEAST(
      COALESCE(
        DATE_SUB(DATE(h.valid_to), INTERVAL 1 DAY),
        p.free_state_max_date
      ),
      p.free_state_max_date
    ) AS interval_end

  FROM {{ ref('solidgate_subscription_history') }} h

  JOIN mrr_account_coverage c
    ON h.account_name = c.account_name

  LEFT JOIN identity_customer i
    ON h.account_name = i.account_name
   AND h.customer_id = i.customer_id

  LEFT JOIN product_map pm
    ON h.account_name = pm.account_name

  CROSS JOIN params p

  WHERE h.status = 'active'
    AND h.trial = FALSE
    AND h.valid_from <
        TIMESTAMP(DATE_ADD(p.free_state_max_date, INTERVAL 1 DAY))
),

-- Product row + All Products row.
active_identity_intervals_scoped AS (
  SELECT
    product_name,
    subscriber_identity,
    interval_start,
    interval_end
  FROM active_identity_intervals_base

  UNION ALL

  SELECT
    'All Products' AS product_name,
    subscriber_identity,
    interval_start,
    interval_end
  FROM active_identity_intervals_base
),

active_identity_intervals_valid AS (
  SELECT
    product_name,
    subscriber_identity,
    interval_start,
    interval_end
  FROM active_identity_intervals_scoped
  WHERE subscriber_identity IS NOT NULL
    AND interval_start <= interval_end
),

-- ============================================================
-- 10. MERGE OVERLAPPING ACTIVE INTERVALS
-- ============================================================

active_interval_previous AS (
  SELECT
    *,
    MAX(interval_end) OVER (
      PARTITION BY product_name, subscriber_identity
      ORDER BY interval_start, interval_end
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS previous_max_end
  FROM active_identity_intervals_valid
),

active_interval_flags AS (
  SELECT
    *,
    CASE
      WHEN previous_max_end IS NULL
        OR interval_start > DATE_ADD(previous_max_end, INTERVAL 1 DAY)
      THEN 1
      ELSE 0
    END AS starts_new_island
  FROM active_interval_previous
),

active_interval_groups AS (
  SELECT
    *,
    SUM(starts_new_island) OVER (
      PARTITION BY product_name, subscriber_identity
      ORDER BY interval_start, interval_end
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS island_id
  FROM active_interval_flags
),

active_intervals AS (
  SELECT
    product_name,
    subscriber_identity,
    island_id,
    MIN(interval_start) AS interval_start,
    MAX(interval_end) AS interval_end
  FROM active_interval_groups
  GROUP BY 1,2,3
),

-- ============================================================
-- 11. PAID EXCLUSION INTERVALS FOR FREE
--
-- Product rows exclude Paid only within that Product.
-- All Products excludes Paid anywhere.
-- ============================================================

paid_user_exclusion_intervals AS (
  SELECT
    product_name,
    CONCAT('user:', CAST(user_key AS STRING)) AS subscriber_identity,
    interval_start,
    interval_end
  FROM paid_runs_clipped
),

paid_email_exclusion_intervals_raw AS (
  SELECT
    r.product_name,
    CONCAT('email:', e.email) AS subscriber_identity,
    r.interval_start,
    r.interval_end
  FROM paid_runs_clipped r
  JOIN identity_user_email e
    ON r.user_key = e.user_key
),

paid_exclusion_intervals_raw AS (
  SELECT * FROM paid_user_exclusion_intervals
  UNION ALL
  SELECT * FROM paid_email_exclusion_intervals_raw
),

paid_exclusion_relevant AS (
  SELECT DISTINCT
    p.product_name,
    p.subscriber_identity,
    p.interval_start,
    p.interval_end
  FROM paid_exclusion_intervals_raw p
  JOIN (
    SELECT DISTINCT
      product_name,
      subscriber_identity
    FROM active_intervals
  ) a
    ON p.product_name = a.product_name
   AND p.subscriber_identity = a.subscriber_identity
),

paid_exclusion_previous AS (
  SELECT
    *,
    MAX(interval_end) OVER (
      PARTITION BY product_name, subscriber_identity
      ORDER BY interval_start, interval_end
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS previous_max_end
  FROM paid_exclusion_relevant
),

paid_exclusion_flags AS (
  SELECT
    *,
    CASE
      WHEN previous_max_end IS NULL
        OR interval_start > DATE_ADD(previous_max_end, INTERVAL 1 DAY)
      THEN 1
      ELSE 0
    END AS starts_new_island
  FROM paid_exclusion_previous
),

paid_exclusion_groups AS (
  SELECT
    *,
    SUM(starts_new_island) OVER (
      PARTITION BY product_name, subscriber_identity
      ORDER BY interval_start, interval_end
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS island_id
  FROM paid_exclusion_flags
),

paid_exclusion_intervals AS (
  SELECT
    product_name,
    subscriber_identity,
    island_id,
    MIN(interval_start) AS interval_start,
    MAX(interval_end) AS interval_end
  FROM paid_exclusion_groups
  GROUP BY 1,2,3
),

-- ============================================================
-- 12. FREE INTERVALS USING STATE EVENTS
-- ============================================================

free_state_events AS (
  SELECT
    product_name,
    subscriber_identity,
    interval_start AS event_date,
    1 AS active_delta,
    0 AS paid_delta
  FROM active_intervals

  UNION ALL

  SELECT
    product_name,
    subscriber_identity,
    DATE_ADD(interval_end, INTERVAL 1 DAY) AS event_date,
    -1 AS active_delta,
    0 AS paid_delta
  FROM active_intervals

  UNION ALL

  SELECT
    product_name,
    subscriber_identity,
    interval_start AS event_date,
    0 AS active_delta,
    1 AS paid_delta
  FROM paid_exclusion_intervals

  UNION ALL

  SELECT
    product_name,
    subscriber_identity,
    DATE_ADD(interval_end, INTERVAL 1 DAY) AS event_date,
    0 AS active_delta,
    -1 AS paid_delta
  FROM paid_exclusion_intervals
),

free_state_event_daily AS (
  SELECT
    product_name,
    subscriber_identity,
    event_date,
    SUM(active_delta) AS active_delta,
    SUM(paid_delta) AS paid_delta
  FROM free_state_events
  GROUP BY 1,2,3
),

free_state_cumulative AS (
  SELECT
    product_name,
    subscriber_identity,
    event_date,

    SUM(active_delta) OVER (
      PARTITION BY product_name, subscriber_identity
      ORDER BY event_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS active_count,

    SUM(paid_delta) OVER (
      PARTITION BY product_name, subscriber_identity
      ORDER BY event_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS paid_count

  FROM free_state_event_daily
),

free_state_boundaries AS (
  SELECT
    *,
    LEAD(event_date) OVER (
      PARTITION BY product_name, subscriber_identity
      ORDER BY event_date
    ) AS next_event_date
  FROM free_state_cumulative
),

free_intervals AS (
  SELECT
    product_name,
    subscriber_identity,
    event_date AS interval_start,
    DATE_SUB(next_event_date, INTERVAL 1 DAY) AS interval_end
  FROM free_state_boundaries
  CROSS JOIN params p
  WHERE active_count > 0
    AND paid_count = 0
    AND next_event_date IS NOT NULL
    AND event_date <= p.max_date
    AND DATE_SUB(next_event_date, INTERVAL 1 DAY) >= p.dashboard_start
),

-- ============================================================
-- 13. FREE STOCK USING INTERVAL EVENTS
-- ============================================================

free_count_events AS (
  SELECT
    f.product_name,
    GREATEST(f.interval_start, p.dashboard_start) AS event_date,
    1 AS delta
  FROM free_intervals f
  CROSS JOIN params p

  UNION ALL

  SELECT
    f.product_name,
    DATE_ADD(
      LEAST(f.interval_end, p.max_date),
      INTERVAL 1 DAY
    ) AS event_date,
    -1 AS delta
  FROM free_intervals f
  CROSS JOIN params p
),

free_count_event_daily AS (
  SELECT
    product_name,
    event_date,
    SUM(delta) AS delta
  FROM free_count_events
  GROUP BY 1,2
),

daily_free_stock AS (
  SELECT
    s.product_name,
    s.snapshot_date,
    SUM(COALESCE(e.delta, 0)) OVER (
      PARTITION BY s.product_name
      ORDER BY s.snapshot_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS free_subscribers
  FROM scope_spine s
  LEFT JOIN free_count_event_daily e
    ON s.product_name = e.product_name
   AND s.snapshot_date = e.event_date
),

-- ============================================================
-- 14. ASP / NEW BUSINESS MRR
-- ============================================================

new_business_user_product_day_base AS (
  SELECT
    m.snapshot_date,
    COALESCE(pm.product_name, m._elt_account_name) AS product_name,
    i.user_key,
    SUM(m.movement_mrr_usd) AS new_business_mrr_usd,
    COUNTIF(m.movement_mrr_usd IS NULL) AS missing_mrr_rows
  FROM {{ ref('int_solidgate_mrr_movements_daily') }} m
  JOIN identity_customer i
    ON m._elt_account_name = i.account_name
   AND m.customer_account_id = i.customer_id
  LEFT JOIN product_map pm
    ON m._elt_account_name = pm.account_name
  WHERE m.movement_type = 'new_business'
  GROUP BY 1,2,3
),

new_business_user_day_scoped AS (
  SELECT
    snapshot_date,
    product_name,
    user_key,
    SUM(new_business_mrr_usd) AS new_business_mrr_usd,
    SUM(missing_mrr_rows) AS missing_mrr_rows
  FROM new_business_user_product_day_base
  GROUP BY 1,2,3

  UNION ALL

  SELECT
    snapshot_date,
    'All Products' AS product_name,
    user_key,
    SUM(new_business_mrr_usd) AS new_business_mrr_usd,
    SUM(missing_mrr_rows) AS missing_mrr_rows
  FROM new_business_user_product_day_base
  GROUP BY 1,2,3
),

first_paid_users AS (
  SELECT
    product_name,
    user_key,
    run_start AS first_paid_date
  FROM paid_runs
  WHERE run_number = 1
),

daily_asp AS (
  SELECT
    f.product_name,
    f.first_paid_date AS snapshot_date,

    COUNT(DISTINCT f.user_key) AS asp_new_customers,

    SUM(
      COALESCE(n.new_business_mrr_usd, 0)
    ) AS new_business_mrr_usd,

    COUNT(DISTINCT IF(
      n.user_key IS NULL
      OR n.missing_mrr_rows > 0,
      f.user_key,
      NULL
    )) AS asp_missing_mrr_users

  FROM first_paid_users f

  LEFT JOIN new_business_user_day_scoped n
    ON f.product_name = n.product_name
   AND f.user_key = n.user_key
   AND f.first_paid_date = n.snapshot_date

  GROUP BY 1,2
),

-- ============================================================
-- 15. MONTHLY CHURN
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
    r.user_key
  FROM monthly_scope_spine m
  JOIN paid_runs r
    ON m.product_name = r.product_name
   AND m.opening_snapshot BETWEEN r.run_start AND r.run_last_paid
),

monthly_ending_users AS (
  SELECT DISTINCT
    m.product_name,
    m.month_start,
    r.user_key
  FROM monthly_scope_spine m
  JOIN paid_runs r
    ON m.product_name = r.product_name
   AND m.month_end BETWEEN r.run_start AND r.run_last_paid
),

monthly_churn_counts AS (
  SELECT
    o.product_name,
    o.month_start,
    COUNT(DISTINCT o.user_key) AS opening_paid_subscribers,

    CASE
      WHEN MAX(o.month_end) <= p.max_date
      THEN COUNT(DISTINCT IF(
        e.user_key IS NULL,
        o.user_key,
        NULL
      ))
      ELSE NULL
    END AS churned_from_start

  FROM monthly_opening_users o

  LEFT JOIN monthly_ending_users e
    ON o.product_name = e.product_name
   AND o.month_start = e.month_start
   AND o.user_key = e.user_key

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
-- 16. DAILY METRICS
-- ============================================================

daily_core AS (
  SELECT
    s.product_name,
    s.snapshot_date,

    COALESCE(p.paid_subscribers, 0)
      AS paid_subscribers,

    -- Free is known only through the last date where both
    -- subscription-state history and Paid exclusion data are available.
    CASE
      WHEN s.snapshot_date <= prm.free_state_max_date
      THEN COALESCE(f.free_subscribers, 0)
      ELSE NULL
    END AS free_subscribers,

    -- All shows every state available on the date. After Free coverage ends,
    -- it remains a partial Paid-only value instead of treating missing Free as 0.
    COALESCE(p.paid_subscribers, 0)
      + CASE
          WHEN s.snapshot_date <= prm.free_state_max_date
          THEN COALESCE(f.free_subscribers, 0)
          ELSE 0
        END
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
      NULLIF(p.paid_subscribers, 0)
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
          NULLIF(p.paid_subscribers, 0)
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

    COALESCE(p.paid_subscribers, 0)
      - COALESCE(mrr.observed_mrr_users, 0)
      AS paid_without_mrr_today,

    COALESCE(mrr.missing_fx_rows, 0)
      AS missing_fx_rows

  FROM scope_spine s

  CROSS JOIN params prm

  LEFT JOIN daily_paid_stock p
    ON s.product_name = p.product_name
   AND s.snapshot_date = p.snapshot_date

  LEFT JOIN daily_free_stock f
    ON s.product_name = f.product_name
   AND s.snapshot_date = f.snapshot_date

  LEFT JOIN daily_movements mv
    ON s.product_name = mv.product_name
   AND s.snapshot_date = mv.snapshot_date

  LEFT JOIN observed_mrr_daily mrr
    ON s.product_name = mrr.product_name
   AND s.snapshot_date = mrr.snapshot_date

  LEFT JOIN daily_asp asp
    ON s.product_name = asp.product_name
   AND s.snapshot_date = asp.snapshot_date

  LEFT JOIN monthly_trailing_churn tc
    ON s.product_name = tc.product_name
   AND DATE_TRUNC(s.snapshot_date, MONTH) = tc.month_start
),

-- ============================================================
-- 17. PERIODIZATION
--
-- Stock metrics = End-of-period.
-- Flow metrics  = Sum within period.
-- ASP           = New Business MRR / New Customers in period.
-- ACL           = Monthly only.
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
        missing_fx_rows
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
-- 18. FINAL OUTPUT CONTRACT
-- ============================================================

SELECT
  'solidgate' AS source_system,

  product_name,

  period_grain,
  period_start,
  period_data_start,
  period_end,

  -- Stock metrics
  eop.paid_subscribers,
  eop.free_subscribers,
  eop.all_subscribers,

  -- Subscriber Movements
  new_subscribers,
  reactivated_subscribers,
  churned_subscribers,
  net_change,

  -- MRR / ARPA
  eop.total_mrr_usd,
  eop.arpa_usd,

  -- ASP
  new_business_mrr_usd,

  CASE
    WHEN asp_new_customers = 0
    THEN NULL

    WHEN asp_missing_mrr_users > 0
    THEN NULL

    ELSE SAFE_DIVIDE(
      new_business_mrr_usd,
      asp_new_customers
    )
  END AS asp_usd,

  -- LTV
  eop.customer_churn_rate_6m_avg,

  CASE
    WHEN eop.customer_churn_rate_6m_avg > 0
    THEN SAFE_DIVIDE(
      eop.arpa_usd,
      eop.customer_churn_rate_6m_avg
    )
    ELSE NULL
  END AS ltv_usd,

  -- ACL
  eop.paid_subscriber_churn_rate_6m_avg,

  CASE
    WHEN period_grain = 'month'
      AND eop.paid_subscriber_churn_rate_6m_avg > 0
    THEN SAFE_DIVIDE(
      1,
      eop.paid_subscriber_churn_rate_6m_avg
    )
    ELSE NULL
  END AS acl_months,

  -- QA
  eop.paid_without_mrr_today,
  eop.missing_fx_rows,
  asp_missing_mrr_users,

  TRUE AS free_is_proxy,

  -- True when this displayed product value is an account-name fallback.
  -- All Products is not considered a fallback.
  CASE
    WHEN product_name = 'All Products' THEN FALSE
    ELSE EXISTS (
      SELECT 1
      FROM product_map pm
      WHERE pm.product_name = period_rollup.product_name
        AND pm.product_name_is_fallback = TRUE
    )
  END AS product_name_is_fallback

FROM period_rollup

ORDER BY
  CASE
    WHEN product_name = 'All Products' THEN 0
    ELSE 1
  END,
  product_name,
  CASE period_grain
    WHEN 'day' THEN 1
    WHEN 'week' THEN 2
    WHEN 'month' THEN 3
    WHEN 'quarter' THEN 4
    WHEN 'year' THEN 5
  END,
  period_start
