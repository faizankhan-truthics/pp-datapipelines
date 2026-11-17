{{ config(
    materialized='table',
    tags=["subscribers", "silver", "daily"],
) }}

-- Historical Solidgate free (active, non-trial, not paid anywhere) subscriber stock per
-- product (and 'All Products') x day, from each product's first Solidgate MRR date to the
-- Solidgate free-state coverage date. Paid wins over Free using the combined Stripe +
-- Solidgate paid runs, also matched through the Solidgate e-mail alias of each user_key.
-- Computed as interval sweeps (+1/-1 events) so nothing is exploded to subscriber x day.

WITH params AS (
  SELECT
    dashboard_start,
    solidgate_free_state_max_date AS free_state_max_date
  FROM {{ ref('int_combined_subscribers_params') }}
),

solidgate_product_map AS (
  SELECT
    solidgate_account_name AS account_name,
    COALESCE(product_name, solidgate_account_name) AS product_name
  FROM {{ ref('dim_product_payment_map') }}
  WHERE LOWER(payment_provider) = 'solidgate'
    AND solidgate_account_name IS NOT NULL
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

mrr_account_coverage AS (
  SELECT
    _elt_account_name AS account_name,
    MIN(snapshot_date) AS first_mrr_date
  FROM {{ ref('int_solidgate_mrr_local_subscription_daily') }}
  WHERE mrr_local > 0
  GROUP BY 1
),

product_coverage_base AS (
  SELECT
    COALESCE(pm.product_name, c.account_name) AS product_name,
    MIN(c.first_mrr_date) AS first_data_date
  FROM mrr_account_coverage c
  LEFT JOIN solidgate_product_map pm
    ON c.account_name = pm.account_name
  GROUP BY 1
),

product_coverage AS (
  SELECT product_name, first_data_date
  FROM product_coverage_base

  UNION ALL

  SELECT
    'All Products' AS product_name,
    MIN(first_data_date) AS first_data_date
  FROM product_coverage_base
),

active_identity_intervals_base AS (
  SELECT
    COALESCE(pm.product_name, h.account_name) AS product_name,
    CASE
      WHEN i.user_key IS NOT NULL
      THEN CONCAT('user:', CAST(i.user_key AS STRING))
      WHEN h.customer_email IS NOT NULL AND TRIM(h.customer_email) != ''
      THEN CONCAT('email:', LOWER(TRIM(h.customer_email)))
      ELSE NULL
    END AS subscriber_identity,
    GREATEST(
      DATE(h.valid_from),
      c.first_mrr_date,
      p.dashboard_start
    ) AS interval_start,
    LEAST(
      COALESCE(DATE_SUB(DATE(h.valid_to), INTERVAL 1 DAY), p.free_state_max_date),
      p.free_state_max_date
    ) AS interval_end
  FROM {{ ref('solidgate_subscription_history') }} h
  JOIN mrr_account_coverage c
    ON h.account_name = c.account_name
  LEFT JOIN solidgate_identity i
    ON h.account_name = i.account_name
   AND h.customer_id = i.customer_id
  LEFT JOIN solidgate_product_map pm
    ON h.account_name = pm.account_name
  CROSS JOIN params p
  WHERE h.status = 'active'
    AND h.trial = FALSE
    AND h.valid_from < TIMESTAMP(DATE_ADD(p.free_state_max_date, INTERVAL 1 DAY))
),

active_identity_intervals_valid AS (
  SELECT product_name, subscriber_identity, interval_start, interval_end
  FROM active_identity_intervals_base
  WHERE subscriber_identity IS NOT NULL
    AND interval_start <= interval_end

  UNION ALL

  SELECT 'All Products', subscriber_identity, interval_start, interval_end
  FROM active_identity_intervals_base
  WHERE subscriber_identity IS NOT NULL
    AND interval_start <= interval_end
),

paid_intervals_direct AS (
  SELECT
    product_name,
    subscriber_key AS subscriber_identity,
    run_start AS interval_start,
    run_last_paid AS interval_end
  FROM {{ ref('int_combined_subscribers_paid_runs') }}
),

paid_exclusion_raw AS (
  SELECT * FROM paid_intervals_direct

  UNION ALL

  SELECT
    r.product_name,
    CONCAT('email:', e.email) AS subscriber_identity,
    r.interval_start,
    r.interval_end
  FROM paid_intervals_direct r
  JOIN solidgate_identity_user_email e
    ON r.subscriber_identity = CONCAT('user:', CAST(e.user_key AS STRING))
),

-- Only paid intervals of identities that were ever Solidgate-active can affect the result.
paid_exclusion_relevant AS (
  SELECT
    p.product_name,
    p.subscriber_identity,
    p.interval_start,
    p.interval_end
  FROM paid_exclusion_raw p
  JOIN (
    SELECT DISTINCT product_name, subscriber_identity
    FROM active_identity_intervals_valid
  ) a
    ON p.product_name = a.product_name
   AND p.subscriber_identity = a.subscriber_identity
),

-- Overlapping intervals of the same kind are handled by counting (> 0), so they do not
-- need to be merged into islands first.
free_state_events AS (
  SELECT product_name, subscriber_identity, interval_start AS event_date, 1 AS active_delta, 0 AS paid_delta
  FROM active_identity_intervals_valid
  UNION ALL
  SELECT product_name, subscriber_identity, DATE_ADD(interval_end, INTERVAL 1 DAY), -1, 0
  FROM active_identity_intervals_valid
  UNION ALL
  SELECT product_name, subscriber_identity, interval_start, 0, 1
  FROM paid_exclusion_relevant
  UNION ALL
  SELECT product_name, subscriber_identity, DATE_ADD(interval_end, INTERVAL 1 DAY), 0, -1
  FROM paid_exclusion_relevant
),

free_state_cumulative AS (
  SELECT
    product_name,
    subscriber_identity,
    event_date,
    SUM(SUM(active_delta)) OVER w AS active_count,
    SUM(SUM(paid_delta)) OVER w AS paid_count,
    LEAD(event_date) OVER (
      PARTITION BY product_name, subscriber_identity
      ORDER BY event_date
    ) AS next_event_date
  FROM free_state_events
  GROUP BY 1,2,3
  WINDOW w AS (
    PARTITION BY product_name, subscriber_identity
    ORDER BY event_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  )
),

free_intervals AS (
  SELECT
    product_name,
    event_date AS interval_start,
    DATE_SUB(next_event_date, INTERVAL 1 DAY) AS interval_end
  FROM free_state_cumulative
  CROSS JOIN params p
  WHERE active_count > 0
    AND paid_count = 0
    AND next_event_date IS NOT NULL
    AND event_date <= p.free_state_max_date
    AND DATE_SUB(next_event_date, INTERVAL 1 DAY) >= p.dashboard_start
),

free_count_event_daily AS (
  SELECT product_name, event_date, SUM(delta) AS delta
  FROM (
    SELECT
      f.product_name,
      GREATEST(f.interval_start, p.dashboard_start) AS event_date,
      1 AS delta
    FROM free_intervals f
    CROSS JOIN params p

    UNION ALL

    SELECT
      f.product_name,
      DATE_ADD(LEAST(f.interval_end, p.free_state_max_date), INTERVAL 1 DAY) AS event_date,
      -1 AS delta
    FROM free_intervals f
    CROSS JOIN params p
  )
  GROUP BY 1,2
),

scope_spine AS (
  SELECT
    pc.product_name,
    d AS snapshot_date,
    pc.first_data_date
  FROM product_coverage pc
  CROSS JOIN params p
  CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(p.dashboard_start, p.free_state_max_date)) AS d
  WHERE d >= pc.first_data_date
)

SELECT
  s.product_name,
  s.snapshot_date,
  SUM(COALESCE(e.delta, 0)) OVER (
    PARTITION BY s.product_name
    ORDER BY s.snapshot_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS free_subscribers,
  s.first_data_date
FROM scope_spine s
LEFT JOIN free_count_event_daily e
  ON s.product_name = e.product_name
 AND s.snapshot_date = e.event_date
