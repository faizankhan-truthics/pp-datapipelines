-- Billing health, Section B (retry performance): one row per active product for
-- {{ params.report_date }} (D-1, UTC).
--
-- Cohort: subscription charges after sign-up (first full charge after a trial, and renewals)
-- whose FIRST attempt failed, first attempt in the 7 days ending report_date - 30. The 30-day
-- retry window of every charge in the cohort has closed, so the row is final when reported.
--   Solidgate products: term >= 1 invoices and their orders (int_solidgate_subscription_invoice_orders).
--   Stripe-direct products: subscription_cycle invoices and the charges on their payment intent.
--
-- Columns: first-charge failures (= 100%), recovered by retries % (paid on a later attempt
-- within 30 days of the first), failed after all retries % (not paid within 30 days), avg
-- attempts per failed charge (within those 30 days), baseline, target 20%, status.
-- "Paid" = auth_ok / settle_ok / refunded (a refunded order was paid first), Stripe succeeded.
--
-- Baseline = the product's 4-week avg: mean of the 4 earlier non-overlapping 7-day cohorts
-- (ending 7, 14, 21 and 28 days before this one, >= 50 failures each, at least 3 of them). With
-- less history, the frozen baseline from seed_billing_health_baselines is used (ref_source).
-- Status: green at/below baseline + 5 pts, amber above + 5 pts, red above + 10 pts;
-- fewer than 50 first-charge failures -> low_volume. alert_now: red today (every day it is red).

WITH params AS (
    SELECT DATE '{{ params.report_date }}' AS report_date,
           DATE_SUB(DATE '{{ params.report_date }}', INTERVAL 30 DAY) AS cohort_end,
           -- earliest first attempt needed: yesterday's cohort (ends cohort_end - 1) and the 4
           -- cohorts before it (back to cohort_end - 1 - 28 - 6)
           DATE_SUB(DATE '{{ params.report_date }}', INTERVAL 30 + 35 DAY) AS first_attempt_from,
           TIMESTAMP(DATE_ADD(DATE '{{ params.report_date }}', INTERVAL 1 DAY)) AS as_of
),

seed AS (
    SELECT *
    FROM `{{ params.project }}.{{ params.bronze_dataset }}.seed_billing_health_products`
),

-- Accounts with activity that are not in the seed yet (a new launch, a new Solidgate account or
-- Stripe product) are reported as 'Unmapped', as in Section A.
new_solidgate_accounts AS (
    SELECT DISTINCT a._elt_account_name AS account
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_card_order_attempts` a
    CROSS JOIN params
    WHERE a.attempt_date BETWEEN DATE_SUB(params.report_date, INTERVAL 9 DAY) AND params.report_date
      AND a._elt_account_name NOT LIKE '%SANDBOX%'
      AND a._elt_account_name NOT IN (SELECT solidgate_account FROM seed WHERE solidgate_account IS NOT NULL)
),

new_stripe_products AS (
    SELECT DISTINCT COALESCE(acc.Product, c._elt_stripe_account_id) AS stripe_product
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_charges` c
    LEFT JOIN `{{ params.project }}.{{ params.bronze_dataset }}.stripe_accounts` acc
      ON acc.ID = c._elt_stripe_account_id
    CROSS JOIN params
    WHERE c.created_date BETWEEN DATE_SUB(params.report_date, INTERVAL 9 DAY) AND params.report_date
      AND COALESCE(acc.Product, c._elt_stripe_account_id)
          NOT IN (SELECT stripe_product FROM seed WHERE stripe_product IS NOT NULL)
),

products AS (
    SELECT product_key, product_name, entity, solidgate_account, stripe_product, payments_owner_slack_id
    FROM seed
    WHERE is_active
    UNION ALL
    SELECT LOWER(account), account, 'Unmapped', account, CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM new_solidgate_accounts
    UNION ALL
    SELECT stripe_product, stripe_product, 'Unmapped', CAST(NULL AS STRING), stripe_product, CAST(NULL AS STRING)
    FROM new_stripe_products
),

-- Solidgate: one row per term >= 1 invoice whose first attempt (order_retry_attempt = 0) is in
-- range. Invoices first attempted earlier are left out, so a later retry never counts as first.
sg_invoices AS (
    SELECT p.product_key, io.subscription_id, io.invoice_id,
           MIN(io.order_created_at) AS first_ts,
           ARRAY_AGG(io.order_status ORDER BY io.order_retry_attempt, io.order_created_at LIMIT 1)[OFFSET(0)]
               IN ('auth_ok', 'settle_ok', 'refunded') AS first_ok,
           ARRAY_AGG(STRUCT(io.order_created_at AS ts, io.order_status IN ('auth_ok', 'settle_ok', 'refunded') AS ok))
               AS attempts
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_subscription_invoice_orders` io
    JOIN `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_subscription_invoices` i
      USING (_elt_account_name, subscription_id, invoice_id)
    JOIN products p ON p.solidgate_account = io._elt_account_name
    CROSS JOIN params
    WHERE i.subscription_term_number >= 1
      AND io.order_created_at >= TIMESTAMP(params.first_attempt_from)
      AND io.order_created_at < params.as_of
    GROUP BY 1, 2, 3
    HAVING COUNTIF(io.order_retry_attempt = 0) > 0
),

-- Stripe-direct: subscription_cycle invoices created in range and every charge on their payment
-- intent (Stripe retries an invoice on the same payment intent).
st_invoices_raw AS (
    SELECT p.product_key, i.id AS invoice_id, i.payment_intent, i.created_date, i.status, i.attempt_count
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_invoices` i
    LEFT JOIN `{{ params.project }}.{{ params.bronze_dataset }}.stripe_accounts` acc
      ON acc.ID = i._elt_stripe_account_id
    JOIN products p ON p.stripe_product = COALESCE(acc.Product, i._elt_stripe_account_id)
    CROSS JOIN params
    WHERE p.solidgate_account IS NULL
      AND i.billing_reason = 'subscription_cycle'
      AND i.attempted
      AND i.amount_due_local > 0
      AND i.created_date BETWEEN DATE_SUB(params.first_attempt_from, INTERVAL 1 DAY) AND params.report_date
),

st_invoices AS (
    SELECT i.product_key, CAST(NULL AS STRING) AS subscription_id, i.invoice_id,
           MIN(c.created_ts) AS first_ts,
           ARRAY_AGG(c.status ORDER BY c.created_ts LIMIT 1)[OFFSET(0)] = 'succeeded' AS first_ok,
           ARRAY_AGG(STRUCT(c.created_ts AS ts, c.status = 'succeeded' AS ok)) AS attempts
    FROM st_invoices_raw i
    JOIN `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_charges` c
      ON c.payment_intent = i.payment_intent
    CROSS JOIN params
    WHERE c.created_date BETWEEN DATE_SUB(params.first_attempt_from, INTERVAL 1 DAY) AND params.report_date
      AND c.created_ts < params.as_of
    GROUP BY 1, 2, 3
),

-- Invoices with no payment intent can't be linked to their charges: accounts on Stripe API
-- version dahlia moved that link (Online QR Generator since mid-Sep 2026). Their invoice
-- status / attempt_count are not a substitute (stale: they show ~97% failed where the charges
-- showed ~65%), so they are left out and counted, and the report says so.
st_unlinked AS (
    SELECT product_key, COUNT(*) AS unlinked_invoices
    FROM st_invoices_raw
    CROSS JOIN params
    WHERE payment_intent IS NULL
      AND created_date BETWEEN DATE_SUB(params.cohort_end, INTERVAL 6 DAY) AND params.cohort_end
    GROUP BY 1
),

invoices AS (
    SELECT product_key, DATE(first_ts) AS first_date, first_ok,
           EXISTS (SELECT 1 FROM UNNEST(attempts) a
                   WHERE a.ok AND a.ts <= TIMESTAMP_ADD(first_ts, INTERVAL 30 DAY)) AS paid_in_30d,
           (SELECT COUNT(*) FROM UNNEST(attempts) a
            WHERE a.ts <= TIMESTAMP_ADD(first_ts, INTERVAL 30 DAY)) AS attempts_in_30d
    FROM (
        SELECT * FROM sg_invoices
        UNION ALL
        SELECT * FROM st_invoices
    )
    CROSS JOIN params
    WHERE DATE(first_ts) >= params.first_attempt_from
),

-- 7-day cohorts ending on each of the last 30 days up to cohort_end.
cohorts AS (
    SELECT i.product_key, e AS cohort_end,
           COUNT(*)                                                    AS first_charges,
           COUNTIF(NOT i.first_ok)                                     AS failures,
           COUNTIF(NOT i.first_ok AND i.paid_in_30d)                   AS recovered,
           COUNTIF(NOT i.first_ok AND NOT i.paid_in_30d)               AS failed,
           AVG(IF(NOT i.first_ok, i.attempts_in_30d, NULL))            AS avg_attempts_per_failed
    FROM params
    CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(DATE_SUB(params.cohort_end, INTERVAL 29 DAY), params.cohort_end)) e
    JOIN invoices i ON i.first_date BETWEEN DATE_SUB(e, INTERVAL 6 DAY) AND e
    GROUP BY 1, 2
),

rates AS (
    SELECT *,
           SAFE_DIVIDE(recovered, failures) AS recovered_pct,
           SAFE_DIVIDE(failed, failures)    AS failed_pct
    FROM cohorts
),

baselines AS (
    SELECT product_key, baseline
    FROM `{{ params.project }}.{{ params.bronze_dataset }}.seed_billing_health_baselines`
    WHERE section = 'B' AND metric = 'failed_after_all_retries' AND n_periods >= 3
),

scored AS (
    SELECT r.*,
           CASE WHEN h.n_prior >= 3 THEN h.rolling_4w ELSE b.baseline END AS ref_4w,
           CASE WHEN h.n_prior >= 3 THEN '4w' WHEN b.baseline IS NOT NULL THEN 'baseline' END AS ref_source
    FROM rates r
    LEFT JOIN (
        SELECT cur.product_key, cur.cohort_end,
               AVG(prior.failed_pct) AS rolling_4w, COUNT(prior.failed_pct) AS n_prior
        FROM rates cur
        JOIN rates prior
          ON prior.product_key = cur.product_key
         AND DATE_DIFF(cur.cohort_end, prior.cohort_end, DAY) IN (7, 14, 21, 28)
         AND prior.failures >= 50
        GROUP BY 1, 2
    ) h USING (product_key, cohort_end)
    LEFT JOIN baselines b USING (product_key)
),

statuses AS (
    SELECT *,
           CASE
               WHEN failed_pct IS NULL OR failures < 50 THEN 'low_volume'
               WHEN ref_4w IS NULL THEN 'no_baseline'
               WHEN failed_pct > ref_4w + 0.10 THEN 'red'
               WHEN failed_pct > ref_4w + 0.05 THEN 'amber'
               ELSE 'green'
           END AS status
    FROM scored
),

today AS (
    SELECT s.*, prev.status AS prev_status,
           s.status = 'red' AS alert_now
    FROM statuses s
    CROSS JOIN params
    LEFT JOIN statuses prev
      ON prev.product_key = s.product_key
     AND prev.cohort_end = DATE_SUB(s.cohort_end, INTERVAL 1 DAY)
    WHERE s.cohort_end = params.cohort_end
)

SELECT
    params.report_date,
    p.product_key,
    p.product_name,
    p.entity,
    p.solidgate_account IS NULL AS is_stripe_direct,
    p.payments_owner_slack_id,
    DATE_SUB(params.cohort_end, INTERVAL 6 DAY) AS cohort_start,
    params.cohort_end,
    t.first_charges,
    t.failures AS first_charge_failures,
    t.recovered_pct,
    t.failed_pct AS failed_after_all_retries_pct,
    t.avg_attempts_per_failed,
    t.ref_4w AS baseline,
    t.ref_source,
    0.20 AS target,
    COALESCE(t.status, 'low_volume') AS status,
    t.prev_status,
    COALESCE(t.alert_now, FALSE) AS alert_now,
    u.unlinked_invoices
FROM products p
CROSS JOIN params
LEFT JOIN today t USING (product_key)
LEFT JOIN st_unlinked u USING (product_key)
ORDER BY p.entity, p.product_name
