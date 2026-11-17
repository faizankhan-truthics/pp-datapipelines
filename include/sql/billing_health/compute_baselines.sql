-- Billing health baselines per product, from {{ params.period_start }} to {{ params.period_end }}.
-- Output (one row per product x section x metric x segment) is what goes into
-- dags/dbt/bigquery_dbt/seeds/seed_billing_health_baselines.csv. Re-run after the shadow period
-- to refresh it.
--
--   A  approval_signups, approval_renewals_first_attempt, blocked_pct, duplicate_pct,
--      retries_after_do_not_retry: mean of the period's daily values (days with >= 50 attempts).
--      Every A metric is judged against its rolling 7-day avg; this is the fallback when there
--      are not 5 recent days. Blocked, duplicates and do-not-retry are red only above their
--      fixed limit AND on a spike above this reference (section_a_status.sql).
--   B  failed_after_all_retries: mean of the last 4 weekly cohorts of first-attempt failures
--      (term >= 1 invoices) whose 30-day retry window closed by period_end ("4-week avg").
--   C  trial_total_converted, trial_cancelled_before_end per trial length: mean of the last 4
--      final weekly trial cohorts (trial + 30 days passed by period_end, not no_source).
-- Solidgate products from Solidgate orders and invoices, Stripe-direct products from Stripe
-- charges, payment intents, invoices and subscriptions, with the same rules as the report SQL.

WITH params AS (
    SELECT DATE '{{ params.period_start }}' AS period_start, DATE '{{ params.period_end }}' AS period_end
),

products AS (
    SELECT product_key, product_name, solidgate_account, stripe_product
    FROM `{{ params.project }}.{{ params.bronze_dataset }}.seed_billing_health_products`
    WHERE is_active
),

-- ------------------------------------------------------------------ Section A (daily metrics)
sg AS (
    SELECT p.product_key, a.attempt_date AS metric_date, a.is_recurring, a.is_approved,
           a._elt_account_name, a.order_id, a.attempt_seq_in_order
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_card_order_attempts` a
    JOIN products p ON p.solidgate_account = a._elt_account_name
    CROSS JOIN params
    WHERE a.attempt_date BETWEEN params.period_start AND params.period_end
      AND a.is_final AND a.amount_minor > 0 AND COALESCE(a.psp_name, '') != 'Fake'
),

first_attempt_orders AS (
    SELECT DISTINCT io._elt_account_name, io.order_id
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_subscription_invoice_orders` io
    CROSS JOIN params
    WHERE io.order_retry_attempt = 0
      AND DATE(io.order_created_at) BETWEEN DATE_SUB(params.period_start, INTERVAL 1 DAY) AND params.period_end
),

sg_daily AS (
    SELECT sg.product_key, sg.metric_date,
           COUNTIF(NOT sg.is_recurring) AS signup_n,
           COUNTIF(NOT sg.is_recurring AND sg.is_approved) AS signup_ok,
           -- the order's first transaction only, not a cascade to another route (as Section A)
           COUNTIF(sg.is_recurring AND f.order_id IS NOT NULL AND sg.attempt_seq_in_order = 1) AS renewal_n,
           COUNTIF(sg.is_recurring AND f.order_id IS NOT NULL AND sg.attempt_seq_in_order = 1
                   AND sg.is_approved) AS renewal_ok
    FROM sg
    LEFT JOIN first_attempt_orders f USING (_elt_account_name, order_id)
    GROUP BY 1, 2
),

-- Stripe-direct approvals, as in section_a_status.sql: sign-ups = charges whose payment intent
-- saves the card (setup_future_usage = 'off_session'), first-attempt renewals = the first charge
-- on a subscription_cycle invoice's payment intent.
direct_charges AS (
    SELECT p.product_key, c.created_date AS metric_date, c.status, c.payment_intent,
           ROW_NUMBER() OVER (PARTITION BY c.payment_intent ORDER BY c.created_ts, c.id) AS seq_in_intent
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_charges` c
    LEFT JOIN `{{ params.project }}.{{ params.bronze_dataset }}.stripe_accounts` acc
      ON acc.ID = c._elt_stripe_account_id
    JOIN products p ON p.stripe_product = COALESCE(acc.Product, c._elt_stripe_account_id)
    CROSS JOIN params
    WHERE p.solidgate_account IS NULL
      AND c.payment_intent IS NOT NULL
      AND c.created_date BETWEEN DATE_SUB(params.period_start, INTERVAL 40 DAY) AND params.period_end
),

direct_daily AS (
    SELECT c.product_key, c.metric_date,
           COUNTIF(pi.setup_future_usage = 'off_session') AS signup_n,
           COUNTIF(pi.setup_future_usage = 'off_session' AND c.status = 'succeeded') AS signup_ok,
           COUNTIF(ci.payment_intent IS NOT NULL AND c.seq_in_intent = 1) AS renewal_n,
           COUNTIF(ci.payment_intent IS NOT NULL AND c.seq_in_intent = 1 AND c.status = 'succeeded') AS renewal_ok
    FROM direct_charges c
    CROSS JOIN params
    LEFT JOIN (
        SELECT id AS payment_intent, setup_future_usage
        FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_payment_intents`
        CROSS JOIN params
        WHERE created_date BETWEEN DATE_SUB(params.period_start, INTERVAL 40 DAY) AND params.period_end
    ) pi USING (payment_intent)
    LEFT JOIN (
        SELECT DISTINCT payment_intent
        FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_invoices`
        CROSS JOIN params
        WHERE billing_reason = 'subscription_cycle' AND payment_intent IS NOT NULL
          AND created_date BETWEEN DATE_SUB(params.period_start, INTERVAL 40 DAY) AND params.period_end
    ) ci USING (payment_intent)
    WHERE c.metric_date BETWEEN params.period_start AND params.period_end
    GROUP BY 1, 2
),

approval_daily AS (
    SELECT * FROM sg_daily
    UNION ALL
    SELECT * FROM direct_daily
),

st AS (
    SELECT p.product_key, c.created_date AS metric_date, c.created_ts, c.outcome_type, c.outcome_reason,
           c.amount_local,
           JSON_VALUE(c.payment_method_details, '$.card.fingerprint') AS fingerprint,
           CONCAT(JSON_VALUE(c.payment_method_details, '$.card.last4'), '|',
                  JSON_VALUE(c.payment_method_details, '$.card.exp_month'), '|',
                  JSON_VALUE(c.payment_method_details, '$.card.exp_year'), '|',
                  JSON_VALUE(c.payment_method_details, '$.card.brand')) AS card_key
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_charges` c
    LEFT JOIN `{{ params.project }}.{{ params.bronze_dataset }}.stripe_accounts` acc
      ON acc.ID = c._elt_stripe_account_id
    JOIN products p ON p.stripe_product = COALESCE(acc.Product, c._elt_stripe_account_id)
    CROSS JOIN params
    WHERE c.created_date BETWEEN DATE_SUB(params.period_start, INTERVAL 1 DAY) AND params.period_end
),

st_daily AS (
    SELECT product_key, metric_date,
           COUNT(*) AS charges,
           COUNTIF(outcome_type = 'blocked') AS blocked_n,
           COUNTIF(secs <= 10) AS duplicate_n,
           COUNTIF(outcome_reason = 'previously_declined_do_not_retry') AS dnr_n
    FROM (
        -- every charge counts; only duplicate detection needs the card key. Both fingerprints
        -- set and different = different cards (as Section A).
        SELECT *, IF(card_key IS NOT NULL AND NOT IFNULL(fingerprint != LAG(fingerprint) OVER same_card, FALSE),
                  TIMESTAMP_DIFF(created_ts, LAG(created_ts) OVER same_card, SECOND), NULL) AS secs
        FROM st
        WINDOW same_card AS (PARTITION BY product_key, card_key, amount_local ORDER BY created_ts)
    )
    CROSS JOIN params
    WHERE metric_date BETWEEN params.period_start AND params.period_end
    GROUP BY 1, 2
),

a_daily AS (
    SELECT product_key, 'approval_signups' AS metric, SAFE_DIVIDE(signup_ok, signup_n) AS value, signup_n AS volume FROM approval_daily
    UNION ALL SELECT product_key, 'approval_renewals_first_attempt', SAFE_DIVIDE(renewal_ok, renewal_n), renewal_n FROM approval_daily
    UNION ALL SELECT product_key, 'blocked_pct', SAFE_DIVIDE(blocked_n, charges), charges FROM st_daily
    UNION ALL SELECT product_key, 'duplicate_pct', SAFE_DIVIDE(duplicate_n, charges), charges FROM st_daily
    UNION ALL SELECT product_key, 'retries_after_do_not_retry', dnr_n, charges FROM st_daily
),

a_base AS (
    SELECT product_key, 'A' AS section, metric, '' AS segment,
           AVG(value) AS baseline, STDDEV(value) AS sd, COUNT(*) AS n_periods,
           'mean of daily values (days with >= 50 attempts)' AS method
    FROM a_daily
    WHERE volume >= 50 AND value IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

-- ------------------------------------------------------------------ Section B (retry cohorts)
b_orders AS (
    SELECT p.product_key, io.subscription_id, io.invoice_id, io.order_retry_attempt,
           io.order_created_at, io.order_status
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_subscription_invoice_orders` io
    JOIN `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_subscription_invoices` i
      USING (_elt_account_name, subscription_id, invoice_id)
    JOIN products p ON p.solidgate_account = io._elt_account_name
    CROSS JOIN params
    WHERE i.subscription_term_number >= 1
      AND DATE(io.order_created_at) BETWEEN DATE_SUB(params.period_end, INTERVAL 100 DAY) AND params.period_end
),

b_invoices AS (
    SELECT product_key, invoice_id,
           MIN(order_created_at) AS first_ts,
           ARRAY_AGG(order_status ORDER BY order_retry_attempt, order_created_at LIMIT 1)[OFFSET(0)]
               IN ('auth_ok', 'settle_ok', 'refunded') AS first_ok,
           MIN(IF(order_status IN ('auth_ok', 'settle_ok', 'refunded'), order_created_at, NULL)) AS paid_ts
    FROM b_orders
    GROUP BY product_key, subscription_id, invoice_id
    UNION ALL
    -- Stripe-direct, as in section_b_retry.sql: subscription_cycle invoices and every charge on
    -- their payment intent; invoices with no payment intent (Stripe API dahlia) can't be linked
    SELECT i.product_key, i.invoice_id,
           MIN(c.created_ts),
           ARRAY_AGG(c.status ORDER BY c.created_ts LIMIT 1)[OFFSET(0)] = 'succeeded',
           MIN(IF(c.status = 'succeeded', c.created_ts, NULL))
    FROM (
        SELECT p.product_key, i.id AS invoice_id, i.payment_intent
        FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_invoices` i
        LEFT JOIN `{{ params.project }}.{{ params.bronze_dataset }}.stripe_accounts` acc
          ON acc.ID = i._elt_stripe_account_id
        JOIN products p ON p.stripe_product = COALESCE(acc.Product, i._elt_stripe_account_id)
        CROSS JOIN params
        WHERE p.solidgate_account IS NULL
          AND i.billing_reason = 'subscription_cycle'
          AND i.attempted
          AND i.amount_due_local > 0
          AND i.payment_intent IS NOT NULL
          AND i.created_date BETWEEN DATE_SUB(params.period_end, INTERVAL 100 DAY) AND params.period_end
    ) i
    JOIN `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_charges` c
      ON c.payment_intent = i.payment_intent
    CROSS JOIN params
    WHERE c.created_date BETWEEN DATE_SUB(params.period_end, INTERVAL 100 DAY) AND params.period_end
    GROUP BY i.product_key, i.invoice_id
),

-- The first data week is partial (history start), so cohorts start the week after the first
-- invoice seen per product; derived from the data, not a fixed date.
b_weeks AS (
    SELECT product_key, DATE_TRUNC(DATE(first_ts), ISOWEEK) AS wk, COUNT(*) AS n,
           COUNTIF(paid_ts IS NULL OR paid_ts > TIMESTAMP_ADD(first_ts, INTERVAL 30 DAY)) / COUNT(*) AS failed
    FROM b_invoices
    WHERE NOT first_ok
    GROUP BY 1, 2
),

b_base AS (
    SELECT product_key, 'B' AS section, 'failed_after_all_retries' AS metric, '' AS segment,
           AVG(failed) AS baseline, STDDEV(failed) AS sd, COUNT(*) AS n_periods,
           'mean of last 4 mature weekly cohorts (>= 50 first-attempt failures)' AS method
    FROM (
        SELECT b.*,
               ROW_NUMBER() OVER (PARTITION BY b.product_key ORDER BY b.wk DESC) AS rn
        FROM b_weeks b
        CROSS JOIN params
        WHERE b.n >= 50
          AND DATE_ADD(b.wk, INTERVAL 6 + 30 DAY) <= params.period_end           -- window closed
          AND b.wk > (SELECT MIN(w.wk) FROM b_weeks w WHERE w.product_key = b.product_key)
    )
    WHERE rn <= 4
    GROUP BY 1, 2, 3, 4
),

-- ------------------------------------------------------------------ Section C (trial cohorts)
c_full_price AS (
    SELECT _elt_account_name AS account, product_id, MAX(invoice_amount_cents) AS full_price_cents
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_subscription_invoices`
    WHERE subscription_term_number >= 1
    GROUP BY 1, 2
),

c_history AS (
    SELECT account_name AS account, subscription_id,
           MIN(valid_from) AS first_seen_ts,
           MIN(IF(cancelled_at IS NOT NULL, valid_from, NULL)) AS cancel_action_ts
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_subscription_history`
    GROUP BY 1, 2
),

c_trials AS (
    SELECT p.product_key, i._elt_account_name AS account, i.subscription_id,
           i.billing_period_started_at AS started_at, i.billing_period_ended_at AS trial_end,
           CAST(ROUND(TIMESTAMP_DIFF(i.billing_period_ended_at, i.billing_period_started_at, MINUTE) / 1440) AS INT64) AS trial_days,
           -- history saw the subscription while still in trial, so a cancel during the trial
           -- has its real time (history only captures subscriptions near their start from late
           -- Sep for some accounts; earlier ones are first seen after the trial)
           COALESCE(h.first_seen_ts <= i.billing_period_ended_at, FALSE) AS has_history,
           -- cancelled during the trial: cancel time from history before the trial end, or the
           -- subscription's cancelled_at at / before it (a cancel during the trial takes effect
           -- at its end); same rule as section_c_trials.sql
           COALESCE(h.cancel_action_ts < i.billing_period_ended_at, FALSE)
               OR COALESCE(s.cancelled_at <= i.billing_period_ended_at, FALSE) AS cancelled_in_trial
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_subscription_invoices` i
    JOIN products p ON p.solidgate_account = i._elt_account_name
    LEFT JOIN c_full_price fp ON fp.account = i._elt_account_name AND fp.product_id = i.product_id
    LEFT JOIN c_history h ON h.account = i._elt_account_name AND h.subscription_id = i.subscription_id
    LEFT JOIN `{{ params.project }}.{{ params.bronze_dataset }}.solidgate_subscriptions` s
      ON s._elt_account_name = i._elt_account_name AND s.id = i.subscription_id
    CROSS JOIN params
    WHERE i.subscription_term_number = 0
      AND TIMESTAMP_DIFF(i.billing_period_ended_at, i.billing_period_started_at, DAY) < 20
      AND (fp.full_price_cents IS NULL OR i.invoice_amount_cents < fp.full_price_cents)
      AND DATE(i.billing_period_started_at) BETWEEN DATE_SUB(params.period_end, INTERVAL 100 DAY) AND params.period_end
),

c_first_charge AS (
    SELECT io._elt_account_name AS account, io.subscription_id,
           MIN(io.order_created_at) AS first_ts,
           MIN(IF(io.order_status IN ('auth_ok', 'settle_ok', 'refunded'), io.order_created_at, NULL)) AS paid_ts,
           ARRAY_AGG(IF(io.order_status IN ('auth_ok', 'settle_ok', 'refunded'), io.order_id, NULL)
                     IGNORE NULLS LIMIT 1)[SAFE_OFFSET(0)] AS paid_order_id,
           ANY_VALUE(i.billing_period_ended_at) AS period_end
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_subscription_invoice_orders` io
    JOIN `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_subscription_invoices` i
      USING (_elt_account_name, subscription_id, invoice_id)
    WHERE i.subscription_term_number = 1
    GROUP BY 1, 2
),

c_refunds AS (
    SELECT _elt_account_name AS account, order_id, refunded_at
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_card_orders`
    WHERE refunded_at IS NOT NULL
),

c_chargebacks AS (
    SELECT DISTINCT _elt_account_name AS account, order_id
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_chargebacks`
),

-- Stripe-direct trials, as in section_c_trials.sql: subscriptions with a trial under 20 days,
-- their first attempted cycle invoice and the charges on its payment intent. A first cycle
-- invoice with no payment intent (Stripe API dahlia) reached the first charge but its outcome
-- can't be read (unlinked).
c_st_trials AS (
    SELECT p.product_key, s.id AS subscription_id, s.trial_start AS started_at,
           CAST(ROUND(TIMESTAMP_DIFF(s.trial_end, s.trial_start, MINUTE) / 1440) AS INT64) AS trial_days,
           COALESCE(s.canceled_at <= s.trial_end, FALSE) AS cancelled_in_trial
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_subscriptions` s
    LEFT JOIN `{{ params.project }}.{{ params.bronze_dataset }}.stripe_accounts` acc
      ON acc.ID = s._elt_stripe_account_id
    JOIN products p ON p.stripe_product = COALESCE(acc.Product, s._elt_stripe_account_id)
    CROSS JOIN params
    WHERE p.solidgate_account IS NULL
      AND s.trial_start IS NOT NULL
      AND TIMESTAMP_DIFF(s.trial_end, s.trial_start, DAY) < 20
      AND DATE(s.trial_start) BETWEEN DATE_SUB(params.period_end, INTERVAL 100 DAY) AND params.period_end
),

c_st_first_cycle AS (
    SELECT i.subscription,
           ARRAY_AGG(STRUCT(i.id, i.payment_intent, i.period_end_date) ORDER BY i.created_date, i.id LIMIT 1)[OFFSET(0)]
               AS first_cycle
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_invoices` i
    CROSS JOIN params
    WHERE i.billing_reason = 'subscription_cycle'
      AND i.amount_due_local > 0
      AND i.attempted
      AND i.created_date BETWEEN DATE_SUB(params.period_end, INTERVAL 100 DAY) AND params.period_end
    GROUP BY 1
),

c_st_first_charge AS (
    SELECT c.payment_intent,
           MIN(c.created_ts) AS first_ts,
           MIN(IF(c.status = 'succeeded', c.created_ts, NULL)) AS paid_ts,
           ARRAY_AGG(IF(c.status = 'succeeded', c.id, NULL) IGNORE NULLS ORDER BY c.created_ts LIMIT 1)[SAFE_OFFSET(0)]
               AS paid_charge_id
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_charges` c
    CROSS JOIN params
    WHERE c.payment_intent IS NOT NULL
      AND c.created_date BETWEEN DATE_SUB(params.period_end, INTERVAL 100 DAY) AND params.period_end
    GROUP BY 1
),

c_st_refunded_or_disputed AS (
    SELECT r.charge, MIN(r.created_ts) AS happened_at
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_refunds` r
    WHERE r.status = 'succeeded'
    GROUP BY 1
    UNION ALL
    SELECT DISTINCT d.charge, TIMESTAMP '1970-01-01'
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_disputes` d
),

c_classified AS (
    SELECT t.product_key, t.trial_days, FALSE AS unlinked, f.first_ts IS NOT NULL AS reached_first_charge,
           DATE_TRUNC(DATE(t.started_at), ISOWEEK) AS wk,
           CASE
               WHEN f.first_ts IS NULL AND t.cancelled_in_trial THEN 'cancelled'
               WHEN f.first_ts IS NULL THEN 'other'
               WHEN f.paid_ts <= TIMESTAMP_ADD(f.first_ts, INTERVAL 30 DAY)
                    AND ((r.order_id IS NOT NULL
                          AND r.refunded_at <= COALESCE(f.period_end, TIMESTAMP_ADD(f.paid_ts, INTERVAL 31 DAY)))
                         OR cb.order_id IS NOT NULL) THEN 'other'
               WHEN f.paid_ts <= TIMESTAMP_ADD(f.first_ts, INTERVAL 30 DAY) THEN 'converted'
               ELSE 'failed'
           END AS bucket
    FROM c_trials t
    LEFT JOIN c_first_charge f ON f.account = t.account AND f.subscription_id = t.subscription_id
    LEFT JOIN c_refunds r ON r.account = t.account AND r.order_id = f.paid_order_id
    LEFT JOIN c_chargebacks cb ON cb.account = t.account AND cb.order_id = f.paid_order_id
    UNION ALL
    SELECT t.product_key, t.trial_days,
           fc.first_cycle.id IS NOT NULL AND fc.first_cycle.payment_intent IS NULL,
           ch.first_ts IS NOT NULL OR (fc.first_cycle.id IS NOT NULL AND fc.first_cycle.payment_intent IS NULL),
           DATE_TRUNC(DATE(t.started_at), ISOWEEK),
           CASE
               WHEN ch.first_ts IS NULL AND t.cancelled_in_trial THEN 'cancelled'
               WHEN fc.first_cycle.id IS NOT NULL AND fc.first_cycle.payment_intent IS NULL THEN 'unlinked'
               WHEN ch.first_ts IS NULL THEN 'other'
               WHEN ch.paid_ts <= TIMESTAMP_ADD(ch.first_ts, INTERVAL 30 DAY)
                    AND rd.happened_at <= TIMESTAMP(DATE_ADD(fc.first_cycle.period_end_date, INTERVAL 1 DAY)) THEN 'other'
               WHEN ch.paid_ts <= TIMESTAMP_ADD(ch.first_ts, INTERVAL 30 DAY) THEN 'converted'
               ELSE 'failed'
           END
    FROM c_st_trials t
    LEFT JOIN c_st_first_cycle fc ON fc.subscription = t.subscription_id
    LEFT JOIN c_st_first_charge ch ON ch.payment_intent = fc.first_cycle.payment_intent
    LEFT JOIN (SELECT charge, MIN(happened_at) AS happened_at FROM c_st_refunded_or_disputed GROUP BY 1) rd
      ON rd.charge = ch.paid_charge_id
),

-- A week with over 5% of its first charges unlinked is no_source in section_c_trials.sql and no
-- reference there, so it is no baseline either.
c_weeks AS (
    SELECT product_key, trial_days, wk, COUNT(*) AS n,
           COUNTIF(bucket = 'converted') / COUNT(*) AS converted,
           COUNTIF(bucket = 'cancelled') / COUNT(*) AS cancelled,
           COUNTIF(unlinked) <= 0.05 * COUNTIF(reached_first_charge) AS source_ok
    FROM c_classified
    GROUP BY 1, 2, 3
),

c_final AS (
    SELECT c.*,
           ROW_NUMBER() OVER (PARTITION BY c.product_key, c.trial_days ORDER BY c.wk DESC) AS rn_all
    FROM c_weeks c
    CROSS JOIN params
    WHERE c.n >= 50
      AND c.source_ok
      AND DATE_ADD(c.wk, INTERVAL 6 + c.trial_days + 30 DAY) <= params.period_end   -- cohort final
      AND c.wk > (SELECT MIN(w.wk) FROM c_weeks w WHERE w.product_key = c.product_key)
),

c_base AS (
    SELECT product_key, 'C' AS section, 'trial_total_converted' AS metric,
           CONCAT(CAST(trial_days AS STRING), '-day trial') AS segment,
           AVG(converted) AS baseline, STDDEV(converted) AS sd, COUNT(*) AS n_periods,
           'mean of last 4 final weekly trial cohorts (>= 50 trials)' AS method
    FROM c_final WHERE rn_all <= 4
    GROUP BY 1, 2, 3, 4
    UNION ALL
    SELECT product_key, 'C', 'trial_cancelled_before_end',
           CONCAT(CAST(trial_days AS STRING), '-day trial'),
           AVG(cancelled), STDDEV(cancelled), COUNT(*),
           'mean of last 4 final weekly trial cohorts (>= 50 trials)'
    FROM c_final WHERE rn_all <= 4
    GROUP BY 1, 2, 3, 4
)

SELECT b.product_key, p.product_name, b.section, b.metric, b.segment,
       ROUND(b.baseline, 4) AS baseline, ROUND(b.sd, 4) AS sd, b.n_periods,
       params.period_start, params.period_end, b.method
FROM (
    SELECT * FROM a_base
    UNION ALL SELECT * FROM b_base
    UNION ALL SELECT * FROM c_base
) b
JOIN products p USING (product_key)
CROSS JOIN params
ORDER BY b.section, p.product_name, b.metric, b.segment
