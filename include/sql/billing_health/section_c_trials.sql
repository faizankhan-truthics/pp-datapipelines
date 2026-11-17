-- Billing health, Section C (trial-to-paid): weekly cohorts of trial starts, one row per active
-- product x trial length x cohort week (the last 13 weeks: the
-- latest final cohort of a 14-day trial and the 4 before it), as of {{ params.report_date }} (D-1, UTC).
-- A product with no trials returns one row with cohort_week NULL.
--
-- Trial = (Solidgate) a term-0 invoice whose billing period is under 20 days and cheaper than the
-- product's regular price; the period is the trial. The current subscription / product tables
-- can't be used: Solidgate swaps the product on conversion, so converted trials vanish.
-- (Stripe-direct) a subscription with trial_start / trial_end under 20 days; its first
-- subscription_cycle invoice is the first full charge.
-- Cohort week = ISO week (Mon, UTC) of the trial start.
--
-- Every trial lands in exactly one bucket, so cols 6 + 10 + 11 + 12 + 13 = 100%:
--   6  cancelled before trial end: no first full charge, cancelled during the trial (cancel
--      time before trial end, or the subscription's cancelled_at at / before trial end: a cancel
--      during the trial takes effect at its end)
--   10 total converted = 8 converted on first attempt + 9 recovered by retries
--      (paid within 30 days of the first attempt, not refunded / disputed in cycle 1)
--   11 failed after all retries: not paid within 30 days of the first attempt
--   12 other: paid, then refunded / disputed in cycle 1 (and the rare trial that ended with no
--      charge and was cancelled only after the trial)
--   13 still pending: in trial (incl. extended trials: no charge, subscription still active),
--      in the 30-day retry window, or Stripe first charges whose outcome can't be read (API
--      version dahlia: invoice not linked to its charges)
--   7  reached first charge (info): has a first full charge attempt
--   14 top decline reason at the failed first attempt (Stripe outcome_reason, else the
--      Solidgate code through seed_billing_decline_reason_map)
-- Cancel time (Solidgate) = first subscription history version with cancelled_at, which history
-- only has for subscriptions it saw from their start (late Sep 2026 for some accounts); without
-- it, a cancelled_at at / before the trial end (a cancel made during the trial) counts.
--
-- A cohort is final once trial length + 30 days has passed for the whole week; until then it
-- shows pending and gets no colour. Final cohorts are judged against the avg of the 4 previous
-- final cohorts (>= 50 trials, not no_source, at least 3), else the frozen seed baseline:
--   total converted:        amber 5 pts below, red 10 pts below
--   cancelled before end:   amber 5 pts above, red 10 pts above
-- Fewer than 50 trials -> low_volume; over 5% of first charges unlinked -> no_source.
-- alert_now: red and the cohort became final between alert_from (the day after the last report
-- date whose alerts were posted; the report date itself on a normal daily run) and the report
-- date, so a cohort that turned final on a day whose run failed is still alerted.
-- Prices: minor units / minor_unit_divisor (1 for zero-decimal currencies, else 100; same list as
-- the dbt macro), in the invoice's currency, returned next to each price (*_currency).

{% macro minor_unit_divisor(currency_column) -%}
    CASE WHEN UPPER({{ currency_column }}) IN ('BIF', 'CLP', 'DJF', 'GNF', 'JPY', 'KMF', 'KRW', 'MGA',
                                               'PYG', 'RWF', 'VND', 'VUV', 'XAF', 'XOF', 'XPF')
         THEN 1 ELSE 100 END
{%- endmacro %}

WITH params AS (
    SELECT DATE '{{ params.report_date }}' AS report_date,
           DATE '{{ params.alert_from }}' AS alert_from,
           DATE_TRUNC(DATE_SUB(DATE '{{ params.report_date }}', INTERVAL 13 WEEK), ISOWEEK) AS cohort_from,
           TIMESTAMP(DATE_ADD(DATE '{{ params.report_date }}', INTERVAL 1 DAY)) AS as_of
),

seed AS (
    SELECT *
    FROM `{{ params.project }}.{{ params.bronze_dataset }}.seed_billing_health_products`
),

-- Accounts with activity that are not in the seed yet are reported as 'Unmapped', as in Section A.
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
    SELECT product_key, product_name, entity, solidgate_account, stripe_product,
           payments_owner_slack_id, product_owner_slack_id
    FROM seed
    WHERE is_active
    UNION ALL
    SELECT LOWER(account), account, 'Unmapped', account, CAST(NULL AS STRING),
           CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM new_solidgate_accounts
    UNION ALL
    SELECT stripe_product, stripe_product, 'Unmapped', CAST(NULL AS STRING), stripe_product,
           CAST(NULL AS STRING), CAST(NULL AS STRING)
    FROM new_stripe_products
),

-- ------------------------------------------------------------------ Solidgate trials
sg_full_price AS (
    SELECT _elt_account_name AS account, product_id, MAX(invoice_amount_cents) AS full_price_cents
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_subscription_invoices`
    WHERE subscription_term_number >= 1
    GROUP BY 1, 2
),

sg_history AS (
    SELECT account_name AS account, subscription_id,
           MIN(valid_from) AS first_seen_ts,
           MIN(IF(cancelled_at IS NOT NULL, valid_from, NULL)) AS cancel_action_ts
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_subscription_history`
    GROUP BY 1, 2
),

sg_trial_invoices AS (
    SELECT p.product_key, i._elt_account_name AS account, i.subscription_id,
           i.billing_period_started_at AS started_at, i.billing_period_ended_at AS trial_end,
           i.invoice_amount_cents / {{ minor_unit_divisor('sp.currency') }} AS trial_price,
           UPPER(sp.currency) AS trial_currency
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_subscription_invoices` i
    JOIN products p ON p.solidgate_account = i._elt_account_name
    LEFT JOIN `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_subscription_products` sp
      ON sp._elt_account_name = i._elt_account_name AND sp.subscription_id = i.subscription_id
    LEFT JOIN sg_full_price fp ON fp.account = i._elt_account_name AND fp.product_id = i.product_id
    CROSS JOIN params
    WHERE i.subscription_term_number = 0
      AND i.billing_period_started_at >= TIMESTAMP(params.cohort_from)
      AND TIMESTAMP_DIFF(i.billing_period_ended_at, i.billing_period_started_at, DAY) < 20
      AND (fp.full_price_cents IS NULL OR i.invoice_amount_cents < fp.full_price_cents)
),

-- term-1 invoice: its attempts up to the report date
sg_first_charge AS (
    SELECT io._elt_account_name AS account, io.subscription_id,
           MIN(io.order_created_at) AS first_ts,
           ARRAY_AGG(io.order_status ORDER BY io.order_retry_attempt, io.order_created_at LIMIT 1)[OFFSET(0)]
               IN ('auth_ok', 'settle_ok', 'refunded') AS first_ok,
           ARRAY_AGG(io.order_id ORDER BY io.order_retry_attempt, io.order_created_at LIMIT 1)[OFFSET(0)]
               AS first_order_id,
           MIN(IF(io.order_status IN ('auth_ok', 'settle_ok', 'refunded'), io.order_created_at, NULL)) AS paid_ts,
           ARRAY_AGG(IF(io.order_status IN ('auth_ok', 'settle_ok', 'refunded'), io.order_id, NULL)
                     IGNORE NULLS ORDER BY io.order_created_at LIMIT 1)[SAFE_OFFSET(0)] AS paid_order_id,
           ANY_VALUE(i.billing_period_ended_at) AS cycle_end,
           ANY_VALUE(i.invoice_amount_cents / {{ minor_unit_divisor('sp.currency') }}) AS full_price,
           ANY_VALUE(UPPER(sp.currency)) AS full_currency
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_subscription_invoice_orders` io
    JOIN `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_subscription_invoices` i
      USING (_elt_account_name, subscription_id, invoice_id)
    LEFT JOIN `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_subscription_products` sp
      USING (_elt_account_name, subscription_id)
    CROSS JOIN params
    WHERE i.subscription_term_number = 1
      AND io.order_created_at >= TIMESTAMP(params.cohort_from)
      AND io.order_created_at < params.as_of
    GROUP BY 1, 2
),

-- decline reason of a failed first attempt: Stripe's when it went to Stripe, else the mapped code
sg_first_fail_reason AS (
    SELECT a._elt_account_name AS account, a.order_id,
           -- earliest failed attempt, its reason and psp_order_id together
           ARRAY_AGG(COALESCE(m.decline_reason, a.error_code)
                     ORDER BY a.attempt_ts, a.transaction_id LIMIT 1)[OFFSET(0)] AS solidgate_reason,
           ARRAY_AGG(a.psp_order_id ORDER BY a.attempt_ts, a.transaction_id LIMIT 1)[OFFSET(0)] AS psp_order_id
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_card_order_attempts` a
    LEFT JOIN `{{ params.project }}.{{ params.bronze_dataset }}.seed_billing_decline_reason_map` m
      ON m.code = a.error_code
    CROSS JOIN params
    WHERE a.attempt_date >= params.cohort_from
      AND a.is_final
      AND NOT a.is_approved
    GROUP BY 1, 2
),

stripe_reason_by_order AS (
    SELECT JSON_VALUE(c.metadata, '$.psp_order_id') AS psp_order_id,
           ARRAY_AGG(c.outcome_reason ORDER BY c.created_ts, c.id LIMIT 1)[OFFSET(0)] AS reason
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_charges` c
    CROSS JOIN params
    WHERE c.created_date >= params.cohort_from
      AND c.status != 'succeeded'
      AND JSON_VALUE(c.metadata, '$.psp_order_id') IS NOT NULL
    GROUP BY 1
),

sg_refunds AS (
    SELECT o._elt_account_name AS account, o.order_id, o.refunded_at
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_card_orders` o
    CROSS JOIN params
    WHERE o.refunded_at IS NOT NULL AND o.refunded_at < params.as_of
),

sg_chargebacks AS (
    SELECT DISTINCT cb._elt_account_name AS account, cb.order_id
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_chargebacks` cb
    CROSS JOIN params
    WHERE cb.created_date <= params.report_date
),

sg_trials AS (
    SELECT t.product_key, t.started_at, t.trial_end, t.trial_price, t.trial_currency, f.full_price, f.full_currency,
           -- history saw the subscription while still in trial, so its cancel time is real
           COALESCE(h.first_seen_ts <= t.trial_end, FALSE) AS has_history,
           COALESCE(h.cancel_action_ts < t.trial_end AND h.cancel_action_ts < p.as_of, FALSE)
               OR COALESCE(s.cancelled_at <= t.trial_end AND s.cancelled_at < p.as_of, FALSE) AS cancelled_in_trial,
           COALESCE(s.status = 'active', FALSE) AS still_active,
           f.first_ts, f.first_ok, f.paid_ts, FALSE AS unlinked,
           (r.order_id IS NOT NULL AND r.refunded_at <= COALESCE(f.cycle_end, TIMESTAMP_ADD(f.paid_ts, INTERVAL 31 DAY)))
               OR cb.order_id IS NOT NULL AS refunded_or_disputed,
           IF(f.first_ok, NULL, COALESCE(st.reason, fr.solidgate_reason)) AS first_decline_reason
    FROM sg_trial_invoices t
    CROSS JOIN params p
    LEFT JOIN sg_first_charge f ON f.account = t.account AND f.subscription_id = t.subscription_id
    LEFT JOIN sg_history h ON h.account = t.account AND h.subscription_id = t.subscription_id
    LEFT JOIN `{{ params.project }}.{{ params.bronze_dataset }}.solidgate_subscriptions` s
      ON s._elt_account_name = t.account AND s.id = t.subscription_id
    LEFT JOIN sg_refunds r ON r.account = t.account AND r.order_id = f.paid_order_id
    LEFT JOIN sg_chargebacks cb ON cb.account = t.account AND cb.order_id = f.paid_order_id
    LEFT JOIN sg_first_fail_reason fr ON fr.account = t.account AND fr.order_id = f.first_order_id
    LEFT JOIN stripe_reason_by_order st ON st.psp_order_id = fr.psp_order_id
    WHERE t.started_at >= TIMESTAMP(p.cohort_from)
      AND t.started_at < p.as_of
),

-- ------------------------------------------------------------------ Stripe-direct trials
st_subscriptions AS (
    SELECT p.product_key, s.id AS subscription_id, s.trial_start, s.trial_end, s.canceled_at
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_subscriptions` s
    LEFT JOIN `{{ params.project }}.{{ params.bronze_dataset }}.stripe_accounts` acc
      ON acc.ID = s._elt_stripe_account_id
    JOIN products p ON p.stripe_product = COALESCE(acc.Product, s._elt_stripe_account_id)
    WHERE p.solidgate_account IS NULL
      AND s.trial_start IS NOT NULL
      AND TIMESTAMP_DIFF(s.trial_end, s.trial_start, DAY) < 20
),

st_invoices AS (
    SELECT i.subscription, i.billing_reason, i.id, i.payment_intent, i.created_date,
           i.period_end_date, i.amount_due_local, i.currency, i.attempted
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_invoices` i
    CROSS JOIN params
    WHERE i.billing_reason IN ('subscription_create', 'subscription_cycle')
      AND i.created_date BETWEEN DATE_SUB(params.cohort_from, INTERVAL 1 DAY) AND params.report_date
),

st_first_invoices AS (
    SELECT subscription,
           ANY_VALUE(IF(billing_reason = 'subscription_create',
                        STRUCT(amount_due_local / {{ minor_unit_divisor('currency') }} AS price,
                               UPPER(currency) AS currency), NULL)) AS trial,
           -- first full charge = first cycle invoice Stripe attempted to charge (not a $0 cycle,
           -- e.g. a 100% coupon, nor a draft never attempted)
           ARRAY_AGG(IF(billing_reason = 'subscription_cycle' AND amount_due_local > 0 AND attempted, i, NULL)
                     IGNORE NULLS
                     ORDER BY created_date, id LIMIT 1)[SAFE_OFFSET(0)] AS first_cycle
    FROM st_invoices i
    GROUP BY 1
),

st_charges AS (
    SELECT c.id, c.payment_intent, c.created_ts, c.status, c.outcome_reason
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_charges` c
    CROSS JOIN params
    WHERE c.created_date BETWEEN params.cohort_from AND params.report_date
      AND c.payment_intent IS NOT NULL
),

st_first_charge AS (
    SELECT payment_intent,
           MIN(created_ts) AS first_ts,
           ARRAY_AGG(STRUCT(status, outcome_reason) ORDER BY created_ts LIMIT 1)[OFFSET(0)] AS first_charge,
           MIN(IF(status = 'succeeded', created_ts, NULL)) AS paid_ts,
           ARRAY_AGG(IF(status = 'succeeded', id, NULL) IGNORE NULLS ORDER BY created_ts LIMIT 1)[SAFE_OFFSET(0)]
               AS paid_charge_id
    FROM st_charges
    GROUP BY 1
),

st_refunds AS (
    SELECT r.charge, MIN(r.created_ts) AS refunded_at
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_refunds` r
    CROSS JOIN params
    WHERE r.created_date BETWEEN params.cohort_from AND params.report_date
      AND r.status = 'succeeded'
    GROUP BY 1
),

st_disputes AS (
    SELECT DISTINCT d.charge
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_disputes` d
    CROSS JOIN params
    WHERE d.created_date BETWEEN params.cohort_from AND params.report_date
),

st_trials AS (
    SELECT s.product_key, s.trial_start AS started_at, s.trial_end,
           fi.trial.price AS trial_price,
           fi.trial.currency AS trial_currency,
           fi.first_cycle.amount_due_local / {{ minor_unit_divisor('fi.first_cycle.currency') }} AS full_price,
           UPPER(fi.first_cycle.currency) AS full_currency,
           TRUE AS has_history,
           COALESCE(s.canceled_at <= s.trial_end AND s.canceled_at < p.as_of, FALSE) AS cancelled_in_trial,
           -- Stripe subscription status is stale here (still 'trialing' after the first charge)
           FALSE AS still_active,
           -- first cycle invoice with no payment intent (API dahlia): it reached the first charge,
           -- but its outcome can't be read
           COALESCE(fc.first_ts,
                    IF(fi.first_cycle.payment_intent IS NULL, TIMESTAMP(fi.first_cycle.created_date), NULL))
               AS first_ts,
           fc.first_charge.status = 'succeeded' AS first_ok,
           fc.paid_ts,
           fi.first_cycle.id IS NOT NULL AND fi.first_cycle.payment_intent IS NULL AS unlinked,
           (r.charge IS NOT NULL
            AND r.refunded_at <= TIMESTAMP(DATE_ADD(fi.first_cycle.period_end_date, INTERVAL 1 DAY)))
               OR d.charge IS NOT NULL AS refunded_or_disputed,
           IF(fc.first_charge.status = 'succeeded', NULL, fc.first_charge.outcome_reason) AS first_decline_reason
    FROM st_subscriptions s
    CROSS JOIN params p
    LEFT JOIN st_first_invoices fi ON fi.subscription = s.subscription_id
    LEFT JOIN st_first_charge fc ON fc.payment_intent = fi.first_cycle.payment_intent
    LEFT JOIN st_refunds r ON r.charge = fc.paid_charge_id
    LEFT JOIN st_disputes d ON d.charge = fc.paid_charge_id
    WHERE s.trial_start >= TIMESTAMP(p.cohort_from)
      AND s.trial_start < p.as_of
),

-- ------------------------------------------------------------------ Cohorts
-- The first week of a product's data is partial, so cohorts start the week after it.
data_start AS (
    SELECT p.product_key, DATE_TRUNC(DATE(MIN(i.billing_period_started_at)), ISOWEEK) AS first_week
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_subscription_invoices` i
    JOIN products p ON p.solidgate_account = i._elt_account_name
    WHERE i.subscription_term_number = 0
    GROUP BY 1
    UNION ALL
    SELECT product_key, DATE_TRUNC(DATE(MIN(trial_start)), ISOWEEK)
    FROM st_subscriptions
    GROUP BY 1
),

classified AS (
    SELECT t.*,
           DATE_TRUNC(DATE(t.started_at), ISOWEEK) AS cohort_week,
           CAST(ROUND(TIMESTAMP_DIFF(t.trial_end, t.started_at, MINUTE) / 1440) AS INT64) AS trial_days,
           CASE
               WHEN t.first_ts IS NULL AND t.cancelled_in_trial THEN 'cancelled'
               WHEN t.first_ts IS NULL
                    AND (t.still_active OR p.as_of < TIMESTAMP_ADD(t.trial_end, INTERVAL 30 DAY)) THEN 'pending'
               WHEN t.first_ts IS NULL THEN 'other'
               WHEN t.unlinked THEN 'pending'
               WHEN t.paid_ts <= TIMESTAMP_ADD(t.first_ts, INTERVAL 30 DAY) AND t.refunded_or_disputed THEN 'other'
               WHEN t.paid_ts <= TIMESTAMP_ADD(t.first_ts, INTERVAL 30 DAY) AND t.first_ok THEN 'converted_first'
               WHEN t.paid_ts <= TIMESTAMP_ADD(t.first_ts, INTERVAL 30 DAY) THEN 'recovered'
               WHEN p.as_of < TIMESTAMP_ADD(t.first_ts, INTERVAL 30 DAY) THEN 'pending'
               ELSE 'failed'
           END AS bucket
    FROM (
        SELECT * FROM sg_trials
        UNION ALL
        SELECT * FROM st_trials
    ) t
    CROSS JOIN params p
    JOIN data_start ds USING (product_key)
    WHERE DATE_TRUNC(DATE(t.started_at), ISOWEEK) > ds.first_week
),

top_reason AS (
    SELECT product_key, trial_days, cohort_week, first_decline_reason AS reason
    FROM classified
    WHERE first_decline_reason IS NOT NULL
    GROUP BY 1, 2, 3, 4
    QUALIFY ROW_NUMBER() OVER (PARTITION BY product_key, trial_days, cohort_week ORDER BY COUNT(*) DESC, reason) = 1
),

cohorts AS (
    SELECT c.product_key, c.trial_days, c.cohort_week,
           DATE_ADD(c.cohort_week, INTERVAL 7 + c.trial_days + 30 DAY) AS final_on,
           COUNT(*) AS trials_started,
           COUNTIF(c.bucket = 'cancelled') / COUNT(*)                              AS cancelled_pct,
           COUNTIF(c.first_ts IS NOT NULL) / COUNT(*)                              AS reached_first_charge_pct,
           COUNTIF(c.bucket = 'converted_first') / COUNT(*)                        AS converted_first_pct,
           COUNTIF(c.bucket = 'recovered') / COUNT(*)                              AS recovered_pct,
           COUNTIF(c.bucket IN ('converted_first', 'recovered')) / COUNT(*)        AS total_converted_pct,
           COUNTIF(c.bucket = 'failed') / COUNT(*)                                 AS failed_pct,
           COUNTIF(c.bucket = 'other') / COUNT(*)                                  AS other_pct,
           COUNTIF(c.bucket = 'pending') / COUNT(*)                                AS pending_pct,
           COUNTIF(c.unlinked) AS unlinked_first_charges,
           COUNTIF(c.has_history) / COUNT(*) AS history_coverage
    FROM classified c
    GROUP BY 1, 2, 3
),

-- most common price in its currency (trials that never reached the first charge have none):
-- prices in different currencies are different prices. A tie goes to USD, then the higher price,
-- so the same day always shows the same price.
cohort_prices AS (
    SELECT product_key, trial_days, cohort_week,
           ARRAY_AGG(IF(kind = 'trial', STRUCT(price, currency), NULL) IGNORE NULLS
                     ORDER BY n DESC, currency = 'USD' DESC, price DESC, currency LIMIT 1)[SAFE_OFFSET(0)] AS trial_pick,
           ARRAY_AGG(IF(kind = 'full', STRUCT(price, currency), NULL) IGNORE NULLS
                     ORDER BY n DESC, currency = 'USD' DESC, price DESC, currency LIMIT 1)[SAFE_OFFSET(0)] AS full_pick
    FROM (
        SELECT product_key, trial_days, cohort_week, 'trial' AS kind, trial_price AS price,
               trial_currency AS currency, COUNT(*) AS n
        FROM classified
        WHERE trial_price IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5, 6
        UNION ALL
        SELECT product_key, trial_days, cohort_week, 'full', full_price, full_currency, COUNT(*)
        FROM classified
        WHERE full_price IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5, 6
    )
    GROUP BY 1, 2, 3
),

flagged AS (
    SELECT c.*,
           cp.trial_pick.price AS trial_price,
           cp.trial_pick.currency AS trial_price_currency,
           cp.full_pick.price AS first_full_price,
           cp.full_pick.currency AS first_full_price_currency,
           tr.reason AS top_decline_reason,
           c.final_on <= params.report_date AS is_final,
           -- many first charges with unreadable outcomes (Stripe API dahlia): no colour, and not a
           -- reference for later cohorts either
           c.unlinked_first_charges <= 0.05 * c.reached_first_charge_pct * c.trials_started AS source_ok
    FROM cohorts c
    CROSS JOIN params
    LEFT JOIN cohort_prices cp USING (product_key, trial_days, cohort_week)
    LEFT JOIN top_reason tr USING (product_key, trial_days, cohort_week)
),

metrics AS (
    SELECT product_key, trial_days, cohort_week, 'trial_total_converted' AS metric,
           total_converted_pct AS value, trials_started >= 50 AND is_final AND source_ok AS eligible
    FROM flagged
    UNION ALL
    SELECT product_key, trial_days, cohort_week, 'trial_cancelled_before_end',
           cancelled_pct, trials_started >= 50 AND is_final AND source_ok
    FROM flagged
),

-- avg of the 4 previous eligible (final, >= 50 trials, not no_source) cohorts of the same product
-- and trial length
rolling_avg AS (
    SELECT product_key, trial_days, cohort_week, metric, AVG(rolling_4) AS rolling_4, COUNT(*) AS n_prior
    FROM (
        SELECT cur.product_key, cur.trial_days, cur.cohort_week, cur.metric, prior.value AS rolling_4
        FROM metrics cur
        JOIN metrics prior
          ON prior.product_key = cur.product_key
         AND prior.trial_days = cur.trial_days
         AND prior.metric = cur.metric
         AND prior.cohort_week < cur.cohort_week
         AND prior.eligible
        WHERE TRUE
        QUALIFY ROW_NUMBER() OVER (PARTITION BY cur.product_key, cur.trial_days, cur.cohort_week, cur.metric
                                   ORDER BY prior.cohort_week DESC) <= 4
    )
    GROUP BY 1, 2, 3, 4
),

baselines AS (
    SELECT product_key, metric, CAST(REGEXP_EXTRACT(segment, r'^(\d+)-day') AS INT64) AS trial_days, baseline
    FROM `{{ params.project }}.{{ params.bronze_dataset }}.seed_billing_health_baselines`
    WHERE section = 'C' AND n_periods >= 3
),

scored AS (
    SELECT m.*,
           CASE WHEN r.n_prior >= 3 THEN r.rolling_4 ELSE b.baseline END AS ref,
           CASE WHEN r.n_prior >= 3 THEN '4 cohorts' WHEN b.baseline IS NOT NULL THEN 'baseline' END AS ref_source
    FROM metrics m
    LEFT JOIN rolling_avg r USING (product_key, trial_days, cohort_week, metric)
    LEFT JOIN baselines b USING (product_key, trial_days, metric)
),

statuses AS (
    SELECT s.*, f.is_final, f.final_on,
           CASE
               WHEN NOT f.is_final THEN 'pending'
               WHEN f.trials_started < 50 THEN 'low_volume'
               WHEN NOT f.source_ok THEN 'no_source'
               WHEN s.ref IS NULL THEN 'no_baseline'
               WHEN s.metric = 'trial_total_converted' AND s.value < s.ref - 0.10 THEN 'red'
               WHEN s.metric = 'trial_total_converted' AND s.value < s.ref - 0.05 THEN 'amber'
               WHEN s.metric = 'trial_cancelled_before_end' AND s.value > s.ref + 0.10 THEN 'red'
               WHEN s.metric = 'trial_cancelled_before_end' AND s.value > s.ref + 0.05 THEN 'amber'
               ELSE 'green'
           END AS status
    FROM scored s
    JOIN flagged f USING (product_key, trial_days, cohort_week)
),

pivoted AS (
    SELECT product_key, trial_days, cohort_week,
           ANY_VALUE(IF(metric = 'trial_total_converted', ref, NULL))            AS converted_ref,
           ANY_VALUE(IF(metric = 'trial_total_converted', ref_source, NULL))     AS converted_ref_source,
           ANY_VALUE(IF(metric = 'trial_total_converted', status, NULL))         AS converted_status,
           ANY_VALUE(IF(metric = 'trial_cancelled_before_end', ref, NULL))       AS cancelled_ref,
           ANY_VALUE(IF(metric = 'trial_cancelled_before_end', ref_source, NULL)) AS cancelled_ref_source,
           ANY_VALUE(IF(metric = 'trial_cancelled_before_end', status, NULL))    AS cancelled_status
    FROM statuses
    GROUP BY 1, 2, 3
)

SELECT
    params.report_date,
    p.product_key,
    p.product_name,
    p.entity,
    p.solidgate_account IS NULL AS is_stripe_direct,
    p.payments_owner_slack_id,
    p.product_owner_slack_id,
    f.cohort_week,
    f.trial_days,
    f.trial_price,
    f.trial_price_currency,
    f.first_full_price,
    f.first_full_price_currency,
    f.trials_started,
    f.cancelled_pct,
    f.reached_first_charge_pct,
    f.converted_first_pct,
    f.recovered_pct,
    f.total_converted_pct,
    f.failed_pct,
    f.other_pct,
    f.pending_pct,
    f.top_decline_reason,
    f.unlinked_first_charges,
    f.history_coverage,
    f.is_final,
    f.final_on,
    s.converted_ref,
    s.converted_ref_source,
    COALESCE(s.converted_status, 'low_volume') AS converted_status,
    s.cancelled_ref,
    s.cancelled_ref_source,
    COALESCE(s.cancelled_status, 'low_volume') AS cancelled_status,
    COALESCE(f.final_on BETWEEN params.alert_from AND params.report_date
             AND s.converted_status = 'red', FALSE) AS converted_alert_now,
    COALESCE(f.final_on BETWEEN params.alert_from AND params.report_date
             AND s.cancelled_status = 'red', FALSE) AS cancelled_alert_now
FROM products p
CROSS JOIN params
LEFT JOIN flagged f USING (product_key)
LEFT JOIN pivoted s USING (product_key, trial_days, cohort_week)
ORDER BY p.entity, p.product_name, f.trial_days, f.cohort_week
