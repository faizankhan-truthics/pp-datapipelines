-- Billing health, Section A (payment health): status per product x metric for one report day.
--
-- One row per active product and metric for {{ params.report_date }} (D-1, UTC), with the value,
-- its volume, the reference it is judged against, green / amber / red, yesterday's status and
-- whether it should alert now. A product with no data that day returns one row with metric NULL.
--
-- Attempts are bucketed by when they were made (attempt_ts / Stripe created), never by the
-- order's updated_at, which moves ~4 days later when the order settles.
--
-- Rules:
--   approval_signups, approval_renewals_first_attempt: vs the trailing 7-day avg
--       (amber 5 pts below, red 10 pts below). With fewer than 5 earlier days with volume, the
--       product's own baseline from seed_billing_health_baselines is used instead (ref_source).
--   blocked_pct, duplicate_pct, retries_after_do_not_retry: the ticket's fixed limits, but red
--   only on a spike above the product's own usual level (ref_7d):
--       blocked_pct    green < 1%, amber 1-2%, red > 2% AND 1+ pt above ref_7d
--       duplicate_pct  green < 5%, amber 5-10%, red > 10% AND 5+ pts above ref_7d
--                      (same card + same amount within 10 s)
--       retries_after_do_not_retry (count of Stripe 'previously_declined_do_not_retry')
--                      green 0, amber any, red any AND >= max(2 x ref_7d, ref_7d + 5)
--       Above the limit at the usual level is amber. Several products sit above the limits
--       every day (InfoChecker and Location-Tool retry after do-not-retry 100-1,000 times a
--       day; duplicates on InfoChecker / IQ Center / Reversly swing 6-21% since Nov 6), and red
--       on the fixed limit alone re-alerted them most days. With no usual level yet (fewer than
--       5 earlier days with volume and no seed baseline) the fixed limit alone decides.
--   ref_7d: the trailing 7-day avg (days with volume >= 50) once there are 5 such days, else the
--       product's seed baseline (ref_source).
--   Volume under 50 -> low_volume (no colour), except retries_after_do_not_retry.
--   alert_now: every red, every day it is red (the payments team is tagged each day).
--   Prod replay Oct 7 - Nov 15 2026: 493 red product-days on the fixed limits -> 75 (1.9 a day)
--   with this rule.

WITH params AS (
    SELECT DATE '{{ params.report_date }}' AS report_date
),

seed AS (
    SELECT *
    FROM `{{ params.project }}.{{ params.bronze_dataset }}.seed_billing_health_products`
),

-- Accounts with activity that are not in the seed yet (a new launch, a new Solidgate account or
-- Stripe product) are reported as 'Unmapped' from their first day, so nothing is left out
-- silently. Adding a seed row names the product, sets its entity and its Slack owners.
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

-- Solidgate attempts (Solidgate-routed products). Fake PSP = $0 trial placeholders.
sg AS (
    SELECT p.product_key, a.attempt_date AS metric_date, a.is_recurring, a.is_approved,
           a._elt_account_name, a.order_id, a.attempt_seq_in_order
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_card_order_attempts` a
    JOIN products p ON p.solidgate_account = a._elt_account_name
    CROSS JOIN params
    WHERE a.attempt_date BETWEEN DATE_SUB(params.report_date, INTERVAL 9 DAY) AND params.report_date
      AND a.is_final
      AND a.amount_minor > 0
      AND COALESCE(a.psp_name, '') != 'Fake'
),

-- Renewal orders that are the first attempt of their invoice (retries are Section B). Only the
-- order's first transaction counts: a cascade to another route is not the first attempt.
first_attempt_orders AS (
    SELECT DISTINCT io._elt_account_name, io.order_id
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_subscription_invoice_orders` io
    CROSS JOIN params
    WHERE io.order_retry_attempt = 0
      AND DATE(io.order_created_at) BETWEEN DATE_SUB(params.report_date, INTERVAL 10 DAY) AND params.report_date
),

sg_daily AS (
    SELECT sg.product_key, sg.metric_date,
           COUNTIF(NOT sg.is_recurring)                                          AS signup_n,
           COUNTIF(NOT sg.is_recurring AND sg.is_approved)                       AS signup_ok,
           COUNTIF(sg.is_recurring AND f.order_id IS NOT NULL AND sg.attempt_seq_in_order = 1) AS renewal_n,
           COUNTIF(sg.is_recurring AND f.order_id IS NOT NULL AND sg.attempt_seq_in_order = 1
                   AND sg.is_approved)                                                    AS renewal_ok
    FROM sg
    LEFT JOIN first_attempt_orders f USING (_elt_account_name, order_id)
    GROUP BY 1, 2
),

-- Stripe-direct products (no Solidgate account), the same two approvals from Stripe:
--   sign-ups: customer-initiated charges, i.e. their payment intent saves the card for later
--     charges (setup_future_usage = 'off_session'). On Solidgate-routed Stripe charges this
--     matches Solidgate's auth vs recurring-auth for every sign-up and 99% of renewals.
--   first-attempt renewals: the first charge on a subscription_cycle invoice's payment intent
--     (Stripe retries an invoice on the same payment intent). Invoices without a payment intent
--     (Stripe API dahlia: Online QR Generator) can't be linked, so that product has no renewal
--     value.
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
      -- 40 days back (31 before the earliest scored day), so a retry's first charge is seen and
      -- the retry is not counted as first
      AND c.created_date BETWEEN DATE_SUB(params.report_date, INTERVAL 40 DAY) AND params.report_date
),

direct_intents AS (
    SELECT pi.id AS payment_intent, pi.setup_future_usage
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_payment_intents` pi
    CROSS JOIN params
    WHERE pi.created_date BETWEEN DATE_SUB(params.report_date, INTERVAL 40 DAY) AND params.report_date
),

direct_cycle_intents AS (
    SELECT DISTINCT i.payment_intent
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_invoices` i
    CROSS JOIN params
    WHERE i.billing_reason = 'subscription_cycle'
      AND i.payment_intent IS NOT NULL
      AND i.created_date BETWEEN DATE_SUB(params.report_date, INTERVAL 40 DAY) AND params.report_date
),

direct_daily AS (
    SELECT c.product_key, c.metric_date,
           COUNTIF(pi.setup_future_usage = 'off_session')                                AS signup_n,
           COUNTIF(pi.setup_future_usage = 'off_session' AND c.status = 'succeeded')     AS signup_ok,
           COUNTIF(ci.payment_intent IS NOT NULL AND c.seq_in_intent = 1)                AS renewal_n,
           COUNTIF(ci.payment_intent IS NOT NULL AND c.seq_in_intent = 1 AND c.status = 'succeeded') AS renewal_ok
    FROM direct_charges c
    CROSS JOIN params
    LEFT JOIN direct_intents pi USING (payment_intent)
    LEFT JOIN direct_cycle_intents ci USING (payment_intent)
    WHERE c.metric_date BETWEEN DATE_SUB(params.report_date, INTERVAL 9 DAY) AND params.report_date
    GROUP BY 1, 2
),

approval_daily AS (
    SELECT * FROM sg_daily
    UNION ALL
    SELECT * FROM direct_daily
),

-- Stripe charges of every product (all its Stripe accounts via stripe_accounts.Product).
-- Card key: Stripe's fingerprint is NULL for network-token charges, so last4|exp|brand. Two
-- charges whose fingerprints are both set and differ are different cards, not a duplicate.
st AS (
    SELECT p.product_key, c.created_date AS metric_date, c.created_ts, c.status,
           c.outcome_type, c.outcome_reason, c.amount_local, c.amount_usd,
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
    WHERE c.created_date BETWEEN DATE_SUB(params.report_date, INTERVAL 9 DAY) AND params.report_date
),

st_daily AS (
    SELECT product_key, metric_date,
           COUNT(*)                                                       AS charges,
           COUNTIF(outcome_type = 'blocked')                              AS blocked_n,
           SUM(IF(outcome_type = 'blocked', amount_usd, 0))               AS blocked_usd,
           COUNTIF(secs_since_same_card_amount <= 10)                     AS duplicate_n,
           COUNTIF(outcome_reason = 'previously_declined_do_not_retry')   AS dnr_n
    FROM (
        -- every charge counts; only duplicate detection needs the card key
        SELECT *,
               IF(card_key IS NOT NULL AND NOT IFNULL(fingerprint != LAG(fingerprint) OVER same_card, FALSE),
                  TIMESTAMP_DIFF(created_ts, LAG(created_ts) OVER same_card, SECOND),
                  NULL) AS secs_since_same_card_amount
        FROM st
        WINDOW same_card AS (PARTITION BY product_key, card_key, amount_local ORDER BY created_ts)
    )
    GROUP BY 1, 2
),

metrics AS (
    SELECT product_key, metric_date, 'approval_signups' AS metric,
           SAFE_DIVIDE(signup_ok, signup_n) AS value, signup_n AS volume, CAST(NULL AS FLOAT64) AS amount_usd
    FROM approval_daily
    UNION ALL
    SELECT product_key, metric_date, 'approval_renewals_first_attempt',
           SAFE_DIVIDE(renewal_ok, renewal_n), renewal_n, NULL
    FROM approval_daily
    UNION ALL
    SELECT product_key, metric_date, 'blocked_pct', SAFE_DIVIDE(blocked_n, charges), charges, blocked_usd
    FROM st_daily
    UNION ALL
    SELECT product_key, metric_date, 'duplicate_pct', SAFE_DIVIDE(duplicate_n, charges), charges, NULL
    FROM st_daily
    UNION ALL
    SELECT product_key, metric_date, 'retries_after_do_not_retry', dnr_n, charges, NULL
    FROM st_daily
),

thresholds AS (
    SELECT 'approval_signups' AS metric, 'below_reference' AS rule, 0.05 AS amber, 0.10 AS red,
           CAST(NULL AS FLOAT64) AS spike UNION ALL
    SELECT 'approval_renewals_first_attempt', 'below_reference', 0.05, 0.10, NULL UNION ALL
    -- fixed limits: amber from `amber` up to `red`; red above `red` and `spike` above ref_7d
    SELECT 'blocked_pct', 'above_limit', 0.01, 0.02, 0.01 UNION ALL
    SELECT 'duplicate_pct', 'above_limit', 0.05, 0.10, 0.05 UNION ALL
    -- count: amber at `red` or more; red when also >= max(2 x ref_7d, ref_7d + `spike`)
    SELECT 'retries_after_do_not_retry', 'any', NULL, 1, 5
),

-- The product's own baseline (seed), used when the rolling 7 days are too short.
baselines AS (
    SELECT product_key, metric, baseline
    FROM `{{ params.project }}.{{ params.bronze_dataset }}.seed_billing_health_baselines`
    WHERE section = 'A' AND n_periods >= 5
),

rolling AS (
    SELECT m.*, t.rule, t.amber, t.red, t.spike,
           AVG(IF(m.volume >= 50, m.value, NULL)) OVER w AS rolling_7d,
           COUNTIF(m.volume >= 50) OVER w                AS ref_days
    FROM metrics m
    JOIN thresholds t USING (metric)
    WINDOW w AS (
        PARTITION BY m.product_key, m.metric
        ORDER BY UNIX_DATE(m.metric_date)
        RANGE BETWEEN 7 PRECEDING AND 1 PRECEDING
    )
),

scored AS (
    SELECT r.*,
           IF(r.ref_days >= 5, r.rolling_7d, b.baseline) AS ref_7d,
           CASE
               WHEN r.ref_days >= 5 THEN '7d'
               WHEN b.baseline IS NOT NULL THEN 'baseline'
           END AS ref_source
    FROM rolling r
    LEFT JOIN baselines b USING (product_key, metric)
),

statuses AS (
    SELECT *,
           CASE
               -- 'any' (do not retry) is coloured at any volume: one retry of a blocked card is
               -- too many.
               WHEN value IS NULL OR (volume < 50 AND rule != 'any') THEN 'low_volume'
               -- over the limit: red on a spike above the usual level, else amber
               WHEN rule = 'above_limit' AND value > red
                    AND (ref_7d IS NULL OR value >= ref_7d + spike) THEN 'red'
               WHEN rule = 'above_limit' AND value >= amber THEN 'amber'
               WHEN rule = 'any' AND value >= red
                    AND (ref_7d IS NULL OR value >= GREATEST(2 * ref_7d, ref_7d + spike)) THEN 'red'
               WHEN rule = 'any' AND value >= red THEN 'amber'
               WHEN rule IN ('above_limit', 'any') THEN 'green'
               WHEN ref_7d IS NULL THEN 'no_baseline'
               WHEN rule = 'below_reference' AND value < ref_7d - red THEN 'red'
               WHEN rule = 'below_reference' AND value < ref_7d - amber THEN 'amber'
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
     AND prev.metric = s.metric
     AND prev.metric_date = DATE_SUB(s.metric_date, INTERVAL 1 DAY)
    WHERE s.metric_date = params.report_date
)

SELECT
    params.report_date,
    p.product_key,
    p.product_name,
    p.entity,
    p.payments_owner_slack_id,
    t.metric,
    t.value,
    t.volume,
    t.amount_usd,
    t.ref_7d,
    t.ref_source,
    t.rule,
    t.amber,
    t.red,
    t.status,
    t.prev_status,
    COALESCE(t.alert_now, FALSE) AS alert_now
FROM products p
CROSS JOIN params
LEFT JOIN today t USING (product_key)
ORDER BY p.entity, p.product_name, t.metric
