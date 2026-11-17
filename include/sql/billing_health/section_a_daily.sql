-- Billing health, Section A (payment health): the ticket's informational columns, one row per
-- active product for {{ params.report_date }} (D-1, UTC). Coloured metrics come from
-- section_a_status.sql; this adds attempts, approval overall (Stripe charges, the basis of the
-- ticket's manual checks; Solidgate attempts when a product has no Stripe charges), approval on
-- all renewal attempts,
-- bank declines %, attempts per order and the top 3 decline reasons.
--
-- Decline reason / class: Stripe outcome_reason / outcome_type when the failed attempt went to
-- Stripe (joined on psp_order_id), else the Solidgate error code mapped through
-- seed_billing_decline_reason_map, so every PSP reports in the same vocabulary. psp_order_id is
-- per order, so a Stripe outcome is used only when it can belong to that one attempt: the
-- order's only failed attempt that day and its only failed Stripe charge.

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

stripe_charges AS (
    SELECT p.product_key, p.solidgate_account, c.created_date, c.status, c.outcome_type, c.outcome_reason,
           c.payment_intent, JSON_VALUE(c.metadata, '$.psp_order_id') AS psp_order_id
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_charges` c
    LEFT JOIN `{{ params.project }}.{{ params.bronze_dataset }}.stripe_accounts` acc
      ON acc.ID = c._elt_stripe_account_id
    JOIN products p ON p.stripe_product = COALESCE(acc.Product, c._elt_stripe_account_id)
    CROSS JOIN params
    WHERE c.created_date BETWEEN DATE_SUB(params.report_date, INTERVAL 1 DAY)
                             AND DATE_ADD(params.report_date, INTERVAL 1 DAY)
),

stripe_orders AS (
    SELECT DISTINCT psp_order_id
    FROM stripe_charges
    WHERE psp_order_id IS NOT NULL
),

stripe_decline_by_order AS (
    SELECT psp_order_id, ANY_VALUE(outcome_type) AS outcome_type, ANY_VALUE(outcome_reason) AS outcome_reason
    FROM stripe_charges
    WHERE psp_order_id IS NOT NULL
      AND status != 'succeeded'
    GROUP BY 1
    HAVING COUNT(*) = 1
),

-- Solidgate-routed products: every final attempt that day (Fake PSP = $0 trial placeholders).
sg AS (
    SELECT p.product_key, a.is_recurring, a.is_approved,
           COALESCE(s.outcome_type, m.decline_class)                  AS decline_class,
           COALESCE(s.outcome_reason, m.decline_reason, a.error_code) AS decline_reason
    FROM (
        SELECT a.*,
               COUNTIF(NOT a.is_approved) OVER (PARTITION BY a._elt_account_name, a.order_id) AS failed_in_order
        FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_card_order_attempts` a
        CROSS JOIN params
        WHERE a.attempt_date = params.report_date
          AND a.is_final
          AND a.amount_minor > 0
          AND COALESCE(a.psp_name, '') != 'Fake'
    ) a
    JOIN products p ON p.solidgate_account = a._elt_account_name
    LEFT JOIN stripe_decline_by_order s
      ON s.psp_order_id = a.psp_order_id
     AND NOT a.is_approved
     AND a.failed_in_order = 1
    LEFT JOIN `{{ params.project }}.{{ params.bronze_dataset }}.seed_billing_decline_reason_map` m
      ON m.code = a.error_code
),

-- Stripe-direct products (no Solidgate account): every charge that day. A renewal is a
-- merchant-initiated charge: its payment intent does not save the card for later
-- (setup_future_usage = 'off_session' marks a customer-initiated one), as in section_a_status.sql.
direct_intents AS (
    SELECT pi.id AS payment_intent, pi.setup_future_usage
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_payment_intents` pi
    CROSS JOIN params
    WHERE pi.created_date BETWEEN DATE_SUB(params.report_date, INTERVAL 40 DAY) AND params.report_date
),

direct AS (
    SELECT c.product_key, COALESCE(pi.setup_future_usage, '') != 'off_session' AS is_recurring,
           c.status = 'succeeded' AS is_approved,
           IF(c.status = 'succeeded', NULL, c.outcome_type)   AS decline_class,
           IF(c.status = 'succeeded', NULL, c.outcome_reason) AS decline_reason
    FROM stripe_charges c
    CROSS JOIN params
    LEFT JOIN direct_intents pi USING (payment_intent)
    WHERE c.solidgate_account IS NULL
      AND c.created_date = params.report_date
),

attempts AS (
    SELECT * FROM sg
    UNION ALL
    SELECT * FROM direct
),

per_product AS (
    SELECT product_key,
           COUNT(*)                                                          AS attempts,
           SAFE_DIVIDE(COUNTIF(is_approved), COUNT(*))                       AS approval_total,
           SAFE_DIVIDE(COUNTIF(is_recurring AND is_approved), COUNTIF(is_recurring)) AS approval_renewals_all,
           SAFE_DIVIDE(COUNTIF(NOT is_approved AND decline_class = 'issuer_declined'), COUNT(*)) AS bank_declines_pct
    FROM attempts
    GROUP BY 1
),

top_reasons AS (
    SELECT product_key,
           STRING_AGG(FORMAT('%s %.0f%%', decline_reason, 100 * share), ' | ' ORDER BY n DESC, decline_reason)
               AS top_decline_reasons
    FROM (
        SELECT product_key, COALESCE(decline_reason, 'unknown') AS decline_reason, COUNT(*) AS n,
               COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY product_key) AS share
        FROM attempts
        WHERE NOT is_approved
        GROUP BY 1, 2
        -- ties go to the reason first in alphabetical order, so a re-run shows the same three
        QUALIFY ROW_NUMBER() OVER (PARTITION BY product_key ORDER BY COUNT(*) DESC, decline_reason) <= 3
    )
    GROUP BY 1
),

-- Stripe charges per Solidgate order sent to Stripe (should be ~1.0); Solidgate products only.
-- An order counts as sent to Stripe when a Stripe charge carries its psp_order_id, or its MID is
-- a Stripe MID: psp_name alone misses orders whose MID is not in solidgate_mid_lookup.
orders_per_product AS (
    SELECT p.product_key,
           COUNT(DISTINCT IF(s.psp_order_id IS NOT NULL OR o.psp_name = 'Stripe', o.order_id, NULL))
               AS stripe_orders
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_card_orders` o
    JOIN products p ON p.solidgate_account = o._elt_account_name
    CROSS JOIN params
    LEFT JOIN stripe_orders s ON s.psp_order_id = o.psp_order_id
    WHERE o.created_date = params.report_date
    GROUP BY 1
),

-- Overall approval on Stripe charges: every charge that day, including Stripe-only charges with
-- no Solidgate order (e.g. GiftMusic upsell fees), as in the ticket's manual checks.
stripe_approval AS (
    SELECT c.product_key, SAFE_DIVIDE(COUNTIF(c.status = 'succeeded'), COUNT(*)) AS approval
    FROM stripe_charges c
    CROSS JOIN params
    WHERE c.created_date = params.report_date
    GROUP BY 1
),

charges_per_product AS (
    SELECT c.product_key, COUNT(*) AS stripe_charges
    FROM stripe_charges c
    CROSS JOIN params
    WHERE c.created_date = params.report_date
      AND c.psp_order_id IS NOT NULL
    GROUP BY 1
)

SELECT
    p.product_key,
    p.product_name,
    p.entity,
    p.solidgate_account IS NULL AS is_stripe_direct,
    pp.attempts,
    pp.approval_total,
    COALESCE(sa.approval, pp.approval_total) AS approval_overall,
    pp.approval_renewals_all,
    pp.bank_declines_pct,
    SAFE_DIVIDE(cp.stripe_charges, op.stripe_orders) AS attempts_per_order,
    tr.top_decline_reasons
FROM products p
LEFT JOIN per_product pp USING (product_key)
LEFT JOIN top_reasons tr USING (product_key)
LEFT JOIN orders_per_product op USING (product_key)
LEFT JOIN charges_per_product cp USING (product_key)
LEFT JOIN stripe_approval sa USING (product_key)
ORDER BY p.entity, p.product_name
