-- Is {{ params.report_date }} fully loaded and built? The report runs only when:
--   * solidgate_loaded_through / stripe_loaded_through: the latest attempt loaded is late on the
--     report day (UTC), and
--   * missing_loads is NULL: every account that loads daily has rows loaded after the report day
--     ended in every model the report reads, i.e. its ingest ran and dbt built it. A global
--     maximum alone passes while one account's ingest failed (the ingest DAGs load the other
--     accounts anyway).
-- An account is expected in a model when it had loads there on at least 5 of the 7 days up to the
-- report day; accounts that load only now and then (small Stripe accounts) are not waited for.
-- _elt_update_timestamp is the load time, carried from bronze into silver by dbt.

WITH params AS (
    SELECT DATE '{{ params.report_date }}' AS report_date,
           TIMESTAMP(DATE_ADD(DATE '{{ params.report_date }}', INTERVAL 1 DAY)) AS day_end
),

loads AS (
    SELECT 'int_solidgate_card_order_attempts' AS model, _elt_account_name AS account, _elt_update_timestamp AS ts
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_card_order_attempts`
    UNION ALL
    SELECT 'int_solidgate_card_orders', _elt_account_name, _elt_update_timestamp
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_card_orders`
    UNION ALL
    SELECT 'int_solidgate_subscription_invoice_orders', _elt_account_name, _elt_update_timestamp
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_subscription_invoice_orders`
    UNION ALL
    SELECT 'int_solidgate_subscription_invoices', _elt_account_name, _elt_update_timestamp
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_subscription_invoices`
    UNION ALL
    SELECT 'int_stripe_charges', _elt_stripe_account_id, _elt_update_timestamp
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_charges`
    UNION ALL
    SELECT 'int_stripe_payment_intents', _elt_stripe_account_id, _elt_update_timestamp
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_payment_intents`
    UNION ALL
    SELECT 'int_stripe_invoices', _elt_stripe_account_id, _elt_update_timestamp
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_invoices`
    UNION ALL
    SELECT 'int_stripe_subscriptions', _elt_stripe_account_id, _elt_update_timestamp
    FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_subscriptions`
),

per_account AS (
    SELECT l.model, l.account,
           COUNT(DISTINCT IF(l.ts < p.day_end, DATE(l.ts), NULL)) AS load_days_last_7,
           LOGICAL_OR(l.ts >= p.day_end) AS loaded_after_day_end
    FROM loads l
    CROSS JOIN params p
    WHERE l.ts >= TIMESTAMP(DATE_SUB(p.report_date, INTERVAL 6 DAY))
      AND l.account NOT LIKE '%SANDBOX%'
    GROUP BY 1, 2
)

SELECT
    (
        SELECT MAX(attempt_ts)
        FROM `{{ params.project }}.{{ params.silver_dataset }}.int_solidgate_card_order_attempts`
        WHERE attempt_date BETWEEN DATE_SUB(DATE '{{ params.report_date }}', INTERVAL 1 DAY)
                               AND DATE_ADD(DATE '{{ params.report_date }}', INTERVAL 1 DAY)
    ) AS solidgate_loaded_through,
    (
        SELECT MAX(created_ts)
        FROM `{{ params.project }}.{{ params.silver_dataset }}.int_stripe_charges`
        WHERE created_date BETWEEN DATE_SUB(DATE '{{ params.report_date }}', INTERVAL 1 DAY)
                               AND DATE_ADD(DATE '{{ params.report_date }}', INTERVAL 1 DAY)
    ) AS stripe_loaded_through,
    (
        SELECT STRING_AGG(CONCAT(model, ' ', account), ', ' ORDER BY model, account)
        FROM per_account
        WHERE load_days_last_7 >= 5
          AND NOT loaded_after_day_end
    ) AS missing_loads
