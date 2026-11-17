{{ config(
    materialized='table',
    tags=['gold', 'daily']
) }}
-- Monthly MRR growth data for the existing Preset Growth Rate chart.
-- Grain: one row per month + account_name.
-- Keep all six output names, types, and calculations unchanged.
-- Do not restrict history before LAG: Preset filters dates downstream.
WITH fx_intervals AS (
    SELECT
        UPPER(currency) AS currency,
        exchange_date,
        rate,
        LEAD(exchange_date) OVER (
            PARTITION BY UPPER(currency)
            ORDER BY exchange_date
        ) AS next_exchange_date
    FROM {{ ref('int_currency_conversion') }}
),
fx_latest AS (
    SELECT
        UPPER(currency) AS currency,
        ARRAY_AGG(
            rate ORDER BY exchange_date DESC LIMIT 1
        )[OFFSET(0)] AS latest_rate
    FROM {{ ref('int_currency_conversion') }}
    GROUP BY 1
),
solidgate_selected AS (
    SELECT
        snapshot_date,
        _elt_account_name,
        UPPER(currency) AS currency,
        mrr
    FROM {{ ref('int_solidgate_mrr_local_daily') }}
    WHERE _elt_account_name IN (
        'BGR_PROD_PUBLICREGISTRY_US',
        'CHATBOT_PROD_SIMPL',
        'CHATBOX_PROD',
        'MSC_PROD',
        'PDF_PROD_FILES_EDITOR',
        'PS_PROD_FINDFAST_LIVE',
        'PS_PROD_INFOCHECKER',
        'QR_PROD_QGA_LIVE',
        'QUIZ_PROD_IQCENTER_LIVE',
        'REVERSELY_LIVE'
    )
),
stripe_selected AS (
    SELECT
        l.snapshot_date,
        l._elt_stripe_account_id,
        UPPER(l.currency) AS currency,
        l.mrr
    FROM {{ ref('int_stripe_mrr_local_daily') }} l
    WHERE EXISTS (
        SELECT 1
        FROM {{ source('bronze_stripe', 'stripe_accounts') }} a
        WHERE a.id = l._elt_stripe_account_id
          AND a.account IN (
              'LT',
              'OQG',
              'OQG LLC',
              'QCI',
              'TRC',
              'Tracelo (rejected)'
          )
    )
),
snapshot_currency AS (
    SELECT DISTINCT snapshot_date, currency
    FROM (
        SELECT snapshot_date, currency FROM solidgate_selected
        UNION ALL
        SELECT snapshot_date, currency FROM stripe_selected
    )
),
fx_by_date AS (
    SELECT
        d.snapshot_date,
        d.currency,
        CASE
            WHEN d.currency = 'USD' THEN 1
            ELSE COALESCE(f.rate, t.latest_rate)
        END AS fx_rate
    FROM snapshot_currency d
    LEFT JOIN fx_intervals f
      ON d.currency = f.currency
     AND d.snapshot_date >= f.exchange_date
     AND (
          d.snapshot_date < f.next_exchange_date
          OR f.next_exchange_date IS NULL
     )
    LEFT JOIN fx_latest t
      ON d.currency = t.currency
),
solidgate_daily AS (
    SELECT
        s.snapshot_date,
        s._elt_account_name AS account_name,
        ROUND(SUM(s.mrr * f.fx_rate), 2) AS mrr_usd
    FROM solidgate_selected s
    LEFT JOIN fx_by_date f
      ON s.snapshot_date = f.snapshot_date
     AND s.currency = f.currency
    GROUP BY 1, 2
),
stripe_daily AS (
    SELECT
        s.snapshot_date,
        s._elt_stripe_account_id,
        ROUND(SUM(s.mrr * f.fx_rate), 2) AS mrr_usd
    FROM stripe_selected s
    LEFT JOIN fx_by_date f
      ON s.snapshot_date = f.snapshot_date
     AND s.currency = f.currency
    GROUP BY 1, 2
),
solidgate_month_end AS (
    SELECT
        DATE_TRUNC(snapshot_date, MONTH) AS month,
        account_name,
        mrr_usd
    FROM solidgate_daily
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY DATE_TRUNC(snapshot_date, MONTH), account_name
        ORDER BY snapshot_date DESC
    ) = 1
),
stripe_month_end AS (
    SELECT
        DATE_TRUNC(s.snapshot_date, MONTH) AS month,
        a.account AS account_name,
        s.mrr_usd
    FROM stripe_daily s
    LEFT JOIN {{ source('bronze_stripe', 'stripe_accounts') }} a
      ON s._elt_stripe_account_id = a.id
    WHERE a.account IN (
        'LT',
        'OQG',
        'OQG LLC',
        'QCI',
        'TRC',
        'Tracelo (rejected)'
    )
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY DATE_TRUNC(s.snapshot_date, MONTH), a.account
        ORDER BY s.snapshot_date DESC
    ) = 1
),
combined AS (
    SELECT
        month,
        account_name,
        SUM(mrr_usd) AS ending_mrr
    FROM (
        SELECT month, account_name, mrr_usd FROM solidgate_month_end
        UNION ALL
        SELECT month, account_name, mrr_usd FROM stripe_month_end
    )
    GROUP BY 1, 2
),
growth AS (
    SELECT
        month,
        account_name,
        ending_mrr,
        LAG(ending_mrr) OVER (
            PARTITION BY account_name
            ORDER BY month
        ) AS previous_month_mrr
    FROM combined
)
SELECT
    month,
    account_name,
    ending_mrr,
    previous_month_mrr,
    ending_mrr - previous_month_mrr AS mrr_change_usd,
    SAFE_DIVIDE(
        ending_mrr - previous_month_mrr,
        previous_month_mrr
    ) AS growth_rate
FROM growth
