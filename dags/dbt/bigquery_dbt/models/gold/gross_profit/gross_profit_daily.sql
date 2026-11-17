{{ config(
    tags=["revenue", "ads", "chargebackhit", "gross-profit", "gold", "daily"],
) }}

WITH revenue_daily AS (

    SELECT
        date,
        reporting_product AS product,
        SUM(net_revenue_usd) AS net_revenue_usd

    FROM {{ ref('net_revenue_daily') }}

    WHERE reporting_product IS NOT NULL

    GROUP BY 1, 2
),

ads_mapped AS (

    SELECT
        date,

        CASE
            WHEN customer_descriptive_name IN (
                'InfoChecker - Search',
                'InfoChecker - Locator'
            )
                THEN 'InfoChecker'

            WHEN customer_descriptive_name IN (
                'Chatbox AI',
                'Chatbox AI - AI Image'
            )
                THEN 'Chatbox AI'

            WHEN customer_descriptive_name = 'QGA'
                THEN 'QR Generator AI'

            WHEN customer_descriptive_name = 'Online QR Generator'
                THEN 'Online QR Generator'

            WHEN customer_descriptive_name = 'QCI'
                THEN 'QCI'

            WHEN customer_descriptive_name = 'Tracelo'
                THEN 'Tracelo'

            WHEN customer_descriptive_name = 'Location-Tool'
                THEN 'Location Tool'

            WHEN customer_descriptive_name IN (
                'Files Editor',
                'File Editor'
            )
                THEN 'Files Editor'

            WHEN customer_descriptive_name = 'IQ Center'
                THEN 'IQ Center'

            WHEN STARTS_WITH(
                customer_descriptive_name,
                'Public Registry'
            )
                THEN 'Public Registry'

            WHEN customer_descriptive_name = 'simpl.ai'
                THEN 'Simpl AI'

            ELSE NULL
        END AS product,

        ads_cost_usd

    FROM {{ ref('ga4_ads_reconciliation') }}

    WHERE date IS NOT NULL
      AND ads_cost_usd IS NOT NULL
),

ads_daily AS (

    SELECT
        date,
        product,
        SUM(ads_cost_usd) AS ad_spend_usd

    FROM ads_mapped

    WHERE product IS NOT NULL

    GROUP BY 1, 2
),

chargeback_mapped AS (

    SELECT
        DATE(SAFE_CAST(cb.alert_date AS TIMESTAMP)) AS date,

        CASE cb.account
            WHEN 'locationtool' THEN 'Location Tool'
            WHEN 'fileseditor' THEN 'Files Editor'
            WHEN 'tracelo' THEN 'Tracelo'
            WHEN 'infochecker' THEN 'InfoChecker'
            WHEN 'onlineqrgenerator' THEN 'Online QR Generator'
            WHEN 'qrcodeio' THEN 'QCI'
            WHEN 'chatbox' THEN 'Chatbox AI'
            WHEN 'qrgeneratorai' THEN 'QR Generator AI'
            WHEN 'iqcenter' THEN 'IQ Center'
            WHEN 'publicregistry' THEN 'Public Registry'
            ELSE NULL
        END AS product,

        CASE
            WHEN cb.product = 'ethoca'
                THEN 29
            WHEN cb.product = 'rdr'
                THEN 19
            WHEN cb.product = 'cdrn'
                THEN 19
            WHEN cb.alert_type = 'prevented'
                THEN 17
            WHEN cb.alert_type = 'inquiry'
                THEN 0
            WHEN cb.alert_type IN (
                'fraud-notification',
                'dispute-notification'
            )
                THEN 0
            ELSE 0
        END AS chargebackhit_cost_usd

    FROM {{ source('bronze_chargebackhit', 'chargebackhit_alerts') }} cb

    WHERE SAFE_CAST(cb.alert_date AS TIMESTAMP) IS NOT NULL
),

chargeback_daily AS (

    SELECT
        date,
        product,
        COUNT(*) AS chargebackhit_alerts,
        SUM(chargebackhit_cost_usd) AS chargebackhit_cost_usd

    FROM chargeback_mapped

    WHERE product IS NOT NULL

    GROUP BY 1, 2
),

product_universe AS (

    SELECT product
    FROM revenue_daily

    UNION DISTINCT

    SELECT product
    FROM ads_daily

    UNION DISTINCT

    SELECT product
    FROM chargeback_daily
),

revenue_supported_products AS (

    SELECT DISTINCT
        product

    FROM revenue_daily

    WHERE product IS NOT NULL
),

ads_supported_products AS (

    SELECT product

    FROM UNNEST([
        'InfoChecker',
        'Chatbox AI',
        'QR Generator AI',
        'Online QR Generator',
        'QCI',
        'Tracelo',
        'Location Tool',
        'Files Editor',
        'IQ Center',
        'Public Registry',
        'Simpl AI'
    ]) AS product
),

chargeback_supported_products AS (

    SELECT product

    FROM UNNEST([
        'Location Tool',
        'Files Editor',
        'Tracelo',
        'InfoChecker',
        'Online QR Generator',
        'QCI',
        'Chatbox AI',
        'QR Generator AI',
        'IQ Center',
        'Public Registry'
    ]) AS product
),

revenue_source_days AS (

    SELECT DISTINCT
        date

    FROM revenue_daily

    WHERE date IS NOT NULL
),

ads_source_days AS (

    SELECT DISTINCT
        date

    FROM {{ ref('ga4_ads_reconciliation') }}

    WHERE date IS NOT NULL
),

chargeback_source_days AS (

    SELECT DISTINCT
        DATE(SAFE_CAST(alert_date AS TIMESTAMP)) AS date

    FROM {{ source('bronze_chargebackhit', 'chargebackhit_alerts') }}

    WHERE SAFE_CAST(alert_date AS TIMESTAMP) IS NOT NULL
),

reporting_window AS (

    SELECT
        MIN(date) AS start_date,
        MAX(date) AS end_date

    FROM (
        SELECT date
        FROM revenue_source_days

        UNION ALL

        SELECT date
        FROM ads_source_days

        UNION ALL

        SELECT date
        FROM chargeback_source_days
    )
),

date_spine AS (

    SELECT
        date

    FROM reporting_window,
    UNNEST(
        GENERATE_DATE_ARRAY(
            start_date,
            end_date
        )
    ) AS date
),

spine AS (

    SELECT
        d.date,
        p.product

    FROM date_spine d

    CROSS JOIN product_universe p
),

joined AS (

    SELECT
        s.date,
        s.product,

        r.net_revenue_usd AS revenue_value,

        a.ad_spend_usd AS ads_value,

        c.chargebackhit_cost_usd AS chargeback_value,

        c.chargebackhit_alerts AS chargeback_alert_count,

        (
            rsp.product IS NOT NULL
            AND rsd.date IS NOT NULL
        ) AS revenue_available,

        (
            asp.product IS NOT NULL
            AND asd.date IS NOT NULL
        ) AS ads_available,

        (
            csp.product IS NOT NULL
            AND csd.date IS NOT NULL
        ) AS chargeback_available

    FROM spine s

    LEFT JOIN revenue_daily r
        ON s.date = r.date
        AND s.product = r.product

    LEFT JOIN ads_daily a
        ON s.date = a.date
        AND s.product = a.product

    LEFT JOIN chargeback_daily c
        ON s.date = c.date
        AND s.product = c.product

    LEFT JOIN revenue_supported_products rsp
        ON s.product = rsp.product

    LEFT JOIN ads_supported_products asp
        ON s.product = asp.product

    LEFT JOIN chargeback_supported_products csp
        ON s.product = csp.product

    LEFT JOIN revenue_source_days rsd
        ON s.date = rsd.date

    LEFT JOIN ads_source_days asd
        ON s.date = asd.date

    LEFT JOIN chargeback_source_days csd
        ON s.date = csd.date
)

SELECT
    date,
    product,

    CASE
        WHEN revenue_available
            THEN COALESCE(revenue_value, 0)
    END AS net_revenue_usd,

    CASE
        WHEN ads_available
            THEN COALESCE(ads_value, 0)
    END AS ad_spend_usd,

    CASE
        WHEN chargeback_available
            THEN COALESCE(chargeback_value, 0)
    END AS chargebackhit_cost_usd,

    CASE
        WHEN chargeback_available
            THEN COALESCE(chargeback_alert_count, 0)
    END AS chargebackhit_alerts,

    CASE
        WHEN revenue_available
            AND ads_available
            AND chargeback_available
        THEN
            COALESCE(revenue_value, 0)
            - COALESCE(ads_value, 0)
            - COALESCE(chargeback_value, 0)
    END AS gross_profit_usd,

    NOT revenue_available
        AS missing_revenue,

    NOT ads_available
        AS missing_ad_spend,

    NOT chargeback_available
        AS missing_chargebackhit,

    revenue_available
        AND ads_available
        AND chargeback_available
        AS gross_profit_complete

FROM joined
