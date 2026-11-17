-- Reconciles GA4 attributed revenue/funnel counts against Google Ads spend at campaign_id x day
-- grain. FULL OUTER JOIN so revenue-only and spend-only campaign/days both survive. See
-- int_ga4_campaign_day_metrics.sql and int_ads_campaign_day_cost.sql for how each side is built.
--
-- Currency: kept native (ads_cost/ads_currency_code) with a mismatch flag -- 2 of 10 Ads
-- sub-accounts (Online QR Generator, Tracelo) bill in CAD while GA4 revenue is always USD.
-- ads_cost_usd/roas_usd ALSO convert to USD via int_currency_conversion, matching the FX pattern
-- already used by solidgate_mrr_usd_daily.sql -- only 2 currencies here (USD, CAD) vs that
-- model's ~46, so no coverage concern. Both native and USD figures are kept: native for anyone
-- auditing against the Ads UI directly, USD for anything blending totals across currencies (e.g.
-- the Looker Studio report's scorecards -- summing native ads_cost across USD+CAD rows would
-- silently blend two currencies into one number).
--
with campaign_names as (
    select cast(campaign_id as string) as campaign_id, campaign_name
    from {{ source('bronze_google_ads', 'ads_Campaign_2747379648') }}
    qualify row_number() over (partition by campaign_id order by _data_date desc) = 1
),

customer_currency as (
    select customer_id, customer_descriptive_name, customer_currency_code
    from {{ source('bronze_google_ads', 'ads_Customer_2747379648') }}
    qualify row_number() over (partition by customer_id order by _data_date desc) = 1
),

joined as (
    select
        coalesce(f.date, a.date)               as date,
        coalesce(f.campaign_id, a.campaign_id) as campaign_id,
        cn.campaign_name,
        f.property_id,
        a.customer_id,
        cc.customer_descriptive_name,
        f.ga4_revenue_usd,
        f.sessions,
        f.new_users,
        a.ads_cost,
        coalesce(cc.customer_currency_code, 'USD') as ads_currency_code,
        -- reuse the same coalesce as ads_currency_code above -- otherwise this evaluates to NULL
        -- (not FALSE) on GA4-only rows with no Ads match, since NULL != 'USD' is NULL in SQL.
        coalesce(cc.customer_currency_code, 'USD') != 'USD' as currency_mismatch_flag,
        safe_divide(f.ga4_revenue_usd, a.ads_cost)  as roas
    from {{ ref('int_ga4_campaign_day_metrics') }} f
    full outer join {{ ref('int_ads_campaign_day_cost') }} a
        on f.campaign_id = a.campaign_id and f.date = a.date
    left join campaign_names cn on cn.campaign_id = coalesce(f.campaign_id, a.campaign_id)
    left join customer_currency cc on cc.customer_id = a.customer_id
),

fx as (
    select upper(currency) as currency, rate, exchange_date
    from {{ ref('int_currency_conversion') }}
),

fx_asof as (
    -- nearest known rate on or before this row's date, per currency.
    select * except(rn)
    from (
        select
            d.date,
            d.currency,
            f.rate,
            row_number() over (
                partition by d.date, d.currency
                order by f.exchange_date desc
            ) as rn
        from (select distinct date, ads_currency_code as currency from joined) d
        left join fx f
            on f.currency = d.currency
            and f.exchange_date <= d.date
    )
    where rn = 1
),

fx_latest as (
    -- fallback when a date has no rate on/before it yet (e.g. today, before the daily FX
    -- snapshot lands) -- use the latest known rate for that currency instead.
    select * except(rn)
    from (
        select
            currency,
            rate,
            row_number() over (partition by currency order by exchange_date desc) as rn
        from fx
    )
    where rn = 1
)

select
    j.*,
    round(j.ads_cost * case when j.ads_currency_code = 'USD' then 1 else coalesce(a.rate, l.rate) end, 2) as ads_cost_usd,
    safe_divide(
        j.ga4_revenue_usd,
        j.ads_cost * case when j.ads_currency_code = 'USD' then 1 else coalesce(a.rate, l.rate) end
    ) as roas_usd
from joined j
left join fx_asof a on a.date = j.date and a.currency = j.ads_currency_code
left join fx_latest l on l.currency = j.ads_currency_code
