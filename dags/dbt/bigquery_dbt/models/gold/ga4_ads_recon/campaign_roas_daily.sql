{{ config(
tags=["ga4_ads_recon", "gold", "daily"],
) }}

-- Campaign-level ROAS against REAL collected Solidgate/Stripe revenue, as opposed to
-- ga4_ads_reconciliation's ga4_revenue_usd (GA4's own event_value, a proxy). 
-- Grain: campaign_id x date. 
-- subscription-renewal revenue isn't included.
with revenue as (

    -- Roll up across payment_source/account_key: Ads spend has no account dimension to join on.
    select
        pay_date  as date,
        campaign_id,
        sum(revenue_usd)               as revenue_usd,
        sum(txn_cnt)                   as txn_cnt
    from {{ ref('ga4_payment_match_campaign_daily') }}
    where campaign_id is not null
    group by pay_date, campaign_id

),

campaign_names as (
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
        coalesce(r.date, a.date)               as date,
        coalesce(r.campaign_id, a.campaign_id) as campaign_id,
        cn.campaign_name,
        a.customer_id,
        cc.customer_descriptive_name,
        r.revenue_usd,
        r.txn_cnt,
        a.ads_cost,
        coalesce(cc.customer_currency_code, 'USD') as ads_currency_code
    from revenue r
    full outer join {{ ref('int_ads_campaign_day_cost') }} a
        on r.campaign_id = a.campaign_id and r.date = a.date
    left join campaign_names cn on cn.campaign_id = coalesce(r.campaign_id, a.campaign_id)
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
        j.revenue_usd,
        j.ads_cost * case when j.ads_currency_code = 'USD' then 1 else coalesce(a.rate, l.rate) end
    ) as roas
from joined j
left join fx_asof a on a.date = j.date and a.currency = j.ads_currency_code
left join fx_latest l on l.currency = j.ads_currency_code
