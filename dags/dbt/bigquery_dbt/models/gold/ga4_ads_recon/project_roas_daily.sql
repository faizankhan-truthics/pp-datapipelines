{{ config(
tags=["ga4_ads_recon", "gold", "daily"],
) }}

-- Project-grain companion to ga4_ads_reconciliation/campaign_roas_daily: those are both
-- campaign_id-keyed, with no reliable way to split spend by project at that grain -- a Google
-- Ads campaign isn't scoped to one landing page, so its spend can't be divided by project
-- without more granular data (see int_ads_landing_page_day_cost.sql's own description).
-- Sessions and GA4-proxy-revenue, by contrast, already exist per property_id in
-- int_ga4_campaign_day_metrics -- summed here across campaign_id up to property_id x date.
--
-- Currency conversion happens PER customer_id, before summing across customers to domain x
-- date: a single project's domain can span several Ads sub-accounts (publicregistry.us alone
-- runs 6), and while they're all USD today, converting after summing (one currency per domain)
-- would silently misattribute currency the day that stops holding. See
-- int_ads_landing_page_day_cost.sql for why customer_id is kept in its grain rather than
-- collapsed there.
--
-- ga4_revenue_usd is GA4's own event_value (a proxy), same caveat as ga4_ads_reconciliation --
-- NOT real payment-processor revenue. real_revenue_usd below IS real Solidgate/Stripe revenue,
-- pulled from ga4_payment_match_campaign_daily (which already carries ga4_property_name, so no
-- account_key indirection is needed here) and summed to property x date. Covers 5 of 7 projects
-- -- location_tool, infochecker, iq_center, publicregistry, and reversly are all present in
-- seed_ga4_payment_account_map (confirmed against the seed directly, not assumed from an older
-- 3-of-7 estimate); only tracelo/fitday are excluded, same has_ga4_export reason as everywhere
-- else. Same exact-transaction-match caveat as ga4_payment_match_campaign_daily applies --
-- subscription renewals with no matching GA4 transaction aren't attributed, so real_revenue_usd
-- understates true revenue.
--
-- Covers all 7 projects for spend (dim_project.domain is set regardless of
-- has_ga4_export -- tracelo and fitday already show real Ads spend despite having no GA4 data
-- at all). Sessions/ga4_revenue are null for those two, same has_ga4_export limitation as
-- everywhere else in this GA4 work.
--
-- Grain: project_name x date.

with customer_currency as (

    select customer_id, customer_currency_code
    from {{ source('bronze_google_ads', 'ads_Customer_2747379648') }}
    qualify row_number() over (partition by customer_id order by _data_date desc) = 1

),

spend_by_customer as (

    select
        c.domain,
        c.date,
        c.customer_id,
        c.ads_cost,
        coalesce(cc.customer_currency_code, 'USD') as currency_code
    from {{ ref('int_ads_landing_page_day_cost') }} c
    left join customer_currency cc on cc.customer_id = c.customer_id

),

fx as (
    select upper(currency) as currency, rate, exchange_date
    from {{ ref('int_currency_conversion') }}
),

fx_asof as (
    -- nearest known rate on or before this row's date, per currency (same pattern as
    -- ga4_ads_reconciliation.sql)
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
        from (select distinct date, currency_code as currency from spend_by_customer) d
        left join fx f
            on f.currency = d.currency
            and f.exchange_date <= d.date
    )
    where rn = 1
),

fx_latest as (
    select * except(rn)
    from (
        select
            currency,
            rate,
            row_number() over (partition by currency order by exchange_date desc) as rn
        from fx
    )
    where rn = 1
),

-- convert to USD per customer_id FIRST, then sum across customers for this domain/date --
-- never sum native ads_cost across customer_id first and convert once after.
spend_usd_by_customer as (

    select
        s.domain,
        s.date,
        s.customer_id,
        s.ads_cost,
        s.currency_code,
        round(
            s.ads_cost * case when s.currency_code = 'USD' then 1 else coalesce(a.rate, l.rate) end, 2
        ) as ads_cost_usd
    from spend_by_customer s
    left join fx_asof a on a.date = s.date and a.currency = s.currency_code
    left join fx_latest l on l.currency = s.currency_code

),

spend as (

    select
        domain,
        date,
        sum(ads_cost_usd) as ads_cost_usd,
        -- native ads_cost/currency_code no longer mean anything once summed across customers
        -- that may bill in different currencies -- only the USD total is safe to expose
        count(distinct currency_code) > 1 as spans_multiple_currencies
    from spend_usd_by_customer
    group by domain, date

),

ga4_metrics as (

    -- sum across campaign_id: this mart doesn't need campaign-level drill-down, only the
    -- property-level total each project's dashboard row needs
    select
        property_id,
        date,
        sum(ga4_revenue_usd) as ga4_revenue_usd,
        sum(sessions)        as sessions,
        sum(new_users)       as new_users
    from {{ ref('int_ga4_campaign_day_metrics') }}
    group by property_id, date

),

real_revenue as (

    -- already keyed on ga4_property_name (any_value'd in that model), so no account_key
    -- indirection needed here -- just collapse campaign_id/payment_source/account_key to a
    -- property x date total.
    select
        ga4_property_name,
        pay_date as date,
        sum(revenue_usd)               as real_revenue_usd,
        sum(txn_cnt)                   as real_txn_cnt
    from {{ ref('ga4_payment_match_campaign_daily') }}
    group by ga4_property_name, pay_date

)

select
    p.project_name,
    p.ga4_property_name,
    s.date,
    s.ads_cost_usd,
    s.spans_multiple_currencies,
    g.ga4_revenue_usd,
    g.sessions,
    g.new_users,
    safe_divide(g.ga4_revenue_usd, s.ads_cost_usd) as roas_usd,
    rr.real_revenue_usd,
    rr.real_txn_cnt,
    safe_divide(rr.real_revenue_usd, s.ads_cost_usd) as real_roas_usd

from spend s
join {{ ref('dim_project') }} p
    on p.domain = s.domain
left join ga4_metrics g
    on  g.property_id = p.ga4_property_id
    and g.date         = s.date
left join real_revenue rr
    on  rr.ga4_property_name = p.ga4_property_name
    and rr.date               = s.date
