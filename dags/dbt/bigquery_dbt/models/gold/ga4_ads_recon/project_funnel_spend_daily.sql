{{ config(
tags=["ga4_ads_recon", "gold", "daily"],
) }}

-- URL-based Funnel (the landing page's product/topic segment: walking, phone-lookup, adhd,
-- track, ...) x Project x date, with Ads spend that CAN be split this way -- unlike a
-- post-click funnel stage (Search->Paywall / Quiz->Paywall / Trial->Subscription), which the
-- ad data cannot carry. Definition and the
-- Fitday/Tracelo/... URL list come from Manya's sheet; see int_ads_landing_page_day_cost.sql
-- for how the slug is extracted.
--
-- Covers all 7 projects for spend (Tracelo/Fitday included -- the funnel comes from the Ads
-- landing page URL, not GA4). Sessions come from int_ga4_funnel_sessions, so they exist only for
-- the 5 has_ga4_export projects, and only where that model resolved a funnel value; the
-- vocabularies are the same slugs (confirmed for iq_center test_type, infochecker/reversly
-- page_version_lookup, location_tool page_version, publicregistry url_path) but the two sides
-- are resolved independently, so a row can have spend with null sessions or the reverse.
-- Treat funnel values with no spend or no sessions as a mapping gap to investigate, not zero.
--
-- sessions is all GA4 sessions for that funnel (funnel = 'unresolved' for sessions with no funnel
-- signal -- sessions only, never spend); ads_sessions is the subset carrying a gclid, the
-- fairer denominator against ads_cost_usd.
--
-- Currency conversion happens per customer_id before summing, same as project_roas_daily (see
-- that model and int_ads_landing_page_day_cost.sql for why).
--
-- NOT carried: revenue / ROAS. Real revenue can't be attributed to a landing funnel without a
-- user-level bridge, and GA4-proxy revenue is largely
-- null (see project_roas_daily).
--
-- Grain: project_name x funnel x date.

with customer_currency as (

    select customer_id, customer_currency_code
    from {{ source('bronze_google_ads', 'ads_Customer_2747379648') }}
    qualify row_number() over (partition by customer_id order by _data_date desc) = 1

),

spend_by_customer as (

    select
        c.domain,
        c.funnel,
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

spend as (

    select
        s.domain,
        s.funnel,
        s.date,
        sum(round(
            s.ads_cost * case when s.currency_code = 'USD' then 1 else coalesce(a.rate, l.rate) end, 2
        )) as ads_cost_usd
    from spend_by_customer s
    left join fx_asof a on a.date = s.date and a.currency = s.currency_code
    left join fx_latest l on l.currency = s.currency_code
    group by s.domain, s.funnel, s.date

),

sessions as (

    select
        ga4_property_name,
        -- sessions that never fired an event carrying a funnel signal are kept (about half of
        -- all sessions for infochecker / iq_center / location_tool), not dropped: spend is
        -- never filtered this way, so dropping them would understate sessions per project
        -- against spend. They land under 'unresolved', which has sessions but no spend.
        coalesce(lower(funnel), 'unresolved') as funnel,
        session_date                     as date,
        count(*)                         as sessions,
        countif(gclid is not null)       as ads_sessions
    from {{ ref('int_ga4_funnel_sessions') }}
    group by ga4_property_name, coalesce(lower(funnel), 'unresolved'), session_date

),

spend_p as (

    select p.project_name, p.ga4_property_name, s.funnel, s.date, s.ads_cost_usd
    from spend s
    join {{ ref('dim_project') }} p on p.domain = s.domain

)

select
    coalesce(sp.project_name, p.project_name)             as project_name,
    coalesce(sp.ga4_property_name, ss.ga4_property_name) as ga4_property_name,
    coalesce(sp.funnel, ss.funnel)                       as funnel,
    coalesce(sp.date, ss.date)                           as date,
    sp.ads_cost_usd,
    ss.sessions,
    ss.ads_sessions

from spend_p sp
full outer join sessions ss
    on  ss.ga4_property_name = sp.ga4_property_name
    and ss.funnel            = sp.funnel
    and ss.date              = sp.date
left join {{ ref('dim_project') }} p
    on p.ga4_property_name = ss.ga4_property_name
