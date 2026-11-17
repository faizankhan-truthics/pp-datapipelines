{{ config(
tags=["ga4", "gold", "daily"],
) }}

-- Campaign-level companion to ga4_payment_match_daily: attributes real Solidgate/Stripe
-- transaction revenue to the GA4 campaign_id that generated it, instead of just counting
-- matched/unmatched transactions at account grain. Only the exact transaction GA4 observed is
-- attributed -- subscription renewals with no matching GA4 transaction are intentionally
-- excluded 
-- Grain: pay_date x payment_source x account_key x campaign_id.
with account_map as (

    select * from {{ ref('seed_ga4_payment_account_map') }}

),

ga4 as (

    select ga4_date, ga4_property_name, transaction_id, campaign_id, gclid
    from {{ ref('int_ga4_transactions') }}

),

solidgate_txns as (

    select
        sg_date              as pay_date,
        transaction_id,
        'solidgate'           as payment_source,
        _elt_account_name     as account_key,
        case
            when order_status in ('approved', 'settle_ok', 'partial_settled', 'refunded')
            then order_amount_usd
            else 0
        end as revenue_usd
    from {{ ref('int_solidgate_transactions') }}

),

stripe_txns as (

    select
        stripe_date           as pay_date,
        transaction_id,
        'stripe'               as payment_source,
        _elt_stripe_account_id as account_key,
        amount_usd             as revenue_usd
    from {{ ref('int_stripe_transactions') }}

),

payment_txns as (

    select * from solidgate_txns
    union all
    select * from stripe_txns

),

-- Restrict both sides to accounts/properties present in account_map before the join below
payment_mapped as (

    select p.pay_date, p.transaction_id, p.payment_source, p.account_key, p.revenue_usd
    from payment_txns p
    inner join account_map m
        on  p.payment_source = m.payment_source
        and p.account_key    = m.account_key

),

ga4_mapped as (

    select
        g.ga4_date,
        g.ga4_property_name,
        m.payment_source,
        m.account_key,
        g.transaction_id,
        g.campaign_id,
        g.gclid
    from ga4 g
    inner join account_map m
        on g.ga4_property_name = m.ga4_property_name

),

-- Inner join, unlike ga4_payment_match_daily's full outer: revenue can only be attributed to a
-- campaign when both a real payment AND a matching GA4 transaction exist.
stitched as (

    select
        p.pay_date,
        p.payment_source,
        p.account_key,
        g.ga4_property_name,
        g.campaign_id,
        g.gclid,
        p.transaction_id,
        p.revenue_usd
    from ga4_mapped g
    inner join payment_mapped p
        on  g.payment_source = p.payment_source
        and g.account_key    = p.account_key
        and g.transaction_id = p.transaction_id
        and g.ga4_date       = p.pay_date

)

select
    pay_date,
    payment_source,
    account_key,
    campaign_id,
    any_value(ga4_property_name) as ga4_property_name,
    any_value(gclid)             as gclid,
    sum(revenue_usd)             as revenue_usd,
    count(distinct transaction_id) as txn_cnt
from stitched
group by pay_date, payment_source, account_key, campaign_id
