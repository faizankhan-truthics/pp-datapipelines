{{ config(
    materialized='table',
    tags=["identity", "silver", "daily"],
) }}

-- Lifetime collected revenue per customer, across both payment processors -- the LTV side of
-- the attribution chain. Collected cash, not MRR: every successful payment counts, first
-- purchase and renewals alike, less what was refunded. Disputes and PSP fees are not netted
-- off here (neither carries a customer id at this grain); see net_revenue_daily for those at
-- account level.
-- Amounts are in major units (dollars, not cents), converted to USD at the payment date's rate.
-- Grain: source_system x source_account x source_customer_id.

with stripe_collected_ids as (

    -- Stripe charges counted below, by the ids a Solidgate order routed to Stripe stores in
    -- provider_payment_id (payment_intent; a charge id is matched too)
    select distinct pid as provider_payment_id
    from {{ ref('int_stripe_charges') }}
    cross join unnest([payment_intent, id]) as pid
    where customer is not null
      and status = 'succeeded'
      and captured
      and pid is not null

),

solidgate_orders as (

    -- order_amount_usd is already in major units; refunded_amount is still in minor units.
    -- same "counts as revenue" statuses as ga4_payment_match_campaign_daily; `refunded` orders
    -- were collected first, so they count as gross and come back out as refunds.
    -- Orders Solidgate routed to Stripe are the same money as the Stripe charge: they are counted
    -- (and refunded) on the Stripe side only, as in net_revenue_daily.
    select
        o.customer_account_id,
        o._elt_account_name,
        o.created_date,
        o.order_amount_usd as gross_usd,
        -- refunded_amount is in refunded_currency, which is not always the order's currency;
        -- carry it to USD at that currency's rate on the order date (same convention as
        -- int_solidgate_card_orders). a `refunded` order with no amount on file is taken as
        -- fully refunded.
        case
            when safe_cast(o.refunded_amount as float64) > 0
                then safe_cast(o.refunded_amount as float64)
                    * case
                        when coalesce(o.refunded_currency, o.order_currency) = 'USD' then 1
                        when coalesce(o.refunded_currency, o.order_currency) = o.order_currency
                            then safe_divide(
                                o.order_amount_usd,
                                o.order_amount_local / {{ minor_unit_divisor('o.order_currency') }}
                            )
                        else r.rate
                    end
                    / {{ minor_unit_divisor('coalesce(o.refunded_currency, o.order_currency)') }}
            when o.status = 'refunded'
                then o.order_amount_usd
            else 0
        end as refunded_usd
    from {{ ref('int_solidgate_card_orders') }} o
    left join {{ ref('int_currency_conversion') }} r
        on  o.created_date      = r.exchange_date
        and o.refunded_currency = r.currency
    left join stripe_collected_ids s
        on s.provider_payment_id = o.provider_payment_id
    where o.customer_account_id is not null
      and o.status in ('approved', 'settle_ok', 'partial_settled', 'refunded')
      and s.provider_payment_id is null

),

solidgate as (

    select
        'solidgate'              as source_system,
        _elt_account_name        as source_account,
        customer_account_id      as source_customer_id,
        count(*)                 as paid_transactions,
        min(created_date)        as first_payment_date,
        max(created_date)        as last_payment_date,
        sum(coalesce(gross_usd, 0))    as lifetime_gross_revenue_usd,
        sum(coalesce(refunded_usd, 0)) as lifetime_refunded_usd
    from solidgate_orders
    group by 1, 2, 3

),

stripe_charges as (

    -- same zero-decimal currency handling as net_revenue_daily
    select
        c.customer,
        c._elt_stripe_account_id,
        c.created_date,
        c.amount_captured_usd as captured_usd,
        -- refunds at the charge's own rate, so a full refund nets the charge to exactly 0; never
        -- more than was captured (partly captured charges)
        least(c.amount_captured_usd, c.amount_refunded_usd)
            as refunded_usd
    from {{ ref('int_stripe_charges') }} c
    where c.customer is not null
      and c.status = 'succeeded'
      -- uncaptured charges are authorisations that were never collected (e.g. voided trials)
      and c.captured

),

stripe as (

    select
        'stripe'                                                  as source_system,
        _elt_stripe_account_id                                    as source_account,
        customer                                                  as source_customer_id,
        count(*)                                                  as paid_transactions,
        min(created_date)                                         as first_payment_date,
        max(created_date)                                         as last_payment_date,
        sum(coalesce(captured_usd, 0))                            as lifetime_gross_revenue_usd,
        sum(refunded_usd)                                         as lifetime_refunded_usd
    from stripe_charges
    group by 1, 2, 3

),

unioned as (

    select * from solidgate
    union all
    select * from stripe

)

select
    source_system,
    source_account,
    source_customer_id,
    paid_transactions,
    first_payment_date,
    last_payment_date,
    round(lifetime_gross_revenue_usd, 2)                         as lifetime_gross_revenue_usd,
    round(lifetime_refunded_usd, 2)                              as lifetime_refunded_usd,
    round(lifetime_gross_revenue_usd - lifetime_refunded_usd, 2) as lifetime_revenue_usd
from unioned
