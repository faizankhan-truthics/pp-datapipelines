{{ config(
    materialized='table',
    tags=["ga4", "silver", "daily"],
    partition_by={
        "field": "match_date",
        "data_type": "date"
    }
) }}

-- Generalized across both payment sources (previously Solidgate only): GA4 is matched through
-- seed_ga4_payment_account_map rather than int_ga4_transactions._elt_account_name, since that
-- column is null for Stripe-backed properties by design.

with account_map as (

    select * from {{ ref('seed_ga4_payment_account_map') }}

),

ga4 as (

    -- both sides are unique per date, not per transaction, and the join has no date in it,
    -- so each is collapsed to one row per transaction first or the join fans out
    select
        g.ga4_date,
        g.ga4_property_name,
        m.payment_source,
        m.account_key,
        g.transaction_id,
        g.campaign_id,
        g.gclid
    from {{ ref('int_ga4_transactions') }} g
    inner join account_map m
        on g.ga4_property_name = m.ga4_property_name
    -- keep the first day GA4 saw it; later sightings are redeliveries of the same purchase
    qualify row_number() over (
        partition by m.payment_source, m.account_key, g.transaction_id
        order by g.ga4_date
    ) = 1

),

solidgate_payments as (

    select
        sg_date              as pay_date,
        'solidgate'           as payment_source,
        _elt_account_name     as account_key,
        transaction_id,
        subscription_id,
        invoice_id,
        order_status,
        order_amount_usd      as revenue_usd
    from {{ ref('int_solidgate_transactions') }}
    -- an order id can sit under more than one invoice; prefer the attempt that went through
    qualify row_number() over (
        partition by _elt_account_name, transaction_id
        order by
            case when order_status in ('approved', 'settle_ok', 'partial_settled', 'refunded')
                 then 0 else 1 end,
            sg_date,
            invoice_id
    ) = 1

),

stripe_payments as (

    select
        stripe_date             as pay_date,
        'stripe'                 as payment_source,
        _elt_stripe_account_id   as account_key,
        transaction_id,
        subscription_id,
        invoice_id,
        -- int_stripe_transactions already resolves this to a succeeded-only sum, unlike
        -- Solidgate's raw order_status -- there is no separate attempt-level status to carry
        cast(null as string)    as order_status,
        amount_usd              as revenue_usd
    from {{ ref('int_stripe_transactions') }}
    -- int_stripe_transactions' own grain includes stripe_date -- a PaymentIntent retried across
    -- several failed attempts on different days (confirmed live: some retry a dozen+ times
    -- before ever succeeding, or never) otherwise produces one row per attempt-day here and
    -- fans out the join below. Prefer the day revenue actually landed; if it never did, prefer
    -- the earliest attempt (same "first day" convention int_ga4_transactions/int_solidgate_transactions
    -- use elsewhere).
    qualify row_number() over (
        partition by _elt_stripe_account_id, transaction_id
        order by case when amount_usd > 0 then 0 else 1 end, stripe_date
    ) = 1

),

payments as (

    select * from solidgate_payments
    union all
    select * from stripe_payments

)

select
    coalesce(p.pay_date, g.ga4_date)                     as match_date,
    coalesce(p.payment_source, g.payment_source)         as payment_source,
    coalesce(p.account_key, g.account_key)                as account_key,
    coalesce(p.transaction_id, g.transaction_id)          as transaction_id,
    -- kept separately as well as coalesced, so which side is missing is visible on the
    -- row rather than inferred from match_status
    g.transaction_id                                      as ga4_transaction_id,
    p.transaction_id                                      as pay_transaction_id,
    case
        when p.transaction_id is not null and g.transaction_id is not null then 'matched'
        when p.transaction_id is not null                                  then 'payment_only'
        else                                                                    'ga4_only'
    end                                                   as match_status,

    p.pay_date,
    p.subscription_id,
    p.invoice_id,
    p.order_status,
    p.revenue_usd,

    g.ga4_date,
    g.ga4_property_name,
    g.campaign_id,
    g.gclid
from payments p
full outer join ga4 g
    on  p.payment_source = g.payment_source
    and p.account_key    = g.account_key
    and p.transaction_id = g.transaction_id
