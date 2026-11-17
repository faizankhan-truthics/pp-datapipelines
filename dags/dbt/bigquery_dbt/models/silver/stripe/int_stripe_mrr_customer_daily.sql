{{ config(
    materialized='table',
    tags=["stripe", "silver", "daily"],
) }}

with subscription_daily as (

    select * from {{ ref('int_stripe_mrr_local_subscription_daily') }}

),

eligible as (

    select * from {{ ref('int_stripe_mrr_eligible_invoice_lines') }}

),

subscriptions as (

    select id, status, _elt_stripe_account_id from {{ ref('int_stripe_subscriptions') }}

),

fx as (

    select * from {{ ref('int_currency_conversion') }}

),

fx_asof as (

    select * except (rn)
    from (
        select
            d.snapshot_date,
            d.currency,
            f.rate,
            row_number() over (
                partition by d.snapshot_date, d.currency
                order by f.exchange_date desc
            ) as rn
        from (select distinct snapshot_date, currency from subscription_daily) d
        left join fx f
            on  upper(f.currency)  = upper(d.currency)
            and f.exchange_date   <= d.snapshot_date
    )
    where rn = 1

),

fx_latest as (

    select * except (rn)
    from (
        select
            upper(currency) as currency,
            rate,
            row_number() over (partition by upper(currency) order by exchange_date desc) as rn
        from fx
    )
    where rn = 1

),

converted as (

    select
        s.snapshot_date,
        s.customer,
        s._elt_stripe_account_id,
        s.subscription_id,
        s.currency,
        s.mrr_local,
        case
            when upper(s.currency) = 'USD' then 1
            else coalesce(a.rate, t.rate)
        end as fx_rate,
        -- where the subscription stands today, not on this date. we only ever hold
        -- the current status, so a row from June carries whatever it is now.
        sub.status as subscription_status_now
    from subscription_daily s
    left join fx_asof a
        on  s.snapshot_date = a.snapshot_date
        and s.currency      = a.currency
    left join fx_latest t
        on upper(s.currency) = t.currency
    left join subscriptions sub
        on  s.subscription_id      = sub.id
        and s._elt_stripe_account_id = sub._elt_stripe_account_id
    -- every eligible line already requires an invoice customer, but guard anyway
    -- since Stripe permits customer-less invoices
    where s.customer is not null

),

first_revenue as (

    select
        customer,
        _elt_stripe_account_id,
        min(period_start_date) as first_revenue_date
    from eligible
    group by customer, _elt_stripe_account_id

),

by_customer as (

    select
        snapshot_date,
        customer,
        _elt_stripe_account_id,
        any_value(currency)                                as currency,
        count(distinct currency)                           as currencies,
        count(distinct subscription_id)                    as active_subscriptions,
        round(sum(mrr_local), 6)                           as mrr_local,
        any_value(fx_rate)                                 as fx_rate,
        round(sum(mrr_local * fx_rate), 6)                 as mrr_usd,
        logical_or(fx_rate is null)                        as has_missing_fx,
        string_agg(distinct if(fx_rate is null, currency, null), ', ') as missing_fx_currencies,
        -- still live or still retrying a payment. lets churn tell a lapse from a
        -- customer who has actually gone.
        count(distinct if(subscription_status_now in ('active', 'past_due'), subscription_id, null))
            as subscriptions_live_now
    from converted
    group by snapshot_date, customer, _elt_stripe_account_id

)

select
    c.snapshot_date,
    c.customer,
    c._elt_stripe_account_id,
    c.currency,
    c.currencies,
    c.mrr_local,
    c.fx_rate,
    c.mrr_usd,
    c.has_missing_fx,
    c.missing_fx_currencies,
    c.active_subscriptions,
    c.subscriptions_live_now,
    f.first_revenue_date
from by_customer c
left join first_revenue f
    on  c.customer             = f.customer
    and c._elt_stripe_account_id = f._elt_stripe_account_id
