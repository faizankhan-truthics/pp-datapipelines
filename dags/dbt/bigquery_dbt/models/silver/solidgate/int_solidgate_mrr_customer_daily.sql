{{ config(
    materialized='table',
    tags=["solidgate", "silver", "daily"],
) }}

with subscription_daily as (

    select * from {{ ref('int_solidgate_mrr_local_subscription_daily') }}

),

eligible as (

    select * from {{ ref('int_solidgate_mrr_eligible_invoices') }}

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
        s.customer_account_id,
        s._elt_account_name,
        s.subscription_id,
        s.currency,
        s.mrr_local,
        s.subscription_status_now,
        case
            when upper(s.currency) = 'USD' then 1
            else coalesce(a.rate, t.rate)
        end as fx_rate
    from subscription_daily s
    left join fx_asof a
        on  s.snapshot_date = a.snapshot_date
        and s.currency      = a.currency
    left join fx_latest t
        on upper(s.currency) = t.currency
    -- a few subscriptions have no customer object in the source payload; they
    -- cannot be attributed to a customer, so they are out of scope at this grain.
    -- their revenue still counts in the account-level marts.
    where s.customer_account_id is not null

),

first_revenue as (

    select
        customer_account_id,
        _elt_account_name,
        min(date(billing_period_started_at)) as first_revenue_date
    from eligible
    group by customer_account_id, _elt_account_name

),

by_customer as (

    select
        snapshot_date,
        customer_account_id,
        _elt_account_name,
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
        countif(subscription_status_now in ('redemption', 'active')) as subscriptions_live_now
    from converted
    group by snapshot_date, customer_account_id, _elt_account_name

)

select
    c.snapshot_date,
    c.customer_account_id,
    c._elt_account_name,
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
    on  c.customer_account_id = f.customer_account_id
    and c._elt_account_name   = f._elt_account_name
