{{ config(
tags=["solidgate", "gold", "daily"],
) }}

with local_daily as (

    select * from {{ ref('int_solidgate_mrr_local_daily') }}

),

subscription_daily as (

    select * from {{ ref('int_solidgate_mrr_local_subscription_daily') }}

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
            f.exchange_date,
            row_number() over (
                partition by d.snapshot_date, d.currency
                order by f.exchange_date desc
            ) as rn
        from (select distinct snapshot_date, currency from local_daily) d
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
            exchange_date,
            row_number() over (partition by upper(currency) order by exchange_date desc) as rn
        from fx
    )
    where rn = 1

),

converted as (

    select
        l.snapshot_date,
        l._elt_account_name,
        l.currency,
        l.mrr as mrr_local,
        case
            when upper(l.currency) = 'USD' then 1
            else coalesce(a.rate, t.rate)
        end as fx_rate
    from local_daily l
    left join fx_asof a
        on  l.snapshot_date = a.snapshot_date
        and l.currency      = a.currency
    left join fx_latest t
        on upper(l.currency) = t.currency

),

mrr_by_day as (

    select
        snapshot_date,
        _elt_account_name,
        round(sum(mrr_local * fx_rate), 2) as mrr_usd,
        count(distinct currency)           as currencies
    from converted
    group by snapshot_date, _elt_account_name

),

counts_by_day as (

    select
        snapshot_date,
        _elt_account_name,
        count(distinct subscription_id)     as active_subscriptions,
        count(distinct customer_account_id) as customers
    from subscription_daily
    group by snapshot_date, _elt_account_name

)

select
    m.snapshot_date,
    m._elt_account_name,
    m.mrr_usd,
    c.active_subscriptions,
    c.customers,
    m.currencies,
    lag(m.mrr_usd) over (
        partition by m._elt_account_name order by m.snapshot_date
    ) as prev_day_mrr_usd,
    round(m.mrr_usd - lag(m.mrr_usd) over (
        partition by m._elt_account_name order by m.snapshot_date
    ), 2) as mrr_change_usd,
    round(safe_divide(
        m.mrr_usd - lag(m.mrr_usd) over (
            partition by m._elt_account_name order by m.snapshot_date
        ),
        lag(m.mrr_usd) over (
            partition by m._elt_account_name order by m.snapshot_date
        )
    ) * 100, 2) as mrr_change_pct
from mrr_by_day m
left join counts_by_day c
    on  m.snapshot_date     = c.snapshot_date
    and m._elt_account_name = c._elt_account_name
