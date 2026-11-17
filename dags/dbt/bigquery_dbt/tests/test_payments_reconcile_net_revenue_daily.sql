{{ config(enabled=var('run_cross_source_tests', true)) }}

-- fct_payments and gold.net_revenue_daily read the same ledgers, so for the in-scope accounts their
-- daily gross payments and refunds must agree. Fails on any day (last 45 days, up to two days ago,
-- so a ledger that is still loading does not trip it) that differs by more than $1.
with in_scope as (

    select a.account as account_name
    from {{ ref('seed_product_payment_accounts') }} a
    join {{ ref('seed_product_registry') }} r on r.product = a.product
    where r.is_in_scope and a.source_system = 'solidgate'

    union distinct

    select e.account as account_name
    from {{ ref('seed_product_payment_accounts') }} a
    join {{ ref('seed_product_registry') }} r on r.product = a.product
    join {{ source('bronze_stripe', 'stripe_accounts') }} e on e.id = a.account
    where r.is_in_scope and a.source_system = 'stripe'

),

net_revenue as (

    select date, sum(gross_revenue_usd) as gross_usd, sum(refunds_usd) as refunds_usd
    from {{ ref('net_revenue_daily') }}
    where reporting_account in (select account_name from in_scope)
      and date between date_sub(current_date(), interval 45 day) and date_sub(current_date(), interval 2 day)
    group by 1

),

payments as (

    select
        event_date as date,
        sum(if(event_name = 'payment', amount_usd, 0))  as gross_usd,
        -sum(if(event_name = 'refund', amount_usd, 0))  as refunds_usd
    from {{ ref('fct_payments') }}
    where event_date between date_sub(current_date(), interval 45 day) and date_sub(current_date(), interval 2 day)
    group by 1

)

select
    n.date,
    n.gross_usd   as net_revenue_gross_usd,
    p.gross_usd   as payments_gross_usd,
    n.refunds_usd as net_revenue_refunds_usd,
    p.refunds_usd as payments_refunds_usd
from net_revenue n
left join payments p on p.date = n.date
where abs(coalesce(p.gross_usd, 0) - n.gross_usd) > 1
   or abs(coalesce(p.refunds_usd, 0) - n.refunds_usd) > 1
