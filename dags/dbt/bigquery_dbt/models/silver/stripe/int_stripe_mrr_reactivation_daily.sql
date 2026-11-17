{{ config(
    materialized='table',
    tags=["stripe", "silver", "daily"],
) }}


{% set grace_days = 15 %}

with customer_daily as (

    select * from {{ ref('int_stripe_mrr_customer_daily') }}

),

with_previous as (

    select
        *,
        lag(snapshot_date) over (
            partition by customer, _elt_stripe_account_id
            order by snapshot_date
        ) as prev_covered_day
    from customer_daily

),

returns as (

    select *
    from with_previous
    where prev_covered_day is not null
      -- gone long enough to count as having left
      and date_diff(snapshot_date, prev_covered_day, day) > {{ grace_days }}
      -- a first-time customer is new business, not a return
      and first_revenue_date < snapshot_date

)

select
    snapshot_date,
    _elt_stripe_account_id,
    count(distinct customer)                         as reactivated_customers,
    round(sum(mrr_local), 2)                         as reactivation_mrr_local,
    round(sum(mrr_usd), 2)                           as reactivation_mrr_usd,
    countif(has_missing_fx)                          as customers_missing_fx,
    string_agg(distinct missing_fx_currencies, ', ') as missing_fx_currencies
from returns
group by snapshot_date, _elt_stripe_account_id
