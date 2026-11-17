{{ config(
    materialized='table',
    tags=["stripe", "silver", "daily"],
) }}


{% set grace_days = 15 %}

with customer_daily as (

    select * from {{ ref('int_stripe_mrr_customer_daily') }}

),

data_asof as (

    select max(snapshot_date) as max_date from customer_daily

),

-- last day of each run of coverage, and when they next paid
run_ends as (

    select
        snapshot_date as last_covered_day,
        date_add(snapshot_date, interval 1 day) as churn_date,
        date_diff(next_covered_day, snapshot_date, day) as days_until_return,
        customer,
        _elt_stripe_account_id,
        mrr_local,
        mrr_usd,
        subscriptions_live_now,
        has_missing_fx,
        missing_fx_currencies
    from (
        select
            *,
            lead(snapshot_date) over (
                partition by customer, _elt_stripe_account_id
                order by snapshot_date
            ) as next_covered_day
        from customer_daily
    )
    where next_covered_day is null
       or date_diff(next_covered_day, snapshot_date, day) > 1

),

churned as (

    select *
    from run_ends
    -- a churn date past the end of the data has not happened yet
    where churn_date <= (select max_date from data_asof)
      -- still being retried, so not gone
      and coalesce(subscriptions_live_now, 0) = 0
      -- came back soon enough that it was a late payment, not a departure
      and (
            days_until_return is null
            or days_until_return > {{ grace_days }}
          )

)

select
    churn_date                                       as snapshot_date,
    _elt_stripe_account_id,
    count(distinct customer)                         as churned_customers,
    -- negative: revenue lost
    round(sum(-mrr_local), 2)                        as churn_mrr_local,
    round(sum(-mrr_usd), 2)                          as churn_mrr_usd,
    countif(has_missing_fx)                          as customers_missing_fx,
    string_agg(distinct missing_fx_currencies, ', ') as missing_fx_currencies
from churned
group by churn_date, _elt_stripe_account_id
