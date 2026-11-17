{{ config(
materialized='table',
tags=["stripe", "silver", "daily"],
) }}

with daily as (

    select * from {{ ref('int_stripe_mrr_local_subscription_daily') }}

),

by_day as (

    select
        snapshot_date,
        _elt_stripe_account_id,
        currency,
        count(distinct subscription_id) as active_subscriptions,
        count(distinct customer)        as customers,
        round(sum(mrr_local), 2)        as mrr,
        countif(billing_cycle = 'other') as unmapped_cycle_rows
    from daily
    group by snapshot_date, _elt_stripe_account_id, currency

)

select
    snapshot_date,
    _elt_stripe_account_id,
    currency,
    active_subscriptions,
    customers,
    mrr,
    lag(mrr) over (
        partition by _elt_stripe_account_id, currency order by snapshot_date
    ) as prev_day_mrr,
    round(mrr - lag(mrr) over (
        partition by _elt_stripe_account_id, currency order by snapshot_date
    ), 2) as mrr_change,
    round(safe_divide(
        mrr - lag(mrr) over (
            partition by _elt_stripe_account_id, currency order by snapshot_date
        ),
        lag(mrr) over (
            partition by _elt_stripe_account_id, currency order by snapshot_date
        )
    ) * 100, 2) as mrr_change_pct,
    unmapped_cycle_rows
from by_day
