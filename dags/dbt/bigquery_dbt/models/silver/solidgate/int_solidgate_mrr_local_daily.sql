{{ config(
materialized='table',
tags=["solidgate", "silver", "daily"],
) }}

with daily as (

    select * from {{ ref('int_solidgate_mrr_local_subscription_daily') }}

),

by_day as (

    select
        snapshot_date,
        _elt_account_name,
        currency,
        count(distinct subscription_id)     as active_subscriptions,
        count(distinct customer_account_id) as customers,
        round(sum(mrr_local), 2)            as mrr,
        countif(billing_cycle = 'prorated') as prorated_rows
    from daily
    group by snapshot_date, _elt_account_name, currency

)

select
    snapshot_date,
    _elt_account_name,
    currency,
    active_subscriptions,
    customers,
    mrr,
    lag(mrr) over (
        partition by _elt_account_name, currency order by snapshot_date
    ) as prev_day_mrr,
    round(mrr - lag(mrr) over (
        partition by _elt_account_name, currency order by snapshot_date
    ), 2) as mrr_change,
    round(safe_divide(
        mrr - lag(mrr) over (
            partition by _elt_account_name, currency order by snapshot_date
        ),
        lag(mrr) over (
            partition by _elt_account_name, currency order by snapshot_date
        )
    ) * 100, 2) as mrr_change_pct,
    prorated_rows
from by_day
