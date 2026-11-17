{{ config(
    materialized='table',
    tags=["stripe", "silver", "daily"],
) }}

with customer_daily as (

    select * from {{ ref('int_stripe_mrr_customer_daily') }}

),

with_previous as (

    select
        *,
        lag(mrr_local) over (
            partition by customer, _elt_stripe_account_id
            order by snapshot_date
        ) as prev_mrr_local,
        lag(mrr_usd) over (
            partition by customer, _elt_stripe_account_id
            order by snapshot_date
        ) as prev_mrr_usd,
        lag(active_subscriptions) over (
            partition by customer, _elt_stripe_account_id
            order by snapshot_date
        ) as prev_active_subscriptions,
        lag(snapshot_date) over (
            partition by customer, _elt_stripe_account_id
            order by snapshot_date
        ) as prev_covered_day
    from customer_daily

),

-- customers only have a row on days they are paying, so the previous row can be
-- weeks earlier. comparing across a gap is a return, not a price change — those
-- days belong to churn and reactivation instead.
flagged as (

    select
        *,
        prev_covered_day is not null
            and date_diff(snapshot_date, prev_covered_day, day) = 1 as is_consecutive
    from with_previous

)

select
    snapshot_date,
    customer,
    _elt_stripe_account_id,
    currency,
    coalesce(prev_mrr_local, 0)                    as prev_mrr_local,
    mrr_local,
    round(mrr_local - coalesce(prev_mrr_local, 0), 6) as movement_mrr_local,
    coalesce(prev_mrr_usd, 0)                      as prev_mrr_usd,
    mrr_usd,
    round(mrr_usd - coalesce(prev_mrr_usd, 0), 6)  as movement_mrr_usd,
    case
        when mrr_local > 0
             and coalesce(prev_mrr_local, 0) = 0
             and first_revenue_date = snapshot_date
            then 'new_business'
        when is_consecutive
             and mrr_local > prev_mrr_local
             and prev_mrr_local > 0
            then 'expansion'
        when is_consecutive
             and mrr_local < prev_mrr_local
             and mrr_local > 0
            then 'contraction'
        else 'other'
    end as movement_type,
    case
        when is_consecutive and mrr_local < prev_mrr_local and mrr_local > 0 then
            case
                when active_subscriptions < coalesce(prev_active_subscriptions, active_subscriptions)
                    then 'cancellation'
                else 'downgrade_or_discount'
            end
    end as contraction_reason,
    first_revenue_date,
    active_subscriptions,
    has_missing_fx,
    missing_fx_currencies
from flagged
