{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    on_schema_change='append_new_columns',
    tags=["stripe", "gold", "daily"],
    unique_key = 'unique_key',
    partition_by={
        "field": "_elt_update_timestamp",
        "data_type": "timestamp"
    }
    )
}}

with
monthly_vamp as (
    select
        *
    from {{ ref('stripe_monthly_vamp') }}
),

days_elapsed as  (
    select
        date_trunc(current_date(), month) as curr_month,
        date_diff(
            current_date(),
            date_trunc(current_date(), month),
            day
        ) + 1 as days_elapsed,
        extract(
            day from last_day(current_date(), month)
    ) as days_in_month
),

estimates as (
select
    a.*,
    round(safe_divide(
        a.vamp_count * b.days_in_month,
        b.days_elapsed
    ), 0) as estimated_eom_vamp_count,
    round(safe_divide(
        --estimated vamp count
         safe_divide(
            a.vamp_count * b.days_in_month,
            b.days_elapsed
        )
        ,
         --estimated visa transactions
         safe_divide(
            a.visa_transactions * b.days_in_month,
            b.days_elapsed
        )
    ), 4) as estimated_eom_vamp_ratio
from monthly_vamp a
inner join days_elapsed b
on a.Month_ = b.curr_month

)

select
    *,
    case
        when vamp_count > 1500 then 'red'
        when safe_divide(vamp_count, visa_transactions) > (1.5/100) and vamp_count > 500 then 'red'
        when estimated_eom_vamp_count > 1500 then 'yellow'
        when estimated_eom_vamp_ratio > (1.5/100) and estimated_eom_vamp_count > 500 then 'yellow'
        else 'green'
    end as vamp_status,
    case
        when vamp_count > 1500 then 'vamp count exceeded'
        when safe_divide(vamp_count, visa_transactions) > (1.5/100) then 'vamp ratio exceeded'
        when estimated_eom_vamp_count > 1500 then 'vamp count EOM estimate exceeded'
        when estimated_eom_vamp_ratio > (1.5/100) then  'vamp ratio EOM estimate exceeded'
        else 'ok'
    end as vamp_status_reason,
    1.5/100 as threshold_pct,
    1500 as count_threshold
from estimates