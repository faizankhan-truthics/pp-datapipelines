{{ config(
    materialized='table',
    tags=["solidgate", "gold", "daily"],
    description='Calculates daily Visa VAMP ratios using card_orders.'
) }}

with
monthly_vamp as (
    select
        *
    from {{ ref('solidgate_monthly_vamp') }}
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
)

select
    a.*,
    round(safe_divide(
        a.vamp_count * b.days_in_month,
        b.days_elapsed
    ), 2) as estimated_eom_vamp_count,
    round(safe_divide(
        --estimated vamp count
         safe_divide(
            a.vamp_count * b.days_in_month,
            b.days_elapsed
        )
        ,
         --estimated visa transactions
         safe_divide(
            a.orders * b.days_in_month,
            b.days_elapsed
        )
    ), 2) as estimated_eom_vamp_ratio
from monthly_vamp a
inner join days_elapsed b
on a.month_ = b.curr_month