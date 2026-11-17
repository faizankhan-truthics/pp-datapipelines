{{ config(
    materialized='table',
    tags=["solidgate", "gold", "daily"],
    partition_by={
        "field": "activity_month",
        "data_type": "date",
        "granularity": "month"
    }
    ) }}

-- Refund rate = volume refunded in Month X / volume settled in Month X.
-- Settlements are attributed to the settlement month and refunds to the refund month, so
-- each order can contribute to two different months. Rebuilt in full each run: a refund
-- changes the totals for its refund month, which an append/merge by batch would miss.

with
orders as (
select
    order_id,
    settlement_date,
    refund_date,
    psp_name,
    mid_connector,
    acquiring_bank,
    descriptor,
    _elt_account_name,
    region,
    geo_country,
    order_amount_usd,
    _elt_update_timestamp
from {{ ref('int_solidgate_card_orders') }}
where settlement_date is not null
   or refund_date is not null
),

events as (
select
    date_trunc(settlement_date, month) as activity_month,
    * except(settlement_date, refund_date),
    1 as settled,
    0 as refunded
from orders
where settlement_date is not null

union all

select
    date_trunc(refund_date, month) as activity_month,
    * except(settlement_date, refund_date),
    0 as settled,
    1 as refunded
from orders
where refund_date is not null
),

monthly as (
select
    activity_month,
    psp_name,
    mid_connector,
    acquiring_bank,
    descriptor,
    _elt_account_name,
    region,
    geo_country,
    sum(settled) as settled_count,
    sum(refunded) as refunded_count,
    sum(case when settled = 1 then order_amount_usd else 0 end) as settled_amount,
    sum(case when refunded = 1 then order_amount_usd else 0 end) as refunded_amount,
    max(_elt_update_timestamp) as _elt_update_timestamp
from events
group by 1, 2, 3, 4, 5, 6, 7, 8
)

select
    activity_month,
    psp_name,
    mid_connector,
    acquiring_bank,
    descriptor,
    _elt_account_name,
    region,
    geo_country,
    settled_count,
    refunded_count,
    round(safe_divide(refunded_count, settled_count)*100,2) as refund_rate_count,
    settled_amount,
    refunded_amount,
    round(safe_divide(refunded_amount, settled_amount)*100,2) as refund_rate_amount,
    _elt_update_timestamp,
    concat(cast(activity_month as string), coalesce(psp_name,''), coalesce(mid_connector,''), coalesce(acquiring_bank,''), coalesce(descriptor,''), coalesce(_elt_account_name,''), coalesce(region,''), coalesce(geo_country,'')) unique_key
from monthly
