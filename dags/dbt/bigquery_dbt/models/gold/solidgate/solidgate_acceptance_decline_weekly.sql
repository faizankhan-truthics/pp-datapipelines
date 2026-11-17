{{ config(
    materialized='table',
    tags=["solidgate", "gold", "daily"],
    partition_by={
        "field": "week_start",
        "data_type": "date",
        "granularity": "month"
    },
    cluster_by=["account_name", "country", "psp_name", "network"]
) }}

-- Weekly acceptance and decline counts for the Acceptance Rate and Decline dashboards.
-- Accepted statuses match vamp_initiative_acceptance_rate. Decline code/name/action are
-- null on accepted rows, so each week/dimension combo has one accepted row plus one row
-- per decline reason.

with card_orders as (
    select
        date_trunc(created_date, week(monday)) as week_start,
        _elt_account_name as account_name,
        psp_name,
        transaction_card_brand as network,
        geo_country as country,
        region,
        platform,
        if(order_amount_usd < 1.5, '1. <$1.50', '2. >=$1.50') as amount_band,
        payment_type,
        -- Small orders split into first payment (no payment type) and second payment (recurring);
        -- everything else keeps its payment type, '(empty)' when it has none.
        case
            when coalesce(payment_type, '') = '' and order_amount_usd < 1.5 then 'first payment'
            when lower(payment_type) = 'recurring' and order_amount_usd < 1.5 then 'second payment'
            else coalesce(nullif(payment_type, ''), '(empty)')
        end as payment_kind,
        coalesce(status in ('auth_ok', 'settle_ok', 'void_ok', 'refunded'), false) as is_accepted,
        error_code,
        error_message,
        error_next_step,
        order_amount_usd
    from {{ ref('int_solidgate_card_orders') }}
    where created_date >= '2026-05-01'
)

select
    week_start,
    account_name,
    psp_name,
    network,
    country,
    region,
    platform,
    amount_band,
    payment_type,
    payment_kind,
    if(is_accepted, null, error_code) as decline_code,
    if(is_accepted, null, error_message) as decline_name,
    if(is_accepted, null, error_next_step) as decline_action,
    count(*) as attempts,
    countif(is_accepted) as succeeded,
    countif(not is_accepted) as declines,
    sum(if(is_accepted, order_amount_usd, 0)) as succeeded_amount_usd,
    sum(if(is_accepted, 0, order_amount_usd)) as declined_amount_usd
from card_orders
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
