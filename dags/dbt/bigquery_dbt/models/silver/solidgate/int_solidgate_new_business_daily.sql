{{ config(
    materialized='table',
    tags=["solidgate", "silver", "daily"],
) }}

with movements as (

    select * from {{ ref('int_solidgate_mrr_movements_daily') }}
    where movement_type = 'new_business'

)

select
    snapshot_date,
    _elt_account_name,
    count(distinct customer_account_id)                 as new_customers,
    sum(active_subscriptions)                           as new_subscriptions,
    round(sum(mrr_local), 2)                            as new_business_mrr_local,
    round(sum(mrr_usd), 2)                              as new_business_mrr_usd,
    countif(has_missing_fx)                             as customers_missing_fx,
    string_agg(distinct missing_fx_currencies, ', ')    as missing_fx_currencies
from movements
group by snapshot_date, _elt_account_name
