{{ config(
    materialized='table',
    tags=["solidgate", "silver", "daily"],
) }}

with movements as (

    select * from {{ ref('int_solidgate_mrr_movements_daily') }}
    where movement_type = 'expansion'

)

select
    snapshot_date,
    _elt_account_name,
    count(distinct customer_account_id)                 as expansion_customers,
    -- the increase, not the customer's new total
    round(sum(movement_mrr_local), 2)                   as expansion_mrr_local,
    round(sum(movement_mrr_usd), 2)                     as expansion_mrr_usd,
    countif(has_missing_fx)                             as customers_missing_fx,
    string_agg(distinct missing_fx_currencies, ', ')    as missing_fx_currencies
from movements
group by snapshot_date, _elt_account_name
