{{ config(
    materialized='table',
    tags=["solidgate", "silver", "daily"],
) }}

with movements as (

    select * from {{ ref('int_solidgate_mrr_movements_daily') }}
    where movement_type = 'contraction'

)

select
    snapshot_date,
    _elt_account_name,
    count(distinct customer_account_id)                  as contraction_customers,
    countif(contraction_reason = 'cancellation')         as cancellation_customers,
    countif(contraction_reason = 'downgrade_or_discount') as downgrade_customers,
    -- negative: the decrease, not the customer's new total
    round(sum(movement_mrr_local), 2)                    as contraction_mrr_local,
    round(sum(movement_mrr_usd), 2)                      as contraction_mrr_usd,
    countif(has_missing_fx)                              as customers_missing_fx,
    string_agg(distinct missing_fx_currencies, ', ')     as missing_fx_currencies
from movements
group by snapshot_date, _elt_account_name
