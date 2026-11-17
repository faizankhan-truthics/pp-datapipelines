{{ config(
    tags=["solidgate", "gold", "daily"],
) }}


with movements as (

    select * from {{ ref('int_solidgate_mrr_movements_daily') }}

),

by_day as (

    select
        snapshot_date,
        _elt_account_name,
        countif(movement_type = 'new_business')                             as new_business_customers,
        round(sum(if(movement_type = 'new_business', mrr_usd, 0)), 2)       as new_business_mrr_usd,
        countif(movement_type = 'expansion')                                as expansion_customers,
        round(sum(if(movement_type = 'expansion', movement_mrr_usd, 0)), 2) as expansion_mrr_usd,
        countif(movement_type = 'contraction')                              as contraction_customers,
        round(sum(if(movement_type = 'contraction', movement_mrr_usd, 0)), 2) as contraction_mrr_usd,
        count(distinct customer_account_id)                                 as active_customers,
        round(sum(mrr_usd), 2)                                              as active_mrr_usd,
        countif(has_missing_fx)                                             as customers_missing_fx,
        string_agg(distinct missing_fx_currencies, ', ')                    as missing_fx_currencies
    from movements
    group by snapshot_date, _elt_account_name

),

churn as (

    select * from {{ ref('int_solidgate_mrr_churn_daily') }}

),

reactivation as (

    select * from {{ ref('int_solidgate_mrr_reactivation_daily') }}

),

-- churn lands on a day the customer is no longer active, so it can fall outside the
-- days that have movements
days as (

    select snapshot_date, _elt_account_name from by_day
    union distinct
    select snapshot_date, _elt_account_name from churn

)

select
    d.snapshot_date,
    d._elt_account_name,
    coalesce(m.new_business_customers, 0)   as new_business_customers,
    coalesce(m.new_business_mrr_usd, 0)     as new_business_mrr_usd,
    coalesce(r.reactivated_customers, 0)    as reactivation_customers,
    coalesce(r.reactivation_mrr_usd, 0)     as reactivation_mrr_usd,
    coalesce(m.expansion_customers, 0)      as expansion_customers,
    coalesce(m.expansion_mrr_usd, 0)        as expansion_mrr_usd,
    coalesce(m.contraction_customers, 0)    as contraction_customers,
    coalesce(m.contraction_mrr_usd, 0)      as contraction_mrr_usd,
    coalesce(c.churned_customers, 0)        as churn_customers,
    coalesce(c.churn_mrr_usd, 0)            as churn_mrr_usd,
    coalesce(m.active_customers, 0)         as active_customers,
    coalesce(m.active_mrr_usd, 0)           as active_mrr_usd,
    coalesce(m.customers_missing_fx, 0)     as customers_missing_fx,
    m.missing_fx_currencies
from days d
left join by_day m
    on  d.snapshot_date     = m.snapshot_date
    and d._elt_account_name = m._elt_account_name
left join churn c
    on  d.snapshot_date     = c.snapshot_date
    and d._elt_account_name = c._elt_account_name
left join reactivation r
    on  d.snapshot_date     = r.snapshot_date
    and d._elt_account_name = r._elt_account_name
