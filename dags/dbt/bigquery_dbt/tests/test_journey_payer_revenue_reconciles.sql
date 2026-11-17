{{ config(enabled=var('run_cross_source_tests', true)) }}

-- the mart's lifetime revenue per product must equal int_customer_revenue for the same payers:
-- every payer belongs to exactly one user, so nothing is double counted or lost.
with accounts as (

    select a.product, a.source_system, a.account
    from {{ ref('seed_product_payment_accounts') }} a
    join {{ ref('seed_product_registry') }} r on r.product = a.product
    where r.is_in_scope

),

expected as (

    select r.product, round(sum(v.lifetime_revenue_usd), 2) as revenue_usd
    from {{ ref('int_customer_revenue') }} v
    join accounts r on r.source_system = v.source_system and r.account = v.source_account
    group by 1

),

actual as (

    select product, round(sum(ltv_usd), 2) as revenue_usd
    from {{ ref('dim_user_journey') }}
    group by 1

)

select e.product, e.revenue_usd as expected_usd, a.revenue_usd as actual_usd
from expected e
left join actual a on a.product = e.product
where abs(coalesce(a.revenue_usd, 0) - e.revenue_usd) > 1
