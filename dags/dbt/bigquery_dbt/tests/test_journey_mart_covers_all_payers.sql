-- spans several models, so dbt_dag runs it as a detached task after all of them build
-- (should_detach_multiple_parents_tests). the var lets a partial run turn it off.
{{ config(enabled=var('run_cross_source_tests', true)) }}

-- every successful payer of an in-scope product's accounts must resolve to a user that is in
-- dim_user_journey with has_payment_match. if one drops out, customer counts and revenue stop
-- adding up.
with accounts as (

    select a.product, a.source_system, a.account
    from {{ ref('seed_product_payment_accounts') }} a
    join {{ ref('seed_product_registry') }} r on r.product = a.product
    where r.is_in_scope

),

payers as (

    select distinct r.product, 'solidgate_customer' as id_type,
           concat(o._elt_account_name, ':', o.customer_account_id) as id_value
    from {{ ref('int_solidgate_card_orders') }} o
    join accounts r on r.source_system = 'solidgate' and r.account = o._elt_account_name
    where o.customer_account_id is not null
      and o.status in ('approved', 'settle_ok', 'partial_settled', 'refunded')

    union distinct

    select distinct r.product, 'stripe_customer',
           concat(c._elt_stripe_account_id, ':', c.customer)
    from {{ ref('int_stripe_charges') }} c
    join accounts r on r.source_system = 'stripe' and r.account = c._elt_stripe_account_id
    where c.customer is not null and c.status = 'succeeded'

)

select p.*
from payers p
left join {{ ref('int_identity_resolution') }} rs
    on  rs.product  = p.product
    and rs.id_type  = p.id_type
    and rs.id_value = p.id_value
left join {{ ref('dim_user_journey') }} d
    on  d.product  = rs.product
    and d.user_key = rs.user_key
where d.user_key is null
   or not d.has_payment_match
