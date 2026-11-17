{{ config(severity='warn') }}

-- a Solidgate order routed to Stripe and the Stripe charge it became (provider_payment_id =
-- payment_intent) are the same payment, so their customers must resolve to the same user.
-- Warn only: rows left are conflicting evidence to review (e.g. the Solidgate order email and the
-- Stripe payment metadata name different app users; each side keeps its own).
with accounts as (

    select a.product, a.source_system, a.account
    from {{ ref('seed_product_payment_accounts') }} a
    join {{ ref('seed_product_registry') }} r on r.product = a.product
    where r.is_in_scope

),

pairs as (

    select distinct
        r.product,
        concat(o._elt_account_name, ':', o.customer_account_id)    as solidgate_customer,
        concat(c._elt_stripe_account_id, ':', c.customer)          as stripe_customer
    from {{ ref('int_solidgate_card_orders') }} o
    join accounts r on r.source_system = 'solidgate' and r.account = o._elt_account_name
    join {{ ref('int_stripe_charges') }} c
        on  c.payment_intent = o.provider_payment_id
    join accounts rs_acct
        on  rs_acct.product = r.product
        and rs_acct.source_system = 'stripe'
        and rs_acct.account = c._elt_stripe_account_id
    where o.customer_account_id is not null
      and c.customer is not null

)

select p.product, p.solidgate_customer, p.stripe_customer,
       rs.user_key as solidgate_user_key, rt.user_key as stripe_user_key, rt.link_path
from pairs p
join {{ ref('int_identity_resolution') }} rs
    on  rs.product  = p.product
    and rs.id_type  = 'solidgate_customer'
    and rs.id_value = p.solidgate_customer
join {{ ref('int_identity_resolution') }} rt
    on  rt.product  = p.product
    and rt.id_type  = 'stripe_customer'
    and rt.id_value = p.stripe_customer
where rs.user_key != rt.user_key
