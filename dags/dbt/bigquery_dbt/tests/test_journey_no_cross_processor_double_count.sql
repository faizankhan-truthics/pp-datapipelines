{{ config(enabled=var('run_cross_source_tests', true)) }}

-- a Solidgate order routed to Stripe is the same money as the Stripe charge: the journey must
-- carry it once. Fails on any payment present both as a Solidgate and as a Stripe payment row.
with solidgate_paid as (

    select f.product, f.user_key, o.order_id, o.provider_payment_id
    from {{ ref('fct_user_journey_events') }} f
    join {{ ref('int_solidgate_card_orders') }} o on o.order_id = f.ref_id
    where f.event_source = 'solidgate'
      and f.event_name = 'payment'
      and o.provider_payment_id is not null

),

stripe_paid as (

    select f.product, f.user_key, c.id as charge_id, c.payment_intent
    from {{ ref('fct_user_journey_events') }} f
    join {{ ref('int_stripe_charges') }} c on c.id = f.ref_id
    where f.event_source = 'stripe'
      and f.event_name = 'payment'

)

-- provider_payment_id is a payment_intent or a charge id: two equi-joins, not one `in (...)` join,
-- which BigQuery runs as a cross join
select s.product, s.user_key, s.order_id, t.charge_id, s.provider_payment_id
from solidgate_paid s
join stripe_paid t on s.provider_payment_id = t.payment_intent

union distinct

select s.product, s.user_key, s.order_id, t.charge_id, s.provider_payment_id
from solidgate_paid s
join stripe_paid t on s.provider_payment_id = t.charge_id
