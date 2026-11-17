{{ config(
    materialized='table',
    tags=["refunds", "gold", "daily"],
    description='Ticket-grain fact: support-ticket refund requests paid via Mastercard, matched to their Solidgate/Stripe order or charge, with refund outcome. Grain: ticket_id x source_system x order/charge.'
) }}

-- NOTE ON COUNTRY: 
-- Solidgate's `country` is IP-derived (geo_country on the order). Stripe has no reliable
-- billing address in current data, so it falls back to card-issuing country
-- (payment_method_details.card.country). 
with refund_tickets as (
    select * from {{ ref('int_zendesk_refund_tickets') }}
),

-- ============ SOLIDGATE ============

solidgate_customer_link as (
    select distinct
        rt.ticket_id,
        rt.email,
        sc.customer_account_id
    from refund_tickets rt
    join {{ ref('int_solidgate_subscription_customers') }} sc
        on lower(sc.customer_email) = rt.email
),

solidgate_orders as (
    select
        scl.ticket_id,
        scl.email,
        o.order_id,
        o.merchant_id as account_id,
        o._elt_account_name as account_name,
        o.product_name as product,
        o.subscription_id,
        o.geo_country as country
    from solidgate_customer_link scl
    join {{ ref('int_solidgate_card_orders') }} o on o.customer_account_id = scl.customer_account_id
),

solidgate_mastercard as (
    select distinct
        so.ticket_id, so.email, so.account_id, so.account_name, so.product, so.country, so.order_id
    from solidgate_orders so
    join {{ ref('int_solidgate_card_orders_transactions') }} tx on tx.order_id = so.order_id
    where tx.transaction_card_brand = 'MASTERCARD'
),

solidgate_refund_outcome as (
    select
        order_id,
        logical_or(transaction_operation = 'refund' and transaction_status = 'success') as refund_issued
    from {{ ref('int_solidgate_card_orders_transactions') }}
    group by order_id
),

solidgate_final as (
    select
        'solidgate' as source_system,
        sm.account_id,
        sm.account_name,
        coalesce(sm.product, 'unknown') as product,
        sm.country,
        sm.ticket_id,
        sm.email,
        coalesce(ro.refund_issued, false) as refund_issued
    from solidgate_mastercard sm
    left join solidgate_refund_outcome ro on ro.order_id = sm.order_id
),

-- ============ STRIPE ============

stripe_customers_matched as (
    select distinct
        rt.ticket_id, rt.email, sc.id as customer_id, sc._elt_stripe_account_id as account_id,
        coalesce(sc.billing_country, sc.shipping_country) as customer_country
    from refund_tickets rt
    join {{ ref('int_stripe_customers') }} sc on lower(sc.email) = rt.email
),

stripe_charges_mc as (
    select
        scm.ticket_id, scm.email, scm.customer_id, scm.account_id,
        c.id as charge_id,
        coalesce(scm.customer_country, json_value(c.payment_method_details, '$.card.country')) as country
    from stripe_customers_matched scm
    join {{ ref('int_stripe_charges') }} c
        on c.customer = scm.customer_id and c._elt_stripe_account_id = scm.account_id
    where lower(json_value(c.payment_method_details, '$.card.brand')) = 'mastercard'
),

stripe_refund_outcome as (
    select charge, logical_or(status = 'succeeded') as refund_issued
    from {{ ref('int_stripe_refunds') }}
    group by charge
),

stripe_invoice_products as (
    select distinct
        il.charge_id,
        il.product_id
    from {{ ref('int_stripe_invoice_lines') }} il
    where il.charge_id is not null
),

stripe_final as (
    select
        'stripe' as source_system,
        scm.account_id,
        sa.account as account_name,
        coalesce(pr.product_name, ip.product_id, 'unknown') as product,
        scm.country,
        scm.ticket_id,
        scm.email,
        coalesce(ro.refund_issued, false) as refund_issued
    from stripe_charges_mc scm
    left join stripe_refund_outcome ro on ro.charge = scm.charge_id
    left join stripe_invoice_products ip on ip.charge_id = scm.charge_id
    left join {{ ref('int_stripe_products') }} pr on pr.product_id = ip.product_id
    left join {{ source('bronze_stripe', 'stripe_accounts') }} sa on sa.id = scm.account_id
)

select * from solidgate_final
union all
select * from stripe_final
