{{ config(
    materialized='table',
    tags=["solidgate", "gold", "daily"]
) }}

-- Base table for the VAMP initiative dashboards (acceptance rate, recovery
-- rate, EFW/dispute rate). Scoped to PS_PROD_INFOCHECKER's "Pro Access — CE3"
-- subscriptions. Grain: one row per subscription x invoice x order (payment
-- attempt). invoice_number sequences each subscription's real (>$2)
-- invoices oldest first; invoice_outcome is the status of an invoice's last
-- retry attempt.
-- The rolling 126-day window (see invoice_sequence below) is commented out
-- for now, matching the original un-windowed query; uncomment to scope the
-- dashboards to the last 126 days.

with qualifying_subscriptions as (

    select distinct subscription_id
    from {{ ref('int_solidgate_card_orders') }}
    where order_description = 'Pro Access — CE3'
      and order_amount_local > 0

),

invoice_attempts as (

    select
        o.subscription_id,
        o.invoice_id,
        o.order_id,
        o.order_status,
        o.order_operation,
        o.order_retry_attempt,
        o.order_created_at,
        o.order_amount_usd,
        co.order_currency,
        o.psp_order_id,
        o.provider_payment_id,
        o._elt_account_name as account_name,
        first_value(o.order_status) over (
            partition by o.subscription_id, o.invoice_id
            order by o.order_retry_attempt desc
        ) as invoice_outcome
    from {{ ref('int_solidgate_subscription_invoice_orders') }} o
    inner join qualifying_subscriptions qs using (subscription_id)
    left join {{ ref('int_solidgate_card_orders') }} co on co.order_id = o.order_id
    where o._elt_account_name = 'PS_PROD_INFOCHECKER'

),

efw as (

    select payment_intent, created_date as efw_date
    from {{ ref('int_stripe_early_fraud_warnings') }}

),

disputes as (

    select payment_intent, created_date as dispute_date
    from {{ ref('int_stripe_disputes') }}
    where rdr is not true

),

attempts_enriched as (

    select
        a.*,
        efw.efw_date,
        disputes.dispute_date
    from invoice_attempts a
    left join efw on efw.payment_intent = a.provider_payment_id
    left join disputes on disputes.payment_intent = a.provider_payment_id

),

-- number the real (>$2) invoices per subscription, oldest first
invoice_sequence as (

    select
        subscription_id,
        invoice_id,
        min(order_created_at) as invoice_ts,
        max(order_amount_usd) as invoice_amount_usd,
        row_number() over (
            partition by subscription_id
            order by min(order_created_at) asc
        ) as invoice_number
    from attempts_enriched
    group by subscription_id, invoice_id
    having max(order_amount_usd) > 2
       -- rolling 126-day window on each invoice's first attempt — uncomment to enable
       -- and min(order_created_at) >= timestamp_sub(current_timestamp(), interval 126 day)

)

select
    a.subscription_id,
    a.invoice_id,
    a.order_id,
    s.invoice_number,
    s.invoice_ts,
    s.invoice_amount_usd,
    a.order_retry_attempt as retry_attempt,
    a.order_status as status,
    a.order_operation as operation,
    date(a.order_created_at) as created_at,
    a.order_created_at as created_ts,
    a.invoice_outcome,
    a.order_amount_usd,
    a.order_currency,
    a.efw_date,
    a.dispute_date,
    a.account_name,
    a.psp_order_id,
    a.provider_payment_id
from attempts_enriched a
inner join invoice_sequence s using (subscription_id, invoice_id)
