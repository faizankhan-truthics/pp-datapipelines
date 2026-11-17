{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='append_new_columns',
    tags=["stripe", "silver", "daily"],
    partition_by={
        "field": "stripe_date",
        "data_type": "date"
    }
) }}

-- Stripe charges across every account, payment side of the generalized GA4 stitch. GA4's
-- ecommerce.transaction_id is the Stripe PaymentIntent id on every property confirmed to be
-- Stripe-backed (see seeds/seed_ga4_payment_account_map.csv)
-- Grain: stripe_date x transaction_id
--
-- subscription_id/invoice_id are left-joined in from int_stripe_invoices on payment_intent,
-- which the invoices source only carries for ~30-70% of invoices (older/`send_invoice` invoices
-- settle via the legacy `charge` field instead) and which most PaymentIntents don't have an
-- invoice for at all -- most charges on these accounts are one-time purchases, not subscription
-- billing. Both columns are null whenever this PaymentIntent isn't tied to any invoice; that's
-- the normal case, not missing data.

with charges as (

    select
        created_date            as stripe_date,
        payment_intent           as transaction_id,
        _elt_stripe_account_id,
        sum(case when status = 'succeeded' then amount_usd else 0 end) as amount_usd
    from {{ ref('int_stripe_charges') }}
    where payment_intent is not null
    {% if is_incremental() %}
    and created_date >= date_sub(
        current_date(), interval {{ var('ga4_lookback_days', 3) }} day
    )
    {% endif %}
    group by stripe_date, transaction_id, _elt_stripe_account_id

),

invoices as (

    -- payment_intent is unique across invoices project-wide when populated (confirmed empirically
    -- -- no account scoping needed, same as the PaymentIntent id itself), but qualify defensively
    -- rather than trust that holding forever
    select
        payment_intent,
        subscription as subscription_id,
        id           as invoice_id
    from {{ ref('int_stripe_invoices') }}
    where payment_intent is not null
    qualify row_number() over (
        partition by payment_intent
        order by created_date desc
    ) = 1

)

select
    c.stripe_date,
    c.transaction_id,
    c._elt_stripe_account_id,
    c.amount_usd,
    i.subscription_id,
    i.invoice_id
from charges c
left join invoices i
    on c.transaction_id = i.payment_intent
