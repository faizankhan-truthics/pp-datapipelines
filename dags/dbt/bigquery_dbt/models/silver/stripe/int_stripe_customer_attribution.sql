{{ config(
    materialized='table',
    tags=["stripe", "silver", "daily"],
) }}

-- Where each Stripe customer came from, taken from the earliest charge of theirs that GA4 saw.
-- Mirrors int_solidgate_customer_attribution.sql: recurring/server-side renewal charges carry
-- no GA4 event of their own, so attributing charge-by-charge would leave most revenue
-- unattributed; the customer is attributed once and every later charge inherits it via Stripe's
-- customer id. Not restricted to revenue-bearing charges -- a customer who started on a trial
-- was still acquired by whatever brought them in.
-- Grain: one row per customer (per account).
--
-- KNOWN GAP (2026-09-02, confirmed live): OQG (online-qr-generator.com, acct_1PgbrSJaoNXdYiB1)
-- and QCI (qr-code.io, acct_1Pgc8ILQzI0D4HQZ) will show almost no attribution here even once
-- this runs. int_stripe_prices only has 5 rows for each of those two accounts (vs 1,008 for
-- TRC), all from a single ingestion batch -- so nearly every one of their invoice lines fails
-- int_stripe_mrr_eligible_invoice_lines' billing_interval join and never reaches
-- int_stripe_mrr_customer_daily for this model to attribute against. Root cause traced to
-- stripe_hook.py's get_raw_pages(), which filters every endpoint (prices included) by
-- created[gte]/created[lt] on the DAG's daily run window -- correct for continuously-created
-- objects like charges/invoices, but prices are created once and reused indefinitely, so a
-- price created before an account's first backfill is never captured by daily runs alone. TRC
-- got a real backfill (1,008 rows in one batch); OQG and QCI did not. Fix is a
-- run_type=backfill trigger of stripe_financial_pipeline for those two accounts, not a change
-- to this model.

with charges as (

    select
        customer,
        _elt_stripe_account_id,
        payment_intent as transaction_id,
        created_date    as stripe_date
    from {{ ref('int_stripe_charges') }}
    where customer is not null
      and payment_intent is not null

),

-- charges carry no subscription id of their own; the invoice that raised the payment_intent does.
-- one row per payment_intent so the join below cannot fan out.
invoice_subscriptions as (

    select
        payment_intent,
        _elt_stripe_account_id,
        subscription
    from {{ ref('int_stripe_invoices') }}
    where payment_intent is not null
      and subscription is not null
    qualify row_number() over (
        partition by payment_intent, _elt_stripe_account_id
        order by created_date
    ) = 1

),

matched as (

    select
        c.customer,
        c._elt_stripe_account_id,
        c.stripe_date,
        i.subscription as subscription_id,
        g.source,
        g.medium,
        g.campaign,
        g.campaign_id,
        g.gclid
    from charges c
    -- payment_intent ids are globally unique across Stripe (unlike Solidgate's order_id, which
    -- is only unique within an account's own namespace) -- no account scoping needed in the join
    join {{ ref('int_ga4_transactions') }} g
        on c.transaction_id = g.transaction_id
    left join invoice_subscriptions i
        on  c.transaction_id         = i.payment_intent
        and c._elt_stripe_account_id = i._elt_stripe_account_id
    qualify row_number() over (
        partition by c.customer, c._elt_stripe_account_id
        order by c.stripe_date
    ) = 1

),

customers as (

    select distinct customer, _elt_stripe_account_id
    from {{ ref('int_stripe_mrr_customer_daily') }}
    where customer is not null

)

select
    c.customer,
    c._elt_stripe_account_id,
    -- customers we cannot place still belong here, or channel revenue stops adding up
    -- to total revenue
    coalesce(m.source,   'unattributed')  as source,
    coalesce(m.medium,   'unattributed')  as medium,
    coalesce(m.campaign, 'unattributed')  as campaign,
    m.campaign_id,
    m.gclid,
    m.subscription_id                                as attributed_from_subscription_id,
    m.stripe_date                                    as attributed_from_date,
    if(m.customer is null, 'unattributed', 'attributed') as attribution_status
from customers c
left join matched m
    on  c.customer               = m.customer
    and c._elt_stripe_account_id = m._elt_stripe_account_id
