{{ config(
    materialized='view',
    tags=["payments", "revenue", "gold", "daily"]
) }}

-- One row per money movement of a product: a collected payment (positive amount_usd) or a refund
-- (negative amount_usd), so net revenue per user or experiment variant is a plain sum.
--
-- Money and dates come from the payment ledgers, the same ones gold.net_revenue_daily reads:
--   * Stripe:    balance transactions (reporting_category charge / refund / refund_failure),
--                dated by the balance transaction's created date
--   * Solidgate: financial entries (SALE / REFUND whose provider is not Stripe), dated by
--                accounting_date, or by the entry's own transaction date when accounting_date is
--                missing (about 465k entries, ~$3.08M net, Jun 2025 - Apr 2026; net_revenue_daily
--                keeps these under a null date, so totals match and only the dating differs).
--                A Solidgate order routed to Stripe is counted once, on the Stripe side, exactly
--                as net_revenue_daily does.
-- The ledger has no customer, so the user is looked up afterwards: the Stripe charge (or the
-- charge of a refund) gives the Stripe customer, the Solidgate card order gives the Solidgate
-- customer, and int_identity_resolution maps either to user_key. A payment whose customer cannot
-- be found keeps its money and has a null user_key / app_user_id, so totals never lose revenue.
-- app_user_id (the Amplitude user_id) comes from dim_user_journey, so the table joins to
-- fct_ab_test_users on app_user_id.
--
-- Known gaps, to close before this table is treated as official:
--   * Stripe disputes and Solidgate chargebacks are not in the table (net_revenue_daily has them);
--   * fees are not in the table (this is gross payments and refunds only).
-- event_date is the ledger's date (what net_revenue_daily sums by); event_ts is the entry's own
-- time, so same-day payments order correctly and compare with an experiment exposure time.
-- Amounts are not rounded, so sums match the ledger to the cent.
-- Grain: one row per ledger entry (ref_id).

with accounts as (

    select a.product, a.source_system, a.account
    from {{ ref('seed_product_payment_accounts') }} a
    join {{ ref('seed_product_registry') }} r on r.product = a.product
    where r.is_in_scope

),

-- ---------- Stripe ----------

stripe_ledger as (

    select
        rg.product,
        bt.id                                                    as ref_id,
        bt.source                                                as source_id,
        bt._elt_stripe_account_id                                as account_id,
        bt.reporting_category,
        bt.created_date                                          as event_date,
        coalesce(raw.created_ts, timestamp(bt.created_date))     as event_ts,
        bt.amount_usd / {{ minor_unit_divisor('bt.currency') }}  as amount_usd
    from {{ ref('int_stripe_balance_transactions') }} bt
    -- the silver model keeps only the date; the exact time orders same-day payments and lets a
    -- payment be compared with an experiment exposure time
    left join {{ source('bronze_stripe', 'stripe_balance_transactions') }} raw
        on  raw.id                    = bt.id
        and raw._elt_stripe_account_id = bt._elt_stripe_account_id
    join accounts rg
        on  rg.source_system = 'stripe'
        and rg.account       = bt._elt_stripe_account_id
    where bt.reporting_category in ('charge', 'refund', 'refund_failure')

),

stripe_charge_customer as (

    select id as charge_id, _elt_stripe_account_id as account_id, customer
    from {{ ref('int_stripe_charges') }}
    where customer is not null

),

stripe_refund_charge as (

    select id as refund_id, _elt_stripe_account_id as account_id, charge as charge_id
    from {{ ref('int_stripe_refunds') }}
    where charge is not null

),

stripe_payments as (

    select
        l.product,
        l.ref_id,
        l.source_id,
        'stripe'                                                 as event_source,
        if(l.reporting_category = 'charge', 'payment', 'refund') as event_name,
        l.event_date,
        l.event_ts,
        l.amount_usd,
        concat(l.account_id, ':', coalesce(cc.customer, rc_cc.customer)) as customer_key
    from stripe_ledger l
    left join stripe_charge_customer cc
        on  l.reporting_category = 'charge'
        and cc.charge_id   = l.source_id
        and cc.account_id  = l.account_id
    left join stripe_refund_charge rc
        on  l.reporting_category != 'charge'
        and rc.refund_id   = l.source_id
        and rc.account_id  = l.account_id
    left join stripe_charge_customer rc_cc
        on  rc_cc.charge_id  = rc.charge_id
        and rc_cc.account_id = l.account_id

),

-- ---------- Solidgate ----------

solidgate_payments as (

    select
        rg.product,
        fe.id                                                    as ref_id,
        fe.order_id                                              as source_id,
        'solidgate'                                              as event_source,
        if(fe.record_type_key = 'SALE', 'payment', 'refund')     as event_name,
        coalesce(fe.accounting_date, date(fe.transaction_datetime_utc)) as event_date,
        coalesce(fe.transaction_datetime_utc, timestamp(fe.accounting_date)) as event_ts,
        fe.payout_amount_in_major_units                          as amount_usd,
        concat(fe._elt_account_name, ':', o.customer_account_id) as customer_key
    from {{ source('bronze_solidgate', 'solidgate_financial_entries') }} fe
    join accounts rg
        on  rg.source_system = 'solidgate'
        and rg.account       = fe._elt_account_name
    left join {{ ref('int_solidgate_card_orders') }} o
        on  o.order_id = fe.order_id
    where fe.record_type_key in ('SALE', 'REFUND')
      and upper(coalesce(fe.provider, '')) != 'STRIPE'

),

unioned as (

    select * from stripe_payments
    union all
    select * from solidgate_payments

),

resolved as (

    select
        u.product,
        r.user_key,
        u.event_source,
        u.event_name,
        u.event_ts,
        u.event_date,
        u.ref_id,
        u.source_id,
        u.amount_usd
    from unioned u
    left join {{ ref('int_identity_resolution') }} r
        on  r.product  = u.product
        and r.id_type  = concat(u.event_source, '_customer')
        and r.id_value = u.customer_key
    -- a null amount is kept (not dropped by the comparison) so the not_null test catches it
    where coalesce(u.amount_usd, 0) != 0 or u.amount_usd is null

),

staged as (

    select
        *,
        case
            when event_name = 'refund' then 'refund'
            when user_key is null then 'unlinked'
            when row_number() over (
                partition by product, user_key, (event_name = 'payment' and amount_usd > 0)
                order by event_ts, ref_id
            ) = 1 and amount_usd > 0 then 'customer'
            else 'renewal'
        end as stage
    from resolved

)

select
    s.product,
    s.user_key,
    u.app_user_id,
    s.event_source,
    s.event_name,
    s.stage,
    s.event_ts,
    s.event_date,
    s.ref_id,
    s.source_id,
    s.amount_usd,
    s.stage = 'customer' as is_first_payment
from staged s
left join {{ ref('dim_user_journey') }} u
    on  u.product  = s.product
    and u.user_key = s.user_key
