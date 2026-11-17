{{ config(
    tags=["user_subscription_mart", "gold", "daily"],
) }}

-- One row per subscription, current state only (not a daily history — see
-- int_solidgate_mrr_local_subscription_daily / int_stripe_mrr_local_subscription_daily
-- for that). mrr_local/mrr_usd reflect the most recent snapshot_date each source has
-- data for, which is not always "today" for both sources at once — see snapshot_date.
-- Acquisition fields are the customer's (see int_customer_attribution): every subscription
-- inherits the channel that first brought its customer in, renewals and later plans included.
-- Grain: source_system x subscription_id.

with identity_map as (

    select * from {{ ref('int_identity_user_map') }}

),

-- ============ SOLIDGATE ============

solidgate_latest_snapshot as (

    select * except (rn)
    from (
        select
            *,
            row_number() over (
                partition by subscription_id, _elt_account_name
                order by snapshot_date desc
            ) as rn
        from {{ ref('int_solidgate_mrr_local_subscription_daily') }}
    )
    where rn = 1

),

solidgate_products as (

    select subscription_id, _elt_account_name, product_name
    from {{ ref('int_solidgate_subscription_products') }}

),

solidgate_lifecycle as (

    select subscription_id, _elt_account_name, started_date, next_charge_date, cancelled_date
    from {{ ref('int_solidgate_subscription_lifecycle') }}

),

solidgate_final as (

    select
        im.user_key,
        'solidgate'             as source_system,
        ls._elt_account_name    as source_account,
        ls.customer_account_id  as source_customer_id,
        ls.subscription_id,
        ls.subscription_status_now as status,
        sp.product_name          as plan_name,
        ls.billing_cycle,
        ls.currency,
        ls.mrr_local,
        ls.snapshot_date,
        sl.started_date          as start_date,
        sl.next_charge_date      as current_period_end_date,
        sl.cancelled_date
    from solidgate_latest_snapshot ls
    left join solidgate_products sp
        on  ls.subscription_id   = sp.subscription_id
        and ls._elt_account_name = sp._elt_account_name
    left join solidgate_lifecycle sl
        on  ls.subscription_id   = sl.subscription_id
        and ls._elt_account_name = sl._elt_account_name
    left join identity_map im
        on  im.source_system      = 'solidgate'
        and im.source_customer_id = ls.customer_account_id
        and im.source_account     = ls._elt_account_name

),

-- ============ STRIPE ============
-- a Stripe subscription can carry more than one item/price; roll them up to one row
-- per subscription, taking the items as of that subscription's latest snapshot_date.

stripe_daily as (

    select * from {{ ref('int_stripe_mrr_local_subscription_daily') }}

),

stripe_max_snapshot as (

    select
        subscription_id,
        _elt_stripe_account_id,
        max(snapshot_date) as snapshot_date
    from stripe_daily
    group by subscription_id, _elt_stripe_account_id

),

stripe_items_named as (

    select
        d.*,
        p.product_name
    from stripe_daily d
    join stripe_max_snapshot m
        on  d.subscription_id        = m.subscription_id
        and d._elt_stripe_account_id = m._elt_stripe_account_id
        and d.snapshot_date          = m.snapshot_date
    left join {{ ref('int_stripe_products') }} p
        on  d.product_id             = p.product_id
        and d._elt_stripe_account_id = p._elt_stripe_account_id

),

stripe_rolled_up as (

    select
        subscription_id,
        _elt_stripe_account_id,
        customer,
        snapshot_date,
        any_value(currency)     as currency,
        any_value(billing_cycle) as billing_cycle,
        sum(mrr_local)           as mrr_local,
        string_agg(distinct coalesce(product_name, product_id), ', ') as plan_name
    from stripe_items_named
    group by subscription_id, _elt_stripe_account_id, customer, snapshot_date

),

stripe_subscriptions as (

    select
        id as subscription_id,
        _elt_stripe_account_id,
        status,
        start_date,
        current_period_end_date,
        date(canceled_at) as cancelled_date
    from {{ ref('int_stripe_subscriptions') }}

),

stripe_final as (

    select
        im.user_key,
        'stripe'                 as source_system,
        sr._elt_stripe_account_id as source_account,
        sr.customer               as source_customer_id,
        sr.subscription_id,
        ss.status,
        sr.plan_name,
        sr.billing_cycle,
        sr.currency,
        sr.mrr_local,
        sr.snapshot_date,
        ss.start_date,
        ss.current_period_end_date,
        ss.cancelled_date
    from stripe_rolled_up sr
    left join stripe_subscriptions ss
        on  sr.subscription_id        = ss.subscription_id
        and sr._elt_stripe_account_id = ss._elt_stripe_account_id
    left join identity_map im
        on  im.source_system      = 'stripe'
        and im.source_customer_id = sr.customer
        and im.source_account     = sr._elt_stripe_account_id

),

unioned as (

    select * from solidgate_final
    union all
    select * from stripe_final

),

attribution as (

    select * from {{ ref('int_customer_attribution') }}

),

-- ============ FX (same as-of/fallback pattern used by every *_mrr_usd_daily gold model) ============

fx as (

    select * from {{ ref('int_currency_conversion') }}

),

fx_asof as (

    select * except (rn)
    from (
        select
            d.snapshot_date,
            d.currency,
            f.rate,
            row_number() over (
                partition by d.snapshot_date, d.currency
                order by f.exchange_date desc
            ) as rn
        from (select distinct snapshot_date, currency from unioned) d
        left join fx f
            on  upper(f.currency) = upper(d.currency)
            and f.exchange_date  <= d.snapshot_date
    )
    where rn = 1

),

fx_latest as (

    select * except (rn)
    from (
        select
            upper(currency) as currency,
            rate,
            row_number() over (partition by upper(currency) order by exchange_date desc) as rn
        from fx
    )
    where rn = 1

)

select
    u.user_key,
    u.source_system,
    u.source_account,
    u.source_customer_id,
    u.subscription_id,
    u.status,
    u.plan_name,
    u.billing_cycle,
    u.currency,
    u.mrr_local,
    round(u.mrr_local * case
        when upper(u.currency) = 'USD' then 1
        else coalesce(a.rate, l.rate)
    end, 2) as mrr_usd,
    u.snapshot_date as mrr_as_of_date,
    u.start_date,
    u.current_period_end_date,
    u.cancelled_date,
    -- a customer outside the attribution population (never carried MRR) reads as unattributed
    coalesce(attr.attribution_status, 'unattributed') as attribution_status,
    coalesce(attr.source,   'unattributed')          as source,
    coalesce(attr.medium,   'unattributed')          as medium,
    coalesce(attr.campaign, 'unattributed')          as campaign,
    attr.campaign_id,
    attr.gclid
from unioned u
left join attribution attr
    on  u.source_system      = attr.source_system
    and u.source_account     = attr.source_account
    and u.source_customer_id = attr.source_customer_id
left join fx_asof a
    on  u.snapshot_date = a.snapshot_date
    and u.currency       = a.currency
left join fx_latest l
    on upper(u.currency) = l.currency
