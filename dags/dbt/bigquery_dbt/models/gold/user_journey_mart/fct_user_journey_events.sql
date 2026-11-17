-- depends_on: {{ ref('seed_product_registry') }}
{{ config(
    materialized='table',
    tags=["journey", "user_journey_mart", "gold", "daily"],
    partition_by={
        "field": "event_date",
        "data_type": "date"
    },
    cluster_by=['product', 'user_key']
) }}

-- Ordered timeline per journey-mart user: the touches and stage events that trace a user from
-- first visit to their current state (docs/plans/user_journey_mart.md, section 8.5). Not every
-- event -- only what moves the journey:
--   amplitude  first event (visitor), first signup / activation / trial / checkout (registry
--              rules), every purchase* and subscription_* event
--   ga4        session starts, signups and checkouts of GA4 visitors linked to the user
--   anytrack   the first touch of each AnyTrack visitor linked to the user (main_attribution,
--              stage `touch`), and its first signup / trial event (registry rules
--              `anytrack:<event_name>`; products without Amplitude)
--   solidgate  successful orders ($0 = free trial), refunds, attempts (declined / voided /
--              authorised but unsettled orders: stage `payment_attempt`)
--   stripe     succeeded collected charges, refunds of collected charges, attempts (failed or
--              never-collected charges: stage `payment_attempt`)
--   subscriptions  start and cancellation (user_subscription_mart.fct_subscription)
-- Payments are the source of truth for money: amount_usd is in major units (Solidgate and Stripe
-- store minor units). The first paid payment of a user is stage `customer`, later ones `renewal`.
-- Solidgate routes some orders to Stripe as the processor; Stripe then holds the same payment
-- (order provider_payment_id = charge payment_intent), so the Stripe row carries the money, as in
-- net_revenue_daily, and the Solidgate order stays as `order_routed_to_stripe` (stage
-- `processor_order`, no amount). Card setups ($0 `seti_` orders that only save the card) are not
-- payment attempts: `card_saved`, stage `processor_order`.
-- Solidgate event_ts is the order's created_at; rows loaded before that column existed fall back
-- to midnight of created_date until int_solidgate_card_orders is fully refreshed. A Solidgate
-- refund row carries the full order amount (partial refunds included); exact refunded revenue is
-- in int_customer_revenue / ltv_usd.
-- Payments are read through seed_product_payment_accounts (a product may have several accounts).
-- Grain: product x user_key x event_source x event_name x event_ts x ref_id.

{% set products = registry_products() %}

with resolution as (

    select * from {{ ref('int_identity_resolution') }}

),

registry as (

    select * from {{ ref('seed_product_registry') }}
    where is_in_scope

),

accounts as (

    select a.product, a.source_system, a.account
    from {{ ref('seed_product_payment_accounts') }} a
    join registry r on r.product = a.product

),

-- ---------- amplitude ----------

amp as (

    select
        e.*,
        -- an app user id resolves to its own user, or to the web account a backend-only id is an
        -- alias of (int_identity_resolution)
        case
            when e.app_user_id is not null
                then coalesce(ra.user_key, to_hex(md5(concat(e.product, '|app_user|', e.app_user_id))))
            else r.user_key
        end as user_key,
        case
        {%- for p in products %}
            when e.product = '{{ p.product }}' and {{ registry_event_filter(p.signup_events, 'amplitude') }} then 'signup'
            when e.product = '{{ p.product }}' and {{ registry_event_filter(p.activation_rule, 'amplitude') }} then 'activation'
            when e.product = '{{ p.product }}' and {{ registry_event_filter(p.trial_events, 'amplitude') }} then 'trial'
        {%- endfor %}
            when e.event_type = 'checkout_presented' then 'checkout'
            when e.event_type in ('purchase_started', 'purchase', 'purchase_failed') then 'purchase'
            when e.event_type in ('subscription_converted', 'subscription_renewed', 'subscription_upgrade',
                                  'subscription_reenabled') then 'subscription'
            when e.event_type in ('subscription_cancel', 'trial_canceled') then 'cancel'
        end as stage
    from {{ ref('int_amplitude_events') }} e
    left join resolution r
        on  r.product  = e.product
        and r.id_type  = 'amplitude_id'
        and r.id_value = e.amplitude_id
    left join resolution ra
        on  ra.product  = e.product
        and ra.id_type  = 'app_user_id'
        and ra.id_value = e.app_user_id

),

amp_first_seen as (

    select *
    from amp
    where user_key is not null and not is_server_event
    qualify row_number() over (partition by product, user_key order by event_ts, event_uuid) = 1

),

amp_stage_firsts as (

    select *
    from amp
    where user_key is not null and stage in ('signup', 'activation', 'trial', 'checkout')
    qualify row_number() over (partition by product, user_key, stage order by event_ts, event_uuid) = 1

),

amp_lifecycle as (

    select *
    from amp
    where user_key is not null and stage in ('purchase', 'subscription', 'cancel')

),

amp_rows as (

    select product, user_key, event_ts, 'amplitude' as event_source, 'first_seen' as event_name,
           'visitor' as stage, cast(null as float64) as amount_usd,
           initial_utm_source as source, initial_utm_medium as medium, initial_utm_campaign as campaign,
           coalesce(initial_gclid, initial_gbraid, initial_wbraid, initial_msclkid, initial_fbclid, initial_ttclid) as click_id,
           event_uuid as ref_id
    from amp_first_seen

    union all

    select product, user_key, event_ts, 'amplitude', event_type, stage, null,
           null, null, null, null, coalesce(order_id, event_uuid)
    from amp_stage_firsts

    union all

    select product, user_key, event_ts, 'amplitude', event_type, stage, null,
           null, null, null, null, coalesce(order_id, event_uuid)
    from amp_lifecycle

),

-- ---------- ga4 (linked visitors only) ----------

ga4_rows as (

    select
        g.product,
        r.user_key,
        g.event_ts,
        'ga4'                                                   as event_source,
        g.event_name,
        case g.journey_event_kind
            when 'session_start' then 'touch'
            when 'signup'        then 'signup'
            when 'checkout'      then 'checkout'
        end                                                     as stage,
        cast(null as float64)                                   as amount_usd,
        g.utm_source                                            as source,
        g.utm_medium                                            as medium,
        g.utm_campaign                                          as campaign,
        coalesce(g.gclid, g.gbraid, g.wbraid, g.msclkid, g.fbclid, g.ttclid) as click_id,
        coalesce(g.transaction_id,
                 concat(g.user_pseudo_id, ':', cast(g.ga_session_id as string), ':',
                        cast(unix_micros(g.event_ts) as string))) as ref_id
    from {{ ref('int_ga4_journey_events') }} g
    join resolution r
        on  r.product  = g.product
        and r.id_type  = 'ga4_pseudo_id'
        and r.id_value = g.user_pseudo_id

),

-- ---------- anytrack ----------

anytrack as (

    select
        e.*,
        r.user_key,
        -- the AnyTrack visitor (QUALIFY below cannot use click_id: the select list reuses the name)
        coalesce(e.click_id, e.anytrack_event_key)                  as visitor_ref,
        case
        {%- for p in products %}
            when e.product = '{{ p.product }}' and {{ registry_event_filter(p.signup_events, 'anytrack', 'e.event_name') }} then 'signup'
            when e.product = '{{ p.product }}' and {{ registry_event_filter(p.trial_events, 'anytrack', 'e.event_name') }} then 'trial'
        {%- endfor %}
        end as stage
    from {{ ref('int_anytrack_events') }} e
    join resolution r
        on  r.product  = e.product
        and r.id_type  = 'email'
        and r.id_value = e.email_user_key

),

anytrack_rows as (

    -- the visitor's first touch, once per AnyTrack visitor (every event repeats it)
    select product, user_key, touch_ts as event_ts, 'anytrack' as event_source, 'first_touch' as event_name,
           'touch' as stage, cast(null as float64) as amount_usd,
           touch_source as source, touch_medium as medium, touch_campaign as campaign,
           coalesce(gclid, gbraid, wbraid, msclkid, fbclid, ttclid) as click_id,
           visitor_ref as ref_id
    from anytrack
    where touch_ts is not null
    qualify row_number() over (
        partition by product, user_key, visitor_ref order by touch_ts, event_ts, anytrack_event_key
    ) = 1

    union all

    select product, user_key, event_ts, 'anytrack', event_name, stage, cast(null as float64),
           cast(null as string), cast(null as string), cast(null as string), cast(null as string),
           anytrack_event_key
    from anytrack
    where stage is not null
    qualify row_number() over (partition by product, user_key, stage order by event_ts, anytrack_event_key) = 1

),

-- ---------- payments ----------

-- Stripe charges that stand for a Solidgate order routed to Stripe, keyed by the id Solidgate
-- stores in provider_payment_id (a payment_intent; a charge id is matched too). Only charges the
-- stripe CTEs below turn into rows count: in-scope account, with a customer, succeeded or failed.
stripe_routed as (

    select
        pid                                                         as provider_payment_id,
        logical_or(c.status = 'succeeded' and c.captured)           as is_collected
    from {{ ref('int_stripe_charges') }} c
    join accounts rg on rg.source_system = 'stripe' and rg.account = c._elt_stripe_account_id
    cross join unnest([c.payment_intent, c.id]) as pid
    where c.customer is not null and pid is not null
      and c.status in ('succeeded', 'failed')
    group by 1

),

solidgate_payments as (

    select
        rg.product,
        r.user_key,
        o.order_id,
        o.status,
        coalesce(o.created_at, timestamp(o.created_date))           as event_ts,
        o.refunded_at,
        o.order_amount_usd                                          as amount_usd
    from {{ ref('int_solidgate_card_orders') }} o
    join accounts rg on rg.source_system = 'solidgate' and rg.account = o._elt_account_name
    join resolution r
        on  r.product  = rg.product
        and r.id_type  = 'solidgate_customer'
        and r.id_value = concat(o._elt_account_name, ':', o.customer_account_id)
    left join stripe_routed sr on sr.provider_payment_id = o.provider_payment_id
    where o.status in ('approved', 'settle_ok', 'partial_settled', 'refunded')
      -- collected on Stripe: counted (and refunded) from the Stripe charge instead
      and not coalesce(sr.is_collected, false)

),

stripe_payments as (

    select
        rg.product,
        r.user_key,
        c.id                                                        as charge_id,
        c.created_ts                                                as event_ts,
        c.amount_captured_usd as amount_usd
    from {{ ref('int_stripe_charges') }} c
    join accounts rg on rg.source_system = 'stripe' and rg.account = c._elt_stripe_account_id
    join resolution r
        on  r.product  = rg.product
        and r.id_type  = 'stripe_customer'
        and r.id_value = concat(c._elt_stripe_account_id, ':', c.customer)
    where c.status = 'succeeded' and c.customer is not null
      and c.captured

),

stripe_refunds as (

    select
        rg.product,
        r.user_key,
        f.id                                                        as refund_id,
        f.created_ts                                                as event_ts,
        - f.amount_usd                                              as amount_usd
    from {{ ref('int_stripe_refunds') }} f
    join {{ ref('int_stripe_charges') }} c on c.id = f.charge
    join accounts rg on rg.source_system = 'stripe' and rg.account = f._elt_stripe_account_id
    join resolution r
        on  r.product  = rg.product
        and r.id_type  = 'stripe_customer'
        and r.id_value = concat(c._elt_stripe_account_id, ':', c.customer)
    -- a refund on a charge that was never collected only releases the authorisation
    where f.status = 'succeeded' and c.customer is not null
      and c.captured

),

-- attempts the processor saw but that collected no money: declined, authorised but never
-- settled / captured, voided, or still in flight. No amount: it is not revenue.
payment_attempts as (

    select
        rg.product,
        r.user_key,
        coalesce(o.created_at, timestamp(o.created_date))           as event_ts,
        'solidgate'                                                 as event_source,
        case o.status
            when 'auth_failed' then 'payment_failed'
            when 'void_ok'     then 'payment_voided'
            when 'auth_ok'     then 'payment_authorised'
            else 'payment_pending'
        end                                                         as event_name,
        o.order_id                                                  as ref_id
    from {{ ref('int_solidgate_card_orders') }} o
    join accounts rg on rg.source_system = 'solidgate' and rg.account = o._elt_account_name
    join resolution r
        on  r.product  = rg.product
        and r.id_type  = 'solidgate_customer'
        and r.id_value = concat(o._elt_account_name, ':', o.customer_account_id)
    left join stripe_routed sr on sr.provider_payment_id = o.provider_payment_id
    where o.status in ('auth_failed', 'void_ok', 'auth_ok', '3ds_verify', 'processing')
      -- routed to Stripe: the Stripe charge already gives the attempt (or the payment)
      and sr.provider_payment_id is null
      -- card setup, not a payment
      and not starts_with(coalesce(o.provider_payment_id, ''), 'seti_')

    union all

    select
        rg.product,
        r.user_key,
        c.created_ts,
        'stripe',
        if(c.status = 'failed', 'payment_failed', 'payment_voided'),
        c.id
    from {{ ref('int_stripe_charges') }} c
    join accounts rg on rg.source_system = 'stripe' and rg.account = c._elt_stripe_account_id
    join resolution r
        on  r.product  = rg.product
        and r.id_type  = 'stripe_customer'
        and r.id_value = concat(c._elt_stripe_account_id, ':', c.customer)
    where c.customer is not null
      and (c.status = 'failed'
           or (c.status = 'succeeded' and not c.captured))

),

-- Solidgate orders left out of payments / attempts above, kept on the timeline without money:
-- orders Solidgate routed to Stripe (the Stripe row carries the payment or attempt) and card
-- setups. Stage `processor_order` moves no journey stage.
solidgate_processor_orders as (

    select
        rg.product,
        r.user_key,
        coalesce(o.created_at, timestamp(o.created_date))           as event_ts,
        if(starts_with(coalesce(o.provider_payment_id, ''), 'seti_'),
           'card_saved', 'order_routed_to_stripe')                  as event_name,
        o.order_id                                                  as ref_id
    from {{ ref('int_solidgate_card_orders') }} o
    join accounts rg on rg.source_system = 'solidgate' and rg.account = o._elt_account_name
    join resolution r
        on  r.product  = rg.product
        and r.id_type  = 'solidgate_customer'
        and r.id_value = concat(o._elt_account_name, ':', o.customer_account_id)
    left join stripe_routed sr on sr.provider_payment_id = o.provider_payment_id
    where (o.status in ('approved', 'settle_ok', 'partial_settled', 'refunded')
           and coalesce(sr.is_collected, false))
       or (o.status in ('auth_failed', 'void_ok', 'auth_ok', '3ds_verify', 'processing')
           and (sr.provider_payment_id is not null
                or starts_with(coalesce(o.provider_payment_id, ''), 'seti_')))

),

payments as (

    select product, user_key, event_ts, 'solidgate' as event_source,
           if(amount_usd > 0, 'payment', 'free_trial_order') as event_name,
           amount_usd, order_id as ref_id
    from solidgate_payments

    union all

    select product, user_key, event_ts, 'stripe', 'payment', amount_usd, charge_id
    from stripe_payments

),

payment_rows as (

    select
        product, user_key, event_ts, event_source, event_name,
        case
            when coalesce(amount_usd, 0) <= 0 then 'trial'
            when row_number() over (
                partition by product, user_key, (amount_usd > 0)
                order by event_ts, ref_id
            ) = 1 then 'customer'
            else 'renewal'
        end                                                         as stage,
        amount_usd,
        cast(null as string) as source, cast(null as string) as medium,
        cast(null as string) as campaign, cast(null as string) as click_id,
        ref_id
    from payments

    union all

    select product, user_key, refunded_at, 'solidgate', 'refund', 'refund', - amount_usd,
           null, null, null, null, order_id
    from solidgate_payments
    where status = 'refunded' and refunded_at is not null

    union all

    select product, user_key, event_ts, 'stripe', 'refund', 'refund', amount_usd,
           null, null, null, null, refund_id
    from stripe_refunds

    union all

    select product, user_key, event_ts, event_source, event_name, 'payment_attempt', null,
           null, null, null, null, ref_id
    from payment_attempts

    union all

    select product, user_key, event_ts, 'solidgate', event_name, 'processor_order', null,
           null, null, null, null, ref_id
    from solidgate_processor_orders

),

-- ---------- subscriptions ----------

subscription_rows as (

    select
        r.product,
        r.user_key,
        timestamp(s.start_date)                                 as event_ts,
        s.source_system                                         as event_source,
        'subscription_started'                                  as event_name,
        'subscription'                                          as stage,
        cast(null as float64)                                   as amount_usd,
        cast(null as string) as source, cast(null as string) as medium,
        cast(null as string) as campaign, cast(null as string) as click_id,
        s.subscription_id                                       as ref_id
    from {{ ref('fct_subscription') }} s
    join resolution r
        on  r.id_type  = concat(s.source_system, '_customer')
        and r.id_value = concat(s.source_account, ':', s.source_customer_id)
    where s.start_date is not null

    union all

    select
        r.product, r.user_key, timestamp(s.cancelled_date), s.source_system,
        'subscription_cancelled', 'churn', null, null, null, null, null, s.subscription_id
    from {{ ref('fct_subscription') }} s
    join resolution r
        on  r.id_type  = concat(s.source_system, '_customer')
        and r.id_value = concat(s.source_account, ':', s.source_customer_id)
    -- active subscriptions carry their scheduled end as cancelled_date; churn only once it has passed
    where s.cancelled_date is not null
      and s.cancelled_date <= current_date()

),

unioned as (

    select * from amp_rows
    union all
    select * from ga4_rows
    union all
    select * from anytrack_rows
    union all
    select * from payment_rows
    union all
    select * from subscription_rows

)

select distinct
    product,
    user_key,
    event_ts,
    date(event_ts) as event_date,
    event_source,
    event_name,
    stage,
    round(amount_usd, 2) as amount_usd,
    source,
    medium,
    campaign,
    click_id,
    ref_id
from unioned
where user_key is not null
  and event_ts is not null
