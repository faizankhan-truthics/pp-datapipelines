{{ config(
    materialized='table',
    tags=["journey", "identity", "silver", "daily"],
    cluster_by=['product', 'id_type_a']
) }}

-- Every observed link between two identifiers of the same product: the edges of the stitch map in
-- docs/plans/user_journey_mart.md (section 3). int_identity_resolution turns them into one
-- user_key per identifier. Measured rates per edge: docs/plans/user_journey_stitch_checks.sql.
--
--   E1  amplitude_id      - app_user_id         same Amplitude event
--   E2  device_id         - app_user_id         same Amplitude event (server events excluded)
--   E3  solidgate_customer- app_user_id         Amplitude order_id = Solidgate order_id
--   E3c solidgate_customer- app_user_id         the Solidgate customer_account_id IS an app user id of
--                                               the product (the apps create the Solidgate customer
--                                               under the account id: iqcenter, opengen_ai, ...)
--   E5  solidgate/stripe_customer - email       int_identity_user_map
--   E5o solidgate_customer- email               email on any Solidgate order (declined ones too)
--   E6  email             - app_user_id         email on Amplitude purchase events
--   E7+ ga4_pseudo_id     - solidgate_customer  GA4 checkout transaction_id = Solidgate order_id
--   E7s ga4_pseudo_id     - stripe_customer     GA4 checkout transaction_id = Stripe payment_intent
--                                               (products that charge on Stripe directly: oqg, qci,
--                                               location_tool)
--   E7r ga4_pseudo_id     - stripe_customer     GA4 checkout transaction_id = a Solidgate order routed to
--                                               Stripe (order provider_payment_id = charge payment_intent):
--                                               the order may have no Solidgate customer (opengen_ai)
--   E9  stripe_customer   - app_user_id         Stripe charge metadata mc_customer_id
--   E12 stripe_customer   - solidgate_customer  Stripe metadata mc_first/second_order_id = Solidgate order_id
--   E12p stripe_customer  - solidgate_customer  Solidgate order routed to Stripe: order
--                                               provider_payment_id = Stripe charge payment_intent
--   E14 ga4_pseudo_id     - amplitude_id        same ad click id (gclid), unique on both sides
--   E15 ga4_pseudo_id     - site_user_id        GA4 user_id (set on signup / checkout by some products)
--   E9u stripe_customer   - site_user_id        Stripe charge metadata mc_user_id / mc_userId
--   E16 site_user_id      - app_user_id         Amplitude event_properties.user_id next to user_id
--   E17 anytrack_click_id - email               email on the AnyTrack visitor's signup / purchase
--                                               events (int_anytrack_events)
--   E18 email             - app_user_id         AnyTrack signup event carrying the app account id
--                                               (transaction_id `signup:<id>`, opengen_ai) next to the
--                                               email; only ids Amplitude knows as app users
--
-- site_user_id is a product's web account id. On publicregistry it IS the Amplitude user_id (no
-- E16 needed: int_identity_resolution matches it to the app user directly); on reversly it is a
-- separate short id that Amplitude purchase events carry (E16). On InfoChecker mc_user_id is an
-- AnyTrack click id, which matches no app user, so E9u stays inert there.
--
-- Id formats: customers are '<account>:<customer id>' (customer ids are only unique per account);
-- emails are their md5 (the user_key of int_identity_user_map), never the address.
-- Payments are read through seed_product_payment_accounts (a product may have several accounts).
-- Grain: product x id_type_a x id_a x id_type_b x id_b x evidence.

with registry as (

    select * from {{ ref('seed_product_registry') }}
    where is_in_scope

),

accounts as (

    select a.product, a.source_system, a.account
    from {{ ref('seed_product_payment_accounts') }} a
    join registry r on r.product = a.product

),

amp as (

    select * from {{ ref('int_amplitude_events') }}

),

solidgate_orders as (

    select
        r.product,
        o.order_id,
        concat(o._elt_account_name, ':', o.customer_account_id) as solidgate_customer,
        o.customer_email_key,
        o.provider_payment_id,
        timestamp(o.created_date)                               as created_ts
    from {{ ref('int_solidgate_card_orders') }} o
    join accounts r on r.source_system = 'solidgate' and r.account = o._elt_account_name
    where o.customer_account_id is not null
      and nullif(o.order_id, '') is not null

),

stripe_charges as (

    select
        r.product,
        concat(c._elt_stripe_account_id, ':', c.customer)                       as stripe_customer,
        c.created_ts,
        c.payment_intent,
        lower(nullif(trim(json_value(c.metadata, '$.mc_customer_id')), ''))      as mc_customer_id,
        coalesce(nullif(trim(json_value(c.metadata, '$.mc_user_id')), ''),
                 nullif(trim(json_value(c.metadata, '$.mc_userId')), ''))        as mc_user_id,
        nullif(trim(json_value(c.metadata, '$.mc_first_order_id')), '')         as mc_first_order_id,
        nullif(trim(json_value(c.metadata, '$.mc_second_order_id')), '')        as mc_second_order_id
    from {{ ref('int_stripe_charges') }} c
    join accounts r on r.source_system = 'stripe' and r.account = c._elt_stripe_account_id
    where c.customer is not null

),

amp_app_users as (

    select distinct product, app_user_id
    from amp
    where app_user_id is not null

),

-- E12: one row per Solidgate order a Stripe charge points back to (equi-join below)
stripe_order_refs as (

    select distinct product, stripe_customer, created_ts, order_ref
    from stripe_charges, unnest([mc_first_order_id, mc_second_order_id]) as order_ref
    where order_ref is not null

),

payer_emails as (

    select
        r.product,
        concat(m.source_system, '_customer')                    as id_type,
        concat(m.source_account, ':', m.source_customer_id)     as customer_id,
        m.user_key                                              as email_user_key,
        m._elt_update_timestamp                                 as seen_ts
    from {{ ref('int_identity_user_map') }} m
    join accounts r
        on r.source_system = m.source_system and r.account = m.source_account
    where m.user_key is not null

),

-- E14: a click id is only used when it belongs to exactly one visitor on each side
amp_clicks as (

    select product, initial_gclid as gclid, amplitude_id, min(event_ts) as seen_ts
    from amp
    where initial_gclid is not null and not is_server_event
    group by 1, 2, 3

),

ga4_clicks as (

    select t.product, g as gclid, t.user_pseudo_id, t.first_seen_ts as seen_ts
    from {{ ref('int_ga4_user_touchpoints') }} t, unnest(t.gclids) as g

),

unique_amp_clicks as (

    -- HAVING must use the qualified column: bare amplitude_id resolves to the any_value() alias
    select c.product, c.gclid, any_value(c.amplitude_id) as amplitude_id, min(c.seen_ts) as seen_ts
    from amp_clicks c
    group by 1, 2
    having count(distinct c.amplitude_id) = 1

),

unique_ga4_clicks as (

    select c.product, c.gclid, any_value(c.user_pseudo_id) as user_pseudo_id, min(c.seen_ts) as seen_ts
    from ga4_clicks c
    group by 1, 2
    having count(distinct c.user_pseudo_id) = 1

),

edges as (

    -- E1
    select product, 'amplitude_id' as id_type_a, amplitude_id as id_a,
           'app_user_id' as id_type_b, app_user_id as id_b, 'E1_amplitude_event' as evidence,
           event_ts as seen_ts
    from amp
    where app_user_id is not null and amplitude_id is not null and not is_server_event

    union all

    -- E2
    select product, 'device_id', device_id, 'app_user_id', app_user_id, 'E2_amplitude_event', event_ts
    from amp
    where app_user_id is not null and device_id is not null

    union all

    -- E3
    select a.product, 'solidgate_customer', o.solidgate_customer, 'app_user_id', a.app_user_id,
           'E3_order_id', a.event_ts
    from amp a
    join solidgate_orders o on o.product = a.product and o.order_id = a.order_id
    where a.app_user_id is not null

    union all

    -- E3c
    select o.product, 'solidgate_customer', o.solidgate_customer, 'app_user_id', a.app_user_id,
           'E3_customer_account_id', o.created_ts
    from solidgate_orders o
    join amp_app_users a
        on  a.product     = o.product
        and a.app_user_id = split(o.solidgate_customer, ':')[safe_offset(1)]

    union all

    -- E5
    select product, id_type, customer_id, 'email', email_user_key, 'E5_payer_email', seen_ts
    from payer_emails

    union all

    -- E5o: int_identity_user_map only has subscription customers' emails, so a customer whose
    -- orders were all declined (or one-off) has an email only on the order
    select product, 'solidgate_customer', solidgate_customer, 'email', customer_email_key,
           'E5_order_email', created_ts
    from solidgate_orders

    union all

    -- E6
    select product, 'email', email_user_key, 'app_user_id', app_user_id, 'E6_amplitude_email', event_ts
    from amp
    where email_user_key is not null and app_user_id is not null

    union all

    -- E7+
    select g.product, 'ga4_pseudo_id', g.user_pseudo_id, 'solidgate_customer', o.solidgate_customer,
           'E7_checkout_order_id', g.event_ts
    from {{ ref('int_ga4_journey_events') }} g
    join solidgate_orders o on o.product = g.product and o.order_id = g.transaction_id
    where g.journey_event_kind = 'checkout'

    union all

    -- E7s (payment_intent ids are globally unique)
    select g.product, 'ga4_pseudo_id', g.user_pseudo_id, 'stripe_customer', c.stripe_customer,
           'E7_checkout_payment_intent', g.event_ts
    from {{ ref('int_ga4_journey_events') }} g
    join stripe_charges c on c.product = g.product and c.payment_intent = g.transaction_id
    where g.journey_event_kind = 'checkout'

    union all

    -- E7r (Solidgate orders with or without a customer; payment_intent ids are globally unique)
    select g.product, 'ga4_pseudo_id', g.user_pseudo_id, 'stripe_customer', c.stripe_customer,
           'E7_checkout_routed_payment', g.event_ts
    from {{ ref('int_ga4_journey_events') }} g
    join {{ ref('int_solidgate_card_orders') }} o on o.order_id = g.transaction_id
    join accounts ra
        on  ra.product       = g.product
        and ra.source_system = 'solidgate'
        and ra.account       = o._elt_account_name
    join stripe_charges c on c.product = g.product and c.payment_intent = o.provider_payment_id
    where g.journey_event_kind = 'checkout'
      and o.provider_payment_id is not null

    union all

    -- E9 (24-hex only: the app's account id format)
    select product, 'stripe_customer', stripe_customer, 'app_user_id', mc_customer_id,
           'E9_mc_customer_id', created_ts
    from stripe_charges
    where regexp_contains(mc_customer_id, r'^[0-9a-f]{24}$')

    union all

    -- E12
    select c.product, 'stripe_customer', c.stripe_customer, 'solidgate_customer', o.solidgate_customer,
           'E12_mc_first_order_id', c.created_ts
    from stripe_order_refs c
    join solidgate_orders o on o.product = c.product and o.order_id = c.order_ref

    union all

    -- E12p (payment_intent ids are globally unique, so the equi-join cannot cross customers)
    select c.product, 'stripe_customer', c.stripe_customer, 'solidgate_customer', o.solidgate_customer,
           'E12_provider_payment_id', c.created_ts
    from stripe_charges c
    join solidgate_orders o on o.product = c.product and o.provider_payment_id = c.payment_intent

    union all

    -- E14
    select g.product, 'ga4_pseudo_id', g.user_pseudo_id, 'amplitude_id', a.amplitude_id,
           'E14_gclid', least(g.seen_ts, a.seen_ts)
    from unique_ga4_clicks g
    join unique_amp_clicks a on a.product = g.product and a.gclid = g.gclid

    union all

    -- E15
    select product, 'ga4_pseudo_id', user_pseudo_id, 'site_user_id', user_id,
           'E15_ga4_user_id', event_ts
    from {{ ref('int_ga4_journey_events') }}
    where user_id is not null

    union all

    -- E9u
    select product, 'stripe_customer', stripe_customer, 'site_user_id', mc_user_id,
           'E9_mc_user_id', created_ts
    from stripe_charges
    where mc_user_id is not null

    union all

    -- E16
    select product, 'site_user_id', site_user_id, 'app_user_id', app_user_id,
           'E16_amplitude_site_user_id', event_ts
    from amp
    where site_user_id is not null and app_user_id is not null and site_user_id != app_user_id

    union all

    -- E17
    select product, 'anytrack_click_id', click_id, 'email', email_user_key,
           'E17_anytrack_email', event_ts
    from {{ ref('int_anytrack_events') }}
    where click_id is not null

    union all

    -- E18
    select e.product, 'email', e.email_user_key, 'app_user_id', e.signup_app_user_id,
           'E18_anytrack_signup_user_id', e.event_ts
    from {{ ref('int_anytrack_events') }} e
    join amp_app_users a on a.product = e.product and a.app_user_id = e.signup_app_user_id

)

select
    product,
    id_type_a,
    id_a,
    id_type_b,
    id_b,
    evidence,
    min(seen_ts)    as first_seen_ts,
    max(seen_ts)    as last_seen_ts,
    count(*)        as n_obs
from edges
where id_a is not null and id_b is not null
group by 1, 2, 3, 4, 5, 6
