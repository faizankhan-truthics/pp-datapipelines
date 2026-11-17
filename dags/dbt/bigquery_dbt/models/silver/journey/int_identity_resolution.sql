{{ config(
    materialized='table',
    tags=["journey", "identity", "silver", "daily"],
    cluster_by=['product', 'id_type']
) }}

-- Resolves every identifier of a product to one user_key (docs/plans/user_journey_mart.md, section 7).
-- Hub and spoke, not graph closure: the app account id (Amplitude user_id) is the hub, and every
-- other identifier attaches through a fixed rule order -- the first rule that hits wins and is
-- recorded as link_path. An identifier that points at more than one user is attached to nobody
-- (see int_identity_suppressed_ids).
--
--   app_user_id         the hub itself; a backend-only id (seen only in server events) whose
--                       Solidgate customer (E3) paid as exactly one web account (that customer's
--                       Stripe payment metadata, else its email) joins that account's user
--   amplitude_id        E1 -> its app user; never identified -> its own `visitor` user
--   device_id           E2, only when the device maps to exactly one app user
--   email (md5)         E6 -> app user; else the app user all its customers resolved to; else
--                       the payer-email user. On a product without Amplitude, an AnyTrack signup
--                       email (E17) that is not a payer is its own `signup_email` user, so it
--                       becomes the same user as soon as that person pays. On Amplitude products
--                       AnyTrack emails only attach to users that exist (no duplicate of an app
--                       user whose email Amplitude never saw)
--   solidgate_customer  order or customer id (E3 / E3c) -> email (E5, else order email E5o ->
--                       E6 / E18) -> payment
--                       metadata of its Stripe customer (E12 / E12p -> E9) -> own user
--   stripe_customer     payment metadata: mc_customer_id (E9) or mc_user_id via its site user
--                       id (E9u) -> Solidgate customer (first order E12, or an order routed to
--                       Stripe E12p) that is an app user -> email (E5 -> E6) -> that Solidgate
--                       customer's user (any anchor) -> own user
--   site_user_id        the product's web account id: an app user id itself, or E16
--   ga4_pseudo_id       checkout order (E7+), checkout payment_intent (E7s / E7r), click id (E14) and /
--                       or GA4 user_id (E15), only when every route lands on the same user;
--                       unlinked GA4 visitors are left out
--   anytrack_click_id   the user of its email (E17), when the visitor carries exactly one email
--
-- user_key: md5 of product + anchor type + anchor id, so it is stable for as long as the anchor
-- is. A payer without an app user is anchored on their email, which joins the Solidgate and Stripe
-- records of the same person. Keys move when an identity resolves further (e.g. a visitor signs
-- up), so never store user_key outside dbt.
-- Grain: product x id_type x id_value.

with edges as (

    select * from {{ ref('int_identity_edges') }}

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

-- ---------- single-hub maps (n_hubs = 1 means the id may attach) ----------

amp_to_app as (
    select product, id_a as amplitude_id, any_value(id_b) as app_user_id, count(distinct id_b) as n_hubs
    from edges where evidence = 'E1_amplitude_event'
    group by 1, 2
),

device_to_app as (
    select product, id_a as device_id, any_value(id_b) as app_user_id, count(distinct id_b) as n_hubs
    from edges where evidence = 'E2_amplitude_event'
    group by 1, 2
),

email_to_app as (
    select product, id_a as email_user_key, any_value(id_b) as app_user_id, count(distinct id_b) as n_hubs
    from edges where evidence in ('E6_amplitude_email', 'E18_anytrack_signup_user_id')
    group by 1, 2
),

sg_order_raw as (
    select product, id_a as solidgate_customer, id_b as app_user_id
    from edges where evidence in ('E3_order_id', 'E3_customer_account_id')
),

-- E12 (Stripe metadata) and E12p (Solidgate order routed to Stripe) both point a Stripe customer at
-- its Solidgate customer; n_hubs counts across both, so a conflict between them attaches nothing
st_to_sg as (
    select product, id_a as stripe_customer, any_value(id_b) as solidgate_customer, count(distinct id_b) as n_hubs
    from edges where evidence in ('E12_mc_first_order_id', 'E12_provider_payment_id')
    group by 1, 2
),

-- the customer record's email (E5) wins over the email on its orders (E5o); among order emails,
-- the latest
customer_email as (
    select
        product, id_type_a as customer_type, id_a as customer_id,
        array_agg(id_b order by if(evidence = 'E5_payer_email', 0, 1), last_seen_ts desc
                  limit 1)[offset(0)] as email_user_key
    from edges where evidence in ('E5_payer_email', 'E5_order_email')
    group by 1, 2, 3
),

-- ---------- populations ----------

-- every app user, including those seen only in server events (renewals sent from the backend have
-- no edge), so fct_user_journey_events never carries a user_key the dim does not know
app_users as (
    select product, id_b as app_user_id
    from edges where id_type_b = 'app_user_id'
    union distinct
    select product, app_user_id
    from {{ ref('int_amplitude_events') }}
    where app_user_id is not null
),

-- a site user id maps to an app user when it is one (publicregistry sends the Amplitude user_id
-- as GA4 user_id / mc_user_id) or when Amplitude events carry both (E16, reversly)
site_to_app as (
    select
        m.product,
        m.site_user_id,
        any_value(m.app_user_id)                                        as app_user_id,
        count(distinct m.app_user_id)                                   as n_hubs,
        logical_or(m.via_e16)                                           as via_e16
    from (
        select product, id_a as site_user_id, id_b as app_user_id, true as via_e16
        from edges where evidence = 'E16_amplitude_site_user_id'
        union all
        select s.product, s.site_user_id, a.app_user_id, false
        from (select distinct product, id_b as site_user_id from edges where id_type_b = 'site_user_id') s
        join app_users a on a.product = s.product and a.app_user_id = s.site_user_id
    ) m
    group by 1, 2
),

-- Stripe payment metadata: mc_customer_id names the app user (E9); mc_user_id names a site user
-- id (E9u). n_hubs counts across both, so a conflict attaches nothing
st_meta_raw as (
    select product, id_a as stripe_customer, id_b as app_user_id
    from edges where evidence = 'E9_mc_customer_id'
    union all
    select e.product, e.id_a, s.app_user_id
    from edges e
    join site_to_app s on s.product = e.product and s.site_user_id = e.id_b and s.n_hubs = 1
    where e.evidence = 'E9_mc_user_id'
),

-- backend-only app ids: the app's backend sends subscription events under its own account id
-- (publicregistry: 24-hex, while the web uses a 7-char id), and only those events carry order_id
server_only_app_users as (
    select product, app_user_id
    from {{ ref('int_amplitude_events') }}
    where app_user_id is not null
    group by 1, 2
    having logical_and(is_server_event)
),

-- the web account that paid for the same Solidgate customer the backend id's orders belong to.
-- Through Stripe payment metadata the backend id need not be server-only: on iqcenter the 24-hex
-- account id (= the Solidgate customer id) also sends client events, while the web signs up and
-- pays under a 7-char id (mc_user_id). Metadata naming the same id as the order is no alias
-- (web != backend below), so this only merges a payer split across the two ids.
alias_candidates as (
    select o.product, o.app_user_id as backend_app_user_id, m.app_user_id as web_app_user_id
    from sg_order_raw o
    join edges st
        on  st.product = o.product
        and st.id_b    = o.solidgate_customer
        and st.evidence in ('E12_mc_first_order_id', 'E12_provider_payment_id')
    join st_meta_raw m on m.product = st.product and m.stripe_customer = st.id_a

    union all

    select o.product, o.app_user_id, ea.app_user_id
    from sg_order_raw o
    join server_only_app_users b on b.product = o.product and b.app_user_id = o.app_user_id
    join customer_email ce
        on ce.product = o.product and ce.customer_type = 'solidgate_customer' and ce.customer_id = o.solidgate_customer
    join email_to_app ea on ea.product = ce.product and ea.email_user_key = ce.email_user_key and ea.n_hubs = 1
),

app_alias as (
    -- HAVING must use the qualified column: bare names resolve to the any_value() alias
    select c.product, c.backend_app_user_id, any_value(c.web_app_user_id) as web_app_user_id
    from alias_candidates c
    left join server_only_app_users w
        on w.product = c.product and w.app_user_id = c.web_app_user_id
    where w.app_user_id is null and c.web_app_user_id != c.backend_app_user_id
    group by 1, 2
    having count(distinct c.web_app_user_id) = 1
),

sg_order_to_app as (
    select o.product, o.solidgate_customer,
           any_value(coalesce(a.web_app_user_id, o.app_user_id))            as app_user_id,
           count(distinct coalesce(a.web_app_user_id, o.app_user_id))       as n_hubs
    from sg_order_raw o
    left join app_alias a on a.product = o.product and a.backend_app_user_id = o.app_user_id
    group by 1, 2
),

st_meta_to_app as (
    select m.product, m.stripe_customer,
           any_value(coalesce(a.web_app_user_id, m.app_user_id))            as app_user_id,
           count(distinct coalesce(a.web_app_user_id, m.app_user_id))       as n_hubs
    from st_meta_raw m
    left join app_alias a on a.product = m.product and a.backend_app_user_id = m.app_user_id
    group by 1, 2
),

amplitude_visitors as (
    select distinct product, amplitude_id
    from {{ ref('int_amplitude_events') }}
    where amplitude_id is not null and not is_server_event
),

-- every customer of the product's accounts -- payers and those whose attempts were all declined
-- or voided -- with or without an edge, so none is lost and every attempt reaches the journey
solidgate_customers as (
    select distinct r.product, concat(o._elt_account_name, ':', o.customer_account_id) as solidgate_customer
    from {{ ref('int_solidgate_card_orders') }} o
    join accounts r on r.source_system = 'solidgate' and r.account = o._elt_account_name
    where o.customer_account_id is not null
    union distinct
    select distinct product, id_a from edges where id_type_a = 'solidgate_customer'
),

stripe_customers as (
    select distinct r.product, concat(c._elt_stripe_account_id, ':', c.customer) as stripe_customer
    from {{ ref('int_stripe_charges') }} c
    join accounts r on r.source_system = 'stripe' and r.account = c._elt_stripe_account_id
    where c.customer is not null
    union distinct
    select distinct product, id_a from edges where id_type_a = 'stripe_customer'
),

-- reverse of st_to_sg: a Solidgate customer takes the app user its Stripe customers' payment
-- metadata names (E12 / E12p, then E9), when they all name the same one
sg_to_st_app as (
    select e.product, e.id_b as solidgate_customer,
           any_value(m.app_user_id) as app_user_id, count(distinct m.app_user_id) as n_hubs
    from edges e
    join st_meta_to_app m
        on m.product = e.product and m.stripe_customer = e.id_a and m.n_hubs = 1
    where e.evidence in ('E12_mc_first_order_id', 'E12_provider_payment_id')
    group by 1, 2
),

-- ---------- payers ----------

sg_resolved as (

    select
        c.product,
        c.solidgate_customer,
        e.email_user_key,
        case
            when o.n_hubs = 1 then o.app_user_id
            when ea.n_hubs = 1 then coalesce(eal.web_app_user_id, ea.app_user_id)
            when sm.n_hubs = 1 then sm.app_user_id
        end as app_user_id,
        case
            when o.n_hubs = 1 then 'order'
            when ea.n_hubs = 1 then 'email'
            when sm.n_hubs = 1 then 'stripe_metadata'
            else 'unlinked'
        end as link_path
    from solidgate_customers c
    left join sg_order_to_app o
        on o.product = c.product and o.solidgate_customer = c.solidgate_customer
    left join customer_email e
        on e.product = c.product and e.customer_type = 'solidgate_customer' and e.customer_id = c.solidgate_customer
    left join email_to_app ea
        on ea.product = c.product and ea.email_user_key = e.email_user_key
    left join app_alias eal on eal.product = ea.product and eal.backend_app_user_id = ea.app_user_id
    left join sg_to_st_app sm
        on sm.product = c.product and sm.solidgate_customer = c.solidgate_customer

),

sg_keyed as (

    select
        *,
        case
            when app_user_id is not null    then to_hex(md5(concat(product, '|app_user|', app_user_id)))
            when email_user_key is not null then to_hex(md5(concat(product, '|email|', email_user_key)))
            else to_hex(md5(concat(product, '|customer|solidgate|', solidgate_customer)))
        end as user_key,
        case
            when app_user_id is not null    then 'app_user'
            when email_user_key is not null then 'payment_email'
            else 'payment_customer'
        end as anchor_type
    from sg_resolved

),

st_resolved as (

    select
        c.product,
        c.stripe_customer,
        e.email_user_key,
        m.n_hubs                                    as meta_hubs,
        m.app_user_id                               as meta_app_user_id,
        if(s.n_hubs = 1, sg.user_key, null)         as sg_user_key,
        if(s.n_hubs = 1, sg.anchor_type, null)      as sg_anchor_type,
        if(s.n_hubs = 1, sg.app_user_id, null)      as sg_app_user_id,
        ea.n_hubs                                   as email_hubs,
        coalesce(eal.web_app_user_id, ea.app_user_id) as email_app_user_id
    from stripe_customers c
    left join st_meta_to_app m
        on m.product = c.product and m.stripe_customer = c.stripe_customer
    left join st_to_sg s
        on s.product = c.product and s.stripe_customer = c.stripe_customer
    left join sg_keyed sg
        on sg.product = c.product and sg.solidgate_customer = s.solidgate_customer
    left join customer_email e
        on e.product = c.product and e.customer_type = 'stripe_customer' and e.customer_id = c.stripe_customer
    left join email_to_app ea
        on ea.product = c.product and ea.email_user_key = e.email_user_key
    left join app_alias eal on eal.product = ea.product and eal.backend_app_user_id = ea.app_user_id

),

st_keyed as (

    select
        product,
        stripe_customer,
        case
            when meta_hubs = 1                   then to_hex(md5(concat(product, '|app_user|', meta_app_user_id)))
            when sg_app_user_id is not null      then sg_user_key
            when email_hubs = 1                  then to_hex(md5(concat(product, '|app_user|', email_app_user_id)))
            when sg_user_key is not null         then sg_user_key
            when email_user_key is not null      then to_hex(md5(concat(product, '|email|', email_user_key)))
            else to_hex(md5(concat(product, '|customer|stripe|', stripe_customer)))
        end as user_key,
        case
            when meta_hubs = 1 or sg_app_user_id is not null or email_hubs = 1 then 'app_user'
            when sg_user_key is not null    then sg_anchor_type
            when email_user_key is not null then 'payment_email'
            else 'payment_customer'
        end as anchor_type,
        case
            when meta_hubs = 1              then 'payment_metadata'
            when sg_app_user_id is not null then 'solidgate_first_order'
            when email_hubs = 1             then 'email'
            when sg_user_key is not null    then 'solidgate_first_order'
            else 'unlinked'
        end as link_path,
        case
            when meta_hubs = 1 or sg_app_user_id is not null then 'high'
            when email_hubs = 1 or sg_user_key is not null   then 'medium'
            else 'none'
        end as identity_confidence
    from st_resolved

),

-- ---------- visitors ----------

amp_keyed as (

    select
        v.product,
        v.amplitude_id,
        case
            when a.n_hubs = 1 then to_hex(md5(concat(v.product, '|app_user|',
                                                     coalesce(al.web_app_user_id, a.app_user_id))))
            else to_hex(md5(concat(v.product, '|visitor|', v.amplitude_id)))
        end as user_key,
        if(a.n_hubs = 1, 'app_user', 'visitor')     as anchor_type,
        if(a.n_hubs = 1, 'E1', 'anonymous')         as link_path
    from amplitude_visitors v
    left join amp_to_app a
        on a.product = v.product and a.amplitude_id = v.amplitude_id
    left join app_alias al on al.product = a.product and al.backend_app_user_id = a.app_user_id
    -- an amplitude_id seen with two app users is shared: attached to nobody
    where coalesce(a.n_hubs, 0) <= 1

),

ga4_candidates as (

    select e.product, e.id_a as user_pseudo_id, sg.user_key, sg.anchor_type, 'checkout_order' as route
    from edges e
    join sg_keyed sg on sg.product = e.product and sg.solidgate_customer = e.id_b
    where e.evidence = 'E7_checkout_order_id'

    union all

    select e.product, e.id_a, st.user_key, st.anchor_type, 'checkout_payment_intent'
    from edges e
    join st_keyed st on st.product = e.product and st.stripe_customer = e.id_b
    where e.evidence in ('E7_checkout_payment_intent', 'E7_checkout_routed_payment')

    union all

    select e.product, e.id_a, ak.user_key, ak.anchor_type, 'click_id'
    from edges e
    join amp_keyed ak on ak.product = e.product and ak.amplitude_id = e.id_b
    where e.evidence = 'E14_gclid'

    union all

    select e.product, e.id_a,
           to_hex(md5(concat(e.product, '|app_user|', coalesce(al.web_app_user_id, s.app_user_id)))),
           'app_user', 'user_id'
    from edges e
    join site_to_app s on s.product = e.product and s.site_user_id = e.id_b and s.n_hubs = 1
    left join app_alias al on al.product = s.product and al.backend_app_user_id = s.app_user_id
    where e.evidence = 'E15_ga4_user_id'

),

ga4_keyed as (

    -- HAVING must use the qualified column: bare user_key resolves to the any_value() alias
    select
        c.product,
        c.user_pseudo_id,
        any_value(c.user_key)                                           as user_key,
        any_value(c.anchor_type)                                        as anchor_type,
        string_agg(distinct c.route, '+' order by c.route)              as link_path
    from ga4_candidates c
    group by 1, 2
    having count(distinct c.user_key) = 1

),

-- the users the customers carrying each payer email resolved to
email_via_customers as (

    select
        ce.product,
        ce.email_user_key,
        any_value(if(k.anchor_type = 'app_user', k.user_key, null))            as user_key,
        count(distinct if(k.anchor_type = 'app_user', k.user_key, null))        as n_hubs,
        countif(k.anchor_type != 'app_user')                                    as n_not_app_user
    from customer_email ce
    join (
        select product, 'solidgate_customer' as customer_type, solidgate_customer as customer_id,
               user_key, anchor_type
        from sg_keyed
        union all
        select product, 'stripe_customer', stripe_customer, user_key, anchor_type
        from st_keyed
    ) k
        on k.product = ce.product and k.customer_type = ce.customer_type and k.customer_id = ce.customer_id
    group by 1, 2

),

-- ---------- emails and AnyTrack visitors ----------

payer_emails as (
    select distinct product, email_user_key from customer_email
),

-- AnyTrack emails that may start a user of their own: products without Amplitude only
anytrack_signup_emails as (
    -- from the events, not E17: an emailed event without a visitor click id still names a user
    select distinct a.product, a.email_user_key
    from {{ ref('int_anytrack_events') }} a
    join registry r on r.product = a.product
    where nullif(trim(r.amplitude_source_table), '') is null
),

-- emails: their app user (E6); else the app user every customer carrying the email resolved to
-- (e.g. through payment metadata), so no email-only user is left behind; else the payer-email
-- user; else (AnyTrack signup, not a payer yet) the signup-email user -- the same user_key, so
-- the signup and the later payer are one user
email_keyed as (
    select
        coalesce(a.product, p.product)                                  as product,
        coalesce(a.email_user_key, p.email_user_key)                    as email_user_key,
        case
            when a.n_hubs = 1 then to_hex(md5(concat(a.product, '|app_user|',
                                                     coalesce(al.web_app_user_id, a.app_user_id))))
            when v.n_hubs = 1 and v.n_not_app_user = 0 then v.user_key
            else to_hex(md5(concat(coalesce(a.product, p.product), '|email|',
                                   coalesce(a.email_user_key, p.email_user_key))))
        end                                                             as user_key,
        case
            when a.n_hubs = 1 or (v.n_hubs = 1 and v.n_not_app_user = 0) then 'app_user'
            when p.is_payer then 'payment_email'
            else 'signup_email'
        end                                                             as anchor_type,
        case
            when a.n_hubs = 1 then 'E6'
            when v.n_hubs = 1 and v.n_not_app_user = 0 then 'customer'
            when p.is_payer then 'payer_email'
            else 'anytrack_signup'
        end                                                             as link_path,
        if(a.n_hubs = 1 or (v.n_hubs = 1 and v.n_not_app_user = 0), 'medium', 'none')
                                                                        as identity_confidence
    from email_to_app a
    left join app_alias al on al.product = a.product and al.backend_app_user_id = a.app_user_id
    full outer join (
        select product, email_user_key, logical_or(is_payer) as is_payer
        from (
            select product, email_user_key, true as is_payer from payer_emails
            union all
            select product, email_user_key, false from anytrack_signup_emails
        )
        group by 1, 2
    ) p
        on p.product = a.product and p.email_user_key = a.email_user_key
    left join email_via_customers v
        on v.product = p.product and v.email_user_key = p.email_user_key
    where a.n_hubs = 1 or p.email_user_key is not null
),

anytrack_keyed as (
    -- HAVING must use the qualified column: bare names resolve to the any_value() alias
    select e.product, e.id_a as click_id, any_value(k.user_key) as user_key,
           any_value(k.anchor_type) as anchor_type, any_value(k.identity_confidence) as identity_confidence
    from edges e
    left join email_keyed k on k.product = e.product and k.email_user_key = e.id_b
    where e.evidence = 'E17_anytrack_email'
    group by 1, 2
    -- every email of the visitor counts, resolved or not: two emails are conflicting evidence
    having count(distinct e.id_b) = 1 and logical_or(k.user_key is not null)
)

-- ---------- output ----------

select u.product, 'app_user_id' as id_type, u.app_user_id as id_value,
       to_hex(md5(concat(u.product, '|app_user|', coalesce(a.web_app_user_id, u.app_user_id)))) as user_key,
       'app_user' as anchor_type, if(a.web_app_user_id is null, 'hub', 'app_user_alias') as link_path,
       'high' as identity_confidence
from app_users u
left join app_alias a on a.product = u.product and a.backend_app_user_id = u.app_user_id

union all

select product, 'amplitude_id', amplitude_id, user_key, anchor_type, link_path,
       if(anchor_type = 'app_user', 'high', 'none')
from amp_keyed

union all

select d.product, 'device_id', d.device_id,
       to_hex(md5(concat(d.product, '|app_user|', coalesce(al.web_app_user_id, d.app_user_id)))),
       'app_user', 'E2', 'high'
from device_to_app d
left join app_alias al on al.product = d.product and al.backend_app_user_id = d.app_user_id
where d.n_hubs = 1

union all

select product, 'email', email_user_key, user_key, anchor_type, link_path, identity_confidence
from email_keyed

union all

select product, 'solidgate_customer', solidgate_customer, user_key, anchor_type, link_path,
       case link_path when 'order' then 'high' when 'stripe_metadata' then 'high'
                      when 'email' then 'medium' else 'none' end
from sg_keyed

union all

select product, 'stripe_customer', stripe_customer, user_key, anchor_type, link_path, identity_confidence
from st_keyed

union all

select product, 'ga4_pseudo_id', user_pseudo_id, user_key, anchor_type, link_path,
       if(link_path like '%checkout%' or link_path like '%user_id%', 'high', 'medium')
from ga4_keyed

union all

select product, 'anytrack_click_id', click_id, user_key, anchor_type, 'E17', identity_confidence
from anytrack_keyed

union all

select s.product, 'site_user_id', s.site_user_id,
       to_hex(md5(concat(s.product, '|app_user|', coalesce(al.web_app_user_id, s.app_user_id)))), 'app_user',
       if(s.via_e16, 'E16', 'site_user_id'), 'high'
from site_to_app s
left join app_alias al on al.product = s.product and al.backend_app_user_id = s.app_user_id
where s.n_hubs = 1
