-- depends_on: {{ ref('seed_product_registry') }}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['snapshot_date', 'product'],
    on_schema_change='append_new_columns',
    tags=["journey", "user_journey_mart", "gold", "daily"],
    partition_by={
        "field": "snapshot_date",
        "data_type": "date"
    }
) }}

-- One snapshot per product per run day of how well the sources reconcile
-- (docs/plans/user_journey_mart.md, section 8.6). Identity quality only; the unattributed rate
-- lives in KR2 [3/5] so it is defined in one place.
--   A  Amplitude <-> GA4 (agreed with Marie, 2026-10-09): daily signup / visitor counts for the
--      last complete day (no shared id needed), plus per-user link rates over the last
--      link_rate_window_days
--   B  payers linked to an app user (to an AnyTrack signup on products without Amplitude), per
--      provider, for payers whose first payment is after the product's first Amplitude (else
--      AnyTrack) event -- earlier payers have no history to link to
--   C  identity quality: users per anchor, shared / suppressed ids, unlinked GA4 visitors
-- Section A is null for a product without a GA4 export.
-- Grain: snapshot_date x product.

{% set products = registry_products() %}
{% set window_days = var('link_rate_window_days', 21) %}

with registry as (

    select * from {{ ref('seed_product_registry') }}
    where is_in_scope

),

resolution as (

    select * from {{ ref('int_identity_resolution') }}

),

dim as (

    select * from {{ ref('dim_user_journey') }}

),

report_day as (

    select date_sub(current_date(), interval 1 day) as d

),

-- ---------- A. Amplitude <-> GA4 ----------

amplitude_counts as (

    select
        e.product,
        count(distinct if(not e.is_server_event, e.amplitude_id, null))     as amplitude_visitors,
        countif(
            case
            {%- for p in products %}
                when e.product = '{{ p.product }}' then {{ registry_event_filter(p.signup_events, 'amplitude') }}
            {%- endfor %}
                else false
            end
        )                                                                   as amplitude_signups
    from {{ ref('int_amplitude_events') }} e
    cross join report_day r
    where e.event_date = r.d
    group by 1

),

ga4_counts as (

    select
        g.product,
        count(distinct if(g.journey_event_kind = 'session_start', g.user_pseudo_id, null)) as ga4_visitors,
        countif(g.journey_event_kind = 'signup')                            as ga4_signups
    from {{ ref('int_ga4_journey_events') }} g
    cross join report_day r
    where g.ga4_date = r.d
    group by 1

),

ga4_checkout_visitors as (

    select
        g.product,
        count(distinct g.user_pseudo_id)                                    as ga4_checkout_visitors,
        -- linked: to an app user; on a product without Amplitude, to any user (it has no app users)
        count(distinct if(rs.anchor_type = 'app_user'
                          or (nullif(trim(rg.amplitude_source_table), '') is null and rs.user_key is not null),
                          g.user_pseudo_id, null))                          as ga4_checkout_visitors_linked
    from {{ ref('int_ga4_journey_events') }} g
    join registry rg on rg.product = g.product
    left join resolution rs
        on  rs.product  = g.product
        and rs.id_type  = 'ga4_pseudo_id'
        and rs.id_value = g.user_pseudo_id
    where g.journey_event_kind = 'checkout'
      and g.ga4_date >= date_sub(current_date(), interval {{ window_days }} day)
    group by 1

),

non_checkout_signups as (

    select
        product,
        countif(signup_ts is not null and first_payment_ts is null)                       as non_checkout_signups,
        countif(signup_ts is not null and first_payment_ts is null and has_ga4_match)     as non_checkout_signups_in_ga4
    from dim
    where signup_ts >= timestamp_sub(current_timestamp(), interval {{ window_days }} day)
    group by 1

),

-- ---------- B. payers ----------

-- payers are counted from the day Amplitude started receiving the product's events (server time):
-- device clocks put a few events days earlier, and payers from before tracking can never link.
-- A product without Amplitude counts from its first AnyTrack event, and a payer there is linked
-- when it reached an AnyTrack signup (its users are AnyTrack signups, not app users).
amplitude_start as (

    select product, date(min(server_received_time)) as amplitude_start_date
    from {{ ref('int_amplitude_events') }}
    group by 1

),

anytrack_start as (

    select product, date(min(event_ts)) as anytrack_start_date
    from {{ ref('int_anytrack_events') }}
    group by 1

),

payer_users_in_anytrack as (

    select distinct product, user_key
    from resolution
    where id_type = 'anytrack_click_id'

),

-- users with an observed AnyTrack signup (the registry's anytrack: signup events): purchases and
-- renewals carry the email too, so being in AnyTrack alone does not mean the signup was seen
payer_users_with_anytrack_signup as (

    select distinct r.product, r.user_key
    from {{ ref('int_anytrack_events') }} e
    join resolution r on r.product = e.product and r.id_type = 'email' and r.id_value = e.email_user_key
    where false
    {%- for p in products %}
       or (e.product = '{{ p.product }}' and {{ registry_event_filter(p.signup_events, 'anytrack', 'e.event_name') }})
    {%- endfor %}

),

payers as (

    select
        rs.product,
        split(rs.id_type, '_')[offset(0)]                                   as provider,
        rs.user_key,
        rs.anchor_type,
        rs.link_path,
        pa.user_key is not null                                             as is_in_anytrack,
        if(a.product is not null, rs.anchor_type = 'app_user', su.user_key is not null) as is_linked
    from resolution rs
    join {{ ref('int_customer_revenue') }} v
        on  rs.id_type  = concat(v.source_system, '_customer')
        and rs.id_value = concat(v.source_account, ':', v.source_customer_id)
    left join amplitude_start a on a.product = rs.product
    left join anytrack_start t  on t.product = rs.product
    left join payer_users_in_anytrack pa on pa.product = rs.product and pa.user_key = rs.user_key
    left join payer_users_with_anytrack_signup su on su.product = rs.product and su.user_key = rs.user_key
    where rs.id_type in ('solidgate_customer', 'stripe_customer')
      and v.first_payment_date >= coalesce(a.amplitude_start_date, t.anytrack_start_date)

),

payer_users_in_ga4 as (

    select distinct product, user_key
    from resolution
    where id_type = 'ga4_pseudo_id'

),

payer_metrics as (

    select
        p.product,
        countif(p.provider = 'solidgate')                                   as solidgate_payers,
        countif(p.provider = 'solidgate' and p.is_linked)                   as solidgate_payers_linked,
        countif(p.provider = 'solidgate' and p.is_in_anytrack)              as solidgate_payers_in_anytrack,
        countif(p.provider = 'solidgate' and g.user_key is not null)        as solidgate_payers_in_ga4,
        countif(p.provider = 'stripe')                                      as stripe_payers,
        countif(p.provider = 'stripe' and p.is_linked)                      as stripe_payers_linked,
        countif(p.provider = 'stripe' and p.is_in_anytrack)                 as stripe_payers_in_anytrack,
        countif(p.provider = 'stripe' and g.user_key is not null)           as stripe_payers_in_ga4,
        countif(p.provider = 'solidgate' and p.link_path = 'order')         as solidgate_linked_by_order,
        countif(p.provider = 'solidgate' and p.link_path = 'email')         as solidgate_linked_by_email,
        countif(p.provider = 'stripe' and p.link_path = 'payment_metadata') as stripe_linked_by_metadata,
        countif(p.provider = 'stripe' and p.link_path = 'solidgate_first_order') as stripe_linked_by_first_order,
        countif(p.provider = 'stripe' and p.link_path = 'email')            as stripe_linked_by_email
    from payers p
    left join payer_users_in_ga4 g on g.product = p.product and g.user_key = p.user_key
    group by 1

),

-- ---------- C. identity quality ----------

anchors as (

    select
        product,
        countif(anchor_type = 'app_user')                                   as users_app_user,
        countif(anchor_type = 'payment_email')                              as users_payment_email,
        countif(anchor_type = 'signup_email')                               as users_signup_email,
        countif(anchor_type = 'payment_customer')                           as users_payment_customer,
        countif(anchor_type = 'visitor')                                    as users_visitor
    from dim
    group by 1

),

suppressed as (

    select
        product,
        count(distinct if(reason = 'shared', concat(id_type, ':', id_value), null))     as ids_shared,
        count(distinct if(reason = 'super_node', concat(id_type, ':', id_value), null)) as ids_super_node
    from {{ ref('int_identity_suppressed_ids') }}
    group by 1

),

unlinked_ga4 as (

    select
        t.product,
        countif(rs.id_value is null)                                        as unlinked_ga4_visitors
    from {{ ref('int_ga4_user_touchpoints') }} t
    left join resolution rs
        on  rs.product  = t.product
        and rs.id_type  = 'ga4_pseudo_id'
        and rs.id_value = t.user_pseudo_id
    group by 1

)

select
    current_date()                                                          as snapshot_date,
    r.product,
    (select d from report_day)                                              as count_date,

    -- A
    g.ga4_signups,
    a.amplitude_signups,
    round(safe_divide(a.amplitude_signups - g.ga4_signups, g.ga4_signups), 4)       as signup_count_diff_pct,
    g.ga4_visitors,
    a.amplitude_visitors,
    round(safe_divide(a.amplitude_visitors - g.ga4_visitors, g.ga4_visitors), 4)    as visitor_count_diff_pct,
    c.ga4_checkout_visitors,
    round(safe_divide(c.ga4_checkout_visitors_linked, c.ga4_checkout_visitors), 4)  as pct_ga4_checkout_visitors_linked,
    n.non_checkout_signups,
    round(safe_divide(n.non_checkout_signups_in_ga4, n.non_checkout_signups), 4)    as pct_non_checkout_signups_linked_to_ga4,

    -- B
    pm.solidgate_payers,
    round(safe_divide(pm.solidgate_payers_linked, pm.solidgate_payers), 4)          as pct_solidgate_payers_linked,
    round(safe_divide(pm.solidgate_payers_in_ga4, pm.solidgate_payers), 4)          as pct_solidgate_payers_in_ga4,
    round(safe_divide(pm.solidgate_payers_in_anytrack, pm.solidgate_payers), 4)     as pct_solidgate_payers_in_anytrack,
    pm.solidgate_linked_by_order,
    pm.solidgate_linked_by_email,
    pm.stripe_payers,
    round(safe_divide(pm.stripe_payers_linked, pm.stripe_payers), 4)                as pct_stripe_payers_linked,
    round(safe_divide(pm.stripe_payers_in_ga4, pm.stripe_payers), 4)                as pct_stripe_payers_in_ga4,
    round(safe_divide(pm.stripe_payers_in_anytrack, pm.stripe_payers), 4)           as pct_stripe_payers_in_anytrack,
    pm.stripe_linked_by_metadata,
    pm.stripe_linked_by_first_order,
    pm.stripe_linked_by_email,

    -- C
    an.users_app_user,
    an.users_payment_email,
    an.users_signup_email,
    an.users_payment_customer,
    an.users_visitor,
    coalesce(s.ids_shared, 0)                                               as ids_shared,
    coalesce(s.ids_super_node, 0)                                           as ids_super_node,
    u.unlinked_ga4_visitors,

    current_timestamp()                                                     as _built_at
from registry r
left join amplitude_counts a       on a.product  = r.product
left join ga4_counts g             on g.product  = r.product
left join ga4_checkout_visitors c  on c.product  = r.product
left join non_checkout_signups n   on n.product  = r.product
left join payer_metrics pm         on pm.product = r.product
left join anchors an               on an.product = r.product
left join suppressed s             on s.product  = r.product
left join unlinked_ga4 u           on u.product  = r.product
