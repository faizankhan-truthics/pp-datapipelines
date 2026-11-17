{{ config(
    materialized='table',
    tags=["journey", "ga4", "silver", "daily"],
    cluster_by=['product', 'user_pseudo_id']
) }}

-- One row per GA4 visitor (user_pseudo_id) per product: first and last seen, the first session's
-- touch (UTMs, referrer, landing page, click ids), first signup, and every click id the visitor
-- arrived with -- the latter is the GA4 side of the click-id match to Amplitude (E14).
-- Built from int_ga4_journey_events, which keeps the long-term GA4 history, so this can be a plain
-- table. Session source / medium / campaign fall back to int_ga4_sessions (collected traffic
-- source) when the landing URL carries no UTMs.
-- Grain: product x user_pseudo_id.

with events as (

    select * from {{ ref('int_ga4_journey_events') }}

),

sessions as (

    select
        r.product,
        s.user_pseudo_id,
        s.ga_session_id,
        s.source,
        s.medium,
        s.campaign
    from {{ ref('int_ga4_sessions') }} s
    join {{ ref('seed_product_registry') }} r
        on r.ga4_property_name = s.ga4_property_name
       and r.is_in_scope

),

session_starts as (

    select
        e.*,
        s.source   as session_source,
        s.medium   as session_medium,
        s.campaign as session_campaign
    from events e
    left join sessions s
        on  s.product        = e.product
        and s.user_pseudo_id = e.user_pseudo_id
        and s.ga_session_id  = e.ga_session_id
    where e.journey_event_kind = 'session_start'

),

first_session as (

    select *
    from session_starts
    qualify row_number() over (
        partition by product, user_pseudo_id order by event_ts, ga_session_id
    ) = 1

),

per_visitor as (

    select
        product,
        user_pseudo_id,
        min(event_ts)                                                       as first_seen_ts,
        max(event_ts)                                                       as last_seen_ts,
        count(distinct if(journey_event_kind = 'session_start', ga_session_id, null)) as n_sessions,
        min(if(journey_event_kind = 'signup', event_ts, null))              as first_signup_ts,
        min(if(journey_event_kind = 'checkout', event_ts, null))            as first_checkout_ts,
        array_agg(distinct gclid ignore nulls)                              as gclids,
        array_agg(distinct msclkid ignore nulls)                            as msclkids,
        array_agg(distinct fbclid ignore nulls)                             as fbclids
    from events
    group by 1, 2

)

select
    v.product,
    v.user_pseudo_id,
    v.first_seen_ts,
    v.last_seen_ts,
    v.n_sessions,
    v.first_signup_ts,
    v.first_checkout_ts,
    f.event_ts                                                  as first_session_ts,
    coalesce(f.utm_source,   f.session_source)                  as first_touch_source,
    coalesce(f.utm_medium,   f.session_medium)                  as first_touch_medium,
    coalesce(f.utm_campaign, f.session_campaign)                as first_touch_campaign,
    f.utm_term                                                  as first_touch_term,
    f.page_referrer_domain                                      as first_touch_referrer_domain,
    f.landing_page                                              as first_touch_landing_page,
    f.gclid                                                     as first_touch_gclid,
    f.gbraid                                                    as first_touch_gbraid,
    f.wbraid                                                    as first_touch_wbraid,
    f.msclkid                                                   as first_touch_msclkid,
    f.fbclid                                                    as first_touch_fbclid,
    f.ttclid                                                    as first_touch_ttclid,
    f.ad_network                                                as first_touch_ad_network,
    v.gclids,
    v.msclkids,
    v.fbclids
from per_visitor v
left join first_session f
    on  f.product        = v.product
    and f.user_pseudo_id = v.user_pseudo_id
