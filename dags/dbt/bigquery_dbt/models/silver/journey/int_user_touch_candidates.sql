-- depends_on: {{ ref('seed_kr2_domain_lists') }}
{{ config(
    materialized='table',
    tags=["journey", "silver", "daily"],
    cluster_by=['product', 'user_key']
) }}

-- Every first-touch candidate for a journey-mart user, one per system and identifier, with the
-- evidence the KR2 rules read (seeds/seed_kr2_channel_rules.csv). int_user_touch_classified
-- gives each a rule; dim_user_journey picks the journey's touch.
--   amplitude: the initial_* user properties of each amplitude_id, at its first event
--   ga4:       the first session of each GA4 visitor linked to the user
--   anytrack:  AnyTrack's first touch (main_attribution) of each AnyTrack visitor linked to the user
--   payment:   the GA4-based customer attribution of each linked payer (int_customer_attribution)
-- source_resolution_method is the evidence that names the touch's source (KR2 spec, D6): for GA4
-- and payment touches click_id, utm or referrer; an AnyTrack touch is `anytrack`, an Amplitude one
-- `signup_capture` (captured at signup); `none` when the touch carries nothing. method_rank orders
-- them (click_id > utm > referrer > anytrack > signup_capture > none) for touches of one visit.
-- src / med / campaign_norm are the labels as the rules read them (kr2_normalize_label): raw
-- source / medium stay as they came. A touch whose click id came without a network (Amplitude never
-- records it) takes the network AnyTrack or GA4 saw on the same click id: a click id is one ad click.
-- A payment touch only knows its date (is_touch_date_only): dim_user_journey orders it after the
-- timestamped touches of the same day. A touch without a referrer but with medium `referral` and a
-- domain as source (GA4's convention, e.g. ntp.msn.com / referral) takes that domain as referrer.
-- Grain: product x user_key x touch_system x touch_id.

with resolution as (

    select * from {{ ref('int_identity_resolution') }}

),

registry as (

    select product, site_domains
    from {{ ref('seed_product_registry') }}
    where is_in_scope

),

amplitude as (

    select
        e.product,
        r.user_key,
        'amplitude'                                                     as touch_system,
        e.amplitude_id                                                  as touch_id,
        min(e.event_ts)                                                 as touch_ts,
        false                                                           as is_touch_date_only,
        array_agg(struct(
            e.initial_utm_source as source, e.initial_utm_medium as medium,
            e.initial_utm_campaign as campaign, e.initial_utm_term as term,
            e.initial_referring_domain as referrer_domain,
            cast(null as string) as landing_page, cast(null as string) as ad_network,
            e.initial_gclid as gclid, e.initial_gbraid as gbraid, e.initial_wbraid as wbraid,
            e.initial_msclkid as msclkid, e.initial_fbclid as fbclid, e.initial_ttclid as ttclid
        ) order by e.event_ts limit 1)[offset(0)]                       as t
    from {{ ref('int_amplitude_events') }} e
    join resolution r
        on  r.product  = e.product
        and r.id_type  = 'amplitude_id'
        and r.id_value = e.amplitude_id
    where not e.is_server_event
    group by 1, 2, 3, 4

),

ga4 as (

    select
        t.product,
        r.user_key,
        'ga4'                                                           as touch_system,
        t.user_pseudo_id                                                as touch_id,
        coalesce(t.first_session_ts, t.first_seen_ts)                   as touch_ts,
        false                                                           as is_touch_date_only,
        struct(
            t.first_touch_source as source, t.first_touch_medium as medium,
            t.first_touch_campaign as campaign, t.first_touch_term as term,
            t.first_touch_referrer_domain as referrer_domain,
            t.first_touch_landing_page as landing_page, t.first_touch_ad_network as ad_network,
            t.first_touch_gclid as gclid, t.first_touch_gbraid as gbraid,
            t.first_touch_wbraid as wbraid, t.first_touch_msclkid as msclkid,
            t.first_touch_fbclid as fbclid, t.first_touch_ttclid as ttclid
        )                                                               as t
    from {{ ref('int_ga4_user_touchpoints') }} t
    join resolution r
        on  r.product  = t.product
        and r.id_type  = 'ga4_pseudo_id'
        and r.id_value = t.user_pseudo_id

),

-- main_attribution is the same on every event of an AnyTrack visitor: take its earliest event
anytrack as (

    select
        a.product,
        r.user_key,
        'anytrack'                                                      as touch_system,
        a.click_id                                                      as touch_id,
        -- the event's time stands in only for an attribution without its own time: an event
        -- without any attribution proves a conversion, not a recorded visit (no touch, E01)
        coalesce(a.touch_ts,
                 if(coalesce(a.touch_source, a.touch_medium, a.touch_campaign, a.touch_referrer_domain,
                             a.gclid, a.gbraid, a.wbraid, a.msclkid, a.fbclid, a.ttclid) is not null,
                    a.event_ts, null))                                  as touch_ts,
        false                                                           as is_touch_date_only,
        struct(
            a.touch_source as source, a.touch_medium as medium,
            a.touch_campaign as campaign, a.touch_term as term,
            a.touch_referrer_domain as referrer_domain,
            a.touch_landing_page as landing_page, a.ad_network as ad_network,
            a.gclid as gclid, a.gbraid as gbraid, a.wbraid as wbraid,
            a.msclkid as msclkid, a.fbclid as fbclid, a.ttclid as ttclid
        )                                                               as t
    from {{ ref('int_anytrack_events') }} a
    join resolution r
        on  r.product  = a.product
        and r.id_type  = 'anytrack_click_id'
        and r.id_value = a.click_id
    qualify row_number() over (
        partition by a.product, r.user_key, a.click_id order by a.event_ts, a.anytrack_event_key
    ) = 1

),

payment as (

    select
        r.product,
        r.user_key,
        'payment'                                                       as touch_system,
        concat(a.source_account, ':', a.source_customer_id)             as touch_id,
        -- a date: midnight stands for the whole day (is_touch_date_only)
        timestamp(a.attributed_from_date)                               as touch_ts,
        true                                                            as is_touch_date_only,
        struct(
            a.source as source, a.medium as medium, a.campaign as campaign,
            cast(null as string) as term, cast(null as string) as referrer_domain,
            cast(null as string) as landing_page, cast(null as string) as ad_network,
            a.gclid as gclid, cast(null as string) as gbraid, cast(null as string) as wbraid,
            cast(null as string) as msclkid, cast(null as string) as fbclid,
            cast(null as string) as ttclid
        )                                                               as t
    from {{ ref('int_customer_attribution') }} a
    join resolution r
        on  r.id_type  = concat(a.source_system, '_customer')
        and r.id_value = concat(a.source_account, ':', a.source_customer_id)
    where a.attribution_status = 'attributed'
      and a.attributed_from_date is not null

),

-- the ValueTrack network of each ad click id, wherever AnyTrack or GA4 saw it
click_networks as (

    select product, click_id, any_value(ad_network) as ad_network
    from (
        select product, coalesce(gclid, gbraid, wbraid, msclkid, fbclid, ttclid) as click_id, ad_network
        from {{ ref('int_anytrack_events') }}
        where ad_network is not null
        union all
        select product, coalesce(gclid, gbraid, wbraid, msclkid, fbclid, ttclid), ad_network
        from {{ ref('int_ga4_journey_events') }}
        where ad_network is not null and journey_event_kind = 'session_start'
    )
    where click_id is not null
    group by 1, 2

),

unioned as (

    select * from amplitude
    union all
    select * from ga4
    union all
    select * from anytrack
    union all
    select * from payment

),

classified as (

    select
        u.product,
        u.user_key,
        u.touch_system,
        u.touch_id,
        u.touch_ts,
        u.is_touch_date_only,
        u.t.source,
        u.t.medium,
        u.t.campaign,
        u.t.term,
        coalesce(u.t.referrer_domain, {{ kr2_referral_source_domain('u.t.source', 'u.t.medium') }})
                                                                        as referrer_domain,
        u.t.landing_page,
        lower(coalesce(nullif(trim(u.t.ad_network), ''), n.ad_network)) as ad_network,
        case
            when u.t.gclid   is not null then 'gclid'
            when u.t.gbraid  is not null then 'gbraid'
            when u.t.wbraid  is not null then 'wbraid'
            when u.t.msclkid is not null then 'msclkid'
            when u.t.fbclid  is not null then 'fbclid'
            when u.t.ttclid  is not null then 'ttclid'
            else 'none'
        end                                                             as click_id_type,
        coalesce(u.t.gclid, u.t.gbraid, u.t.wbraid, u.t.msclkid, u.t.fbclid, u.t.ttclid) as click_id,
        coalesce(u.t.gclid, u.t.gbraid, u.t.wbraid) is not null         as has_google_click,
        u.t.msclkid is not null                                         as has_ms_click,
        u.t.fbclid is not null                                          as has_meta_click,
        u.t.ttclid is not null                                          as has_tiktok_click,
        {{ kr2_normalize_label('u.t.source') }}                         as src,
        {{ kr2_normalize_label('u.t.medium') }}                         as med,
        {{ kr2_normalize_label('u.t.campaign') }}                       as campaign_norm,
        {{ kr2_is_malformed_label('u.t.source') }}
            or {{ kr2_is_malformed_label('u.t.medium') }}               as is_malformed_label,
        {{ kr2_referrer_class("coalesce(u.t.referrer_domain, " ~ kr2_referral_source_domain('u.t.source', 'u.t.medium') ~ ")",
                              "split(coalesce(g.site_domains, ''), ';')") }}
                                                                        as referrer_class
    from unioned u
    left join registry g on g.product = u.product
    left join click_networks n
        on  n.product  = u.product
        and n.click_id = coalesce(u.t.gclid, u.t.gbraid, u.t.wbraid, u.t.msclkid, u.t.fbclid, u.t.ttclid)

),

methods as (

    select
        *,
        case
            when touch_system = 'anytrack'                              then 'anytrack'
            when touch_system = 'amplitude'                             then 'signup_capture'
            when click_id is not null                                   then 'click_id'
            when src is not null or med is not null                     then 'utm'
            when referrer_class not in ('own', 'payment', 'oauth')      then 'referrer'
            else 'none'
        end                                                             as source_resolution_method
    from classified

)

select
    *,
    (if(has_google_click, 1, 0) + if(has_ms_click, 1, 0) + if(has_meta_click, 1, 0)
     + if(has_tiktok_click, 1, 0))                                      as n_click_platforms,
    case
        when has_google_click then 'google'
        when has_ms_click     then 'microsoft'
        when has_meta_click   then 'meta'
        when has_tiktok_click then 'tiktok'
    end                                                                 as click_platform,
    case
        when src in ('google', 'youtube')                               then 'google'
        when src in ('bing', 'microsoft')                               then 'microsoft'
        when src in ('facebook', 'instagram', 'fb', 'ig', 'meta')       then 'meta'
        when src = 'tiktok'                                             then 'tiktok'
    end                                                                 as source_platform,
    case source_resolution_method
        when 'click_id'       then 1
        when 'utm'            then 2
        when 'referrer'       then 3
        when 'anytrack'       then 4
        when 'signup_capture' then 5
        else 6
    end                                                                 as method_rank
from methods
