-- depends_on: {{ ref('seed_product_registry') }}
{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='append_new_columns',
    full_refresh=false,
    tags=["journey", "ga4", "silver", "daily"],
    partition_by={
        "field": "ga4_date",
        "data_type": "date"
    },
    cluster_by=['product', 'user_pseudo_id']
) }}

-- The GA4 events the user journey mart needs, for every in-scope product in seed_product_registry
-- that has a GA4 export: session starts (landing page, referrer, UTMs and click ids = first touch),
-- the product's signup events, and its checkout events, whose transaction_id is the Solidgate
-- order_id (the GA4 -> Solidgate link, E7+).
-- full_refresh=false: the raw export may expire old days, so this table is the long-term GA4
-- history for the journey mart. Each run rewrites only the last ga4_lookback_days partitions.
-- The email / first_name / last_name / postal_code params GA4 receives are never read.
-- Grain: one row per GA4 event kept (no event id in the export; exact duplicates from the
-- streaming export's at-least-once delivery are dropped).

{% set products = registry_products('ga4_property_name') %}

with events as (

{% for p in products %}
    {%- set property_id = env_var('GA4_PROPERTY_ID_' ~ p.ga4_property_name | upper) %}
    select
        '{{ p.product }}'                                   as product,
        parse_date('%Y%m%d', event_date)                    as ga4_date,
        timestamp_micros(event_timestamp)                   as event_ts,
        user_pseudo_id,
        -- the site's account id, set on signup / checkout events by some products (E15)
        nullif(trim(user_id), '')                           as user_id,
        (select ep.value.int_value from unnest(event_params) ep
          where ep.key = 'ga_session_id')                   as ga_session_id,
        event_name,
        case when event_name in ({{ registry_in_list(p.ga4_checkout_events) }}) then
            (select ep.value.string_value from unnest(event_params) ep
              where ep.key = 'transaction_id' and ep.value.string_value is not null limit 1)
        end                                                 as transaction_id,
        (select ep.value.string_value from unnest(event_params) ep
          where ep.key = 'page_location')                   as page_location,
        (select ep.value.string_value from unnest(event_params) ep
          where ep.key = 'page_referrer')                   as page_referrer,
        collected_traffic_source.manual_source              as cts_source,
        collected_traffic_source.manual_medium              as cts_medium,
        collected_traffic_source.manual_campaign_name       as cts_campaign,
        collected_traffic_source.gclid                      as cts_gclid,
        case
            when event_name = 'session_start' then 'session_start'
            when {{ registry_event_filter(p.signup_events, 'ga4', 'event_name') }} then 'signup'
            when event_name in ({{ registry_in_list(p.ga4_checkout_events) }}) then 'checkout'
        end                                                 as journey_event_kind
    from {{ source('analytics_' ~ property_id, 'events_intraday_*') }}
    where (
            event_name = 'session_start'
         or {{ registry_event_filter(p.signup_events, 'ga4', 'event_name') }}
         or event_name in ({{ registry_in_list(p.ga4_checkout_events) }})
      )
    {% if is_incremental() %}
      and _table_suffix >= format_date(
          '%Y%m%d',
          date_sub(current_date(), interval {{ var('ga4_lookback_days', 3) }} day)
      )
    {% endif %}
    {%- if not loop.last %}

    union all

    {% endif -%}
{% endfor %}

),

-- the click / campaign ids sit on the landing URL of the session start, and on the referrer of
-- the signup event (the landing page the visitor came in on)
parsed as (

    select
        *,
        coalesce(page_location, '') || ' ' || coalesce(page_referrer, '') as urls
    from events

)

select distinct
    product,
    ga4_date,
    event_ts,
    user_pseudo_id,
    user_id,
    ga_session_id,
    event_name,
    journey_event_kind,
    transaction_id,
    regexp_replace(page_location, r'[?#].*$', '')                           as landing_page,
    net.host(page_referrer)                                                 as page_referrer_domain,
    coalesce(cts_source,   regexp_extract(urls, r'[?&]utm_source=([^&# ]+)'))   as utm_source,
    coalesce(cts_medium,   regexp_extract(urls, r'[?&]utm_medium=([^&# ]+)'))   as utm_medium,
    coalesce(cts_campaign, regexp_extract(urls, r'[?&]utm_campaign=([^&# ]+)')) as utm_campaign,
    regexp_extract(urls, r'[?&]utm_term=([^&# ]+)')                         as utm_term,
    coalesce(cts_gclid,    regexp_extract(urls, r'[?&]gclid=([^&# ]+)'))    as gclid,
    regexp_extract(urls, r'[?&]gbraid=([^&# ]+)')                           as gbraid,
    regexp_extract(urls, r'[?&]wbraid=([^&# ]+)')                           as wbraid,
    regexp_extract(urls, r'[?&]msclkid=([^&# ]+)')                          as msclkid,
    regexp_extract(urls, r'[?&]fbclid=([^&# ]+)')                           as fbclid,
    regexp_extract(urls, r'[?&]ttclid=([^&# ]+)')                           as ttclid,
    -- Google / Microsoft ValueTrack network (g, s, x, d, o, a, ...); an unexpanded {network}
    -- placeholder is no value. Filled for partitions written since the column was added.
    nullif(regexp_extract(urls, r'[?&]network=([^&#{} ]+)'), '')            as ad_network
from parsed
where user_pseudo_id is not null
