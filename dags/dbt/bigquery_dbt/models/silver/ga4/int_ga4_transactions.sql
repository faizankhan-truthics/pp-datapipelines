{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='append_new_columns',
    tags=["ga4", "silver", "daily"],
    partition_by={
        "field": "ga4_date",
        "data_type": "date"
    }
) }}

-- Transaction ids as seen by GA4, across every property confirmed to emit one that matches a
-- real payment record (see seeds/seed_ga4_payment_account_map.csv, is_stitchable = true). Grain:
-- ga4_date x ga4_property_name x transaction_id.
{% set property_names = dbt_utils.get_column_values(
    ref('seed_ga4_payment_account_map'), 'ga4_property_name', where='is_stitchable', default=[]
) %}

with events as (

{% for prop_name in property_names %}
    {%- set property_id = env_var('GA4_PROPERTY_ID_' ~ prop_name | upper) %}
    select
        '{{ prop_name }}'                  as ga4_property_name,
        '{{ property_id }}'                as ga4_property_id,
        parse_date('%Y%m%d', event_date)   as ga4_date,
        -- carried so the traffic source can be filled from the session below
        user_pseudo_id,
        (select ep.value.int_value from unnest(event_params) ep
          where ep.key = 'ga_session_id')  as ga_session_id,
        coalesce(
            ecommerce.transaction_id,
            (select ep.value.string_value from unnest(event_params) ep
              where ep.key = 'transaction_id'
                and ep.value.string_value is not null limit 1),
            (select up.value.string_value from unnest(user_properties) up
              where up.key = 'transaction_id'
                and up.value.string_value is not null limit 1)
        ) as transaction_id,
        case
            when ecommerce.transaction_id is not null then 'ecommerce'
            when (select ep.value.string_value from unnest(event_params) ep
                   where ep.key = 'transaction_id'
                     and ep.value.string_value is not null limit 1) is not null
                then 'event_params'
            else 'user_properties'
        end as id_source,
        -- each field lands in a different place depending on how the site is tagged
        coalesce(
            collected_traffic_source.manual_source,
            (select ep.value.string_value from unnest(event_params) ep where ep.key = 'utm_source'),
            regexp_extract(
                (select ep.value.string_value from unnest(event_params) ep
                  where ep.key = 'page_location'), r'[?&]utm_source=([^&]+)')
        ) as source,
        coalesce(
            collected_traffic_source.manual_medium,
            (select ep.value.string_value from unnest(event_params) ep where ep.key = 'utm_medium'),
            regexp_extract(
                (select ep.value.string_value from unnest(event_params) ep
                  where ep.key = 'page_location'), r'[?&]utm_medium=([^&]+)')
        ) as medium,
        coalesce(
            collected_traffic_source.manual_campaign_name,
            (select ep.value.string_value from unnest(event_params) ep where ep.key = 'utm_campaign'),
            regexp_extract(
                (select ep.value.string_value from unnest(event_params) ep
                  where ep.key = 'page_location'), r'[?&]utm_campaign=([^&]+)')
        ) as campaign,
        -- the keys to Google Ads campaign spend
        coalesce(
            collected_traffic_source.manual_campaign_id,
            session_traffic_source_last_click.google_ads_campaign.campaign_id,
            (select ep.value.string_value from unnest(event_params) ep where ep.key = 'gad_campaignid'),
            regexp_extract(
                (select ep.value.string_value from unnest(event_params) ep
                  where ep.key = 'page_location'), r'[?&]gad_campaignid=([^&]+)')
        ) as campaign_id,
        coalesce(
            collected_traffic_source.gclid,
            (select ep.value.string_value from unnest(event_params) ep where ep.key = 'gclid'),
            regexp_extract(
                (select ep.value.string_value from unnest(event_params) ep
                  where ep.key = 'page_location'), r'[?&]gclid=([^&]+)'),
            (select up.value.string_value from unnest(user_properties) up where up.key = 'gclid')
        ) as gclid
    from {{ source('analytics_' ~ property_id, 'events_intraday_*') }}
    {% if is_incremental() %}
    where _table_suffix >= format_date(
        '%Y%m%d',
        date_sub(current_date(), interval {{ var('ga4_lookback_days', 3) }} day)
    )
    {% endif %}
    {%- if not loop.last %}

    union all

    {% endif -%}
{% endfor %}

),

-- Grain is (property, date, transaction_id): some events carry the same transaction_id in both
-- ecommerce.transaction_id AND event_params on the same day (confirmed on location_tool/
-- qr_generator_ai) -- plain `distinct` over all 5 columns lets both survive as separate rows.
-- Keep one row per key, preferring the higher-priority id_source (same priority order as the
-- coalesce above). This also absorbs the streaming export's at-least-once delivery.
deduped as (

    select
        ga4_property_name,
        ga4_property_id,
        ga4_date,
        user_pseudo_id,
        ga_session_id,
        transaction_id,
        id_source,
        source,
        medium,
        campaign,
        campaign_id,
        gclid
    from events
    where transaction_id is not null
      and transaction_id != ''
    qualify row_number() over (
        partition by ga4_property_name, ga4_date, transaction_id
        order by case id_source
            when 'ecommerce' then 1
            when 'event_params' then 2
            else 3
        end,
        -- among equal-priority events, prefer one that carries a traffic source
        source is null,
        gclid is null,
        campaign_id is null
    ) = 1

)

-- _elt_account_name is only meaningful for Solidgate-backed properties (Stripe-backed ones have
-- no Solidgate account at all) -- populated from the same seed the gold mart joins against, so
-- consumers keyed on Solidgate account (e.g. ga4_solidgate_match_daily) can use this model
-- directly without knowing about payment_source.
select
    d.ga4_property_name,
    d.ga4_property_id,
    d.ga4_date,
    d.transaction_id,
    d.id_source,
    -- the purchase event rarely carries a traffic source of its own, so fall back to
    -- the session it happened in
    coalesce(d.source,      s.source)      as source,
    coalesce(d.medium,      s.medium)      as medium,
    coalesce(d.campaign,    s.campaign)    as campaign,
    coalesce(d.campaign_id, s.campaign_id) as campaign_id,
    coalesce(d.gclid,       s.gclid)       as gclid,
    case
        when d.source is not null then 'event'
        when s.source is not null then 'session'
    end as source_scope,
    case when m.payment_source = 'solidgate' then m.account_key end as _elt_account_name
from deduped d
left join {{ ref('int_ga4_sessions') }} s
    on  d.ga4_property_name = s.ga4_property_name
    and d.user_pseudo_id    = s.user_pseudo_id
    and d.ga_session_id     = s.ga_session_id
left join {{ ref('seed_ga4_payment_account_map') }} m
    on d.ga4_property_name = m.ga4_property_name
