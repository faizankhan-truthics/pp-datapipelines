{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['ga4_property_name', 'user_pseudo_id', 'ga_session_id'],
    on_schema_change='append_new_columns',
    tags=["ga4", "silver", "daily"],
    partition_by={
        "field": "session_date",
        "data_type": "date"
    }
) }}

-- Where each GA4 session came from. The traffic source is only recorded on the events that
-- open a session, so a purchase event carries the transaction id and no source at all --
-- resolving it once per session lets anything inside that session inherit it.
-- Grain: ga4_property_name x user_pseudo_id x ga_session_id.
--
-- Merged on the session key rather than overwritten by partition. session_date is
-- min(ga4_date) over whatever events sit in the rolling read window, so a session spanning
-- two or more days resolved to its real first day while that day was in the window and to a
-- later one once the day aged out. Under insert_overwrite that wrote the session into a
-- second partition the run no longer owned, and the original row was never replaced --
-- the uniqueness test failed three times on rows left behind that way. Merge finds the
-- session by its own key, so a date that moves updates the row instead of duplicating it.

{% set property_names = dbt_utils.get_column_values(
    ref('seed_ga4_payment_account_map'), 'ga4_property_name', where='is_stitchable', default=[]
) %}

with events as (

{% for prop_name in property_names %}
    {%- set property_id = env_var('GA4_PROPERTY_ID_' ~ prop_name | upper) %}
    select
        '{{ prop_name }}'                as ga4_property_name,
        '{{ property_id }}'              as ga4_property_id,
        parse_date('%Y%m%d', event_date) as ga4_date,
        event_timestamp,
        user_pseudo_id,
        (select ep.value.int_value from unnest(event_params) ep
          where ep.key = 'ga_session_id') as ga_session_id,
        -- each field lands in a different place depending on how the site is tagged
        coalesce(
            collected_traffic_source.manual_source,
            (select ep.value.string_value from unnest(event_params) ep where ep.key = 'source'),
            (select ep.value.string_value from unnest(event_params) ep where ep.key = 'utm_source'),
            regexp_extract(
                (select ep.value.string_value from unnest(event_params) ep
                  where ep.key = 'page_location'), r'[?&]utm_source=([^&]+)')
        ) as source,
        coalesce(
            collected_traffic_source.manual_medium,
            (select ep.value.string_value from unnest(event_params) ep where ep.key = 'medium'),
            (select ep.value.string_value from unnest(event_params) ep where ep.key = 'utm_medium'),
            regexp_extract(
                (select ep.value.string_value from unnest(event_params) ep
                  where ep.key = 'page_location'), r'[?&]utm_medium=([^&]+)')
        ) as medium,
        coalesce(
            collected_traffic_source.manual_campaign_name,
            (select ep.value.string_value from unnest(event_params) ep where ep.key = 'campaign'),
            (select ep.value.string_value from unnest(event_params) ep where ep.key = 'utm_campaign'),
            regexp_extract(
                (select ep.value.string_value from unnest(event_params) ep
                  where ep.key = 'page_location'), r'[?&]utm_campaign=([^&]+)')
        ) as campaign,
        coalesce(
            collected_traffic_source.manual_campaign_id,
            session_traffic_source_last_click.google_ads_campaign.campaign_id,
            (select ep.value.string_value from unnest(event_params) ep where ep.key = 'campaign_id'),
            (select ep.value.string_value from unnest(event_params) ep where ep.key = 'gad_campaignid'),
            regexp_extract(
                (select ep.value.string_value from unnest(event_params) ep
                  where ep.key = 'page_location'), r'[?&](?:utm_id|campaignId|gad_campaignid)=([^&]+)')
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
        date_sub(current_date(), interval {{ var('ga4_lookback_days', 3) | int + 1 }} day)
    )
    {% endif %}
    {%- if not loop.last %}

    union all

    {% endif -%}
{% endfor %}

)

select
    ga4_property_name,
    any_value(ga4_property_id)                                      as ga4_property_id,
    user_pseudo_id,
    ga_session_id,
    min(ga4_date)                                                   as session_date,
    -- per field, not per row: the campaign id and the source do not always arrive on
    -- the same event
    array_agg(source      ignore nulls order by event_timestamp limit 1)[safe_offset(0)] as source,
    array_agg(medium      ignore nulls order by event_timestamp limit 1)[safe_offset(0)] as medium,
    array_agg(campaign    ignore nulls order by event_timestamp limit 1)[safe_offset(0)] as campaign,
    array_agg(campaign_id ignore nulls order by event_timestamp limit 1)[safe_offset(0)] as campaign_id,
    array_agg(gclid       ignore nulls order by event_timestamp limit 1)[safe_offset(0)] as gclid
from events
where user_pseudo_id is not null
  and ga_session_id is not null
group by ga4_property_name, user_pseudo_id, ga_session_id
