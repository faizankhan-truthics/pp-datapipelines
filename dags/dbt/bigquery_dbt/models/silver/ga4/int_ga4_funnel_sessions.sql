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

-- Which "Funnel" value (the dashboard's Project/Funnel filter -- see seed_ga4_payment_account_map) a
-- session entered through, resolved once per session the same way int_ga4_sessions resolves
-- source/medium/campaign: funnel_type is sent sparsely across events, so whatever event in the
-- session actually carries a usable signal fills it for the whole session.
--
-- Also carries campaign_id/gclid (same extraction as int_ga4_sessions, not re-derived here --
-- duplicated rather than reused because int_ga4_sessions loops over seed_ga4_payment_account_map's
-- stitchable properties, which excludes publicregistry/reversly) so sessions can be joined to
-- campaign-level spend/ROAS marts downstream.
--
-- Every property's raw signal and translation below was confirmed against real event data, not
-- assumed from documentation (which undersold or mislabeled several of these -- see
-- seed_ga4_payment_account_map.yml's funnel_signal column for specifics). Properties with
-- has_ga4_export = false (tracelo, fitday) have no usable raw data and are excluded entirely --
-- not approximated.
--
-- Grain: ga4_property_name x user_pseudo_id x ga_session_id. Merged on the session key for the
-- same reason as int_ga4_sessions (session_date can drift across the incremental window).

{% set properties = dbt_utils.get_column_values(
    ref('seed_ga4_payment_account_map'), 'ga4_property_name',
    where='is_dashboard_project and has_ga4_export', default=[]
) %}

with events as (

{% for prop_name in properties %}
    {%- set property_id = env_var('GA4_PROPERTY_ID_' ~ prop_name | upper) %}
    select
        '{{ prop_name }}'                as ga4_property_name,
        parse_date('%Y%m%d', event_date) as ga4_date,
        event_timestamp,
        user_pseudo_id,
        (select ep.value.int_value from unnest(event_params) ep
          where ep.key = 'ga_session_id') as ga_session_id,
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
        ) as gclid,

        {% if prop_name == 'iq_center' %}
        -- test_type is canonical already (iq/adhd/love/autism/personality/eq), confirmed
        -- present on start_test/signup/paid_trial/free_trial events in live data
        (select ep.value.string_value from unnest(event_params) ep
          where ep.key = 'test_type') as funnel

        {% elif prop_name in ('infochecker', 'reversly') %}
        -- funnel is never populated on this property; page_version is, and empirically
        -- correlates with the real landing slug (DEFAULT_PL -> phone-lookup, DEFAULT_IL ->
        -- image-lookup, LP -> track) -- the documented HOME/DEFAULT/DEFAULT_LP table undersold
        -- this. lookup_type backfills when page_version itself didn't fire.
        coalesce(
            case (select ep.value.string_value from unnest(event_params) ep where ep.key = 'page_version')
                when 'DEFAULT_PL' then 'phone-lookup'
                when 'DEFAULT_IL' then 'image-lookup'
                when 'DEFAULT_LP' then 'tracking'
                when 'LP'         then 'track'
                when 'DEFAULT'    then 'track'
            end,
            case (select ep.value.string_value from unnest(event_params) ep where ep.key = 'lookup_type')
                when 'phone' then 'phone-lookup'
                when 'image' then 'image-lookup'
            end
        ) as funnel

        {% elif prop_name == 'location_tool' %}
        -- page_version is more granular live than the TS/LT/FF/LT doc's HOME/DEFAULT table
        -- shows (LP/TRACK/DEFAULT/HOME); correlated against page_location, LP -> /trace entries
        -- and TRACK -> /track entries, leaving DEFAULT as the remaining /tracking ads path
        case (select ep.value.string_value from unnest(event_params) ep where ep.key = 'page_version')
            when 'LP'      then 'trace'
            when 'TRACK'   then 'track'
            when 'DEFAULT' then 'tracking'
        end as funnel

        {% elif prop_name == 'publicregistry' %}
        -- no dedicated event_param exists for this (event_params.funnel is never populated;
        -- funnel_flow exists but carries unrelated DPF/PTF flow codes, not the
        -- property/court/vehicle/license vertical) -- parsed from page_location instead.
        -- Same optional locale (2-3 letters, optional region) and /quiz/ /start/ wrapper parsing as
        -- int_ads_landing_page_day_cost. Allow-listed against Borys's known values so an
        -- unrelated path (dashboard, admin, login, privacy, terms, ...) never leaks in as a
        -- funnel value.
        case regexp_extract(
            lower((select ep.value.string_value from unnest(event_params) ep where ep.key = 'page_location')),
            r'https?://[^/?#]+/(?:[a-z]{2,3}(?:-[a-z]{2})?/)?(?:quiz/|start/)?([^/?#]+)'
        )
            when 'property' then 'property'
            when 'court'    then 'court'
            when 'vehicle'  then 'vehicle'
            when 'license'  then 'license'
            when 'search'   then 'search'
        end as funnel

        {% endif %}

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

),

sessions as (

    select
        ga4_property_name,
        user_pseudo_id,
        ga_session_id,
        min(ga4_date) as session_date,
        -- first non-null signal in the session, same resolution pattern int_ga4_sessions uses for
        -- source/medium/campaign (the field doesn't fire on every event, but the session only has
        -- one real entry point)
        array_agg(funnel      ignore nulls order by event_timestamp limit 1)[safe_offset(0)] as funnel,
        array_agg(campaign_id ignore nulls order by event_timestamp limit 1)[safe_offset(0)] as campaign_id,
        array_agg(gclid       ignore nulls order by event_timestamp limit 1)[safe_offset(0)] as gclid
    from events
    where user_pseudo_id is not null
      and ga_session_id is not null
    group by ga4_property_name, user_pseudo_id, ga_session_id

)

{% if is_incremental() %}
-- The incremental window only re-reads the last few days of events, so a session that started
-- before the window (or whose sparse entry signal fired before it) is re-aggregated from a
-- partial set of events: the window's own "first non-null" could be null or a LATER value than
-- the one already stored. Keep the stored signal and its date when there is one; the window only
-- fills what is still empty.
select
    s.ga4_property_name,
    s.user_pseudo_id,
    s.ga_session_id,
    least(s.session_date, coalesce(t.session_date, s.session_date)) as session_date,
    coalesce(t.funnel,      s.funnel)      as funnel,
    coalesce(t.campaign_id, s.campaign_id) as campaign_id,
    coalesce(t.gclid,       s.gclid)       as gclid
from sessions s
left join {{ this }} t
    on  t.ga4_property_name = s.ga4_property_name
    and t.user_pseudo_id    = s.user_pseudo_id
    and t.ga_session_id     = s.ga_session_id
{% else %}
select * from sessions
{% endif %}
