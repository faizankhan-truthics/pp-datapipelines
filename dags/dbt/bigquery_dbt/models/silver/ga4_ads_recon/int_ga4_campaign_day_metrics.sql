{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='campaign_day_key',
    on_schema_change='append_new_columns',
    partition_by={
        "field": "date",
        "data_type": "date"
    }
    )
}}


{% set rescan_days = 4 %}
-- has_ga4_export is a DIFFERENT filter than is_stitchable (what int_ga4_transactions.sql uses):
-- this mart attributes via purchase events, not transaction_id matching, so files_editor/
-- view_page (is_stitchable = false) are still fine here. simpl_ai fails both -- no BigQuery
-- Export dataset exists for it at all. See the seed's own yml for the full explanation.
{% set property_names = dbt_utils.get_column_values(
    ref('seed_ga4_payment_account_map'), 'ga4_property_name', where='has_ga4_export', default=[]
) %}

with ga4_raw as (

{% for prop_name in property_names %}
    {%- set property_id = env_var('GA4_PROPERTY_ID_' ~ prop_name | upper) %}
    select
        '{{ property_id }}' as property_id,
        user_pseudo_id,
        event_timestamp,
        event_name,
        parse_date('%Y%m%d', event_date) as event_date,
        (select ep.value.int_value from unnest(event_params) ep where ep.key = 'ga_session_id')     as ga_session_id,
        (select ep.value.int_value from unnest(event_params) ep where ep.key = 'batch_page_id')     as batch_page_id,
        (select ep.value.int_value from unnest(event_params) ep where ep.key = 'batch_event_index') as batch_event_index,
        (select ep.value.int_value from unnest(event_params) ep where ep.key = 'batch_ordering_id') as batch_ordering_id,
        event_value_in_usd,
        coalesce(
            collected_traffic_source.manual_campaign_id,
            (select ep.value.string_value from unnest(event_params) ep where ep.key = 'gad_campaignid'),
            (select ep.value.string_value from unnest(event_params) ep where ep.key = 'campaign_id'),
            (select ep.value.string_value from unnest(event_params) ep where ep.key = 'utm_id'),
            regexp_extract(
                (select ep.value.string_value from unnest(event_params) ep where ep.key = 'page_location'),
                r'[?&](?:campaign_id|gad_campaignid)=([^&]+)'
            )
        ) as campaign_id,
        coalesce(
            collected_traffic_source.gclid,
            (select ep.value.string_value from unnest(event_params) ep where ep.key = 'gclid'),
            (select up.value.string_value from unnest(user_properties) up where up.key = 'gclid')
        ) as gclid
    from {{ source('analytics_' ~ property_id, 'events_intraday_*') }}
    {% if is_incremental() %}
    where _table_suffix >= format_date('%Y%m%d', date_sub(current_date(), interval {{ rescan_days }} day))
    {% endif %}
    {%- if not loop.last %}

    union all

    {% endif -%}
{% endfor %}

),

ga4_dedup as (
    -- property_id is in the partition key even though user_pseudo_id is already
    -- effectively unique per property -- without it, a chance collision between two
    -- DIFFERENT properties' pseudo ids would silently dedup away a real event from one
    -- of them.
    select *,
        row_number() over (
            partition by property_id, user_pseudo_id, event_timestamp, event_name, batch_page_id, batch_event_index, batch_ordering_id
            order by event_timestamp
        ) as rn
    from ga4_raw
),

-- forward-fill gclid AND campaign_id within the session: collected_traffic_source is typically
-- only populated on the session-opening event in the GA4 export, so a later event in the same
-- session otherwise loses attribution entirely even though the session clearly had one (confirmed
-- via ad hoc diagnostic before this model was built: one property had sessions correctly
-- attributed but almost no purchase events carrying their own campaign_id until this fix -- went
-- from 8 to 22,715 purchase events with a campaign_id).
ga4_events as (
    select
        * except(rn),
        last_value(gclid ignore nulls) over (
            partition by property_id, user_pseudo_id, ga_session_id
            order by event_timestamp
            rows between unbounded preceding and current row
        ) as gclid_ff,
        last_value(campaign_id ignore nulls) over (
            partition by property_id, user_pseudo_id, ga_session_id
            order by event_timestamp
            rows between unbounded preceding and current row
        ) as campaign_id_ff
    from ga4_dedup
    where rn = 1
),

ga4_revenue as (
    -- attributable revenue: purchase rows with BOTH campaign_id and gclid resolved (forward-fill
    -- included). Confirmed via source/medium breakdown that the ~20% attribution rate this
    -- produces is expected, not a bug -- most of the rest is genuinely non-Google-Ads traffic
    -- (direct/no-referrer, QR-code scans, Bing Ads, organic search).
    select property_id, campaign_id_ff as campaign_id, event_date as date,
        sum(event_value_in_usd) as ga4_revenue_usd
    from ga4_events
    where event_name = 'purchase' and campaign_id_ff is not null and gclid_ff is not null
    group by 1, 2, 3
),

ga4_sessions as (
    select property_id, campaign_id_ff as campaign_id, event_date as date,
        count(distinct concat(user_pseudo_id, '-', cast(ga_session_id as string))) as sessions
    from ga4_events
    where event_name = 'session_start' and campaign_id_ff is not null
    group by 1, 2, 3
),

ga4_new_users as (
    select property_id, campaign_id_ff as campaign_id, event_date as date,
        count(distinct user_pseudo_id) as new_users
    from ga4_events
    where event_name = 'first_visit' and campaign_id_ff is not null
    group by 1, 2, 3
)

select
    coalesce(r.property_id, s.property_id, n.property_id) as property_id,
    coalesce(r.campaign_id, s.campaign_id, n.campaign_id) as campaign_id,
    coalesce(r.date, s.date, n.date) as date,
    concat(
        coalesce(r.property_id, s.property_id, n.property_id), '-',
        coalesce(r.campaign_id, s.campaign_id, n.campaign_id), '-',
        cast(coalesce(r.date, s.date, n.date) as string)
    ) as campaign_day_key,
    r.ga4_revenue_usd,
    s.sessions,
    n.new_users
from ga4_revenue r
full outer join ga4_sessions s using (property_id, campaign_id, date)
full outer join ga4_new_users n using (property_id, campaign_id, date)
