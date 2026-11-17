-- depends_on: {{ ref('seed_product_registry') }}
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='event_uuid',
    on_schema_change='append_new_columns',
    tags=["journey", "amplitude", "silver", "daily"],
    partition_by={
        "field": "event_date",
        "data_type": "date"
    },
    cluster_by=['product', 'app_user_id', 'amplitude_id']
) }}

-- Slim, deduped Amplitude events for every in-scope product in seed_product_registry, with the
-- identity, stage and first-touch fields the user journey mart needs.
-- event_time is the device clock (seen anywhere from 1978 to 2042), so everything is driven by
-- server time: increments are picked by server_upload_time (merged on uuid, so a re-delivered
-- event updates in place), and event_ts falls back to server_received_time when the device clock
-- is more than amplitude_max_clock_skew_hours off.
-- Emails are never stored: purchase events carry the buyer's email, kept only as its md5, which is
-- the same key int_identity_user_map uses for payers. IP, names, city and the raw property blobs
-- are not selected.
-- amplitude_id is resolved to the last id of its merge chain (MERGE_USERS_<project>), as Amplitude
-- counts users.
-- Merges do not rewrite exported rows, so each incremental run also reloads every event whose
-- exported amplitude_id is in the merge table, picking up new and changed merges.
-- Grain: one row per Amplitude event (uuid).

{% set products = registry_products('amplitude_source_table') %}
{% if execute and products | length == 0 %}
    {{ exceptions.raise_compiler_error("int_amplitude_events: no in-scope product with an amplitude_source_table in seed_product_registry") }}
{% endif %}

with merged_ids as (

{% for p in products %}
    select
        '{{ p.product }}' as product,
        cast(amplitude_id as string) as amplitude_id,
        cast(merged_amplitude_id as string) as merged_amplitude_id,
        merge_server_time
    from {{ source('amplitude', p.amplitude_source_table | replace('events_', 'merge_users_')) }}
    {%- if not loop.last %}

    union all

    {% endif -%}
{% endfor %}

),

merged_ids_latest as (

    select product, amplitude_id, merged_amplitude_id
    from merged_ids
    qualify row_number() over (
        partition by product, amplitude_id order by merge_server_time desc
    ) = 1

),

-- follows chains up to 5 hops to the last id. Not recursive: BigQuery does not
-- allow WITH RECURSIVE inside the incremental MERGE. tests/test_amplitude_events_ids_fully_merged
-- warns if a longer chain appears.
merged_ids_resolved as (

    select
        h0.product,
        h0.amplitude_id,
        coalesce(
            {%- for i in range(4, 0, -1) %}
            h{{ i }}.merged_amplitude_id,
            {%- endfor %}
            h0.merged_amplitude_id
        ) as merged_amplitude_id
    from merged_ids_latest as h0
    {%- for i in range(1, 5) %}
    left join merged_ids_latest as h{{ i }}
        on h{{ i }}.product = h0.product and h{{ i }}.amplitude_id = h{{ i - 1 }}.merged_amplitude_id
    {%- endfor %}

),

src as (

{% for p in products %}
    select
        '{{ p.product }}' as product,
        uuid,
        amplitude_id,
        device_id,
        user_id,
        session_id,
        event_type,
        event_time,
        server_received_time,
        server_upload_time,
        platform,
        country,
        device_type,
        event_properties,
        user_properties
    from {{ source('amplitude', p.amplitude_source_table) }}
    {% if is_incremental() %}
    where server_upload_time > (
        select coalesce(
            timestamp_sub(max(server_upload_time), interval 3 hour),
            timestamp('1970-01-01')
        )
        from {{ this }}
        where product = '{{ p.product }}'
    )
    or cast(amplitude_id as string) in (
        select amplitude_id from merged_ids_latest where product = '{{ p.product }}'
    )
    {% endif %}
    {%- if not loop.last %}

    union all

    {% endif -%}
{% endfor %}

),

deduped as (

    select s.* replace (
        coalesce(m.merged_amplitude_id, cast(s.amplitude_id as string)) as amplitude_id
    )
    from src as s
    left join merged_ids_resolved as m
        on m.product = s.product and m.amplitude_id = cast(s.amplitude_id as string)
    qualify row_number() over (partition by s.uuid order by s.server_upload_time desc) = 1

)

select
    cast(uuid as string)                                                    as event_uuid,
    product,
    case
        when abs(timestamp_diff(event_time, server_received_time, hour))
             <= {{ var('amplitude_max_clock_skew_hours', 48) }}
            then event_time
        else server_received_time
    end                                                                     as event_ts,
    date(case
        when abs(timestamp_diff(event_time, server_received_time, hour))
             <= {{ var('amplitude_max_clock_skew_hours', 48) }}
            then event_time
        else server_received_time
    end)                                                                    as event_date,
    event_time                                                              as device_event_time,
    server_received_time,
    server_upload_time,
    event_type,
    platform = 'node'                                                       as is_server_event,
    amplitude_id,
    -- server events carry a synthetic device id; never use it for device links
    if(platform = 'node', null, nullif(trim(device_id), ''))                as device_id,
    nullif(trim(user_id), '')                                               as app_user_id,
    -- the web account id some products send as event_properties.user_id (reversly: on purchase
    -- events, next to the app's 24-hex user_id). GA4 user_id and Stripe mc_user_id carry it (E16)
    nullif(trim(json_value(event_properties, '$.user_id')), '')             as site_user_id,
    session_id,
    platform,
    country,
    device_type,
    nullif(json_value(event_properties, '$.order_id'), '')                  as order_id,
    json_value(event_properties, '$.screen_id')                             as screen_id,
    json_value(event_properties, '$.signup_type')                           as signup_type,
    json_value(event_properties, '$.funnel_name')                           as funnel_name,
    -- A/B test fields. The flag key and variant are only on [Experiment] Exposure events;
    -- landing_tool (the landing page, e.g. video-gen_generic) is on signup, trial and purchase events.
    -- Rows loaded before these columns existed stay null until a full refresh.
    json_value(event_properties, '$."[Experiment] Flag Key"')               as experiment_flag_key,
    lower(json_value(event_properties, '$."[Experiment] Variant"'))         as experiment_variant,
    coalesce(
        json_value(user_properties, '$.tool'),
        json_value(event_properties, '$.tool')
    )                                                                       as landing_tool,
    json_value(event_properties, '$.product_name')                          as plan_name,
    -- the buyer's email on purchase events; else the account email some products (publicregistry)
    -- keep in user_properties
    to_hex(md5(coalesce(
        case
            when event_type in ('purchase_started', 'purchase', 'purchase_failed')
                then nullif(lower(trim(json_value(event_properties, '$.email'))), '')
        end,
        nullif(lower(trim(json_value(user_properties, '$.email'))), '')
    )))                                                                     as email_user_key,
    {%- for f in ['utm_source', 'utm_medium', 'utm_campaign', 'utm_term', 'utm_content',
                  'gclid', 'gbraid', 'wbraid', 'msclkid', 'fbclid', 'ttclid', 'referring_domain'] %}
    nullif(nullif(json_value(user_properties, '$.initial_{{ f }}'), 'EMPTY'), '') as initial_{{ f }},
    {%- endfor %}
    -- query string dropped: referrer URLs can carry tokens or emails
    regexp_replace(
        nullif(nullif(json_value(user_properties, '$.initial_referrer'), 'EMPTY'), ''),
        r'[?#].*$', ''
    )                                                                       as initial_referrer
from deduped
