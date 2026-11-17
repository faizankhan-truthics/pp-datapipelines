{{ config(
    materialized='view',
    tags=["amplitude", "silver", "daily"]
    )
}}

-- Grain: one Amplitude experiment exposure or conversion event, read by GrowthBook.
-- event_properties is never passed through: purchase events carry PII.

with src as (
    select
        _table_suffix as amplitude_project_id,
        uuid,
        device_id,
        user_id,
        platform,
        event_time,
        server_received_time,
        server_upload_time,
        event_type,
        event_properties
    -- wildcard over every prod project, so new projects are picked up automatically
    from {{ source('amplitude', 'events_all') }}
    where event_type in (
            '[Experiment] Exposure',
            'purchase',
            'purchase_failed',
            'trial_started',
            'subscription_converted',
            'subscription_cancel'
        )
),

events as (
    select
        amplitude_project_id,
        uuid as event_id,
        -- server events carry a synthetic device id; never use it for device links
        if(platform = 'node', null, nullif(trim(device_id), '')) as device_id,
        nullif(trim(user_id), '') as user_id,
        -- event_time is the device clock; fall back to server time when it is far off
        case
            when abs(timestamp_diff(event_time, server_received_time, hour))
                 <= {{ var('amplitude_max_clock_skew_hours', 48) }}
                then event_time
            else server_received_time
        end as event_ts,
        event_type,
        if(
            event_type = '[Experiment] Exposure',
            json_value(event_properties, '$."[Experiment] Flag Key"'),
            null
        ) as experiment_id,
        if(
            event_type = '[Experiment] Exposure',
            lower(json_value(event_properties, '$."[Experiment] Variant"')),
            null
        ) as variant,
        if(
            event_type = '[Experiment] Exposure',
            null,
            safe_cast(json_value(event_properties, '$.price') as float64)
        ) as amount_local,
        if(
            event_type = '[Experiment] Exposure',
            null,
            upper(coalesce(
                json_value(event_properties, '$.currency_code'),
                json_value(event_properties, '$.currency')
            ))
        ) as currency
    from src
    -- the export occasionally loads an event twice (exact copies)
    qualify row_number() over (
        partition by uuid, event_time
        order by server_upload_time
    ) = 1
)

select
    e.event_id,
    e.amplitude_project_id,
    e.device_id,
    e.user_id,
    e.event_ts as timestamp,
    if(e.event_type = '[Experiment] Exposure', 'exposure', 'conversion') as event_category,
    e.event_type,
    e.experiment_id,
    -- GrowthBook variation keys
    case
        when e.variant = 'control' then '0'
        when e.variant = 'test' then '1'
        when e.variant in ('b', 'c', 'd', 'e') then e.variant
    end as variation_id,
    e.amount_local,
    e.currency,
    case
        when e.currency = 'USD' then e.amount_local
        else e.amount_local * r.rate
    end as amount_usd
from events e
left join {{ ref('int_currency_conversion') }} r
    on r.exchange_date = date(e.event_ts)
    and r.currency = e.currency
-- GrowthBook needs an identifier: device_id for client events, user_id for server events
where e.device_id is not null
    or e.user_id is not null
