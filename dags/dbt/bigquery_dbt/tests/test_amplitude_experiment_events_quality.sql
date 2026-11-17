-- All error-level checks on int_amplitude_experiment_events; failure_reason names the check.
-- Row checks cover the last 3 days only: the model is a view over the full Amplitude export.
with recent as (
    select *
    from {{ ref('int_amplitude_experiment_events') }}
    where timestamp >= timestamp_sub(current_timestamp(), interval 3 day)
),

row_failures as (
    select
        event_id,
        case
            when event_id is null or amplitude_project_id is null or event_category is null or event_type is null
                or (device_id is null and user_id is null)
                then 'missing_required_field'
            when event_category = 'exposure' and device_id is null
                then 'exposure_missing_device_id'
            when event_category = 'exposure' and (experiment_id is null or variation_id is null)
                then 'exposure_missing_experiment_or_variant'
            when event_category not in ('exposure', 'conversion')
                or event_type not in (
                    '[Experiment] Exposure', 'purchase', 'purchase_failed',
                    'trial_started', 'subscription_converted', 'subscription_cancel'
                )
                or variation_id not in ('0', '1', 'b', 'c', 'd', 'e')
                then 'unexpected_value'
            when event_category = 'exposure'
                and (amount_local is not null or currency is not null or amount_usd is not null)
                then 'exposure_with_amount'
            when event_category = 'conversion' and (experiment_id is not null or variation_id is not null)
                then 'conversion_with_experiment'
            when amount_usd < 0
                then 'negative_amount_usd'
        end as failure_reason
    from recent
),

-- duplicates are dropped in the model, so each Amplitude event uuid must appear once
duplicate_event_ids as (
    select
        event_id,
        'duplicate_event_id' as failure_reason
    from recent
    group by event_id
    having count(*) > 1
),

-- Amplitude exports hourly; no new event in 100 hours means the export has stopped
stale_export as (
    select
        cast(null as string) as event_id,
        'no_events_in_100h' as failure_reason
    from (
        select count(*) as events_last_100h
        from {{ source('amplitude', 'events_all') }}
        where server_upload_time >= timestamp_sub(current_timestamp(), interval 100 hour)
    )
    where events_last_100h = 0
)

select * from row_failures where failure_reason is not null
union all
select * from duplicate_event_ids
union all
select * from stale_export
