{{ config(materialized='table') }}

-- AnyTrack-side counterpart to int_files_editor_subscription_event_day_counts.sql.
-- AnyTrack's event_name values for these 6 events already match the Bravo-side
-- labels exactly (DPF Trial, CFT Activation, etc.) -- no relabeling needed here,
-- unlike the signup event (see int_anytrack_signup_event_day_counts.sql).

select
    date(event_time) as event_date,
    event_name,
    count(distinct transaction_id) as anytrack_count
from {{ source('bronze_anytrack', 'anytrack_files_editor_events') }}
where event_name in (
    'DPF Trial', 'DPF Activation', 'DPF Renewal',
    'CFT Trial', 'CFT Activation', 'CFT Renewal'
)
group by date(event_time), event_name
