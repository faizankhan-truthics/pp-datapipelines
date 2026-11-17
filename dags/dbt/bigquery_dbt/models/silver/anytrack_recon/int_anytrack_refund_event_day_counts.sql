{{ config(materialized='table') }}

-- AnyTrack-side counterpart to int_files_editor_refund_event_day_counts.sql.
-- No 'Refund (CFT Trial)' here either, matching the Bravo side -- see that
-- model's docstring for why.

select
    date(event_time) as event_date,
    event_name,
    count(distinct transaction_id) as anytrack_count
from {{ source('bronze_anytrack', 'anytrack_files_editor_events') }}
where event_name in (
    'Refund (DPF Trial)', 'Refund (DPF Activation)', 'Refund (DPF Renewal)',
    'Refund (CFT Activation)', 'Refund (CFT Renewal)'
)
group by date(event_time), event_name
