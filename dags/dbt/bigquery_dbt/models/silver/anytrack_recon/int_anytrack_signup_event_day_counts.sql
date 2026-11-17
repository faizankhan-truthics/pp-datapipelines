{{ config(materialized='table') }}

-- AnyTrack reports this event as lowercase 'signup'; relabeled to 'Signup' here so
-- it lines up with int_files_editor_signup_event_day_counts.sql as the SAME row in
-- the gold join, instead of two separate rows differing only by case.

select
    date(event_time) as event_date,
    'Signup' as event_name,
    count(distinct client_id) as anytrack_count
from {{ source('bronze_anytrack', 'anytrack_files_editor_events') }}
where event_name = 'signup'
group by event_date
