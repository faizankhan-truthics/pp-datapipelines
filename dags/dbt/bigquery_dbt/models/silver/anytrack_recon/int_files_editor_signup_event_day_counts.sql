{{ config(materialized='table') }}

-- Signup counts for files_editor -- every client record, regardless of whether
-- they ever converted to a paying customer (that's the whole point of anchoring
-- on billing_clients rather than the order/payment tables: a signup exists the
-- moment an account is created, before any payment flow starts).

select
    date(created_at) as event_date,
    'Signup' as event_name,
    count(distinct id) as bravo_count
from {{ source('bronze_files_editor_db', 'files_editor_billing_clients') }}
where created_at is not null
  and deleted_at is null
group by event_date
