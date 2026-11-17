-- AnyTrack <-> files_editor billing reconciliation at event_date x event_name
-- grain. bravo_count/anytrack_count column names match the original ticket's
-- requested fields (the Preset-facing contract Manya's dashboard expects), even
-- though the underlying tables/models are named files_editor_* internally.
--
-- FULL OUTER JOIN, not an inner join: a date/event combo with data on only one
-- side still needs to surface with the other side at 0 -- that gap is the actual
-- point of a reconciliation dashboard, not something to filter out.
--
-- No config block -- inherits the gold folder's materialized='view' default
-- (dbt_project.yml), same as ga4_ads_reconciliation.sql. Cheap to recompute live
-- since both sides are already small, pre-aggregated tables.

with bravo as (
    select event_date, event_name, bravo_count
    from {{ ref('int_files_editor_subscription_event_day_counts') }}

    union all

    select event_date, event_name, bravo_count
    from {{ ref('int_files_editor_refund_event_day_counts') }}

    union all

    select event_date, event_name, bravo_count
    from {{ ref('int_files_editor_signup_event_day_counts') }}
),

anytrack as (
    select event_date, event_name, anytrack_count
    from {{ ref('int_anytrack_subscription_event_day_counts') }}

    union all

    select event_date, event_name, anytrack_count
    from {{ ref('int_anytrack_refund_event_day_counts') }}

    union all

    select event_date, event_name, anytrack_count
    from {{ ref('int_anytrack_signup_event_day_counts') }}
)

select
    coalesce(b.event_date, a.event_date) as event_date,
    coalesce(b.event_name, a.event_name) as event_name,
    coalesce(b.bravo_count, 0) as bravo_count,
    coalesce(a.anytrack_count, 0) as anytrack_count
from bravo b
full outer join anytrack a
    on b.event_date = a.event_date
   and b.event_name = a.event_name
order by event_date desc, event_name
