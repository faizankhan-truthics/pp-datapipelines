{{ config(materialized='table') }}

-- Subscription-lifecycle event counts (DPF/CFT Trial/Activation/Renewal) for
-- files_editor, feeding the AnyTrack reconciliation gold model. Classification
-- logic as defined by Manya (business/Preset) for the "AnyTrack Reconciliation:
-- Consolidated table" ticket -- not derived here, just reproduced in dbt.
--
-- Bronze lands every row as-is, including soft-deleted ones -- deleted_at IS NULL
-- filtering happens here, not in bronze, per the "bronze is raw, silver filters"
-- decision for this pipeline.
--
-- materialized='table' (full rebuild every run), not the silver folder's
-- incremental default: a trailing-window incremental rescan (like
-- ga4_ads_recon's int_ads_campaign_day_cost.sql) would risk missing a late
-- correction to an old order's is_trial/term_number (updated_at bumps, but
-- event_date -- the original payed_at -- doesn't move). Output grain here is
-- small (date x event_name), so a full rebuild is cheap and avoids that edge
-- case entirely.

with base as (
    select
        date(op.payed_at) as event_date,
        o.id as order_id,
        bc.sign_up_funnel,
        bc.end_funnel_status,
        o.is_trial,
        o.term_number,
        pp.title as payment_plan_title,
        pp.type as payment_plan_type
    from {{ source('bronze_files_editor_db', 'files_editor_billing_order_payments') }} op
    join {{ source('bronze_files_editor_db', 'files_editor_billing_orders') }} o
        on op.order_id = o.id
    join {{ source('bronze_files_editor_db', 'files_editor_billing_customers') }} c
        on o.customer_id = c.id
    join {{ source('bronze_files_editor_db', 'files_editor_billing_clients') }} bc
        on c.client_id = bc.id
    left join {{ source('bronze_files_editor_db', 'files_editor_billing_payment_plans') }} pp
        on o.payment_plan_id = pp.id
    where op.status = 'success'
      and op.operation = 'auth'
      and op.payed_at is not null
      and op.deleted_at is null
      and o.deleted_at is null
      and c.deleted_at is null
      and bc.deleted_at is null
      -- billing_payment_plans has no deleted_at column (confirmed 2026-10-12,
      -- unlike every other billing_* table) -- nothing to filter here.
),

classified as (
    select
        event_date,
        order_id,
        case
            when sign_up_funnel = 'dpf' and is_trial = true and term_number = 0
                then 'DPF Trial'

            when sign_up_funnel = 'dpf' and is_trial = false and term_number = 1
                then 'DPF Activation'

            when sign_up_funnel = 'dpf' and is_trial = false and term_number >= 2
                 and coalesce(lower(payment_plan_type), '') not like '%discount%'
                 and coalesce(lower(payment_plan_title), '') not like '%discount%'
                then 'DPF Renewal'

            when sign_up_funnel <> 'dpf' and end_funnel_status in ('cft', 'dst')
                 and is_trial = true and term_number = 0
                then 'CFT Trial'

            when sign_up_funnel <> 'dpf' and end_funnel_status in ('cft', 'dst')
                 and is_trial = false and term_number = 1
                then 'CFT Activation'

            when sign_up_funnel <> 'dpf' and end_funnel_status in ('cft', 'dst')
                 and is_trial = false and term_number >= 2
                 and coalesce(lower(payment_plan_type), '') not like '%discount%'
                 and coalesce(lower(payment_plan_title), '') not like '%discount%'
                then 'CFT Renewal'
        end as event_name
    from base
)

select
    event_date,
    event_name,
    count(distinct order_id) as bravo_count
from classified
where event_name is not null
group by event_date, event_name
