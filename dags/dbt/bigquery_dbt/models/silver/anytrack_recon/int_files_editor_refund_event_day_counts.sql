{{ config(materialized='table') }}

-- Refund-event counts for files_editor, mirroring
-- int_files_editor_subscription_event_day_counts.sql's join/classification but for
-- op.operation = 'refund' rows. See that model's docstring for the deleted_at and
-- materialization rationale (applies identically here).
--
-- No "Refund (CFT Trial)" branch: CFT/DST trials aren't charged, so there's
-- nothing to refund on that path -- only DPF trials are (DPF charges upfront).

with refund_base as (
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
      and op.operation = 'refund'
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
                then 'Refund (DPF Trial)'

            when sign_up_funnel = 'dpf' and is_trial = false and term_number = 1
                then 'Refund (DPF Activation)'

            when sign_up_funnel = 'dpf' and is_trial = false and term_number >= 2
                 and coalesce(lower(payment_plan_type), '') not like '%discount%'
                 and coalesce(lower(payment_plan_title), '') not like '%discount%'
                then 'Refund (DPF Renewal)'

            when sign_up_funnel <> 'dpf' and end_funnel_status in ('cft', 'dst')
                 and is_trial = false and term_number = 1
                then 'Refund (CFT Activation)'

            when sign_up_funnel <> 'dpf' and end_funnel_status in ('cft', 'dst')
                 and is_trial = false and term_number >= 2
                 and coalesce(lower(payment_plan_type), '') not like '%discount%'
                 and coalesce(lower(payment_plan_title), '') not like '%discount%'
                then 'Refund (CFT Renewal)'
        end as event_name
    from refund_base
)

select
    event_date,
    event_name,
    count(distinct order_id) as bravo_count
from classified
where event_name is not null
group by event_date, event_name
