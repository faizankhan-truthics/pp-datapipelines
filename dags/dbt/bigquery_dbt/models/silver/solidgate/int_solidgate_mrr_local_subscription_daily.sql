{{ config(
materialized='table',
tags=["solidgate", "silver", "daily"],
) }}

with eligible as (

    select * from {{ ref('int_solidgate_mrr_eligible_invoices') }}

),

data_asof as (

    select date(max(_elt_update_timestamp)) as max_data_date
    from eligible

),

spine as (

    select snapshot_date
    from unnest(generate_date_array(
        (select min(date(billing_period_started_at)) from eligible),
        (select max_data_date from data_asof),
        interval 1 day
    )) as snapshot_date

),

covered as (

    select
        sp.snapshot_date,
        e.*,
        row_number() over (
            partition by sp.snapshot_date, e.subscription_id, e._elt_account_name
            order by e.invoice_created_at desc, e.subscription_term_number desc
        ) as rn
    from spine sp
    join eligible e
        on  date(e.billing_period_started_at) <= sp.snapshot_date
        and date(e.billing_period_ended_at)   >  sp.snapshot_date

),

lifecycle as (

    select subscription_id, _elt_account_name, status
    from {{ ref('int_solidgate_subscription_lifecycle') }}

)

select
    c.snapshot_date,
    c.subscription_id,
    c._elt_account_name,
    c.customer_account_id,
    c.currency,
    c.invoice_id,
    c.subscription_term_number,
    c.billing_cycle,
    c.cycle_days,
    c.months_in_period,
    c.invoice_amount_cents,
    c.mrr_local,
    -- where the subscription stands today, not on this date. we only ever hold the
    -- current status, so a row from June carries whatever it is now.
    l.status as subscription_status_now
from covered c
left join lifecycle l
    on  c.subscription_id   = l.subscription_id
    and c._elt_account_name = l._elt_account_name
where c.rn = 1
