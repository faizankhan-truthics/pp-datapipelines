{{ config(
materialized='table',
tags=["stripe", "silver", "daily"],
) }}

with eligible as (

    select * from {{ ref('int_stripe_mrr_eligible_invoice_lines') }}

),

data_asof as (

    select date(max(_elt_update_timestamp)) as max_data_date
    from eligible

),

spine as (

    select snapshot_date
    from unnest(generate_date_array(
        (select min(period_start_date) from eligible),
        (select max_data_date from data_asof),
        interval 1 day
    )) as snapshot_date

),

covered as (

    -- grain is subscription x price (subscription item), not subscription alone,
    -- since a Stripe subscription can carry more than one recurring item
    select
        sp.snapshot_date,
        e.*,
        row_number() over (
            partition by sp.snapshot_date, e.subscription_id, e.price_id, e._elt_stripe_account_id
            order by e.invoice_created_date desc
        ) as rn
    from spine sp
    join eligible e
        on  e.period_start_date <= sp.snapshot_date
        and e.period_end_date   >  sp.snapshot_date

)

select
    snapshot_date,
    subscription_id,
    price_id,
    product_id,
    _elt_stripe_account_id,
    customer,
    currency,
    invoice_id,
    invoice_line_id,
    billing_cycle,
    months_in_period,
    amount_local,
    mrr_local
from covered
where rn = 1
