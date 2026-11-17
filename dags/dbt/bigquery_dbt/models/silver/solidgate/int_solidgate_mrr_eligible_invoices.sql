{{ config(
    materialized='table',
    tags=["solidgate", "silver", "daily"],
    ) }}

with invoices as (

    select * from {{ ref('int_solidgate_subscription_invoices') }}

),

products as (

    select * from {{ ref('int_solidgate_subscription_products') }}

),

customers as (

    select * from {{ ref('int_solidgate_subscription_customers') }}

),

joined as (

    select
        i.subscription_id,
        i.invoice_id,
        i._elt_account_name,
        c.customer_account_id,
        p.currency,
        p.product_trial,
        i.subscription_term_number,
        i.invoice_amount_cents,
        i.invoice_created_at,
        i.billing_period_started_at,
        i.billing_period_ended_at,
        i._elt_update_timestamp,
        date_diff(
            date(i.billing_period_ended_at),
            date(i.billing_period_started_at),
            day
        ) as cycle_days
    from invoices i
    left join products p
        on  i.subscription_id   = p.subscription_id
        and i._elt_account_name = p._elt_account_name
    left join customers c
        on  i.subscription_id   = c.subscription_id
        and i._elt_account_name = c._elt_account_name
    where i.invoice_status = 'success'
      and i.invoice_amount_cents > 0
      and i.billing_period_started_at is not null
      and i.billing_period_ended_at   is not null
      and not (coalesce(p.product_trial, false) and i.subscription_term_number = 0)

),

normalized as (

    select
        *,
        case
            when cycle_days between   6 and   8 then 'weekly'
            when cycle_days between  13 and  16 then 'biweekly'
            when cycle_days between  28 and  31 then 'monthly'
            when cycle_days between  89 and  92 then 'quarterly'
            when cycle_days between 180 and 184 then 'semiannual'
            when cycle_days between 365 and 366 then 'yearly'
            else 'prorated'
        end as billing_cycle,
        case
            when cycle_days between   6 and   8 then 12 / 52.0
            when cycle_days between  13 and  16 then 12 / 26.0
            when cycle_days between  28 and  31 then 1.0
            when cycle_days between  89 and  92 then 3.0
            when cycle_days between 180 and 184 then 6.0
            when cycle_days between 365 and 366 then 12.0
            else cycle_days / 30.44
        end as months_in_period
    from joined
    where cycle_days > 0

)

select
    * except (product_trial),
    (invoice_amount_cents / {{ minor_unit_divisor('currency') }}) / months_in_period as mrr_local
from normalized
