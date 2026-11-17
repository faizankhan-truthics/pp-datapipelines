{{ config(
    materialized='table',
    tags=["stripe", "silver", "daily"],
    ) }}

-- Invoice lines that count as recurring revenue, normalized to a monthly rate.
-- Excludes unpaid, zero-value, non-subscription, proration and non-recurring lines.
-- Unlike Solidgate (which infers a billing cycle from the invoice's date range),
-- Stripe's price object states its own recurring cadence, so that is used directly
-- instead of bucketing by day-count.

with invoice_lines as (

    select * from {{ ref('int_stripe_invoice_lines') }}

),

invoices as (

    select * from {{ ref('int_stripe_invoices') }}

),

prices as (

    select * from {{ ref('int_stripe_prices') }}

),

joined as (

    select
        l.invoice_line_id,
        l.invoice_id,
        l.subscription_id,
        l.price_id,
        l.product_id,
        l._elt_stripe_account_id,
        i.customer,
        i.currency,
        l.amount_local,
        i.created_date as invoice_created_date,
        l.period_start_date,
        l.period_end_date,
        p.billing_interval,
        p.billing_interval_count,
        l._elt_update_timestamp
    from invoice_lines l
    join invoices i
        on  l.invoice_id             = i.id
        and l._elt_stripe_account_id = i._elt_stripe_account_id
    left join prices p
        on  l.price_id               = p.price_id
        and l._elt_stripe_account_id = p._elt_stripe_account_id
    where i.status = 'paid'
      and l.amount_local > 0
      and l.subscription_id is not null
      and not coalesce(l.proration, false)
      and p.billing_interval is not null
      and l.period_start_date is not null
      and l.period_end_date   is not null
      and l.period_end_date   > l.period_start_date

),

normalized as (

    select
        *,
        case
            when billing_interval = 'week'  and billing_interval_count = 1 then 'weekly'
            when billing_interval = 'week'  and billing_interval_count = 2 then 'biweekly'
            when billing_interval = 'month' and billing_interval_count = 1 then 'monthly'
            when billing_interval = 'month' and billing_interval_count = 3 then 'quarterly'
            when billing_interval = 'month' and billing_interval_count = 6 then 'semiannual'
            when billing_interval = 'year'  and billing_interval_count = 1 then 'yearly'
            else 'other'
        end as billing_cycle,
        case
            when billing_interval = 'day'   then billing_interval_count / 30.44
            when billing_interval = 'week'  then billing_interval_count * 12 / 52.0
            when billing_interval = 'month' then billing_interval_count * 1.0
            when billing_interval = 'year'  then billing_interval_count * 12.0
        end as months_in_period
    from joined

)

select
    * except (billing_interval, billing_interval_count),
    (amount_local / {{ minor_unit_divisor('currency') }}) / months_in_period as mrr_local
from normalized
where months_in_period > 0
