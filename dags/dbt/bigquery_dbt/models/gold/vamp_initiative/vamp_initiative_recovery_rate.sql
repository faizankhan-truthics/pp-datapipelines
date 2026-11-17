{{ config(
    materialized='view',
    tags=["solidgate", "gold", "daily"]
) }}

-- Recovery rate: for each invoice, how much of the max amount attempted
-- (across retries) was ultimately recovered via an accepted attempt.

with per_invoice as (

    select
        invoice_number,
        subscription_id,
        invoice_id,
        max(order_amount_usd) as max_amount_usd,
        min(if(
            invoice_outcome in ('auth_ok', 'settle_ok', 'void_ok', 'refunded'),
            order_amount_usd,
            null
        )) as min_amount_usd
    from {{ ref('vamp_initiative_126_days') }}
    group by invoice_number, subscription_id, invoice_id

)

select
    invoice_number,
    count(distinct invoice_id) as invoices,
    round(sum(max_amount_usd), 2) as sum_max_amount_usd,
    round(sum(min_amount_usd), 2) as sum_min_amount_usd
from per_invoice
group by invoice_number
order by invoice_number
