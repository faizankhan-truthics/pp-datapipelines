{{ config(
    materialized='view',
    tags=["solidgate", "gold", "daily"]
) }}

-- Acceptance rate by retry attempt and invoice sequence number.
-- Accepted statuses match int_solidgate_card_orders.vamp_include.

select
    retry_attempt,
    invoice_number,
    count(*) as attempts_made,
    countif(status in ('auth_ok', 'settle_ok', 'refunded', 'void_ok')) as accepted,
    round(100 * safe_divide(
        countif(status in ('auth_ok', 'settle_ok', 'refunded', 'void_ok')),
        count(*)
    ), 2) as acceptance_rate_pct
from {{ ref('vamp_initiative_126_days') }}
group by retry_attempt, invoice_number
order by retry_attempt, invoice_number
