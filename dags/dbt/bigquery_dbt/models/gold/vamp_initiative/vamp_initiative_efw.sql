{{ config(
    materialized='view',
    tags=["solidgate", "gold", "daily"]
) }}

-- Early fraud warning and non-RDR dispute rate by invoice sequence number,
-- at order (attempt) grain.

select
    invoice_number,
    count(distinct order_id) as total_orders,
    count(distinct if(efw_date is not null, order_id, null)) as efw_orders,
    round(100 * safe_divide(
        count(distinct if(efw_date is not null, order_id, null)),
        count(distinct order_id)
    ), 4) as efw_rate_pct,
    count(distinct if(dispute_date is not null, order_id, null)) as dispute_orders_non_rdr
from {{ ref('vamp_initiative_126_days') }}
group by invoice_number
order by invoice_number
