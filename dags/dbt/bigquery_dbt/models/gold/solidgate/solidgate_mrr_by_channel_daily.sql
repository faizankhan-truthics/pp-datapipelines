{{ config(
    tags=["solidgate", "gold", "daily"],
) }}

-- Daily MRR split by where the customer originally came from. The channel belongs to the
-- customer, not the payment, so every day they keep paying counts towards it. Customers we
-- could not place appear as `unattributed`, so the channels always add up to total MRR.
-- Grain: snapshot_date x _elt_account_name x source x medium x campaign.

with customer_daily as (

    select * from {{ ref('int_solidgate_mrr_customer_daily') }}

),

attribution as (

    select * from {{ ref('int_solidgate_customer_attribution') }}

)

select
    c.snapshot_date,
    c._elt_account_name,
    a.source,
    a.medium,
    a.campaign,
    a.campaign_id,
    count(distinct c.customer_account_id) as customers,
    round(sum(c.mrr_usd), 2)              as mrr_usd
from customer_daily c
left join attribution a
    on  c.customer_account_id = a.customer_account_id
    and c._elt_account_name   = a._elt_account_name
group by
    c.snapshot_date,
    c._elt_account_name,
    a.source,
    a.medium,
    a.campaign,
    a.campaign_id
