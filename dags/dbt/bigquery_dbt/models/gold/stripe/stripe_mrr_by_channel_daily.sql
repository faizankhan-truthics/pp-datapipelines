{{ config(
    tags=["stripe", "gold", "daily"],
) }}

-- Daily MRR split by where the customer originally came from. The channel belongs to the
-- customer, not the payment, so every day they keep paying counts towards it. Customers we
-- could not place appear as `unattributed`, so the channels always add up to total MRR.
-- Mirrors solidgate_mrr_by_channel_daily.sql for the Stripe-billed products.
-- Grain: snapshot_date x _elt_stripe_account_id x source x medium x campaign.

with customer_daily as (

    select * from {{ ref('int_stripe_mrr_customer_daily') }}

),

attribution as (

    select * from {{ ref('int_stripe_customer_attribution') }}

)

select
    c.snapshot_date,
    c._elt_stripe_account_id,
    a.source,
    a.medium,
    a.campaign,
    a.campaign_id,
    count(distinct c.customer) as customers,
    round(sum(c.mrr_usd), 2)   as mrr_usd
from customer_daily c
left join attribution a
    on  c.customer               = a.customer
    and c._elt_stripe_account_id = a._elt_stripe_account_id
group by
    c.snapshot_date,
    c._elt_stripe_account_id,
    a.source,
    a.medium,
    a.campaign,
    a.campaign_id
