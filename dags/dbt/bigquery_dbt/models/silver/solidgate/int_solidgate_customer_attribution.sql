{{ config(
    materialized='table',
    tags=["solidgate", "silver", "daily"],
) }}

-- Where each customer came from, taken from the earliest payment of theirs that GA4 saw.
-- Renewals carry no GA4 event, so attributing payment by payment would leave most revenue
-- unattributed; the customer is attributed once and later payments inherit it via
-- customer_account_id. Not restricted to revenue-bearing invoices -- a customer who started
-- on a trial was still acquired by whatever brought them in.
-- Grain: one row per customer (per account).

with orders as (

    select
        c.customer_account_id,
        t._elt_account_name,
        t.subscription_id,
        t.transaction_id,
        t.sg_date
    from {{ ref('int_solidgate_transactions') }} t
    join {{ ref('int_solidgate_subscription_customers') }} c
        on  t.subscription_id   = c.subscription_id
        and t._elt_account_name = c._elt_account_name
    where c.customer_account_id is not null

),

matched as (

    select
        o.customer_account_id,
        o._elt_account_name,
        o.subscription_id,
        o.sg_date,
        g.source,
        g.medium,
        g.campaign,
        g.campaign_id,
        g.gclid
    from orders o
    -- matched within the product, since order ids are only unique inside an account. no
    -- date in the join: Solidgate is UTC and GA4 is property timezone, so a payment near
    -- midnight would miss.
    join {{ ref('int_ga4_transactions') }} g
        on  o.transaction_id    = g.transaction_id
        and o._elt_account_name = g._elt_account_name
    qualify row_number() over (
        partition by o.customer_account_id, o._elt_account_name
        order by o.sg_date
    ) = 1

),

customers as (

    select distinct customer_account_id, _elt_account_name
    from {{ ref('int_solidgate_mrr_customer_daily') }}

)

select
    c.customer_account_id,
    c._elt_account_name,
    -- customers we cannot place still belong here, or channel revenue stops adding up
    -- to total revenue
    coalesce(m.source,   'unattributed')  as source,
    coalesce(m.medium,   'unattributed')  as medium,
    coalesce(m.campaign, 'unattributed')  as campaign,
    m.campaign_id,
    m.gclid,
    m.subscription_id                     as attributed_from_subscription_id,
    m.sg_date                             as attributed_from_date,
    if(m.customer_account_id is null, 'unattributed', 'attributed') as attribution_status
from customers c
left join matched m
    on  c.customer_account_id = m.customer_account_id
    and c._elt_account_name   = m._elt_account_name
