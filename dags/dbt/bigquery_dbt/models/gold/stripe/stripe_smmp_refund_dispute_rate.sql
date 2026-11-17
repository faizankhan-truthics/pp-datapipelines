{{
    config(
        materialized='table',
        tags=["stripe", "gold", "daily"],
        cluster_by=["account_id"]
    )
}}

-- Mastercard SMMP refund and dispute rates per Stripe account over the trailing 30 days.
-- Sales, disputes and refunds are each bucketed by their own date, so the rates compare
-- 30 days of disputes / refunds with 30 days of sales rather than following a cohort.

with payments as (
  select
    p._elt_stripe_account_id as account_id,
    p.account_name,
    count(distinct p.payment_intent) as sales_count
  from {{ref('int_stripe_charges')}} p
  where p.captured_date >= date_sub(current_date(), interval 30 day)
    and p.status = 'succeeded'
    and json_value(p.payment_method_details, '$.type') = 'card'
    and lower(p.card_brand) = 'mastercard'
  group by 1,2
),

disputes as (
  select
    c._elt_stripe_account_id as account_id,
    count(distinct d.id) as dispute_count_all,
    count(distinct if(d.rdr is not true, d.id, null)) as dispute_count_non_rdr
  from {{ref('int_stripe_disputes')}} d
  join {{ref('int_stripe_charges')}} c on d.charge = c.id
  where d.created_date >= date_sub(current_date(), interval 30 day)
    and lower(c.card_brand) = 'mastercard'
  group by 1
),

refunds as (
  select
    c._elt_stripe_account_id as account_id,
    count(distinct r.id) as refund_count
  from {{ref('int_stripe_refunds')}} r
  join {{ref('int_stripe_charges')}} c on r.charge = c.id
  where date(r.created_ts) >= date_sub(current_date(), interval 30 day)
    and lower(c.card_brand) = 'mastercard'
    and c.captured_date is not null
  group by 1
)

select
  p.account_id,
  p.account_name,
  'MasterCard' as network,
  p.sales_count,
  coalesce(d.dispute_count_all, 0)     as dispute_count_all,
  coalesce(d.dispute_count_non_rdr, 0) as dispute_count_non_rdr,
  coalesce(r.refund_count, 0)          as refund_count,
  round(100 * safe_divide(coalesce(d.dispute_count_all, 0), p.sales_count), 4) as dispute_rate_pct,
  round(100 * safe_divide(coalesce(r.refund_count, 0),      p.sales_count), 4) as refund_rate_pct
from payments p
left join disputes d using (account_id)
left join refunds  r using (account_id)
