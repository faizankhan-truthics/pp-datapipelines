{{ config(
    materialized='table',
    tags=["stripe", "gold", "daily"]
) }}


with
bounds as (
  select date_trunc(date_sub(current_date('UTC'), interval 5 month), month) as window_start
),
-- filtered once, reused by all three branches below instead of each
-- one independently re-scanning + re-filtering int_stripe_charges
visa_charges as (
  select
    id,
    calculated_statement_descriptor,
    _elt_stripe_account_id as account_id,
    account_name,
    captured_date,
    created_ts,
    status,
    captured,
    payment_method_details,
    -- card identity for CE 3.0 history matching: Stripe fingerprint, else
    -- customer|last4|exp_month|exp_year|brand (null if any part is missing).
    -- customer is in the fallback because different cards can share
    -- last4 + expiry on an account; without it they'd share history
    coalesce(
      json_value(payment_method_details, '$.card.fingerprint'),
      concat(
        customer, '|',
        json_value(payment_method_details, '$.card.last4'), '|',
        json_value(payment_method_details, '$.card.exp_month'), '|',
        json_value(payment_method_details, '$.card.exp_year'), '|',
        json_value(payment_method_details, '$.card.brand')
      )
    ) as card_key
  from {{ ref('int_stripe_charges') }}
  where card_brand = 'visa'
),

payments as (
  select *,
    -- computed here (not after the joins) so efw/disputes only ever
    -- get joined against the top-11 descriptors that survive qualify
    row_number() over (
      partition by account_id, start_of_month
      order by sales_count desc
    ) as rn
  from (
    select c.calculated_statement_descriptor,
      c.account_id, c.account_name,
      date_trunc(c.captured_date, month) as start_of_month,
      count(*) as sales_count
    from visa_charges c, bounds
    where c.captured_date >= bounds.window_start
      and c.status = 'succeeded'
      and json_value(c.payment_method_details, '$.type') = 'card'
    group by 1,2,3,4
  )
  qualify rn < 12
),
efw as (
  select c.calculated_statement_descriptor,
    c.account_id, c.account_name,
    date_trunc(date(e.created_ts), month) as start_of_month,
    count(distinct c.id) as efw_count
  from {{ source('bronze_stripe', 'stripe_early_fraud_warnings') }} e
  join visa_charges c on e.charge = c.id
  cross join bounds
  where date(e.created_ts) >= bounds.window_start
  group by 1,2,3,4
),
disputes as (
  select c.calculated_statement_descriptor,
    d._elt_stripe_account_id as account_id, d.account_name,
    date_trunc(date(d.created_date), month) as start_of_month,
    count(distinct d.id) as dispute_count_all,
    count(distinct if(d.rdr is not true, d.id, null)) as dispute_count_non_rdr,
    count(distinct if(d.rdr, d.id, null)) as dispute_count_rdr
  from {{ ref('int_stripe_disputes') }} d
  join visa_charges c on d.charge = c.id
  cross join bounds
  where d.created_date >= bounds.window_start
  group by 1,2,3,4
),

-- Visa Compelling Evidence 3.0 (history-eligible): an EFW'd charge's card has
-- >= 2 prior succeeded + captured Visa charges on the same account, settled
-- (captured) 120-365 days before the EFW date, with no dispute and no EFW on
-- those prior charges.
-- Visa's IP / device-id match isn't in the warehouse, so this is an upper bound.
ce3_events as (
  -- same EFW population and month as efw above, one row per charge; the
  -- EFW date (not the charge date) anchors the 120-365 day window
  select c.id as charge_id, c.account_id, c.calculated_statement_descriptor,
    date_trunc(date(e.created_ts), month) as start_of_month,
    min(date(e.created_ts)) as efw_date, c.card_key
  from {{ source('bronze_stripe', 'stripe_early_fraud_warnings') }} e
  join visa_charges c on e.charge = c.id
  cross join bounds
  where date(e.created_ts) >= bounds.window_start
    and c.card_key is not null
  group by 1,2,3,4,6
),
flagged_charges as (
  -- bronze, not int_stripe_disputes: that drops disputes with no amount_usd,
  -- and only the dispute's presence matters here
  select charge from {{ source('bronze_stripe', 'stripe_disputes') }} where charge is not null
  union distinct
  select charge from {{ source('bronze_stripe', 'stripe_early_fraud_warnings') }} where charge is not null
),
history as (
  -- only cards that have an EFW in the window; the 120-365 day range is
  -- applied per event in eligible, so no date cutoff is needed here
  select c.id, c.account_id, c.card_key, c.captured_date
  from visa_charges c
  join (select distinct account_id, card_key from ce3_events) k
    on k.account_id = c.account_id and k.card_key = c.card_key
  left join flagged_charges f on f.charge = c.id
  where c.status = 'succeeded'
    and c.captured
    and f.charge is null
),
eligible as (
  select ev.charge_id, ev.account_id, ev.calculated_statement_descriptor,
    ev.start_of_month, ev.card_key
  from ce3_events ev
  join history h
    on h.account_id = ev.account_id
   and h.card_key = ev.card_key
   -- prior charge settled 120-365 days before the EFW date
   and h.captured_date between date_sub(ev.efw_date, interval 365 day)
                           and date_sub(ev.efw_date, interval 120 day)
  group by 1,2,3,4,5
  having count(distinct h.id) >= 2
),
ce3 as (
  select account_id, calculated_statement_descriptor, start_of_month,
    count(distinct card_key) as ce3_eligible_cards,
    count(distinct charge_id) as ce3_eligible_efw
  from eligible
  group by 1,2,3
)
select
  p.account_id,
  p.account_name,
  p.start_of_month,
  p.calculated_statement_descriptor,
  'Visa' as network,
  p.sales_count,
  coalesce(e.efw_count, 0) as efw_count,
  coalesce(d.dispute_count_all, 0) as dispute_count_all,
  coalesce(d.dispute_count_non_rdr, 0) as dispute_count_non_rdr,
  coalesce(d.dispute_count_rdr, 0) as dispute_count_rdr,
  coalesce(e.efw_count,0) + coalesce(d.dispute_count_non_rdr,0) as vamp_tnx,
  safe_divide(coalesce(e.efw_count,0) + coalesce(d.dispute_count_non_rdr,0), p.sales_count) as vamp_rate,
  -- projected VAMP tnx (current month only): completed days (excl today) -> scale to actual days in month -> +10%
  case when p.start_of_month = date_trunc(current_date('UTC'), month)
       then (coalesce(e.efw_count,0) + coalesce(d.dispute_count_non_rdr,0))
            / nullif(date_diff(current_date('UTC'), p.start_of_month, day), 0)
            * date_diff(date_add(p.start_of_month, interval 1 month), p.start_of_month, day)
            * 1.10
  end as projected_vamp_tnx,
  case when p.start_of_month = date_trunc(current_date('UTC'), month)
       then safe_divide(coalesce(e.efw_count,0) + coalesce(d.dispute_count_non_rdr,0), p.sales_count) * 1.10
  end as projected_vamp_rate,
  p.rn,
  -- CE 3.0 adjusted (upper bound): only EFWs are subtracted, never disputes
  coalesce(x.ce3_eligible_cards, 0) as ce3_eligible_cards,
  coalesce(x.ce3_eligible_efw, 0) as ce3_eligible_efw,
  coalesce(e.efw_count,0) + coalesce(d.dispute_count_non_rdr,0)
    - coalesce(x.ce3_eligible_efw,0) as vamp_tnx_ce3_adjusted,
  safe_divide(coalesce(e.efw_count,0) + coalesce(d.dispute_count_non_rdr,0)
    - coalesce(x.ce3_eligible_efw,0), p.sales_count) as vamp_rate_ce3_adjusted,
  case when p.start_of_month = date_trunc(current_date('UTC'), month)
       then (coalesce(e.efw_count,0) + coalesce(d.dispute_count_non_rdr,0)
             - coalesce(x.ce3_eligible_efw,0))
            / nullif(date_diff(current_date('UTC'), p.start_of_month, day), 0)
            * date_diff(date_add(p.start_of_month, interval 1 month), p.start_of_month, day)
            * 1.10
  end as projected_vamp_tnx_ce3_adjusted,
  case when p.start_of_month = date_trunc(current_date('UTC'), month)
       then safe_divide(coalesce(e.efw_count,0) + coalesce(d.dispute_count_non_rdr,0)
             - coalesce(x.ce3_eligible_efw,0), p.sales_count) * 1.10
  end as projected_vamp_rate_ce3_adjusted
from payments p
left join efw e
  on e.start_of_month = p.start_of_month
 and e.calculated_statement_descriptor = p.calculated_statement_descriptor
 and e.account_id = p.account_id
left join disputes d
  on d.start_of_month = p.start_of_month
 and d.calculated_statement_descriptor = p.calculated_statement_descriptor
 and d.account_id = p.account_id
 and d.account_name=p.account_name
left join ce3 x
  on x.start_of_month = p.start_of_month
 and x.calculated_statement_descriptor = p.calculated_statement_descriptor
 and x.account_id = p.account_id
order by account_id, start_of_month desc, sales_count desc