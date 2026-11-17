{{ config(
    materialized='table',
    tags=["stripe", "gold", "daily"]
) }}

-- Mastercard ECM (Excessive Chargeback Merchant) + EFM (Excessive Fraud Merchant), account level.
-- Reports the current month + 5 full prior months; payments reach back one extra
-- month so the earliest reported row still gets a prior-month denominator.
with
bounds as (
  select
    date_trunc(current_date('UTC'), month)                                  as current_month,
    date_trunc(date_sub(current_date('UTC'), interval 5 month), month)      as report_start,
    date_trunc(date_sub(current_date('UTC'), interval 6 month), month)      as lag_start
),

-- filtered once to Mastercard, reused by both branches below
mastercard_charges as (
  select
    id,
    _elt_stripe_account_id as account_id,
    account_name,
    captured_date,
    status,
    payment_method_details
  from {{ ref('int_stripe_charges') }}
  where card_brand = 'mastercard'
),

payments as (
  select
    c.account_id,
    c.account_name,
    date_trunc(c.captured_date, month) as start_of_month,
    count(*) as ecom_txn_count
  from mastercard_charges c, bounds
  where c.captured_date >= bounds.lag_start
    and c.status = 'succeeded'
    and json_value(c.payment_method_details, '$.type') = 'card'
  group by 1,2,3
),

-- only needs the reported months: the extra lag month exists solely for the
-- payments denominator, its disputes would be dropped by the final filter.
-- dispute id and charge id are both unique (tested in silver), so plain
-- counts are equivalent to count(distinct d.id).
disputes as (
  select
    d._elt_stripe_account_id as account_id,
    d.account_name,
    date_trunc(d.created_date, month) as start_of_month,
    count(*) as dispute_count_all,
    countif(d.network_reason_code in ('4837','4863')) as fraud_dispute_count,
    sum(if(d.network_reason_code in ('4837','4863'), d.amount_usd, 0)) as net_fraud_volume_usd
  from {{ ref('int_stripe_disputes') }} d
  join mastercard_charges c on d.charge = c.id
  cross join bounds
  where d.created_date >= bounds.report_start
  group by 1,2,3
),

joined as (
  select
    p.account_id,
    p.account_name,
    p.start_of_month,
    p.ecom_txn_count,
    coalesce(d.dispute_count_all, 0)    as dispute_count_all,
    coalesce(d.fraud_dispute_count, 0)  as fraud_dispute_count,
    coalesce(d.net_fraud_volume_usd, 0) as net_fraud_volume_usd,
    -- previous row with payments for the account, not strictly the previous calendar month
    lag(p.ecom_txn_count) over (
      partition by p.account_id
      order by p.start_of_month
    ) as prior_month_txn_count,
    -- projection factor (31 * 1.05) / days elapsed, current month only;
    -- null on the 1st (0 days elapsed), which nulls every projected column
    case when p.start_of_month = b.current_month
         then 31 * 1.05 / nullif(date_diff(current_date('UTC'), p.start_of_month, day), 0)
    end as f,
    b.report_start
  from payments p
  cross join bounds b
  left join disputes d
    on  d.start_of_month = p.start_of_month
    and d.account_id = p.account_id
),

rates as (
  select
    *,
    safe_divide(dispute_count_all,   prior_month_txn_count) as ecm_rate,
    safe_divide(fraud_dispute_count, prior_month_txn_count) as efm_rate,
    dispute_count_all    * f as proj_disputes,
    fraud_dispute_count  * f as proj_fraud_disputes,
    net_fraud_volume_usd * f as proj_fraud_volume,
    ecom_txn_count       * f as proj_ecom_txns,
    safe_divide(dispute_count_all   * f, prior_month_txn_count) as proj_ecm_rate,
    safe_divide(fraud_dispute_count * f, prior_month_txn_count) as proj_efm_rate
  from joined
  -- lag is already computed above, so the extra lag month can be dropped here
  where start_of_month >= report_start
)

select
  account_id,
  account_name,
  start_of_month,
  'Mastercard' as network,
  ecom_txn_count,
  prior_month_txn_count,
  dispute_count_all,
  fraud_dispute_count,
  round(net_fraud_volume_usd, 2) as net_fraud_volume_usd,

  round(100 * ecm_rate, 3) as ecm_chargeback_rate_pct,
  round(100 * efm_rate, 3) as efm_fraud_rate_pct,

  -- actual status (month-to-date)
  case
    when dispute_count_all >= 300 and ecm_rate >= 0.03  then 'HECM (300+ / 3%+)'
    when dispute_count_all >= 100 and ecm_rate >= 0.015 then 'ECM (100-299 / 1.5-2.99%)'
    else 'OK'
  end as ecm_status,
  (ecom_txn_count >= 1000
   and net_fraud_volume_usd >= 50000
   and efm_rate >= 0.005) as efm_breach,

  -- ===== PROJECTED (current month only) =====
  round(proj_disputes, 1)        as projected_dispute_count_all,
  round(proj_fraud_disputes, 1)  as projected_fraud_dispute_count,
  round(proj_fraud_volume, 2)    as projected_net_fraud_volume_usd,
  round(100 * proj_ecm_rate, 3)  as projected_ecm_rate_pct,
  round(100 * proj_efm_rate, 3)  as projected_efm_rate_pct,

  case when f is null then null
       when proj_disputes >= 300 and proj_ecm_rate >= 0.03  then 'HECM (300+ / 3%+)'
       when proj_disputes >= 100 and proj_ecm_rate >= 0.015 then 'ECM (100-299 / 1.5-2.99%)'
       else 'OK'
  end as projected_ecm_status,

  case when f is null then null
       else (proj_ecom_txns >= 1000
             and proj_fraud_volume >= 50000
             and proj_efm_rate >= 0.005)
  end as projected_efm_breach

from rates
