{{ config(
    materialized='table',
    tags=["stripe", "gold", "daily"],
    ) }}

with
charges as (
select
b.id as charge_id,
b.amount_local as charge_amount_local,
b.amount_usd as charge_amount_usd,
b.amount_captured_local as charge_amount_captured_local,
b.amount_captured_usd as charge_amount_captured_usd,
b.amount_refunded_local as charge_amount_refunded_local,
b.amount_refunded_usd as charge_amount_refunded_usd,
b.currency as charge_currency,
b.status as charge_status,
b.refunded as charge_refunded,
disputed as charge_disputed,
DATE_TRUNC(created_date, MONTH) AS charge_created_month,
DATE_TRUNC(captured_date, MONTH) AS charge_captured_month,
b._elt_stripe_account_id,
b._elt_update_timestamp
from {{ ref('int_stripe_charges') }} b

),

refunds as (
select
c.id as refund_id,
c.amount_local as refund_amount_local,
c.amount_usd as refund_amount_usd,
c.charge as charge_id,
c.created_date as refund_created_date,
DATE_TRUNC(created_date, MONTH) AS refunds_created_month,
DATE_TRUNC(captured_date, MONTH) AS refunds_captured_month,
c.currency as refund_currency,
c.status as refund_status,
c._elt_stripe_account_id,
c._elt_update_timestamp
from {{ ref('int_stripe_refunds') }} c
),

fraud_warnings as (
select
d.id as fraud_id,
d.charge as charge_id,
DATE_TRUNC(DATE(d.created_ts), MONTH) AS fraud_created_month,
b.amount_local as fraud_amount_local,
b.amount_usd as fraud_amount_usd,
d._elt_stripe_account_id,
d._elt_update_timestamp
from {{ source('bronze_stripe', 'stripe_early_fraud_warnings') }} d
left join {{ ref('int_stripe_charges') }} b
on d.charge = b.id
),

disputes as (
select
d.id as dispute_id,
d.amount_local as dispute_amount_local,
d.amount_usd as dispute_amount_usd,
d.charge as charge_id,
d.created_date as dispute_created_date,
DATE_TRUNC(d.created_date, MONTH) AS disputes_created_month,
DATE_TRUNC(captured_date, MONTH) AS disputes_captured_month,
d.currency as dispute_currency,
d.reason as dispute_reason,
d.status as dispute_status,
d._elt_stripe_account_id,
d._elt_update_timestamp
from {{ ref('int_stripe_disputes') }} d
),

charges_summary as (
select
_elt_stripe_account_id as account_id,
coalesce(charge_captured_month, charge_created_month) charge_captured_month,
count(distinct charge_id) as charges_created,
sum(charge_amount_usd) charge_amount_usd,
sum(charge_amount_captured_usd) charge_amount_captured_usd,
sum(charge_amount_refunded_usd) charge_amount_refunded_usd,
max(_elt_update_timestamp) _elt_update_timestamp
from charges
group by 1, 2
),

refunds_summary as (
select
_elt_stripe_account_id as account_id,
coalesce( refunds_captured_month, refunds_created_month) refunds_captured_month,
count(distinct refund_id) as refund_count,
sum(refund_amount_usd) as refund_amount_usd,
max(_elt_update_timestamp) _elt_update_timestamp
from refunds
group by 1,2
),

fraud_warnings_summary as (
select
_elt_stripe_account_id as account_id,
fraud_created_month,
count(distinct fraud_id) as fraud_count,
sum(fraud_amount_usd) as fraud_amount_usd,
max(_elt_update_timestamp) _elt_update_timestamp
from fraud_warnings
group by 1,2
),

disputes_summary as (
select
_elt_stripe_account_id as account_id,
coalesce( disputes_captured_month, disputes_created_month) disputes_captured_month,
count(distinct dispute_id) as dispute_count,
sum(dispute_amount_usd) as dispute_amount_usd,
max(_elt_update_timestamp) _elt_update_timestamp
from disputes
group by 1,2
)



select
e.account,
e.product,
coalesce(a.charge_captured_month, b.refunds_captured_month, c.fraud_created_month, d.disputes_captured_month) as reporting_month,
sum(a.charges_created) as charges_created,
sum(a.charge_amount_usd) as charge_amount_usd,
sum(a.charge_amount_captured_usd) as charge_amount_captured_usd,
sum(a.charge_amount_refunded_usd) as charge_amount_refunded_usd,
sum(b.refund_count) as refund_count,
sum(b.refund_amount_usd) as refund_amount_usd,
sum(c.fraud_count) as fraud_count,
sum(c.fraud_amount_usd) as fraud_amount_usd,
sum(d.dispute_amount_usd) as dispute_amount_usd,
concat(
    coalesce(coalesce(a.account_id, b.account_id, c.account_id, d.account_id),''),
    coalesce(coalesce(a.charge_captured_month, b.refunds_captured_month, c.fraud_created_month, d.disputes_captured_month),'2000-01-01')
    ) as unique_key,
max(coalesce(a._elt_update_timestamp, b._elt_update_timestamp, c._elt_update_timestamp, d._elt_update_timestamp)) _elt_update_timestamp
from charges_summary a
full outer join refunds_summary b
on a.account_id = b.account_id
and a.charge_captured_month = b.refunds_captured_month
full outer join fraud_warnings_summary c
on coalesce(a.account_id, b.account_id) = c.account_id
and coalesce(a.charge_captured_month, b.refunds_captured_month) = c.fraud_created_month
full outer join disputes_summary d
on coalesce(a.account_id, b.account_id, c.account_id) = d.account_id
and coalesce(a.charge_captured_month, b.refunds_captured_month, c.fraud_created_month) = d.disputes_captured_month
left join {{ source('bronze_stripe', 'stripe_accounts') }} e
on coalesce(a.account_id, b.account_id, c.account_id, d.account_id) = e.id
group by account, product, reporting_month, unique_key
