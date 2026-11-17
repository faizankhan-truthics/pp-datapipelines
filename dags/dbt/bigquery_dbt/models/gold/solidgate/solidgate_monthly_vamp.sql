{{ config(
    materialized='table',
    tags=["solidgate", "gold", "daily"],
    description='Calculates daily Visa VAMP ratios using card_orders.'
) }}

with
financial_entries as (
  SELECT
    id,
    order_id,
    card_brand
  FROM {{ source('bronze_solidgate', 'solidgate_financial_entries') }}
),

card_orders_financial as (
    select
        a.*,
        DATE_TRUNC(DATE(created_date), MONTH) created_month,
        b.card_brand
    from {{ ref('int_solidgate_card_orders') }} a
    inner join financial_entries b
    on a.order_id = b.order_id
    where a.vamp_include is true
    and b.card_brand = 'VISA'
),

charge_backs_financial as (
    select
        a.*,
        DATE_TRUNC(DATE(chargeback_created_at), MONTH) dispute_month,
        b.card_brand
    from {{ ref('int_solidgate_chargebacks_transactions') }} a
    inner join financial_entries b
    on a.chargeback_order_id = b.order_id
    where a.vamp_include is true
    and b.card_brand = 'VISA'
),

fraud_alerts as (
  select
    _elt_account_name account,
    DATE_TRUNC(DATE(fraud_report_date), MONTH) fraud_report_month,
    order_id,
    fraud_alert_id,
    fraud_amount_usd
  from {{ ref('int_solidgate_fraud_alerts') }}
  where vamp_alerts is true
),

joined as (
    select
        a._elt_account_name account,
        a.order_description,
        a.psp_name,
        coalesce(a.created_month, b.dispute_month, c.fraud_report_month) month_, --filter to previous months
        count(distinct a.order_id) orders,
        sum(coalesce(a.order_amount_usd, 0)) order_amount_usd,
        count(distinct b.chargeback_id) chargebacks,
        sum(coalesce(b.chargeback_amount_usd, 0)) chargeback_amount_usd,
        count(distinct c.fraud_alert_id) fraud_alerts,
        sum(coalesce(c.fraud_amount_usd, 0)) fraud_amount_usd,
    from card_orders_financial a
    left join charge_backs_financial b
        on a.order_id = b.chargeback_order_id
    left join fraud_alerts c
        on a.order_id = c.order_id
    group by account, order_description, psp_name, month_
)

select
    *,
    chargebacks + fraud_alerts as vamp_count,
    round(safe_divide((chargebacks + fraud_alerts), orders ), 2) as vamp_ratio
from joined

