{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='append_new_columns',
    tags=["solidgate", "silver", "daily"],
    partition_by={
        "field": "sg_date",
        "data_type": "date"
    }
) }}

-- Solidgate orders across every account, payment side of the generalized GA4 stitch. Sourced
-- from the subscription invoice orders rather than financial_entries: financial_entries carries
-- no subscription id, so the stitch dead-ended there, while invoice orders carry subscription_id
-- and invoice_id, which is what lets GA4 attribution reach the MRR models later. Every order
-- attempt counts, successful or not.
-- Grain: sg_date x _elt_account_name x transaction_id -- order_id is only unique within an
-- account's own namespace, not globally. Defensively filters blank order_id even though the
-- source doesn't currently have any (financial_entries did, before this source switch).

select distinct
    -- when the payment was attempted, which is when the customer was on the site.
    -- order_processed_at can land hours later and cross midnight.
    date(order_created_at) as sg_date,
    order_id                as transaction_id,
    _elt_account_name,
    subscription_id,
    invoice_id,
    order_status,
    order_amount_usd
from {{ ref('int_solidgate_subscription_invoice_orders') }}
where order_id is not null
  and order_id != ''
  and order_created_at is not null
{% if is_incremental() %}
  and date(order_created_at) >= date_sub(
    current_date(), interval {{ var('ga4_lookback_days', 3) }} day
  )
{% endif %}
