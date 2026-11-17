{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = 'transaction_id',
    on_schema_change='append_new_columns',
    tags=["solidgate", "silver", "daily"],
    partition_by={
        "field": "_elt_update_timestamp",
        "data_type": "timestamp"
    }
    )
}}

with source as (
  SELECT
    a.order_id,
    a.error_code,
    a.product_type,
    a.platform,
    a.product_id,
    a.is_secured,
    a.geo_country,
    json_value(t, '$.authorization_type') as transaction_authorization_type,
    json_value(t, '$.id') as transaction_id,
    json_value(t, '$.operation') as transaction_operation,
    json_value(t, '$.currency') as transaction_currency,
    json_value(t, '$.refund_reason') as transaction_refund_reason,
    json_value(t, '$.refund_reason_code') as transaction_refund_reason_code,
    json_value(t, '$.status') as transaction_status,
    json_value(t, '$.amount') as transaction_amount,
    json_value(t, '$.card.brand') as transaction_card_brand,
    json_value(t, '$.card.bin') as transaction_card_bin,
    json_value(t, '$.card.bank') as transaction_card_bank,
    json_value(t, '$.created_at') as transaction_created_at,
    json_value(t, '$.updated_at') as transaction_updated_at,
    a.mid,
    a._elt_account_name,
    a._elt_update_timestamp,
    row_number() over (
        partition by a.order_id
        order by json_value(t, '$.updated_at') desc
    ) as transaction_rank
  from {{ source('bronze_solidgate', 'solidgate_card_orders') }} a,
  unnest(json_query_array(a.transactions)) as t
  {% if is_incremental() %}
  where a._elt_update_timestamp >= (
      select max(_elt_update_timestamp) from {{ this }}
      )
  {% endif %}
)

select
    s.* except (mid),
    m.psp_name,
    e.error_message,
    e.error_next_step
from source s
left join {{ source('bronze_solidgate', 'solidgate_mid_lookup') }} m
    on s.mid = m.mid
left join {{ source('bronze_solidgate', 'solidgate_error_codes') }} e
on s.error_code = e.code
