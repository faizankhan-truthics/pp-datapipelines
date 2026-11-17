{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = 'chargeback_id',
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
      a.order_id as chargeback_order_id,
      a._elt_account_name,
      a._elt_update_timestamp,
      json_value(c, '$.id') as chargeback_id,
      json_value(c, '$.reason_code') as chargeback_reason_code,
      json_value(c, '$.reason_description') as chargeback_reason_description,
      json_value(c, '$.reason_group') as chargeback_reason_group,
      json_value(c, '$.type') as chargeback_type,
      json_value(c, '$.status') as chargeback_status,
      safe_cast(json_value(c, '$.amount') as numeric) as chargeback_amount,
      json_value(c, '$.created_at') as chargeback_created_at,
      json_value(c, '$.updated_at') as chargeback_updated_at,
      json_value(c, '$.currency') as chargeback_currency,
      date(json_value(c, '$.dispute_date')) as chargeback_dispute_date, --issuer
      json_value(c, '$.settlement_date') as chargeback_settlement_date,
      case
        when json_value(c, '$.status') in ('resolved','resolved_reversal') then false
        else true
      end as vamp_include --count chargeback_ids
    FROM {{ source('bronze_solidgate', 'solidgate_chargebacks') }} a,
    UNNEST(JSON_QUERY_ARRAY(a.chargebacks)) as c
    {% if is_incremental() %}
where a._elt_update_timestamp >= (
    select max(_elt_update_timestamp) from {{ this }}
    )
{% endif %}
),
data as (
select
    a.*,
    co.psp_name,
    -- chargeback_amount is in minor units; chargeback_amount_usd is in major units (dollars)
     case
        when a.chargeback_currency = 'USD' then a.chargeback_amount / {{ minor_unit_divisor('a.chargeback_currency') }}
        else a.chargeback_amount / {{ minor_unit_divisor('a.chargeback_currency') }} * b.rate
    end as chargeback_amount_usd
from source a
left join {{ ref('int_solidgate_card_orders') }} co
    on a.chargeback_order_id = co.order_id
left join {{ ref('int_currency_conversion') }} b
on a.chargeback_dispute_date = b.exchange_date
and a.chargeback_currency = b.currency
)

select * from data
where chargeback_amount_usd is not null