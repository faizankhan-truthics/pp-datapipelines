{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['subscription_id', '_elt_account_name'],
    on_schema_change='append_new_columns',
    tags=["solidgate", "silver", "daily"],
    partition_by={
        "field": "_elt_update_timestamp",
        "data_type": "timestamp"
    }
) }}

-- Grain: one row per subscription (per account).
-- Amounts stay in minor units (cents); convert at the point of use.

select
    s.id                                                        as subscription_id,
    json_value(s.product, '$.id')                               as product_id,
    json_value(s.product, '$.name')                             as product_name,
    json_value(s.product, '$.payment_action')                   as payment_action,
    safe_cast(json_value(s.product, '$.amount') as int64)       as product_amount_cents,
    upper(json_value(s.product, '$.currency'))                  as currency,
    safe_cast(json_value(s.product, '$.trial') as bool)         as product_trial,
    safe_cast(json_value(s.product, '$.trial_period') as int64) as trial_period_minutes,
    s._elt_account_name,
    s._elt_update_timestamp
from {{ source('bronze_solidgate', 'subscriptions') }} s
{% if is_incremental() %}
where s._elt_update_timestamp >= (
    select max(_elt_update_timestamp) from {{ this }}
    )
{% endif %}
