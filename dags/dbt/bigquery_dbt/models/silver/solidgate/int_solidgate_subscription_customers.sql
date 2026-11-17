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

-- Customer object shredded out of the Solidgate subscriptions payload.
-- Grain: one row per subscription (per account).

select
    s.id                                            as subscription_id,
    json_value(s.customer, '$.customer_account_id') as customer_account_id,
    json_value(s.customer, '$.customer_email')      as customer_email,
    s._elt_account_name,
    s._elt_update_timestamp
from {{ source('bronze_solidgate', 'subscriptions') }} s
{% if is_incremental() %}
where s._elt_update_timestamp >= (
    select max(_elt_update_timestamp) from {{ this }}
    )
{% endif %}
