{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = 'price_id',
    on_schema_change='append_new_columns',
    tags=["stripe", "silver", "daily"],
    partition_by={
        "field": "_elt_update_timestamp",
        "data_type": "timestamp"
    }
    )
}}

-- Grain: one row per price (per account).

select
    id as price_id,
    product as product_id,
    nickname,
    active,
    type,
    upper(currency) as currency,
    unit_amount,
    unit_amount_decimal,
    billing_scheme,
    lookup_key,
    tax_behavior,
    tiers_mode,
    metadata,
    json_value(recurring, '$.interval') as billing_interval,
    safe_cast(json_value(recurring, '$.interval_count') as int64) as billing_interval_count,
    date(created_ts) as created_date,
    is_deleted,
    _elt_stripe_account_id,
    _elt_update_timestamp
from {{ source('bronze_stripe', 'stripe_prices') }}
{% if is_incremental() %}
where _elt_update_timestamp >= (
    select max(_elt_update_timestamp) from {{ this }}
    )
{% endif %}
