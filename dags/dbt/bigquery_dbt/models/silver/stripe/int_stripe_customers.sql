{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = ['id', '_elt_stripe_account_id'],
    on_schema_change='append_new_columns',
    tags=["stripe", "silver", "daily"],
    partition_by={
        "field": "_elt_update_timestamp",
        "data_type": "timestamp"
    }
    )
}}

-- Grain: one row per customer (per account).

select
    id,
    email,
    name,
    phone,
    delinquent,
    balance,
    upper(currency) as currency,
    default_source,
    description,
    invoice_prefix,
    next_invoice_sequence,
    tax_exempt,
    test_clock,
    metadata,
    json_value(invoice_settings, '$.default_payment_method') as invoice_default_payment_method,
    json_value(discount, '$.coupon.id') as discount_coupon_id,
    safe_cast(json_value(discount, '$.coupon.amount_off') as int64) as discount_amount_off,
    safe_cast(json_value(discount, '$.coupon.percent_off') as float64) as discount_percent_off,
    timestamp_seconds(safe_cast(json_value(discount, '$.end') as int64)) as discount_end,
    json_value(preferred_locales, '$[0]') as preferred_locale,
    date(created_ts) as created_date,
    json_value(address, '$.country') as billing_country,
    json_value(shipping, '$.address.country') as shipping_country,
    is_deleted,
    _elt_stripe_account_id,
    _elt_update_timestamp
from {{ source('bronze_stripe', 'stripe_customers') }}
{% if is_incremental() %}
where _elt_update_timestamp >= (
    select max(_elt_update_timestamp) from {{ this }}
    )
{% endif %}
