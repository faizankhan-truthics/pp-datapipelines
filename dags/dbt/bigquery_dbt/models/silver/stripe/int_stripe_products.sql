{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = ['product_id', '_elt_stripe_account_id'],
    on_schema_change='append_new_columns',
    tags=["stripe", "silver", "daily"],
    partition_by={
        "field": "_elt_update_timestamp",
        "data_type": "timestamp"
    }
    )
}}

-- Grain: one row per product (per account).

select
    id as product_id,
    name as product_name,
    active,
    default_price,
    description,
    shippable,
    statement_descriptor,
    tax_code,
    unit_label,
    url,
    metadata,
    json_value(images, '$[0]') as product_image_url,
    date(created_ts) as created_date,
    date(updated_ts) as updated_date,
    is_deleted,
    _elt_stripe_account_id,
    _elt_update_timestamp
from {{ source('bronze_stripe', 'stripe_products') }}
{% if is_incremental() %}
where _elt_update_timestamp >= (
    select max(_elt_update_timestamp) from {{ this }}
    )
{% endif %}
