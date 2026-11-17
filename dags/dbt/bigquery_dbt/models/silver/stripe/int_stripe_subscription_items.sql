{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = 'subscription_item_id',
    on_schema_change='append_new_columns',
    tags=["stripe", "silver", "daily"],
    partition_by={
        "field": "_elt_update_timestamp",
        "data_type": "timestamp"
    }
    )
}}

-- Item (price/product) detail shredded out of each subscription's `items` payload.
-- Grain: subscription_item_id.

select
    s.id as subscription_id,
    json_value(item, '$.id') as subscription_item_id,
    json_value(item, '$.price.id') as price_id,
    json_value(item, '$.price.product') as product_id,
    safe_cast(json_value(item, '$.quantity') as int64) as quantity,
    s._elt_stripe_account_id,
    s._elt_update_timestamp
from {{ source('bronze_stripe', 'stripe_subscriptions') }} s,
unnest(json_query_array(s.items, '$.data')) as item
{% if is_incremental() %}
where s._elt_update_timestamp >= (
    select max(_elt_update_timestamp) from {{ this }}
    )
{% endif %}
