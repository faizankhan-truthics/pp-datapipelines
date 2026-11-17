{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = 'invoice_line_id',
    on_schema_change='append_new_columns',
    tags=["stripe", "silver", "daily"],
    partition_by={
        "field": "_elt_update_timestamp",
        "data_type": "timestamp"
    }
    )
}}

-- Line item (price/product) detail shredded out of each invoice's `lines` payload.
-- Grain: invoice_line_id. This is the join path from a charge to a product:
-- charge -> invoice (via invoice.charge) -> invoice line -> price -> product.

select
    i.id as invoice_id,
    i.charge as charge_id,
    i.subscription as subscription_id,
    json_value(line, '$.id') as invoice_line_id,
    json_value(line, '$.price.id') as price_id,
    json_value(line, '$.price.product') as product_id,
    safe_cast(json_value(line, '$.amount') as int64) as amount_local,
    upper(json_value(line, '$.currency')) as currency,
    json_value(line, '$.description') as description,
    safe_cast(json_value(line, '$.proration') as bool) as proration,
    date(timestamp_seconds(safe_cast(json_value(line, '$.period.start') as int64))) as period_start_date,
    date(timestamp_seconds(safe_cast(json_value(line, '$.period.end') as int64))) as period_end_date,
    i._elt_stripe_account_id,
    i._elt_update_timestamp
from {{ source('bronze_stripe', 'stripe_invoices') }} i,
unnest(json_query_array(i.lines, '$.data')) as line
{% if is_incremental() %}
where i._elt_update_timestamp >= (
    select max(_elt_update_timestamp) from {{ this }}
    )
{% endif %}
