{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = 'order_id',
    on_schema_change='append_new_columns',
    tags=["solidgate", "silver", "daily"],
    partition_by={
        "field": "_elt_update_timestamp",
        "data_type": "timestamp"
    }
    )
}}

-- routing.cascade_steps holds one entry per cascade attempt; keep the final step (highest
-- cascade_number), i.e. the route that processed the order, so the model stays one row per order
select
    order_id,
    json_value(r, '$.mid') as mid,
    json_value(r, '$.mid_descriptor') as mid_descriptor,
    json_value(r, '$.route_id') as route_id,
    json_value(r, '$.segment_id') as segment_id,
    safe_cast(json_value(r, '$.cascade_number') as int64) as cascade_number,
    json_value(r, '$.connector_account_id') as connector_account_id,
    count(*) over (partition by order_id) as cascade_steps,
    _elt_account_name,
    _elt_update_timestamp
from {{ source('bronze_solidgate', 'solidgate_card_orders') }},
unnest(json_query_array(routing, '$.cascade_steps')) as r
{% if is_incremental() %}
where _elt_update_timestamp >= (
    select max(_elt_update_timestamp) from {{ this }}
    )
{% endif %}
qualify row_number() over (
    partition by order_id
    order by safe_cast(json_value(r, '$.cascade_number') as int64) desc
) = 1
