{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['subscription_id', 'invoice_id', 'order_id', '_elt_account_name'],
    on_schema_change='append_new_columns',
    tags=["solidgate", "silver", "daily"],
    partition_by={
        "field": "_elt_update_timestamp",
        "data_type": "timestamp"
    }
) }}

-- Orders (payment attempts) nested inside each invoice of the Solidgate
-- subscriptions payload. Both `invoices` and its `orders` are objects keyed by id,
-- so each level is unnested via its keys.
-- Grain: one row per subscription x invoice x order.
-- Kept separate from int_solidgate_subscription_invoices: retries live at this
-- grain and would double count anything aggregated per invoice.

with source as (

    select
        s.id as subscription_id,
        s.invoices,
        s._elt_account_name,
        s._elt_update_timestamp
    from {{ source('bronze_solidgate', 'subscriptions') }} s
    {% if is_incremental() %}
    where s._elt_update_timestamp >= (
        select max(_elt_update_timestamp) from {{ this }}
        )
    {% endif %}

),

invoices as (

    select
        s.subscription_id,
        inv_key                     as invoice_id,
        json_query(inv, '$.orders') as orders,
        s._elt_account_name,
        s._elt_update_timestamp
    from source s,
    unnest(ifnull(json_keys(s.invoices, 1), [])) as inv_key,
    unnest([s.invoices[inv_key]])                as inv

),

orders as (

    select
        i.subscription_id,
        i.invoice_id,
        ord_key                                              as order_id,
        json_value(o, '$.status')                            as order_status,
        json_value(o, '$.operation')                         as order_operation,
        safe_cast(json_value(o, '$.amount') as int64)        as order_amount_cents,
        safe_cast(json_value(o, '$.retry_attempt') as int64) as order_retry_attempt,
        safe.parse_timestamp('%Y-%m-%d %H:%M:%S', json_value(o, '$.created_at'))   as order_created_at,
        safe.parse_timestamp('%Y-%m-%d %H:%M:%S', json_value(o, '$.processed_at')) as order_processed_at,
        json_value(o, '$.payment_details.invoice_id')        as payment_details_invoice_id,
        json_value(o, '$.payment_details.payer_email')       as payment_details_payer_email,
        i._elt_account_name,
        i._elt_update_timestamp
    from invoices i,
    unnest(ifnull(json_keys(i.orders, 1), [])) as ord_key,
    unnest([i.orders[ord_key]])                as o

)

select
    o.*,
    co.psp_name,
    co.product_id,
    co.product_name,
    co.product_type,
    co.order_description,
    co.order_amount_usd,
    co.psp_order_id,
    co.provider_payment_id
from orders o
left join {{ ref('int_solidgate_card_orders') }} co
    on o.order_id = co.order_id
