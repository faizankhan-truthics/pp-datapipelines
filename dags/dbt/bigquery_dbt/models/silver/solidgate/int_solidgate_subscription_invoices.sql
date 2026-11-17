{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['subscription_id', 'invoice_id', '_elt_account_name'],
    on_schema_change='append_new_columns',
    tags=["solidgate", "silver", "daily"],
    partition_by={
        "field": "_elt_update_timestamp",
        "data_type": "timestamp"
    }
) }}

-- Invoices map shredded out of the Solidgate subscriptions payload.
-- The source JSON is an object keyed by invoice id, so the key is unnested and
-- Grain: one row per subscription x invoice.
-- Amounts stay in minor units (cents); timestamps carry no tz marker and are UTC.
-- product_* is rolled up from int_solidgate_subscription_invoice_orders (order
-- grain): invoices carry no product field of their own, only their orders do.
-- Retries on one invoice should share a single product; ties break toward the
-- settled order, then the most recent attempt.

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

invoice_products as (

    select
        subscription_id,
        invoice_id,
        product_id,
        product_name,
        product_type,
        _elt_account_name
    from {{ ref('int_solidgate_subscription_invoice_orders') }}
    where product_id is not null
    qualify row_number() over (
        partition by subscription_id, invoice_id, _elt_account_name
        order by
            case when order_status = 'settle_ok' then 0 else 1 end,
            order_retry_attempt desc
    ) = 1

)

select
    s.subscription_id,
    inv_key                                                           as invoice_id,
    json_value(inv, '$.status')                                       as invoice_status,
    safe_cast(json_value(inv, '$.amount') as int64)                   as invoice_amount_cents,
    safe_cast(json_value(inv, '$.subscription_term_number') as int64) as subscription_term_number,
    safe.parse_timestamp('%Y-%m-%d %H:%M:%S', json_value(inv, '$.billing_period_started_at')) as billing_period_started_at,
    safe.parse_timestamp('%Y-%m-%d %H:%M:%S', json_value(inv, '$.billing_period_ended_at'))   as billing_period_ended_at,
    safe.parse_timestamp('%Y-%m-%d %H:%M:%S', json_value(inv, '$.created_at'))                as invoice_created_at,
    safe.parse_timestamp('%Y-%m-%d %H:%M:%S', json_value(inv, '$.updated_at'))                as invoice_updated_at,
    ip.product_id,
    ip.product_name,
    ip.product_type,
    s._elt_account_name,
    s._elt_update_timestamp
from source s,
unnest(ifnull(json_keys(s.invoices, 1), [])) as inv_key,
unnest([s.invoices[inv_key]])                as inv
left join invoice_products ip
    on ip.subscription_id = s.subscription_id
    and ip.invoice_id = inv_key
    and ip._elt_account_name = s._elt_account_name
