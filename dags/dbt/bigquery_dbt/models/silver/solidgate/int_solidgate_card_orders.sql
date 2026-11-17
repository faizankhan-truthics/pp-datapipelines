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

with transaction_dates as (
    select
        order_id,
        min(case when transaction_operation = 'settle'
            then safe_cast(transaction_created_at as timestamp) end) as settled_at,
        min(case when transaction_operation = 'refund'
            then safe_cast(transaction_created_at as timestamp) end) as refunded_at
    from {{ ref('int_solidgate_card_orders_transactions') }}
    where transaction_operation in ('settle', 'refund')
      and transaction_status = 'success'
    group by order_id
),

source as (
    SELECT
        order_id,
        psp_order_id,
        provider_payment_id,
        status,
        case
            when status in ('approved', 'settle_ok', 'partial_settled', 'refunded') then true
            else false
        end as vamp_include, --count order_ids
        case when status = 'settle_ok' then 1 else 0 end as settled,
        case when status = 'refunded' then 1 else 0 end as refunded,
        geo_country,
        case
            when geo_country = 'USA' THEN 'USA'
            when geo_country in (
                'ARG', -- Argentina
                'BOL', -- Bolivia
                'BRA', -- Brazil
                'CHL', -- Chile
                'COL', -- Colombia
                'CRI', -- Costa Rica
                'CUB', -- Cuba
                'DOM', -- Dominican Republic
                'ECU', -- Ecuador
                'SLV', -- El Salvador
                'GTM', -- Guatemala
                'HND', -- Honduras
                'MEX', -- Mexico
                'NIC', -- Nicaragua
                'PAN', -- Panama
                'PRY', -- Paraguay
                'PER', -- Peru
                'URY', -- Uruguay
                'VEN'  -- Venezuela
            ) then 'LATAM'
            else 'Other'
        end as region,
        type,
        amount as order_amount_local,
        currency order_currency,
        settled_amount,
        settled_currency,
        refunded_amount,
        refunded_currency,
        refund,
        order_description,
        mid as merchant_id,
        processing_amount,
        processing_currency,
        customer_account_id,
        -- md5 of the order's email, the identity key the journey mart joins on; the address is not kept
        to_hex(md5(nullif(lower(trim(customer_email)), ''))) as customer_email_key,
        fraudulent,
        safe_cast(created_at as timestamp) as created_at,
        date(created_at) created_date,
        updated_at,
        settled_at,
        date(settled_at) as settlement_date,
        refunded_at,
        date(refunded_at) as refund_date,
        subscription_id,
        product_name,
        payment_type,
        error_code,
        product_type,
        platform,
        product_id,
        is_secured,
        --transactions --need to unnest this
        _elt_account_name,
        _elt_update_timestamp
    from {{ source('bronze_solidgate', 'solidgate_card_orders') }}
    left join transaction_dates using (order_id)
    {% if is_incremental() %}
    where _elt_update_timestamp >= (
        select max(_elt_update_timestamp) from {{ this }}
        )
    {% endif %}
),

data as (
select
    a.* except (_elt_account_name, _elt_update_timestamp),
    m.psp_name,
    m.connector as mid_connector,
    m.acquiring_bank,
    m.descriptor,
    -- order_amount_local is in minor units; order_amount_usd is in major units (dollars)
     case
        when a.order_currency = 'USD' then a.order_amount_local / {{ minor_unit_divisor('a.order_currency') }}
        when a.order_amount_local = 0 then 0
        else a.order_amount_local / {{ minor_unit_divisor('a.order_currency') }} * b.rate
    end as order_amount_usd,
    e.error_message,
    e.error_next_step,
    cot.transaction_card_brand,
    cot.transaction_card_bin,
    cot.transaction_card_bank,
    a._elt_account_name,
    a._elt_update_timestamp
from source a
left join {{ source('bronze_solidgate', 'solidgate_mid_lookup') }} m
on a.merchant_id = m.mid
left join {{ ref('int_currency_conversion') }} b
on a.created_date = b.exchange_date
and a.order_currency = b.currency
left join {{ source('bronze_solidgate', 'solidgate_error_codes') }} e
on a.error_code = e.code
left join {{ ref('int_solidgate_card_orders_transactions') }} cot
on a.order_id = cot.order_id and cot.transaction_rank = 1
)

select * from data
where order_amount_usd is not null