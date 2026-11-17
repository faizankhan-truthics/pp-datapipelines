{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['_elt_account_name', 'transaction_id'],
    on_schema_change='append_new_columns',
    tags=["solidgate", "silver", "daily"],
    partition_by={
        "field": "attempt_date",
        "data_type": "date"
    },
    cluster_by=['_elt_account_name']
    )
}}

-- One row per card payment attempt (auth / recurring-auth / apple-pay / google-pay transaction).
-- Unlike int_solidgate_card_orders_transactions, the error code, status and card are taken from
-- each transaction, not the order: a cascaded order has several attempts with their own outcome.
-- Partitioned by attempt_date because consumers report attempts by the day they were made.
with source as (
    select
        a._elt_account_name,
        a.order_id,
        a.psp_order_id,
        a.provider_payment_id,
        a.subscription_id,
        a.product_id,
        a.product_name,
        a.type as order_type,
        a.payment_type,
        a.mid,
        json_value(t, '$.id') as transaction_id,
        json_value(t, '$.operation') as operation,
        json_value(t, '$.status') as status,
        json_value(t, '$.error.code') as error_code,
        json_value(t, '$.authorization_type') as authorization_type,
        json_value(t, '$.transaction_type') as transaction_type,
        safe_cast(json_value(t, '$.amount') as int64) as amount_minor,
        upper(json_value(t, '$.currency')) as currency,
        json_value(t, '$.card.card_id') as card_id,
        json_value(t, '$.card.bin') as card_bin,
        json_value(t, '$.card.brand') as card_brand,
        json_value(t, '$.card.card_type') as card_type,
        json_value(t, '$.card.country') as card_country,
        json_value(t, '$.card.bank') as card_bank,
        safe.timestamp(json_value(t, '$.created_at')) as attempt_ts,
        safe.timestamp(json_value(t, '$.updated_at')) as transaction_updated_ts,
        a._elt_update_timestamp
    from {{ source('bronze_solidgate', 'solidgate_card_orders') }} a,
    unnest(json_query_array(a.transactions)) as t
    where json_value(t, '$.operation') in ('auth', 'recurring-auth', 'apple-pay', 'google-pay')
    {% if is_incremental() %}
      and a._elt_update_timestamp >= (
        select coalesce(max(_elt_update_timestamp), timestamp('1970-01-01')) from {{ this }}
      )
    {% endif %}
)

select
    s._elt_account_name,
    s.order_id,
    s.psp_order_id,
    s.provider_payment_id,
    s.subscription_id,
    s.product_id,
    s.product_name,
    s.order_type,
    s.payment_type,
    s.transaction_id,
    s.operation,
    -- merchant-initiated renewal vs customer-initiated checkout payment (sign-ups, upsells, one-offs)
    s.operation = 'recurring-auth' as is_recurring,
    s.status,
    s.status = 'success' as is_approved,
    s.status in ('success', 'fail') as is_final,
    s.error_code,
    trim(e.error_message) as error_message,
    trim(e.error_next_step) as error_next_step,
    s.authorization_type,
    s.transaction_type,
    s.amount_minor,
    s.currency,
    s.card_id,
    s.card_bin,
    s.card_brand,
    s.card_type,
    s.card_country,
    s.card_bank,
    s.attempt_ts,
    date(s.attempt_ts) as attempt_date,
    -- 1 = first attempt of the order; > 1 = cascade attempt on another route within the same order
    row_number() over (
        partition by s._elt_account_name, s.order_id
        order by s.attempt_ts, s.transaction_id
    ) as attempt_seq_in_order,
    s.transaction_updated_ts,
    s.mid,
    m.psp_name,
    s._elt_update_timestamp
from source s
left join {{ source('bronze_solidgate', 'solidgate_mid_lookup') }} m
    on s.mid = m.mid
left join {{ source('bronze_solidgate', 'solidgate_error_codes') }} e
    on s.error_code = e.code
