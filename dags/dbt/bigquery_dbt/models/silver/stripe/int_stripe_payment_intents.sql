{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = 'id',
    on_schema_change='append_new_columns',
    tags=["stripe", "silver", "daily"],
    partition_by={
        "field": "_elt_update_timestamp",
        "data_type": "timestamp"
    }
    )
}}

-- Note: bronze's client_secret and shared_payment_granted_token are deliberately
-- excluded — they are short-lived client-side auth tokens, not analytical data.

with raw as (
    select
        id,
        amount as amount_local,
        amount_capturable as amount_capturable_local,
        amount_received as amount_received_local,
        application_fee_amount,
        date(created_ts) as created_date,
        upper(currency) as currency,
        customer,
        customer_account,
        description,
        latest_charge,
        payment_method,
        capture_method,
        confirmation_method,
        status,
        cancellation_reason,
        canceled_at_ts,
        application,
        on_behalf_of,
        receipt_email,
        review,
        setup_future_usage,
        source,
        statement_descriptor,
        statement_descriptor_suffix,
        metadata,
        transfer_group,
        safe_cast(json_value(automatic_payment_methods, '$.enabled') as bool) as automatic_payment_methods_enabled,
        safe_cast(json_value(managed_payments, '$.enabled') as bool) as managed_payments_enabled,
        json_value(payment_method_configuration_details, '$.id') as payment_method_configuration_id,
        json_value(payment_method_types, '$[0]') as primary_payment_method_type,
        json_value(last_payment_error, '$.code') as last_payment_error_code,
        json_value(last_payment_error, '$.decline_code') as last_payment_error_decline_code,
        json_value(last_payment_error, '$.message') as last_payment_error_message,
        json_value(last_payment_error, '$.type') as last_payment_error_type,
        json_value(next_action, '$.type') as next_action_type,
        _elt_stripe_account_id,
        _elt_update_timestamp
    from {{ source('bronze_stripe', 'stripe_payment_intents') }}
    {% if is_incremental() %}
    where _elt_update_timestamp >= (
        select max(_elt_update_timestamp) from {{ this }}
        )
    {% endif %}
),

data as (
select
    a.*,
    case
        when a.currency = 'USD' then a.amount_local / {{ minor_unit_divisor('a.currency') }}
        else a.amount_local / {{ minor_unit_divisor('a.currency') }} * b.rate
    end as amount_usd
from raw a
left join {{ ref('int_currency_conversion') }} b
on a.created_date = b.exchange_date
and a.currency = b.currency
)

select * from data
where amount_usd is not null
