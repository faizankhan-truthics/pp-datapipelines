{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['transaction_id', '_elt_account_name'],
    on_schema_change='append_new_columns',
    tags=["paypal", "silver", "daily"],
    partition_by={
        "field": "_elt_update_timestamp",
        "data_type": "timestamp"
    }
) }}

select
    transaction_id,
    transaction_event_code,
    transaction_initiation_date,
    transaction_updated_date,
    transaction_amount,
    transaction_currency,
    fee_amount,
    fee_currency,
    ending_balance,
    ending_balance_currency,
    transaction_status,
    transaction_subject,
    invoice_id,
    custom_field,
    protection_eligibility,
    _elt_account_name,
    _elt_update_timestamp
from {{ source('bronze_paypal', 'paypal_transactions') }}
{% if is_incremental() %}
where _elt_update_timestamp >= (
    select coalesce(
        timestamp_sub(max(_elt_update_timestamp), interval 5 day),
        timestamp('1970-01-01')
    )
    from {{ this }}
)
{% endif %}
