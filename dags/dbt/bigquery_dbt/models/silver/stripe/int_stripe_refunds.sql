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

with raw as (
    select
        a.id,
        amount as amount_local,
        balance_transaction,
        charge,
        created_ts,
        date(created_ts) as created_date,
        upper(currency) currency,
        customer,
        customer_account,
        payment_intent,
        payment_method,
        reason,
        status,
        receipt_number,
        source_transfer_reversal,
        transfer_reversal,
        metadata,
        json_value(destination_details, '$.type') as destination_type,
        json_value(destination_details, '$.card.reference_status') as destination_card_reference_status,
        json_value(destination_details, '$.card.reference_type') as destination_card_reference_type,
        json_value(payment_method_details, '$.type') as payment_method_type,
        json_value(payment_method_details, '$.card.acquirer_reference_number') as acquirer_reference_number,
        _elt_stripe_account_id,
        e.account as account_name,
         e.Product,
        _elt_update_timestamp
    from {{ source('bronze_stripe', 'stripe_refunds') }} a
    left join {{ source('bronze_stripe', 'stripe_accounts') }} e
    on a._elt_stripe_account_id = e.id
    {% if is_incremental() %}
    where _elt_update_timestamp >= (
        select coalesce(timestamp_sub(max(_elt_update_timestamp), interval 5 day), timestamp('1970-01-01')) from {{ this }}
        )
    {% endif %}
),

data as (
select
    a.*,
    -- Stripe amounts are minor units: divide to major units, then convert to USD
    case
        when a.currency = 'USD' then a.amount_local / {{ minor_unit_divisor('a.currency') }}
        else a.amount_local / {{ minor_unit_divisor('a.currency') }} * b.rate
    end as amount_usd,
    captured_date,
from raw a
left join {{ ref('int_currency_conversion') }} b
on a.created_date = b.exchange_date
and a.currency = b.currency
left join {{ ref('int_stripe_charges') }} c
on a.charge =  c.id
)

select * from data
where amount_usd is not null