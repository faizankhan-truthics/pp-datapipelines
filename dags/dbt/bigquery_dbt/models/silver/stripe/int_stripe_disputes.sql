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
        date(created_ts) as created_date,
        created_ts,
        upper(currency) currency,
        is_charge_refundable,
        payment_intent,
        reason,
        status,
        metadata,
        json_value(payment_method_details,'$.card.brand') as card_brand,
        case
            when coalesce(
                JSON_VALUE(network_details, '$.visa.rapid_dispute_resolution'),
                'false')
            = 'true' then true
            else false
            end as rdr,
        json_value(network_details, '$.type') as network_type,
        json_value(payment_method_details, '$.card.brand') as dispute_card_brand,
        json_value(payment_method_details, '$.card.network_reason_code') as network_reason_code,
        safe_cast(json_value(evidence_details, '$.has_evidence') as bool) as has_evidence,
        safe_cast(json_value(evidence_details, '$.submission_count') as int64) as evidence_submission_count,
        timestamp_seconds(safe_cast(json_value(evidence_details, '$.due_by') as int64)) as evidence_due_by,
        json_value(evidence, '$.customer_email_address') as evidence_customer_email,
        json_value(evidence, '$.customer_purchase_ip') as evidence_customer_purchase_ip,
        json_value(evidence, '$.customer_name') as evidence_customer_name,
        json_value(evidence, '$.billing_address') as evidence_billing_address,
        json_value(evidence, '$.uncategorized_file') as evidence_uncategorized_file,
        _elt_stripe_account_id,
        e.account as account_name,
        e.Product,
        _elt_update_timestamp
    from {{ source('bronze_stripe', 'stripe_disputes') }} a
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