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

-- Grain: one row per invoice (per account).
-- Unlike other int_stripe_* models this does not drop rows with no USD conversion
-- rate: an invoice is primarily used here to link a charge to a subscription/product,
-- and dropping unconvertable-currency rows would silently break that join.

with raw as (
    select
        id,
        customer,
        subscription,
        charge,
        payment_intent,
        status,
        collection_method,
        billing_reason,
        amount_due as amount_due_local,
        amount_paid as amount_paid_local,
        amount_remaining as amount_remaining_local,
        starting_balance,
        ending_balance,
        subtotal,
        tax,
        total,
        number,
        upper(currency) as currency,
        account_country,
        account_name,
        application,
        attempt_count,
        attempted,
        auto_advance,
        customer_email,
        customer_name,
        default_payment_method,
        description,
        due_date,
        footer,
        hosted_invoice_url,
        invoice_pdf,
        metadata,
        next_payment_attempt,
        receipt_number,
        statement_descriptor,
        json_value(customer_address, '$.country') as customer_country,
        json_value(customer_address, '$.city') as customer_city,
        json_value(customer_address, '$.line1') as customer_address_line1,
        json_value(customer_address, '$.postal_code') as customer_postal_code,
        json_value(discount, '$.coupon.id') as discount_coupon_id,
        safe_cast(json_value(discount, '$.coupon.amount_off') as int64) as discount_amount_off,
        safe_cast(json_value(discount, '$.coupon.percent_off') as float64) as discount_percent_off,
        timestamp_seconds(safe_cast(json_value(discount, '$.end') as int64)) as discount_end,
        date(created_ts) as created_date,
        date(period_start) as period_start_date,
        date(period_end) as period_end_date,
        paid,
        is_deleted,
        _elt_stripe_account_id,
        _elt_update_timestamp
    from {{ source('bronze_stripe', 'stripe_invoices') }}
    {% if is_incremental() %}
    where _elt_update_timestamp >= (
        select max(_elt_update_timestamp) from {{ this }}
        )
    {% endif %}
)

select
    a.*,
    case
        when a.currency = 'USD' then a.amount_due_local
        else a.amount_due_local * b.rate
    end as amount_due_usd,
    case
        when a.currency = 'USD' then a.amount_paid_local
        else a.amount_paid_local * b.rate
    end as amount_paid_usd
from raw a
left join {{ ref('int_currency_conversion') }} b
on a.created_date = b.exchange_date
and a.currency = b.currency
