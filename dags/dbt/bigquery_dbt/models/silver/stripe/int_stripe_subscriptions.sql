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

-- Grain: one row per subscription (per account). Item/price/product detail lives in
-- int_stripe_subscription_items since a subscription can have more than one item.

select
    id,
    customer,
    status,
    upper(currency) as currency,
    date(current_period_start) as current_period_start_date,
    date(current_period_end) as current_period_end_date,
    date(start_date) as start_date,
    cancel_at_period_end,
    cancel_at,
    canceled_at,
    ended_at,
    trial_start,
    trial_end,
    latest_invoice,
    collection_method,
    days_until_due,
    default_payment_method,
    default_source,
    description,
    application,
    application_fee_percent,
    billing_cycle_anchor,
    pending_setup_intent,
    schedule,
    metadata,
    json_value(cancellation_details, '$.reason') as cancellation_reason,
    json_value(cancellation_details, '$.feedback') as cancellation_feedback,
    json_value(cancellation_details, '$.comment') as cancellation_comment,
    json_value(discount, '$.coupon.id') as discount_coupon_id,
    safe_cast(json_value(discount, '$.coupon.amount_off') as int64) as discount_amount_off,
    safe_cast(json_value(discount, '$.coupon.percent_off') as float64) as discount_percent_off,
    timestamp_seconds(safe_cast(json_value(discount, '$.end') as int64)) as discount_end,
    safe_cast(json_value(automatic_tax, '$.enabled') as bool) as automatic_tax_enabled,
    _elt_stripe_account_id,
    _elt_update_timestamp
from {{ source('bronze_stripe', 'stripe_subscriptions') }}
{% if is_incremental() %}
where _elt_update_timestamp >= (
    select max(_elt_update_timestamp) from {{ this }}
    )
{% endif %}
