{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['event_id', '_elt_stripe_account_id'],
    on_schema_change='append_new_columns',
    tags=["stripe", "silver", "daily"],
    partition_by={
        "field": "event_created_ts",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=["_elt_stripe_account_id", "id"]
    )
}}

-- Every historical version of each subscription, from the customer.subscription.* events.
-- Same columns as int_stripe_subscriptions (which keeps only the latest version), plus the
-- event columns. Grain: one row per subscription event (event_id, per account); each row is
-- the subscription as it stood right after that event.
-- Item (price/product) detail lives in int_stripe_subscription_items_history.

with events as (

    select
        id as event_id,
        type as event_type,
        created_ts as event_created_ts,
        json_query(data, '$.object') as sub,
        json_query(data, '$.previous_attributes') as previous_attributes,
        _elt_stripe_account_id,
        _elt_update_timestamp
    from {{ source('bronze_stripe', 'stripe_events') }}
    -- every Stripe event type whose data.object is a subscription
    where type in (
        'customer.subscription.created',
        'customer.subscription.updated',
        'customer.subscription.deleted',
        'customer.subscription.paused',
        'customer.subscription.resumed',
        'customer.subscription.pending_update_applied',
        'customer.subscription.pending_update_expired',
        'customer.subscription.trial_will_end'
    )
      and json_value(data, '$.object.object') = 'subscription'
    {% if is_incremental() %}
      and _elt_update_timestamp >= (
        select max(_elt_update_timestamp) from {{ this }}
        )
    {% endif %}

)

select
    event_id,
    event_type,
    event_created_ts,
    json_value(sub, '$.id') as id,
    json_value(sub, '$.customer') as customer,
    json_value(sub, '$.status') as status,
    upper(json_value(sub, '$.currency')) as currency,
    timestamp_seconds(safe_cast(json_value(sub, '$.created') as int64)) as created_ts,
    -- API versions from 2025-03-31 (basil) onward moved the period fields onto each item
    date(coalesce(
        timestamp_seconds(safe_cast(json_value(sub, '$.current_period_start') as int64)),
        (select min(timestamp_seconds(safe_cast(json_value(i, '$.current_period_start') as int64)))
         from unnest(json_query_array(sub, '$.items.data')) i)
    )) as current_period_start_date,
    date(coalesce(
        timestamp_seconds(safe_cast(json_value(sub, '$.current_period_end') as int64)),
        (select max(timestamp_seconds(safe_cast(json_value(i, '$.current_period_end') as int64)))
         from unnest(json_query_array(sub, '$.items.data')) i)
    )) as current_period_end_date,
    date(timestamp_seconds(safe_cast(json_value(sub, '$.start_date') as int64))) as start_date,
    safe_cast(json_value(sub, '$.cancel_at_period_end') as bool) as cancel_at_period_end,
    timestamp_seconds(safe_cast(json_value(sub, '$.cancel_at') as int64)) as cancel_at,
    timestamp_seconds(safe_cast(json_value(sub, '$.canceled_at') as int64)) as canceled_at,
    timestamp_seconds(safe_cast(json_value(sub, '$.ended_at') as int64)) as ended_at,
    timestamp_seconds(safe_cast(json_value(sub, '$.trial_start') as int64)) as trial_start,
    timestamp_seconds(safe_cast(json_value(sub, '$.trial_end') as int64)) as trial_end,
    json_value(sub, '$.latest_invoice') as latest_invoice,
    json_value(sub, '$.collection_method') as collection_method,
    safe_cast(json_value(sub, '$.days_until_due') as int64) as days_until_due,
    json_value(sub, '$.default_payment_method') as default_payment_method,
    json_value(sub, '$.default_source') as default_source,
    json_value(sub, '$.description') as description,
    json_value(sub, '$.application') as application,
    safe_cast(json_value(sub, '$.application_fee_percent') as float64) as application_fee_percent,
    timestamp_seconds(safe_cast(json_value(sub, '$.billing_cycle_anchor') as int64)) as billing_cycle_anchor,
    json_value(sub, '$.pending_setup_intent') as pending_setup_intent,
    json_value(sub, '$.schedule') as schedule,
    json_query(sub, '$.metadata') as metadata,
    json_value(sub, '$.cancellation_details.reason') as cancellation_reason,
    json_value(sub, '$.cancellation_details.feedback') as cancellation_feedback,
    json_value(sub, '$.cancellation_details.comment') as cancellation_comment,
    -- the single `discount` object is only on older API versions; newer ones carry
    -- discount ids only, without the coupon amounts
    json_value(sub, '$.discount.coupon.id') as discount_coupon_id,
    safe_cast(json_value(sub, '$.discount.coupon.amount_off') as int64) as discount_amount_off,
    safe_cast(json_value(sub, '$.discount.coupon.percent_off') as float64) as discount_percent_off,
    timestamp_seconds(safe_cast(json_value(sub, '$.discount.end') as int64)) as discount_end,
    safe_cast(json_value(sub, '$.automatic_tax.enabled') as bool) as automatic_tax_enabled,
    previous_attributes,
    _elt_stripe_account_id,
    _elt_update_timestamp
from events
