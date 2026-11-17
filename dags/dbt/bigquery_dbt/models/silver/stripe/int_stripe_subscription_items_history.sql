{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['event_id', 'subscription_item_id', '_elt_stripe_account_id'],
    on_schema_change='append_new_columns',
    tags=["stripe", "silver", "daily"],
    partition_by={
        "field": "event_created_ts",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=["_elt_stripe_account_id", "subscription_id"]
    )
}}

-- Every historical version of each subscription's items, shredded out of the `items`
-- payload of each customer.subscription.* event. Same columns as
-- int_stripe_subscription_items (which keeps only the latest version), plus the event columns.
-- Grain: one row per subscription item per subscription event (event_id x subscription_item_id).
-- Join to int_stripe_subscriptions_history on event_id + _elt_stripe_account_id.

select
    e.id as event_id,
    e.type as event_type,
    e.created_ts as event_created_ts,
    json_value(e.data, '$.object.id') as subscription_id,
    json_value(item, '$.id') as subscription_item_id,
    json_value(item, '$.price.id') as price_id,
    json_value(item, '$.price.product') as product_id,
    safe_cast(json_value(item, '$.quantity') as int64) as quantity,
    e._elt_stripe_account_id,
    e._elt_update_timestamp
from {{ source('bronze_stripe', 'stripe_events') }} e,
unnest(json_query_array(e.data, '$.object.items.data')) as item
-- every Stripe event type whose data.object is a subscription
where e.type in (
    'customer.subscription.created',
    'customer.subscription.updated',
    'customer.subscription.deleted',
    'customer.subscription.paused',
    'customer.subscription.resumed',
    'customer.subscription.pending_update_applied',
    'customer.subscription.pending_update_expired',
    'customer.subscription.trial_will_end'
)
  and json_value(e.data, '$.object.object') = 'subscription'
{% if is_incremental() %}
  and e._elt_update_timestamp >= (
    select max(_elt_update_timestamp) from {{ this }}
    )
{% endif %}
