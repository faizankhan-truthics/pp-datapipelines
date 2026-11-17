{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['account_name', 'subscription_id', 'updated_at'],
    tags=["solidgate", "silver", "daily"]
) }}

-- Historical subscription lifecycle: one row per subscription version (state as of a
-- given updated_at), with the period that version was valid. Source is the append-only
-- `subscriptions_release_history` bronze table -- NOT the upsert `subscriptions` bronze
-- table, which only ever holds the current state.
--
-- Incremental: only reads bronze rows landed since the last run, and only recomputes
-- the subscriptions those rows touch -- pulling each touched subscription's existing
-- versions back in so valid_from/valid_to stay correct across the merge (closing out
-- the previously-open row, chaining multiple new versions if more than one arrived
-- since the last run). Dedup/versioning logic matches the validated sandbox table
-- (data-499514.gold_sandbox.tmp_solidgate_subscription_history) exactly.

with new_raw_versions as (

    select
        _elt_account_name as account_name,

        json_value(raw_data, '$.id')     as subscription_id,
        json_value(raw_data, '$.status') as status,

        safe_cast(json_value(raw_data, '$.created_at')   as timestamp) as created_at,
        safe_cast(json_value(raw_data, '$.updated_at')   as timestamp) as updated_at,
        safe_cast(json_value(raw_data, '$.started_at')   as timestamp) as started_at,
        safe_cast(json_value(raw_data, '$.expired_at')   as timestamp) as expired_at,
        safe_cast(json_value(raw_data, '$.cancelled_at') as timestamp) as cancelled_at,

        safe_cast(json_value(raw_data, '$.trial') as bool) as trial,
        json_value(raw_data, '$.payment_type')              as payment_type,

        json_value(raw_data, '$.customer.customer_account_id') as customer_id,
        json_value(raw_data, '$.customer.customer_email')      as customer_email,

        json_value(raw_data, '$.product.id')                             as product_id,
        json_value(raw_data, '$.product.name')                           as product_name,
        safe_cast(json_value(raw_data, '$.product.amount') as numeric)   as product_amount,
        json_value(raw_data, '$.product.currency')                       as product_currency,

        safe_cast(_elt_insert_timestamp as timestamp) as captured_at

    from {{ source('bronze_solidgate', 'subscriptions_release_history') }}
    where json_value(raw_data, '$.id') is not null
    {% if is_incremental() %}
    and safe_cast(_elt_insert_timestamp as timestamp) > (
        select max(captured_at) from {{ this }}
        )
    {% endif %}

),

{% if is_incremental() %}

touched_subscriptions as (

    select distinct account_name, subscription_id
    from new_raw_versions

),

existing_touched_versions as (

    select
        t.account_name, t.subscription_id, t.status, t.created_at, t.updated_at, t.started_at,
        t.expired_at, t.cancelled_at, t.trial, t.payment_type, t.customer_id, t.customer_email,
        t.product_id, t.product_name, t.product_amount, t.product_currency, t.captured_at
    from {{ this }} t
    where exists (
        select 1 from touched_subscriptions ts
        where ts.account_name = t.account_name and ts.subscription_id = t.subscription_id
        )

),

raw_versions as (

    select * from existing_touched_versions
    union all
    select * from new_raw_versions

),

{% else %}

raw_versions as (

    select * from new_raw_versions

),

{% endif %}

deduped as (

    select *
    from raw_versions
    qualify row_number() over (
        partition by account_name, subscription_id, updated_at
        order by captured_at desc
    ) = 1

),

history as (

    select
        *,
        updated_at as valid_from,
        lead(updated_at) over (
            partition by account_name, subscription_id
            order by updated_at
        ) as valid_to
    from deduped

)

select *
from history
where updated_at is not null
