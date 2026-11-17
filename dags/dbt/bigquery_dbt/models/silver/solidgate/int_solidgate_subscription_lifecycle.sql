{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['subscription_id', '_elt_account_name'],
    on_schema_change='append_new_columns',
    tags=["solidgate", "silver", "daily"],
    partition_by={
        "field": "_elt_update_timestamp",
        "data_type": "timestamp"
    }
) }}

-- Status and lifecycle dates of each subscription, from the same payload the other
-- subscription models shred.
-- Grain: one row per subscription (per account).

select
    s.id                   as subscription_id,
    s.status,
    date(s.started_at)     as started_date,
    date(s.expired_at)     as expired_date,
    date(s.cancelled_at)   as cancelled_date,
    date(s.next_charge_at) as next_charge_date,
    s._elt_account_name,
    s._elt_update_timestamp
from {{ source('bronze_solidgate', 'subscriptions') }} s
{% if is_incremental() %}
where s._elt_update_timestamp >= (
    select max(_elt_update_timestamp) from {{ this }}
    )
{% endif %}
