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

-- NOTE: stripe_monthly_recon still joins bronze stripe_early_fraud_warnings
-- directly — left as-is; stripe_monthly_vamp was rewired to this model.

select
    a.id,
    charge,
    payment_intent,
    fraud_type,
    actionable,
    livemode,
    date(created_ts) as created_date,
    _elt_stripe_account_id,
     e.account as account_name,
     e.Product,
    _elt_update_timestamp
from {{ source('bronze_stripe', 'stripe_early_fraud_warnings') }} a
left join {{ source('bronze_stripe', 'stripe_accounts') }} e
on a._elt_stripe_account_id = e.id
{% if is_incremental() %}
where _elt_update_timestamp >= (
    select max(_elt_update_timestamp) from {{ this }}
    )
{% endif %}
