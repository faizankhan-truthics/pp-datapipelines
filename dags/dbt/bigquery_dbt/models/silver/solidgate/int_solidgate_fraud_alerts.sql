{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = 'fraud_alert_id',
    on_schema_change='append_new_columns',
    tags=["solidgate", "silver", "daily"],
    partition_by={
        "field": "_elt_update_timestamp",
        "data_type": "timestamp"
    }
    )
}}

with source as (
    SELECT
        id fraud_alert_id, --count chargeback_ids for vamp
        date(created_at) created_date,
        updated_at,
        order_id,
        fraud_amount AS fraud_amount_local,
        fraud_currency,
        fraud_amount_usd / 100 as fraud_amount_usd, -- Solidgate sends USD cents
        fraud_type,
        card_scheme,
        case
            when card_scheme = 'VISA' then true
            else false
        end as vamp_alerts,
        fraud_report_date,
        reason_code_description,
        _elt_account_name,
        _elt_endpoint,
        _elt_insert_timestamp,
        _elt_update_timestamp
    FROM {{ source('bronze_solidgate', 'solidgate_fraud_alerts') }}
    {% if is_incremental() %}
    where _elt_update_timestamp >= (
        select max(_elt_update_timestamp) from {{ this }}
        )
    {% endif %}
)

select
    s.*,
    co.psp_name
from source s
left join {{ ref('int_solidgate_card_orders') }} co
    on s.order_id = co.order_id