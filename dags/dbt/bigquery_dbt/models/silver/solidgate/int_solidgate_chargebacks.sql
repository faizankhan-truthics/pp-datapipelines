{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key = 'order_id',
    on_schema_change='append_new_columns',
    tags=["solidgate", "silver", "daily"],
    partition_by={
        "field": "_elt_update_timestamp",
        "data_type": "timestamp"
    }
    )
}}

with source as (
    select
        order_id,
        status,
        type,
        amount amount_local,
        currency,
        order_description,
        customer_account_id,
        fraudulent,
        date(created_at) created_date,
        updated_at,
        _elt_account_name,
        _elt_update_timestamp
    from {{ source('bronze_solidgate', 'solidgate_chargebacks') }}
    {% if is_incremental() %}
        where _elt_update_timestamp >= (
        select max(_elt_update_timestamp) from {{ this }}
        )
    {% endif %}
),

data as(
select
    a.*,
    co.psp_name,
    -- amount_local is in minor units; amount_usd is in major units (dollars)
     case
        when a.currency = 'USD' then a.amount_local / {{ minor_unit_divisor('a.currency') }}
        else a.amount_local / {{ minor_unit_divisor('a.currency') }} * b.rate
    end as amount_usd
from source a
left join {{ ref('int_solidgate_card_orders') }} co
    on a.order_id = co.order_id
left join {{ ref('int_currency_conversion') }} b
on a.created_date = b.exchange_date
and a.currency = b.currency
)

select * from data
where amount_usd is not null