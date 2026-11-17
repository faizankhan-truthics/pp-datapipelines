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

-- NOTE: int_stripe_charges already resolves captured_date by joining bronze
-- stripe_balance_transactions directly (not through this model) — left as-is since
-- this model's `where amount_usd is not null` filter would drop rows lacking an FX
-- rate that the bronze-direct join currently still carries through.

with raw as (
    select
        id,
        amount as amount_local,
        fee as fee_local,
        net as net_local,
        date(created_ts) as created_date,
        date(available_on_ts) as available_on_date,
        upper(currency) as currency,
        type,
        reporting_category,
        balance_type,
        status,
        source,
        exchange_rate,
        description,
        json_value(fee_details, '$[0].type') as fee_type,
        json_value(fee_details, '$[0].description') as fee_description,
        _elt_stripe_account_id,
        _elt_update_timestamp
    from {{ source('bronze_stripe', 'stripe_balance_transactions') }}
    {% if is_incremental() %}
    where _elt_update_timestamp >= (
        select max(_elt_update_timestamp) from {{ this }}
        )
    {% endif %}
),

data as (
select
    a.*,
    case
        when a.currency = 'USD' then a.amount_local
        else a.amount_local * b.rate
    end as amount_usd,
    case
        when a.currency = 'USD' then a.net_local
        else a.net_local * b.rate
    end as net_usd
from raw a
left join {{ ref('int_currency_conversion') }} b
on a.created_date = b.exchange_date
and a.currency = b.currency
)

select * from data
where amount_usd is not null
