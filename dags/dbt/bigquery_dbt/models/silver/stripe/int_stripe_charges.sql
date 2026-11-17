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

-- the charge row is only fetched on its creation day, so later refunds and captures never reach
-- it: the refunded amount comes from the refunds, the collected state from the balance transaction
with refunds as (
    select
        charge,
        _elt_stripe_account_id,
        sum(amount) as refunded_local
    from {{ source('bronze_stripe', 'stripe_refunds') }}
    where status = 'succeeded'
      and charge is not null
    group by 1, 2
),

raw as (
    select
        a.id,
        a.amount as amount_local,
        if(not coalesce(a.captured, false) and b.amount is not null, a.amount, a.amount_captured)
            as amount_captured_local,
        greatest(coalesce(a.amount_refunded, 0), coalesce(r.refunded_local, 0)) as amount_refunded_local,
        a.balance_transaction,
        a.created_ts,
        date(a.created_ts) as created_date,
        upper(a.currency) currency,
        a.customer,
        a.`order` as order_,
        a.paid,
        coalesce(a.captured, false) or b.amount is not null as captured,
        a.calculated_statement_descriptor,
        a.payment_intent,
        a.payment_method,
        a.refunded,
        a.disputed,
        a.status,
        a.description,
        a.application,
        a.application_fee,
        a.application_fee_amount,
        a.destination,
        a.dispute,
        a.failure_balance_transaction,
        a.failure_code,
        a.failure_message,
        a.on_behalf_of,
        a.receipt_email,
        a.receipt_number,
        a.receipt_url,
        a.review,
        a.source,
        a.source_transfer,
        a.statement_descriptor,
        a.statement_descriptor_suffix,
        a.transfer_group,
        json_value(payment_method_details,'$.card.brand') as card_brand,
        a.payment_method_details,
        a.metadata,
        json_value(billing_details, '$.address.country') as billing_country,
        json_value(billing_details, '$.address.city') as billing_city,
        json_value(billing_details, '$.address.line1') as billing_line1,
        json_value(billing_details, '$.address.postal_code') as billing_postal_code,
        json_value(billing_details, '$.address.state') as billing_state,
        json_value(billing_details, '$.name') as billing_name,
        json_value(billing_details, '$.email') as billing_email,
        json_value(fraud_details, '$.user_report') as fraud_user_report,
        json_value(outcome, '$.type') as outcome_type,
        json_value(outcome, '$.network_status') as outcome_network_status,
        json_value(outcome, '$.reason') as outcome_reason,
        json_value(outcome, '$.risk_level') as outcome_risk_level,
        safe_cast(json_value(outcome, '$.risk_score') as int64) as outcome_risk_score,
        json_value(outcome, '$.seller_message') as outcome_seller_message,
        json_value(outcome, '$.advice_code') as outcome_advice_code,
        json_value(outcome, '$.network_advice_code') as outcome_network_advice_code,
        json_value(outcome, '$.network_decline_code') as outcome_network_decline_code,
        b.amount as balance_transaction_amount,
        date(b.created_ts) as captured_date,
        a._elt_stripe_account_id,
        e.account as account_name,
        e.Product,
        a._elt_update_timestamp
    from {{ source('bronze_stripe', 'stripe_charges') }} a
    left join {{ ref('int_stripe_balance_transactions_dedupe') }} b
    on a.id =  b.source
    left join refunds r
    on a.id = r.charge
    and a._elt_stripe_account_id = r._elt_stripe_account_id
    left join {{ source('bronze_stripe', 'stripe_accounts') }} e
    on a._elt_stripe_account_id = e.id
    {% if is_incremental() %}
    where a._elt_update_timestamp >= (
        select coalesce(timestamp_sub(max(_elt_update_timestamp), interval 5 day), timestamp('1970-01-01')) from {{ this }}
        )
       -- the balance transaction often lands after the charge row was loaded: re-read charges whose
       -- balance transaction arrived recently, or they stay uncollected (captured is false since 2026-07)
       or a.id in (
        select source
        from {{ source('bronze_stripe', 'stripe_balance_transactions') }}
        where type = 'charge'
          and _elt_update_timestamp >= (
            select coalesce(timestamp_sub(max(_elt_update_timestamp), interval 5 day), timestamp('1970-01-01')) from {{ this }}
          )
       )
       or a.id in (
        select charge
        from {{ source('bronze_stripe', 'stripe_refunds') }}
        where _elt_update_timestamp >= (
            select coalesce(timestamp_sub(max(_elt_update_timestamp), interval 5 day), timestamp('1970-01-01')) from {{ this }}
          )
       )
    {% endif %}
),

data as (
-- *_local are Stripe minor units; *_usd are major units (dollars), zero-decimal currencies handled
select
    a.*,
    case
        when a.currency = 'USD' then a.amount_local / {{ minor_unit_divisor('a.currency') }}
        else a.amount_local / {{ minor_unit_divisor('a.currency') }} * b.rate
    end as amount_usd,
    case
        when a.currency = 'USD' then a.amount_captured_local / {{ minor_unit_divisor('a.currency') }}
        else a.amount_captured_local / {{ minor_unit_divisor('a.currency') }} * b.rate
    end as amount_captured_usd,
    case
        when a.currency = 'USD' then a.amount_refunded_local / {{ minor_unit_divisor('a.currency') }}
        else a.amount_refunded_local / {{ minor_unit_divisor('a.currency') }} * b.rate
    end as amount_refunded_usd
from raw a
left join {{ ref('int_currency_conversion') }} b
on a.created_date = b.exchange_date
and a.currency = b.currency
)

select * from data
where amount_usd is not null