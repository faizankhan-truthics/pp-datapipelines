-- int_stripe_charges / refunds / disputes / payment_intents *_usd columns must be in USD major units
-- (dollars), not Stripe minor units (cents). USD rows have no FX rate, so each *_usd column must equal
-- its *_local counterpart divided by the currency's minor-unit divisor. Guards against the ~100x
-- inflation fixed in 2026-11.
select 'charge.amount_usd' as check_name, id, currency, amount_local as local_amount, amount_usd as usd_amount
from {{ ref('int_stripe_charges') }}
where currency = 'USD'
  and abs(amount_usd - amount_local / {{ minor_unit_divisor('currency') }}) > 0.005

union all

select 'charge.amount_captured_usd', id, currency, amount_captured_local, amount_captured_usd
from {{ ref('int_stripe_charges') }}
where currency = 'USD'
  and abs(amount_captured_usd - amount_captured_local / {{ minor_unit_divisor('currency') }}) > 0.005

union all

select 'charge.amount_refunded_usd', id, currency, amount_refunded_local, amount_refunded_usd
from {{ ref('int_stripe_charges') }}
where currency = 'USD'
  and abs(amount_refunded_usd - amount_refunded_local / {{ minor_unit_divisor('currency') }}) > 0.005

union all

select 'refund.amount_usd', id, currency, amount_local, amount_usd
from {{ ref('int_stripe_refunds') }}
where currency = 'USD'
  and abs(amount_usd - amount_local / {{ minor_unit_divisor('currency') }}) > 0.005

union all

select 'dispute.amount_usd', id, currency, amount_local, amount_usd
from {{ ref('int_stripe_disputes') }}
where currency = 'USD'
  and abs(amount_usd - amount_local / {{ minor_unit_divisor('currency') }}) > 0.005

union all

select 'payment_intent.amount_usd', id, currency, amount_local, amount_usd
from {{ ref('int_stripe_payment_intents') }}
where currency = 'USD'
  and abs(amount_usd - amount_local / {{ minor_unit_divisor('currency') }}) > 0.005
