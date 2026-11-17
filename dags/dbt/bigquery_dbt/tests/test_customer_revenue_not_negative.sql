-- a customer cannot be refunded more than they paid, so lifetime revenue is never negative.
-- small tolerance for rounding across fx conversions.
-- warn, not error: kept visible without blocking dim_user. Stripe refunds are capped at the
-- captured amount and converted at the charge's rate (2026-10-14), which cleared the known cases.
{{ config(severity='warn') }}

select source_system, source_account, source_customer_id,
       lifetime_gross_revenue_usd, lifetime_refunded_usd, lifetime_revenue_usd
from {{ ref('int_customer_revenue') }}
where lifetime_revenue_usd < -0.01
