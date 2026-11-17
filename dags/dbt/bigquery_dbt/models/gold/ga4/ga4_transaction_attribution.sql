{{ config(
tags=["ga4", "gold", "daily"],
) }}

-- Transaction/subscription-grain companion to ga4_payment_match_campaign_daily: instead of
-- collapsing to campaign x day, keeps one row per payment-processor transaction so a specific
-- subscription's revenue can be traced back to the GA4 campaign_id/gclid that acquired it.
-- Built for downstream LTV-by-campaign rollups (group by payment_source + subscription_id,
-- take the earliest non-null campaign_id as that subscription's acquisition channel, sum
-- revenue_usd across all of its transactions) -- the rollup itself is intentionally left
-- downstream; this model only guarantees the keys survive the join.
--
-- Only real payment-processor transactions are kept -- a ga4_only row (a transaction_id GA4
-- emitted that the processor never processed) isn't revenue and has no subscription to attach
-- to. Renewal/rebill transactions with no matching GA4 event are kept too, with
-- campaign_id/gclid null, so a subscription's full revenue history survives even though only
-- its GA4-observed transaction(s) carry attribution -- same "no renewal fallback" scope
-- decision as ga4_payment_match_campaign_daily.
--
-- revenue_usd is NOT on one recognition basis across payment_source -- Solidgate is zeroed
-- here for non-successful order_status; Stripe's int_stripe_transactions already sums
-- succeeded-only charges, so it passes through as-is. Sum within a payment_source, not across.
--
-- subscription_id/invoice_id are null for most Stripe rows: unlike Solidgate (sourced from
-- subscription_invoice_orders, so every row has one), most Stripe PaymentIntents on these
-- accounts are one-time purchases with no invoice/subscription at all -- see
-- int_stripe_transactions.sql. Those rows still carry real revenue and campaign attribution,
-- just nothing to group a subscription-level LTV rollup by.
-- Grain: payment_source x account_key x transaction_id.

select
    payment_source,
    account_key,
    transaction_id,
    pay_date,
    subscription_id,
    invoice_id,
    order_status,
    case
        when payment_source = 'solidgate' and order_status not in
            ('approved', 'settle_ok', 'partial_settled', 'refunded')
        then 0
        else revenue_usd
    end as revenue_usd,
    match_status,
    ga4_property_name,
    campaign_id,
    gclid
from {{ ref('int_ga4_payment_stitch') }}
where match_status != 'ga4_only'
