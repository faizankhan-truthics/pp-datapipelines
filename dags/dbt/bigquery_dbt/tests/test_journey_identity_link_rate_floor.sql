{{ config(severity='warn') }}

-- link rates of the latest snapshot must stay above the floors set from the Phase 0 baselines
-- (Solidgate 98.5%, Stripe 89.7%, GA4 checkout visitors 96.1%; docs/plans/user_journey_stitch_checks.sql)
with latest as (

    select *
    from {{ ref('user_identity_reconciliation_daily') }}
    qualify snapshot_date = max(snapshot_date) over ()

)

select product, snapshot_date,
       pct_solidgate_payers_linked, pct_stripe_payers_linked, pct_ga4_checkout_visitors_linked
from latest
where pct_solidgate_payers_linked      < {{ var('min_solidgate_link_rate', 0.95) }}
   or pct_stripe_payers_linked         < {{ var('min_stripe_link_rate', 0.85) }}
   or pct_ga4_checkout_visitors_linked < {{ var('min_ga4_checkout_link_rate', 0.90) }}
