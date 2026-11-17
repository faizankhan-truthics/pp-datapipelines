{{ config(severity='warn') }}

-- GA4 and Amplitude count signups independently; on a complete day they agree within a few
-- percent (Phase 0: within +/-5% on most days). A bigger gap means one tracker broke.
select product, snapshot_date, count_date, ga4_signups, amplitude_signups, signup_count_diff_pct
from {{ ref('user_identity_reconciliation_daily') }}
where snapshot_date = (select max(snapshot_date) from {{ ref('user_identity_reconciliation_daily') }})
  and abs(signup_count_diff_pct) > {{ var('max_signup_count_diff_pct', 0.20) }}
