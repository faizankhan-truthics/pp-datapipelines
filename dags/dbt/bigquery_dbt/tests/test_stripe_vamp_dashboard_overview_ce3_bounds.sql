-- CE 3.0 eligible EFWs are a subset of the row's EFWs, so they can never
-- exceed efw_count, and the adjusted VAMP count stays between the non-RDR
-- disputes (disputes are never subtracted) and the unadjusted vamp_tnx.
select *
from {{ ref('stripe_vamp_dashboard_overview') }}
where ce3_eligible_efw > efw_count
   or ce3_eligible_cards > ce3_eligible_efw
   or vamp_tnx_ce3_adjusted < dispute_count_non_rdr
   or vamp_tnx_ce3_adjusted > vamp_tnx
