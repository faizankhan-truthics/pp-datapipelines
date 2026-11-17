-- contraction is a decrease, so it can never be zero or positive
select snapshot_date, _elt_stripe_account_id, contraction_mrr_local, contraction_mrr_usd
from {{ ref('int_stripe_contraction_daily') }}
where contraction_mrr_local >= 0
