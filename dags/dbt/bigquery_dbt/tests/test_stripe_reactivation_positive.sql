-- a returning customer is paying again, so this can never be zero or negative
select snapshot_date, _elt_stripe_account_id, reactivation_mrr_local, reactivation_mrr_usd
from {{ ref('int_stripe_mrr_reactivation_daily') }}
where reactivation_mrr_local <= 0
