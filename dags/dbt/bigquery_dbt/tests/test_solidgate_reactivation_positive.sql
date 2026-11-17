-- a returning customer is paying again, so this can never be zero or negative
select snapshot_date, _elt_account_name, reactivation_mrr_local, reactivation_mrr_usd
from {{ ref('int_solidgate_mrr_reactivation_daily') }}
where reactivation_mrr_local <= 0
