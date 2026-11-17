-- churn is revenue lost, so it can never be zero or positive
select snapshot_date, _elt_stripe_account_id, churn_mrr_local, churn_mrr_usd
from {{ ref('int_stripe_mrr_churn_daily') }}
where churn_mrr_local >= 0
