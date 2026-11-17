-- churn is revenue lost, so it can never be zero or positive
select snapshot_date, _elt_account_name, churn_mrr_local, churn_mrr_usd
from {{ ref('int_solidgate_mrr_churn_daily') }}
where churn_mrr_local >= 0
