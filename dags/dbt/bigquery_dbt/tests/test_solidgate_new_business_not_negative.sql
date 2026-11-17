select
    snapshot_date,
    _elt_account_name,
    new_business_mrr_local,
    new_business_mrr_usd
from {{ ref('int_solidgate_new_business_daily') }}
where new_business_mrr_local < 0
   or new_business_mrr_usd < 0
