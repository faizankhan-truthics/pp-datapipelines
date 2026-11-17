-- expansion is an increase, so it can never be zero or negative
select
    snapshot_date,
    _elt_account_name,
    expansion_mrr_local,
    expansion_mrr_usd
from {{ ref('int_solidgate_expansion_daily') }}
where expansion_mrr_local <= 0
