-- expansion is an increase, so it can never be zero or negative
select
    snapshot_date,
    _elt_stripe_account_id,
    expansion_mrr_local,
    expansion_mrr_usd
from {{ ref('int_stripe_expansion_daily') }}
where expansion_mrr_local <= 0
