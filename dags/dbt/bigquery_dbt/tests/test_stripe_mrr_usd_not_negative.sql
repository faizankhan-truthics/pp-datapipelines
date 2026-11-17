select
    snapshot_date,
    _elt_stripe_account_id,
    mrr_usd
from {{ ref('stripe_mrr_usd_daily') }}
where mrr_usd < 0
