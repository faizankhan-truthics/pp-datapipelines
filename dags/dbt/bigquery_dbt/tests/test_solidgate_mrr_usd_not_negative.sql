select
    snapshot_date,
    _elt_account_name,
    mrr_usd
from {{ ref('solidgate_mrr_usd_daily') }}
where mrr_usd < 0
