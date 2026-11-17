select
    customer_account_id,
    _elt_account_name,
    count(distinct snapshot_date) as days_counted_as_new
from {{ ref('int_solidgate_mrr_movements_daily') }}
where movement_type = 'new_business'
group by customer_account_id, _elt_account_name
having count(distinct snapshot_date) > 1
