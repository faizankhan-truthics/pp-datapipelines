select
    customer,
    _elt_stripe_account_id,
    count(distinct snapshot_date) as days_counted_as_new
from {{ ref('int_stripe_mrr_movements_daily') }}
where movement_type = 'new_business'
group by customer, _elt_stripe_account_id
having count(distinct snapshot_date) > 1
