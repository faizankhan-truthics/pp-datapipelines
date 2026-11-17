-- projected_* columns are only ever computed for the current month's row
-- (see the projection factor f in the model); any other month
-- should always come through as NULL.
select *
from {{ ref('stripe_emc_efm_report') }}
where start_of_month != date_trunc(current_date('UTC'), month)
  and (projected_dispute_count_all is not null
       or projected_ecm_status is not null
       or projected_efm_breach is not null)
