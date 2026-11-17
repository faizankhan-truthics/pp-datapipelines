-- projected_vamp_tnx / projected_vamp_rate (and their CE 3.0 adjusted
-- versions) are only ever computed for the current month's row (see the
-- CASE WHEN in the model); any other month should always come through as NULL.
select *
from {{ ref('stripe_vamp_dashboard_overview') }}
where start_of_month != date_trunc(current_date('UTC'), month)
  and (projected_vamp_tnx is not null or projected_vamp_rate is not null
       or projected_vamp_tnx_ce3_adjusted is not null
       or projected_vamp_rate_ce3_adjusted is not null)
