{{ config(enabled=var('run_cross_source_tests', true)) }}

-- a user with an active subscription on EITHER provider must not be churned: trials often start
-- on Solidgate and continue on Stripe, so reading one provider alone would churn them wrongly.
-- active = still in the source's latest MRR snapshot (fct_subscription.mrr_usd is the last
-- snapshot's MRR, which a long-cancelled subscription keeps)
with latest_mrr_snapshot as (

    select source_system, max(mrr_as_of_date) as latest_mrr_as_of_date
    from {{ ref('fct_subscription') }}
    group by 1

),

active_payers as (

    select distinct rs.product, rs.user_key
    from {{ ref('fct_subscription') }} s
    join latest_mrr_snapshot l
        on  l.source_system         = s.source_system
        and l.latest_mrr_as_of_date = s.mrr_as_of_date
    join {{ ref('int_identity_resolution') }} rs
        on  rs.id_type  = concat(s.source_system, '_customer')
        and rs.id_value = concat(s.source_account, ':', s.source_customer_id)
    where s.mrr_usd > 0

)

select d.product, d.user_key, d.current_state
from {{ ref('dim_user_journey') }} d
join active_payers a
    on  a.product  = d.product
    and a.user_key = d.user_key
where d.current_state = 'churned_customer'
