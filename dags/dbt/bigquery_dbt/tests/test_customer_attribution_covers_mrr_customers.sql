-- spans 3 models, so dbt_dag runs it as a detached task after all of them build
-- (should_detach_multiple_parents_tests). the var lets a partial run that rebuilds MRR but not
-- int_customer_attribution turn it off.
{{ config(enabled=var('run_cross_source_tests', true)) }}

-- every customer that ever carried MRR must appear in int_customer_attribution, attributed or
-- not. if one drops out, channel/campaign totals stop adding up to total MRR.
with mrr_customers as (

    select distinct 'solidgate' as source_system, _elt_account_name as source_account, customer_account_id as source_customer_id
    from {{ ref('int_solidgate_mrr_customer_daily') }}

    union distinct

    select distinct 'stripe', _elt_stripe_account_id, customer
    from {{ ref('int_stripe_mrr_customer_daily') }}
    where customer is not null

)

select m.*
from mrr_customers m
left join {{ ref('int_customer_attribution') }} a
    on  m.source_system      = a.source_system
    and m.source_account     = a.source_account
    and m.source_customer_id = a.source_customer_id
where a.source_customer_id is null
