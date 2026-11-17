-- Every product in seed_ga4_payment_account_map has a prod Amplitude project or says why not.
select
    g.ga4_property_name,
    case
        when a.ga4_property_name is null and g.amplitude_not_applicable_reason is null
            then 'no_prod_project_and_no_reason'
        else 'has_project_but_marked_not_applicable'
    end as failure_reason
from {{ ref('seed_ga4_payment_account_map') }} g
left join {{ ref('seed_amplitude_project_map') }} a
    on a.ga4_property_name = g.ga4_property_name
    and a.environment = 'prod'
where (a.ga4_property_name is null and g.amplitude_not_applicable_reason is null)
   or (a.ga4_property_name is not null and g.amplitude_not_applicable_reason is not null)
