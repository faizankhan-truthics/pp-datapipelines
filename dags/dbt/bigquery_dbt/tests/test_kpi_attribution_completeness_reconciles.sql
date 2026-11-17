-- kpi_attribution_completeness_daily must account for every dim_user_journey row: per product,
-- the summed journeys and unattributed journeys equal the dim's row and is_unattributed counts.
with dim as (

    select product, count(*) as journeys, countif(is_unattributed) as unattributed_journeys
    from {{ ref('dim_user_journey') }}
    group by 1

),

kpi as (

    select product, sum(journeys) as journeys, sum(unattributed_journeys) as unattributed_journeys
    from {{ ref('kpi_attribution_completeness_daily') }}
    group by 1

)

select
    coalesce(d.product, k.product) as product,
    d.journeys                     as dim_journeys,
    k.journeys                     as kpi_journeys,
    d.unattributed_journeys        as dim_unattributed_journeys,
    k.unattributed_journeys        as kpi_unattributed_journeys
from dim d
full outer join kpi k on k.product = d.product
where d.product is null
   or k.product is null
   or d.journeys != k.journeys
   or d.unattributed_journeys != k.unattributed_journeys
