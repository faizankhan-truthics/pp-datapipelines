{{ config(
    materialized='table',
    tags=["journey", "user_journey_mart", "kr2", "gold", "daily"],
    partition_by={
        "field": "date",
        "data_type": "date"
    },
    cluster_by=['product', 'channel_group']
) }}

-- KR2: attribution completeness KPI. The single source for the KR2 dashboard.
-- Grain: date x product x channel_group x source_resolution_method x reached_stage x
--        payment_provider (date_status follows from date).
--
-- Source: dim_user_journey (the ticket calls it fct_customer_journey).
-- Rebuilt in full every run: user_key, attribution and stage are restated as identities resolve,
-- so past days change too and an incremental merge would drift from the dim.
-- Undated journeys (journey_start_date null) are kept as date_status = 'undated' so the totals
-- still reconcile with dim_user_journey.
-- payment_provider is 'none' for journeys with no payment, so every grain key is non-null.
--
-- Counts only. Compute the rate downstream as SUM(unattributed_journeys) / SUM(journeys),
-- never as an average of percentages.

with journeys as (

    select
        journey_start_date                                  as date,
        if(journey_start_date is null, 'undated', 'dated')  as date_status,
        product,
        channel_group,
        source_resolution_method,
        reached_stage,
        coalesce(payment_provider, 'none')                  as payment_provider,
        is_unattributed
    from {{ ref('dim_user_journey') }}

)

select
    date,
    date_status,
    product,
    channel_group,
    source_resolution_method,
    reached_stage,
    payment_provider,
    count(*)                                                as journeys,
    countif(is_unattributed)                                as unattributed_journeys
from journeys
group by
    date,
    date_status,
    product,
    channel_group,
    source_resolution_method,
    reached_stage,
    payment_provider
