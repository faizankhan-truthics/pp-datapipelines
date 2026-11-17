{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='campaign_day_key',
    on_schema_change='append_new_columns',
    partition_by={
        "field": "date",
        "data_type": "date"
    }
    )
}}

-- Ads data has ~1 day of lag and occasional snapshot revisions, so re-scan a small trailing
-- window each run (same rationale as int_ga4_campaign_day_metrics.sql, though the ceiling problem
-- doesn't apply here -- Ads already has 473+ days of history sitting in the source).
{% set rescan_days = 4 %}

with ads_latest_snapshot as (
    -- dedup snapshots on the latest _DATA_DATE per (campaign_id, segments_date); summing whatever
    -- segments_device/segments_ad_network_type/segments_slot rows exist at that snapshot is
    -- correct -- they're meant to roll up to campaign x day (the original EDA prototype this
    -- reconciliation supersedes summed across ALL snapshots instead, double-counting re-snapshots).
    select campaign_id, segments_date, max(_data_date) as latest_data_date
    from {{ source('bronze_google_ads', 'ads_CampaignBasicStats_2747379648') }}
    {% if is_incremental() %}
    where segments_date >= date_sub(current_date(), interval {{ rescan_days }} day)
    {% endif %}
    group by 1, 2
)

select
    c.customer_id,
    cast(c.campaign_id as string) as campaign_id,
    c.segments_date as date,
    concat(
        cast(c.customer_id as string), '-',
        cast(c.campaign_id as string), '-',
        cast(c.segments_date as string)
    ) as campaign_day_key,
    sum(c.metrics_cost_micros) / 1e6 as ads_cost
from {{ source('bronze_google_ads', 'ads_CampaignBasicStats_2747379648') }} c
join ads_latest_snapshot l
    on c.campaign_id = l.campaign_id
   and c.segments_date = l.segments_date
   and c._data_date = l.latest_data_date
{% if is_incremental() %}
where c.segments_date >= date_sub(current_date(), interval {{ rescan_days }} day)
{% endif %}
group by 1, 2, 3, 4
