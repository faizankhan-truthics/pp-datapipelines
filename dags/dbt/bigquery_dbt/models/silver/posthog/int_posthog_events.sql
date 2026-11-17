{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    on_schema_change='append_new_columns',
    partition_by={
        "field": "timestamp",
        "data_type": "timestamp",
        "granularity": "day"
    },
    cluster_by=['team_id', 'event'],
    tags=["posthog", "silver", "daily"]
) }}

{%- set new_event_dates = [] -%}
{%- if execute and is_incremental() -%}
    {%- set watermark = run_query(
        "select format_timestamp('%F %T', coalesce(timestamp_sub(max(bq_ingested_timestamp), interval 6 hour), timestamp '1970-01-01')) from " ~ this
    ).columns[0].values()[0] -%}
    {%- set new_event_dates = run_query(
        "select distinct format_date('%F', date(timestamp)) from " ~ source('bronze_posthog', 'posthog_events')
        ~ " where timestamp is not null and bq_ingested_timestamp > timestamp '" ~ watermark ~ "'"
    ).columns[0].values() -%}
{%- endif %}

with affected as (

    select *
    from {{ source('bronze_posthog', 'posthog_events') }}
    where uuid is not null
      and team_id is not null
      and timestamp is not null  -- would land in the null partition, which is never replaced
    {% if is_incremental() %}
      {#- Whole partitions, not just the rows loaded since the watermark: insert_overwrite
          replaces each of these event dates outright. -#}
      and (
        {%- for d in new_event_dates %}
        (timestamp >= timestamp '{{ d }}' and timestamp < timestamp_add(timestamp '{{ d }}', interval 1 day)){{ " or" if not loop.last }}
        {%- else %}
        false
        {%- endfor %}
      )
    {% endif %}

),

-- A re-sent event can carry a different timestamp, so its copies can sit in different
-- partitions: the first-loaded copy is picked across all of bronze.
first_copies as (

    select b.team_id, b.uuid, b.timestamp, b.bq_ingested_timestamp
    from {{ source('bronze_posthog', 'posthog_events') }} b
    {% if is_incremental() %}
    inner join (select distinct team_id, uuid from affected) k
        on k.team_id = b.team_id and k.uuid = b.uuid
    {% endif %}
    where b.uuid is not null
      and b.team_id is not null
      and b.timestamp is not null
    qualify row_number() over (
        partition by b.team_id, b.uuid
        order by b.bq_ingested_timestamp, b.timestamp
    ) = 1

),

source_events as (

    select a.*
    from affected a
    inner join first_copies f
        on f.team_id = a.team_id
       and f.uuid = a.uuid
       and f.timestamp = a.timestamp
       and f.bq_ingested_timestamp = a.bq_ingested_timestamp
    qualify row_number() over (
        partition by a.team_id, a.uuid
        order by a.bq_ingested_timestamp
    ) = 1

),

posthog_products as (

    select posthog_id, product_name
    from {{ ref('seed_ga4_payment_account_map') }}
    where posthog_id is not null

)

select
    e.uuid,
    e.team_id,
    p.product_name,
    e.event,
    e.timestamp,
    e.distinct_id,
    e.person_id,
    e.properties,
    e.person_properties,
    e.`set`,
    e.set_once,
    e.ip,
    e.elements,
    e.site_url,
    e.bq_ingested_timestamp,
    current_timestamp() as _dbt_loaded_at

from source_events e
left join posthog_products p
    on p.posthog_id = e.team_id
