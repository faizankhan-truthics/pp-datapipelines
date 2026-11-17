-- warn: a late export row can land below silver's newest bq_ingested_timestamp after the build;
-- the next run's overlap picks it up.
{{ config(severity='warn') }}

-- silver must hold one row per distinct bronze (team_id, uuid) loaded so far, so no event is lost.
select s.n as silver_rows, b.n as bronze_keys
from (select count(*) as n from {{ ref('int_posthog_events') }}) s,
     (select count(distinct concat(team_id, '|', uuid)) as n
      from {{ source('bronze_posthog', 'posthog_events') }}
      where uuid is not null and team_id is not null and timestamp is not null
        and bq_ingested_timestamp <= (select max(bq_ingested_timestamp) from {{ ref('int_posthog_events') }})) b
where s.n != b.n
