{{ config(severity='warn') }}

-- Checks on bronze.growthbook_projects; failure_reason names the check.
-- Warn only: GrowthBook data must never fail dbt_dag (see dags/growthbook/README.md).
-- The tables are created by the growthbook_flags_pipeline DAG, so each check only runs once its
-- table exists: a missing table gives no rows instead of a "Not found" error.
{% if not source_table_exists('bronze_growthbook', 'growthbook_projects') %}
select cast(null as string) as id, cast(null as string) as failure_reason from (select 1) where false
{% else %}
with projects as (
    select *
    from {{ source('bronze_growthbook', 'growthbook_projects') }}
),

row_failures as (
    select
        id,
        case
            when name is null or trim(name) = ''
                then 'missing_name'
            when date_created is null or date_updated is null
                then 'missing_date'
            when date_updated < date_created
                then 'updated_before_created'
        end as failure_reason
    from projects
),

-- the MERGE keys on id, so each project must appear once
duplicate_ids as (
    select
        id,
        'duplicate_id' as failure_reason
    from projects
    group by id
    having count(*) > 1
)

select * from row_failures where failure_reason is not null
union all
select * from duplicate_ids
{% if source_table_exists('bronze_growthbook', 'growthbook_features') %}
union all
-- every flag's project must exist here, or its name can't be looked up
select
    f.id,
    'flag_project_not_in_projects' as failure_reason
from {{ source('bronze_growthbook', 'growthbook_features') }} f
left join projects p
    on p.id = f.project
where f.project is not null
    and p.id is null
{% endif %}
{% endif %}
