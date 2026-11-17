{{ config(severity='warn') }}

-- Checks on bronze.growthbook_features; failure_reason names the check.
-- Warn only: GrowthBook data must never fail dbt_dag (see dags/growthbook/README.md).
{% if not source_table_exists('bronze_growthbook', 'growthbook_features') %}
-- the table is created by the growthbook_flags_pipeline DAG; nothing to check before its first run
select cast(null as string) as id, cast(null as string) as failure_reason from (select 1) where false
{% else %}
with features as (
    select *
    from {{ source('bronze_growthbook', 'growthbook_features') }}
),

row_failures as (
    select
        id,
        case
            when value_type is null or value_type not in ('boolean', 'string', 'number', 'json')
                then 'unexpected_value_type'
            when archived is null or default_value is null or date_created is null
                or date_updated is null or revision_version is null
                then 'missing_required_field'
            when environments is null or json_type(environments) != 'object'
                or rules is null or json_type(rules) != 'array'
                then 'environments_or_rules_wrong_shape'
            when date_updated < date_created
                then 'updated_before_created'
        end as failure_reason
    from features
),

-- the MERGE keys on id, so each flag must appear once
duplicate_ids as (
    select
        id,
        'duplicate_id' as failure_reason
    from features
    group by id
    having count(*) > 1
)

select * from row_failures where failure_reason is not null
union all
select * from duplicate_ids
{% endif %}
